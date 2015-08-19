require "#{File.dirname(__FILE__)}/backup"

class TungstenXtrabackupScript < TungstenBackupScript
  include MySQLServiceScript
  
  INCREMENTAL_BASEDIR_FILE = "xtrabackup_incremental_basedir"
  MASTER_BACKUP_POSITION_SQL = "xtrabackup_tungsten_master_backup_position.sql"
  
  def configure
    super()
    
    add_option(:tar, {
      :on => "--tar",
      :default => "false",
      :help => "Create the backup as a TAR file"
    })
    
    add_option(:restore_to_datadir, {
      :on => "--restore-to-datadir",
      :default => "false",
      :help => "Use the MySQL data directory for staging and preparation"
    })
    
    add_option(:incremental, {
      :on => "--incremental",
      :default => "false",
      :help => "Create the backup as an incremental snapshot since last backup"
    })
  end
  
  def backup
    if @options[:tar] == "true"
      backup_tar()
    else
      backup_dir()
    end
  end
  
  def backup_tar()
    begin
      cleanup_xtrabackup_storage()

      id = build_timestamp_id("full")
      tar_file = @options[:directory] + "/" + id + ".tar"

      additional_args = []
      additional_args << "--no-timestamp"
      additional_args << "--stream=tar"
      
      if xtrabackup_supports_argument("--no-version-check")
        additional_args << "--no-version-check"
      end

      TU.output("Create full backup in #{tar_file}")  
      TU.cmd_stderr("#{get_xtrabackup_command()} #{additional_args.join(" ")} #{@options[:directory]} > #{tar_file}") {
        |line|
        if line =~ /MySQL binlog position/
          m = line.match(/filename \'([a-zA-Z0-9\.\-\_]*)\', position ([0-9]*)/)
          if m
            @binlog_file = m[1]
            @binlog_position = m[2]
          end
        end
      }
      
      if File.exist?("#{@options[:directory]}/xtrabackup_binary")
        # We must add a file to the tar package without extracting the entire
        # package. This command appends the file directly.
        TU.cmd_result("cd #{@options[:directory]};tar rf #{tar_file} xtrabackup_binary")
        File.unlink("#{@options[:directory]}/xtrabackup_binary")
      end

      # Change the directory ownership if run with sudo
      if ENV.has_key?('SUDO_USER') && TU.whoami() == "root"
        TU.cmd_result("chown -R #{ENV['SUDO_USER']}: #{tar_file}")
      end

      return tar_file
    rescue => e
      TU.error(e.message)

      if tar_file && File.exists?(tar_file)
        TU.output("Remove #{tar_file} due to the error")
        TU.cmd_result("rm #{tar_file}")
      end

      raise e
    end
  end
  
  def backup_dir
    most_recent_dir = nil
    lineage = nil
    
    begin
      cleanup_xtrabackup_storage()
      
      # Validate the incremental backup before starting in case the  
      # lineage is not complete
      if @options[:incremental] == "true"
        begin
          # Find the most recent xtrabackup directory which we will start from
          most_recent_dir = get_last_backup()

          # Check that the lineage for this directory is intact
          # If it cannot find the full backup that the most recent snapshot is
          # based on, we need to do a full backup instead.
          lineage = get_snapshot_lineage(most_recent_dir)
          
          retention = TI.setting(TI.setting_key(REPL_SERVICES, opt(:service), "repl_backup_retention")).to_i()
          if lineage.size() == retention
            TU.debug("The incremental lineage takes up the full backup retention. Forcing a full backup since the base backup will be deleted.")
            @options[:incremental] = "false"
          end
        rescue
          TU.debug("Forcing a full backup because the incremental backup lineage is not complete")
          @options[:incremental] = "false"
        end
      end
      
      if @options[:incremental] == "false"
        execute_backup()
      else
        begin
          execute_backup(most_recent_dir)
        rescue BrokenLineageError => ble
          TU.warning(ble.message)
          execute_backup()
        end
      end

      # Tungsten Replicator requires a single file as the result of this script.
      # We write the directory name of the backup just created into a file
      # and present that as the backup result.  The restore command will read
      # the backup directory from the file to identify the proper restore point.
      # We are using the basename of the backup directory so it is easier to
      # identify which files are related.
      storage_file = @storage_dir + "/" + File.basename(@storage_dir)
      File.open(storage_file, "w") {
        |tm|
        tm.write(@storage_dir)
      }  

      # Change the directory ownership if run with sudo
      if ENV.has_key?('SUDO_USER') && TU.whoami() == "root"
        TU.cmd_result("chown -R #{ENV['SUDO_USER']}: #{@storage_dir}")
      end

      return storage_file
    rescue => e
      TU.error(e.message)

      if @storage_dir && File.exists?(@storage_dir)
        TU.output("Remove #{@storage_dir} due to the error")
        TU.cmd_result("rm -r #{@storage_dir}")
      end

      raise e
    end
  end
  
  def execute_backup(incremental_basedir = nil)
    id = build_timestamp_id((incremental_basedir == nil ? "full" : "incr"))
    @storage_dir = @options[:directory] + "/" + id

    additional_args = []
    additional_args << "--no-timestamp"
    
    if xtrabackup_supports_argument("--no-version-check")
      additional_args << "--no-version-check"
    end

    # Build the command and run it
    # All STDERR output from the command is processed before going to STDERR
    # When the MySQL binlog position is found, it is saved for later use
    if incremental_basedir == nil
      TU.output("Create full backup in #{@storage_dir}")
      TU.cmd_stderr("#{get_xtrabackup_command()} #{additional_args.join(" ")} #{@storage_dir}") {
        |line|
        if line =~ /MySQL binlog position/
          m = line.match(/filename \'([a-zA-Z0-9\.\-\_]*)\', position ([0-9]*)/)
          if m
            @binlog_file = m[1]
            @binlog_position = m[2]
          end
        end
      }
    else
      additional_args << "--incremental"

      incremental_lsn = read_property_from_file("to_lsn", incremental_basedir.to_s + "/xtrabackup_checkpoints")
      additional_args << "--incremental-lsn=#{incremental_lsn}"

      TU.output("Create an incremental backup from LSN #{incremental_lsn} in #{@storage_dir}")
      TU.cmd_stderr("#{get_xtrabackup_command()} #{additional_args.join(" ")} #{@storage_dir}") {
        |line|
        if line =~ /MySQL binlog position/
          m = line.match(/filename \'([a-zA-Z0-9\.\-\_]*)\', position ([0-9]*)/)
          if m
            @binlog_file = m[1]
            @binlog_position = m[2]
          end
        end
      }

      # Store the directory that this incremental backup was based on
      # That information is needed when doing the restore
      File.open(@storage_dir + "/#{INCREMENTAL_BASEDIR_FILE}", "w") {
        |f|
        f.puts(incremental_basedir)
      }
    end
  end
  
  def store_master_position_sql(sql, storage_file)
    # This will store the SQL statement in a file in the backup directory
    # The restore process will run the SQL file after copying files back
    # to the data directory and starting the server
    
    if @options[:tar] == "true"
      path = "#{@options[:directory]}/#{MASTER_BACKUP_POSITION_SQL}"
      begin
        File.open(path, "w"){
          |f|
          sql.each{
            |line|
            f.puts(line)
          }
        }
        
        # We must add a file to the tar package without extracting the entire
        # package. This command appends the file directly.
        TU.cmd_result("cd #{File.dirname(path)};tar rf #{storage_file} #{MASTER_BACKUP_POSITION_SQL}")
        
        if File.exist?(path)
          File.unlink(path)
        end
      rescue => e
        if File.exist?(path)
          File.unlink(path)
        end
        
        raise e
      end
    else
      File.open("#{@storage_dir}/#{MASTER_BACKUP_POSITION_SQL}", "w"){
        |f|
        sql.each{
          |line|
          f.puts(line)
        }
      }
    end
  end
  
  def restore
    begin
      storage_file = TU.cmd_result(". #{@options[:properties]}; echo $file")

      # If the tungsten_restore_to_datadir file exists, we will restore to the
      # datadir. This can't work if innodb_data_home_dir or innodb_log_group_home_dir 
      # are in the my.cnf file because the files need to go to different directories
      if File.exist?("#{@options[:mysqldatadir]}/tungsten_restore_to_datadir")
        @options[:restore_to_datadir] = "true"
      end

      # If the InnoDB files are stored somewhere other than datadir we are
      # not able to put them all in the correct position at this time
      # This only matters if we are restoring directly to the data directory
      if @options[:restore_to_datadir] == "true"  
        if @options[:mysqlibdatadir].to_s() != "" || @options[:mysqliblogdir].to_s() != ""
          raise("Unable to restore to #{@options[:mysqldatadir]} because #{@options[:my_cnf]} includes a definition for 'innodb_data_home_dir' or 'innodb_log_group_home_dir'")
        end
      end

      # Does this version of innobackupex-1.5.1 support the faster 
      # --move-back instead of --copy-back
      supports_move_back = xtrabackup_supports_argument("--move-back")

      # Prepare the staging directory for the data
      # If we are restoring directly to the data directory then MySQL
      # must be shutdown and the data directory emptied
      id = build_timestamp_id("restore")
      if @options[:restore_to_datadir] == "true"
        empty_mysql_directory()

        staging_dir = @options[:mysqldatadir]
      else
        staging_dir = @options[:directory] + "/" + id
      end
      FileUtils.mkdir_p(staging_dir)

      if @options[:tar] == "true"
        TU.output("Unpack '#{storage_file}' to the staging directory '#{staging_dir}'")
        TU.cmd_result("cd #{staging_dir}; tar -xif #{storage_file}")
      else
        restore_directory = TU.cmd_result("cat #{storage_file}")

        TU.debug("Restore from #{restore_directory}")

        lineage = get_snapshot_lineage(restore_directory)
        fullbackup_dir = lineage.shift()
        TU.output("Copy the full base directory '#{fullbackup_dir}' to the staging directory '#{staging_dir}'")
        TU.cmd_result("cp -r #{fullbackup_dir}/* #{staging_dir}")

        TU.output("Apply the redo-log to #{staging_dir}")
        TU.cmd_result("#{get_xtrabackup_command()} --apply-log --redo-only #{staging_dir}")
        
        # Some cleanup to prevent issues that were popping up
        if File.exists?("#{staging_dir}/#{INCREMENTAL_BASEDIR_FILE}")
          TU.cmd_result("rm -f #{staging_dir}/#{INCREMENTAL_BASEDIR_FILE}")
        end

        lineage.each{
          |incremental_dir|
          TU.output("Apply the incremental updates from #{incremental_dir}")
          TU.cmd_result("#{get_xtrabackup_command()} --apply-log --incremental-dir=#{incremental_dir} #{staging_dir}")
          
          # Some cleanup to prevent issues that were popping up
          if File.exists?("#{staging_dir}/#{INCREMENTAL_BASEDIR_FILE}")
            TU.cmd_result("rm -f #{staging_dir}/#{INCREMENTAL_BASEDIR_FILE}")
          end
        }
      end

      TU.output("Finish preparing #{staging_dir}")
      TU.cmd_result("#{get_xtrabackup_command()} --apply-log #{staging_dir}")

      unless @options[:restore_to_datadir] == "true"
        # Shutdown MySQL and empty the data directory in preparation for the 
        # restored data
        empty_mysql_directory()

        # Copy the backup files to the mysql data directory
        if supports_move_back == true
          restore_cmd = "--move-back"
        else
          restore_cmd = "--copy-back"
        end
        TU.cmd_result("#{get_xtrabackup_command()} #{restore_cmd} #{staging_dir}")
      end

      # Fix the permissions and restart the service
      TU.cmd_result("chown -RL #{@options[:mysqluser]}: #{@options[:mysqldatadir]}")

      if @options[:mysqlibdatadir].to_s() != ""
        TU.cmd_result("chown -RL #{@options[:mysqluser]}: #{@options[:mysqlibdatadir]}")
      end

      if @options[:mysqliblogdir].to_s() != ""
        TU.cmd_result("chown -RL #{@options[:mysqluser]}: #{@options[:mysqliblogdir]}")
      end

      start_mysql_server()
      
      if File.exist?("#{@options[:mysqldatadir]}/#{MASTER_BACKUP_POSITION_SQL}")
        TU.cmd_result("cat #{@options[:mysqldatadir]}/#{MASTER_BACKUP_POSITION_SQL} | #{get_mysql_command()}")
      end

      if @options[:restore_to_datadir] == "false" && staging_dir != "" && File.exists?(staging_dir)
        TU.output("Cleanup #{staging_dir}")
        TU.cmd_result("rm -r #{staging_dir}")
      elsif @options[:restore_to_datadir] == "true" && File.exists?("#{staging_dir}/#{INCREMENTAL_BASEDIR_FILE}")
        TU.cmd_result("rm -f #{staging_dir}/#{INCREMENTAL_BASEDIR_FILE}")
      end
    rescue => e
      TU.error(e.message)

      if @options[:restore_to_datadir] == "false" && staging_dir != "" && File.exists?(staging_dir)
        TU.output("Remove #{staging_dir} due to the error")
        TU.cmd_result("rm -r #{staging_dir}")
      elsif @options[:restore_to_datadir] == "true" && File.exists?("#{staging_dir}/#{INCREMENTAL_BASEDIR_FILE}")
        TU.cmd_result("rm -f #{staging_dir}/#{INCREMENTAL_BASEDIR_FILE}")
      end

      raise e
    end
  end
  
  def validate
    super()
    
    # Read data locations from the my.cnf file
    @options[:mysqldatadir] = get_mysql_option("datadir")
    if @options[:mysqldatadir].to_s() == ""
      # The configuration file doesn't have a datadir value
      # See if MySQL will give one and store it in wrapper config file
      @options[:mysqldatadir] = get_mysql_variable("datadir")
      if @options[:mysqldatadir].to_s() != ""
        set_mysql_defaults_value("datadir=#{@options[:mysqldatadir]}")
      end
    end

    if @options[:action] == ACTION_BACKUP
      # Read innodb_log_file_size from the my.cnf file
      @options[:innodb_log_file_size] = get_mysql_option("innodb_log_file_size")
      if @options[:innodb_log_file_size].to_s() == ""
        # The configuration file doesn't have a innodb_log_file_size value
        # See if MySQL will give one and store it in wrapper config file
        @options[:innodb_log_file_size] = get_mysql_variable("innodb_log_file_size")
        if @options[:innodb_log_file_size].to_s() != ""
          set_mysql_defaults_value("innodb_log_file_size=#{@options[:innodb_log_file_size]}")
        end
      end
  
      if @options[:innodb_log_file_size].to_s() == ""
        TU.info "The configuration file at #{@options[:my_cnf]} does not define a innodb_log_file_size value - this can cause problems with xtrabackup."
      end
    end
    
    if @options[:action] == ACTION_RESTORE
      # Read innodb_log_file_size from the my.cnf file
      @options[:innodb_log_file_size] = get_mysql_option("innodb_log_file_size")
      if @options[:innodb_log_file_size].to_s() == ""
        # The configuration file doesn't have a innodb_log_file_size value
        # get it from the actual file and store it in wrapper config file
        if File.exist?("#{@options[:mysqldatadir]}/ib_logfile0")
          @options[:innodb_log_file_size] = File.size("#{@options[:mysqldatadir]}/ib_logfile0")
        end
        if @options[:innodb_log_file_size].to_s() != ""
          set_mysql_defaults_value("innodb_log_file_size=#{@options[:innodb_log_file_size]}")
        end
      end
  
      if @options[:innodb_log_file_size].to_s() == ""
        TU.info "The configuration file at #{@options[:my_cnf]} does not define a innodb_log_file_size value - this can cause problems with xtrabackup."
      end
    end            
    
    @options[:mysqlibdatadir] = get_mysql_option("innodb_data_home_dir")
    @options[:mysqliblogdir] = get_mysql_option("innodb_log_group_home_dir")
    
    @storage_dir = nil
    
    # Make sure the xtrabackup storage directory is created properly
    if File.exist?(@options[:directory])
      unless File.directory?(@options[:directory])
        TU.error "The path #{@options[:directory]} is not a directory"
      end
    else
      FileUtils.mkdir_p(@options[:directory])
      # Change the directory ownership if run with sudo
      if ENV.has_key?('SUDO_USER') && TU.whoami() == "root"
        TU.cmd_result("chown -R #{ENV['SUDO_USER']}: #{@options[:directory]}")
      end
    end
    
    if @options[:directory] == nil
      TU.error "You must specify a directory for storing Xtrabackup files"
    end

    unless File.writable?(@options[:directory])
      TU.error "The directory '#{@options[:directory]}' is not writeable"
    end

    if @options[:tungsten_backups] == nil
      TU.error "You must specify the Tungsten backups storage directory"
    else
      unless File.writable?(@options[:tungsten_backups])
        TU.error "The directory '#{@options[:tungsten_backups]}' is not writeable"
      end
    end

    unless File.writable?(@options[:mysqllogdir])
      TU.error "The MySQL log dir '#{@options[:mysqllogdir]}' is not writeable"
    end

    if @options[:mysqldatadir].to_s() == ""
      TU.error "The configuration file at #{@options[:my_cnf]} does not define a datadir value."
    else
      unless File.writable?(@options[:mysqldatadir])
        TU.error "The MySQL data dir '#{@options[:mysqldatadir]}' is not writeable"
      end
    end
    
    if ENV['USER'] != @options[:mysqluser] && ENV['USER'] != "root"
      TU.error("The current user is not the #{@options[:mysqluser]} system user or root. You must run Tungsten as #{@options[:mysqluser]} or enable sudo by running `tpm update --enable-sudo-access=true`.")
    end
    
    if @options[:action] == ACTION_BACKUP
      if @master_backup == true && @options[:incremental] == "true"
        # An incremental backup of the master does not provide the correct 
        # binary log position. We can't allow this to run or else we will
        # not be able to set trep_commit_seqno to the right value.
        TU.error("Unable to take an incremental backup of the master. Try running `trepctl -service #{@options[:service]} -backup xtrabackup-full")
      end
    end
  end
  
  def empty_mysql_directory
    stop_mysql_server()

    TU.cmd_result("#{sudo_prefix()}find #{@options[:mysqllogdir]}/ -name #{@options[:mysqllogpattern]}.* -delete")

    [
      @options[:mysqldatadir],
      @options[:mysqlibdatadir],
      @options[:mysqliblogdir]
    ].uniq().each{
      |dir|
      if dir.to_s() == ""
        next
      end
      
      TU.cmd_result("#{sudo_prefix()}find #{dir}/ -mindepth 1 -delete")
    }
  end

  # Cleanup xtrabackup snapshots that no longer have a matching entry in 
  # the Tungsten backups directory.  If the Xtrabackup directory does not have 
  # a file in the Tungsten backups directory with a matching filename, we will 
  # remove the Xtrabackup snapshot
  def cleanup_xtrabackup_storage
    # Loop over each of the Tungsten backup storage files
    tungsten_storage_files = Pathname.new(@options[:tungsten_backups]).children.collect{
      |child|
      child.to_s
    }

    # Loop over each of the Xtrabackup storage directories
    Pathname.new(@options[:directory]).children.each{
      |xtrabackup_dir|
      basename = xtrabackup_dir.basename.to_s
      unless basename =~ /^full/ || basename =~ /^incr/
        next
      end

      regex = Regexp.new("store-[0-9]+-#{basename}")

      tungsten_storage_matches = tungsten_storage_files.select{
        |tungsten_storage_name|
        (tungsten_storage_name =~ regex)
      }    
      if tungsten_storage_matches.length == 0
        # There aren't any matching files in the Tungsten backups directory
        TU.cmd_result("rm -r #{xtrabackup_dir.to_s}")
      end
    }
  end
  
  def get_last_backup
    last_backup = Pathname.new(@options[:directory]).most_recent_dir()
    if last_backup == nil
      raise BrokenLineageError.new "Unable to find a previous directory for an incremental backup"
    end

    return last_backup
  end

  # Get the list of incremental snapshots and the full backup needed to
  # restore a given incremental snapshot directory. This will recurse until
  # a full backup is found.
  def get_snapshot_lineage(restore_directory)
    lineage = []

    TU.output("Validate lineage of '#{restore_directory}'")

    basedir_path = restore_directory.to_s + "/" + INCREMENTAL_BASEDIR_FILE
    checkpoints_file = restore_directory.to_s + "/xtrabackup_checkpoints"
    backup_type = read_property_from_file("backup_type", checkpoints_file)
    
    if backup_type == "full-backuped"
      # There should not be an incremntal basedir reference in a full backup,
      # this backup must not be valid.
      if File.exists?(basedir_path)
        TU.warning("Unexpected #{INCREMENTAL_BASEDIR_FILE} found in full backup directory '#{restore_directory}.")
        TU.cmd_result("rm -r #{basedir_path.to_s}")
      end
      lineage << restore_directory
    elsif backup_type == "incremental"
      if File.exists?(basedir_path)
        # Change the incremental basedir from a symlink to a file
        # We previously used symlinks but that was causing issues during some
        # restore operations.
        if File.symlink?(basedir_path)
          basedir = File.readlink(basedir_path)
          File.unlink(basedir_path)
          File.open(basedir_path, "w") {
            |f|
            f.puts(basedir)
          }
        end
      else
        raise BrokenLineageError.new "Unable to find #{INCREMENTAL_BASEDIR_FILE} symlink in incremental backup directory '#{restore_directory}'"
      end

      # Read the base directory of this snapshot and load the lineage for it
      basedir = TU.cmd_result("cat #{basedir_path}")
      lineage = get_snapshot_lineage(basedir)
      lineage << restore_directory
    else
      raise BrokenLineageError.new "Invalid backup_type '#{backup_type}' found in #{checkpoints_file}"
    end

    return lineage
  end

  def read_property_from_file(property, filename)
    value = nil
    regex = Regexp.new(property)
    File.open(filename, 'r') do |file|
      file.read.each_line do |line|
        line.strip!
        if line =~ regex
          value = line.split("=")[1].strip
        end
      end
    end
    if value == nil
      raise "Unable to find the '#{property}' value in #{filename}"
    end

    return value
  end
  
  def build_timestamp_id(prefix)
    return prefix + "_xtrabackup_" + Time.now.strftime("%Y-%m-%d_%H-%M") + "_" + rand(100).to_s
  end
  
  def require_local_mysql_service?
    true
  end
  
  def script_name
    "xtrabackup.sh"
  end
end

class BrokenLineageError < StandardError
end

class Pathname
  def most_recent_dir(matching=/./)
    dirs = self.children.collect { |entry| self+entry }
    dirs.reject! { |entry| ((entry.directory? and entry != nil and entry.to_s =~ matching) ? false : true) }
    dirs.sort! { |entry1,entry2| entry1.mtime <=> entry2.mtime }
    dirs.last
  end
end