# ToDo
# - mysqldump
# - Create a TungstenBackupScript subclass for sending output to remote systems

class TungstenReplicatorProvisionSlave
  include TungstenScript
  include MySQLServiceScript
  include OfflineServicesScript
  
  MASTER_BACKUP_POSITION_SQL = "xtrabackup_tungsten_master_backup_position.sql"
  AUTODETECT = 'autodetect'
  
  def main
    if @options[:mysqldump] == false
      provision_with_xtrabackup()
    else
      if opt(:topology) == "direct"
        provision_from_direct_master()
      else
        provision_with_mysqldump()
      end
    end
  end
  
  def provision_with_xtrabackup
    begin
      # Does this version of innobackupex-1.5.1 support the faster 
      # --move-back instead of --copy-back
      supports_move_back = xtrabackup_supports_argument("--move-back")

      # Prepare the staging directory for the data
      # If we are restoring directly to the data directory then MySQL
      # must be shutdown and the data directory emptied
      id = build_timestamp_id("provision_xtrabackup_")
      if @options[:direct] == true
        TU.notice("Stop MySQL and empty all data directories")
        empty_mysql_directory()

        staging_dir = @options[:mysqldatadir]
      else
        staging_dir = TI.setting(TI.setting_key(REPL_SERVICES, opt(:service), "repl_backup_directory")) + "/" + id
      end
      FileUtils.mkdir_p(staging_dir)
      TU.cmd_result("#{sudo_prefix()}chown -RL #{TI.user()} #{staging_dir}")

      # SSH to the source server and run the backup. It will place the snapshot
      # into the staging_dir directory
      TU.notice("Create a backup of #{@options[:source]} in #{staging_dir}")
      TU.forward_cmd_results?(true)
      
      backup_options=[]
      backup_options << TU.get_tungsten_command_options()
      backup_options << "--backup"
      backup_options << "--target=#{opt(:target)}"
      backup_options << "--storage-directory=#{staging_dir}"
      if opt(:is_clustered) == false
        backup_options << "--service=#{@options[:service]}"
      end
      
      TU.ssh_result("#{TI.root()}/#{CURRENT_RELEASE_DIRECTORY}/tungsten-replicator/bin/xtrabackup_to_slave #{backup_options.join(' ')}", 
        @options[:source], TI.user())
      TU.forward_cmd_results?(false)
    
      # This must be done before the MySQL server is started
      TU.notice("Prepare the files for MySQL to run")
      TU.forward_cmd_results?(true)
      TU.cmd_result("#{get_xtrabackup_command()} --apply-log #{staging_dir}")
      TU.forward_cmd_results?(false)

      # If we didn't take the backup directly into the data directory,
      # then we need to stop MySQL, empty the data directory and move the
      # files into the proper location
      unless @options[:direct] == true
        TU.notice("Stop MySQL and empty all data directories")
        # Shutdown MySQL and empty the data directory in preparation for the 
        # restored data
        empty_mysql_directory()

        # Copy the backup files to the mysql data directory
        if supports_move_back == true
          restore_cmd = "--move-back"
        else
          restore_cmd = "--copy-back"
        end
        
        TU.notice("Transfer data files to the MySQL data directory")
        TU.forward_cmd_results?(true)
        TU.cmd_result("#{sudo_prefix()}#{get_xtrabackup_command()} #{restore_cmd} #{staging_dir}")
        TU.forward_cmd_results?(false)
      end

      # Fix the permissions and restart the service
      [
        @options[:mysqldatadir],
        @options[:mysqlibdatadir],
        @options[:mysqliblogdir]
      ].uniq().each{
        |dir|
        if dir.to_s() == ""
          next
        end
        
        TU.cmd_result("#{sudo_prefix()}chown -RL #{@options[:mysqluser]}: #{dir}")
      }

      start_mysql_server()
      
      # This updates the trep_commit_seqno table with the proper location
      # if the backup was taken from a master server
      if TU.cmd("#{sudo_prefix()}test -f #{@options[:mysqldatadir]}/#{MASTER_BACKUP_POSITION_SQL}")
        TU.cmd_result("#{sudo_prefix()}cat #{@options[:mysqldatadir]}/#{MASTER_BACKUP_POSITION_SQL} | #{get_mysql_command()}")
      end
      
      TU.notice("Backup and restore complete")
    ensure
      if @options[:direct] == false && staging_dir != "" && File.exists?(staging_dir)
        TU.debug("Remove #{staging_dir} due to the error")
        TU.cmd_result("rm -r #{staging_dir}")
      end
      
      # If the backup/restore failed, the MySQL data directory ownership may
      # be left in a broken state
      if @options[:direct] == true
        TU.cmd_result("#{sudo_prefix()}chown -RL #{@options[:mysqluser]}: #{staging_dir}")
      end
    end
  end
  
  def provision_with_mysqldump
    begin
      # Create a directory to hold the mysqldump output
      id = build_timestamp_id("provision_mysqldump_")
      staging_dir = TI.setting(TI.setting_key(REPL_SERVICES, opt(:service), "repl_backup_directory")) + "/" + id
      FileUtils.mkdir_p(staging_dir)
      
      # SSH to the source server and run the backup. It will place the output
      # into the staging_dir directory
      TU.notice("Create a mysqldump backup of #{@options[:source]} in #{staging_dir}")
      TU.forward_cmd_results?(true)
      
      backup_options=[]
      backup_options << TU.get_tungsten_command_options()
      backup_options << "--backup"
      backup_options << "--target=#{opt(:target)}"
      backup_options << "--storage-file=#{staging_dir}/provision.sql.gz"
      if opt(:is_clustered) == false
        backup_options << "--service=#{@options[:service]}"
      end
      
      TU.ssh_result("#{TI.root()}/#{CURRENT_RELEASE_DIRECTORY}/tungsten-replicator/bin/mysqldump_to_slave #{backup_options.join(' ')}", 
        @options[:source], TI.user())
      TU.forward_cmd_results?(false)
      
      TU.notice("Load the mysqldump file")
      begin
        TU.cmd_result("gunzip -c #{staging_dir}/provision.sql.gz | #{get_mysql_command()}")
      rescue CommandError => ce
        raise MessageError.new("Unable to load the mysqldump file: #{ce.errors}")
      end
    ensure
      if staging_dir != "" && File.exists?(staging_dir)
        TU.debug("Remove #{staging_dir} due to the error")
        TU.cmd_result("rm -r #{staging_dir}")
      end
    end
  end
  
  def provision_from_direct_master
    # Create a directory to hold the mysqldump output
    id = build_timestamp_id("provision_from_direct_master_")
    staging_dir = TI.setting(TI.setting_key(REPL_SERVICES, opt(:service), "repl_backup_directory")) + "/" + id
    FileUtils.mkdir_p(staging_dir)
    opt(:change_master_file, "#{staging_dir}/change_master.sql")
    
    extraction_conf = TI.setting(TI.setting_key(REPL_SERVICES, opt(:service), "repl_direct_datasource_mysql_service_conf"))
    extraction_host = TI.setting(TI.setting_key(REPL_SERVICES, opt(:service), "repl_direct_datasource_host"))
    extraction_port = TI.setting(TI.setting_key(REPL_SERVICES, opt(:service), "repl_direct_datasource_port"))
    mysqldump = "mysqldump --defaults-file=#{extraction_conf} --host=#{extraction_host} --port=#{extraction_port} --opt --single-transaction --all-databases --add-drop-database --master-data=2"
    
    # Run the mysqldump command and load the contents directly into mysql
    # The output is parsed by egrep to find the CHANGE MASTER statement
    # and write it to another file
    begin      
      script = File.open("#{staging_dir}/mysqldump.sh", "w")
      script.puts("#!/bin/bash")
      script.puts("#{mysqldump} | tee >(egrep \"^-- CHANGE MASTER\" > #{opt(:change_master_file)}) | #{get_mysql_command()}")
      script.close()
      File.chmod(0755, script.path())
      TU.cmd_result("#{script.path()}")
    rescue CommandError => ce
      TU.debug(ce)
      raise "Unable to extract and apply the MySQL data for provisioning"
    end
    
    # Parse the CHANGE MASTER information for the file number and position
    change_master_line = TU.cmd_result("cat #{opt(:change_master_file)}")
    if change_master_line != nil
      binlog_file = nil
      binlog_position = nil
      
      m = change_master_line.match(/MASTER_LOG_FILE='([a-zA-Z0-9\.\-\_]*)', MASTER_LOG_POS=([0-9]*)/)
      if m
        binlog_file = m[1]
        binlog_position = m[2]
      else
        raise "Unable to parse CHANGE MASTER data"
      end
      
      TU.notice("#{TI.hostname()} has been provisioned to #{binlog_file}:#{binlog_position} on #{extraction_host}:#{extraction_port}")
      
      begin
        TU.forward_cmd_results?(true)

        set_position_command = []
        set_position_command << "#{TI.root()}/#{CURRENT_RELEASE_DIRECTORY}/tungsten-replicator/bin/tungsten_set_position"
        set_position_command << "--seqno=0"
        set_position_command << "--epoch=0"
        set_position_command << "--source-id=#{extraction_host}"
        set_position_command << "--event-id=#{binlog_file}:#{binlog_position}"
        set_position_command << TU.get_tungsten_command_options()
        TU.cmd_result(set_position_command.join(' '))
      rescue CommandError => ce
        TU.debug(ce)
        raise "Unable to set the new replication position based on provisioning information"
      ensure
        TU.forward_cmd_results?(false)
      end
    else
      raise "Unable to find CHANGE MASTER data"
    end
  end
  
  def validate
    # All replication must be OFFLINE
    unless TI.is_replicator?()
      TU.error("This server is not configured for replication")
      return
    end
    
    if opt(:source) == AUTODETECT
      @options[:source] = nil
      
      if opt(:service) == nil
        TU.error("Unable to autodetect a value for --source without --service")
      else
        # Find all hosts that are replicating this service
        available_sources = JSON.parse(TU.cmd_result("#{TI.root()}/#{CURRENT_RELEASE_DIRECTORY}/tungsten-replicator/bin/multi_trepctl --service=#{opt(:service)} --path=self --output=json"))
        
        # Find a non-master service that is ONLINE or OFFLINE:NORMAL
        available_sources.each{
          |svc|
          if svc["host"] == TI.hostname()
            next
          end
          
          if svc["role"] == "master"
            next
          end
          
          if svc["state"] == "ONLINE"
            opt(:source, svc["host"])
          end
        }
        
        # Accept any master service that is ONLINE
        if opt(:source) == nil
          available_sources.each{
            |svc|
            if svc["host"] == TI.hostname()
              next
            end
            
            if svc["role"] != "master"
              next
            end
            
            if svc["state"] == "ONLINE"
              opt(:source, svc["host"])
            end
          }
        end
        
        if opt(:source) == nil
          TU.error("Unable to autodetect a value for --source. Make sure that the replicator is running on all other datasources for the #{opt(:service)} replication service.")
        end
      end
    end
    
    valid_service_option = false
    if opt(:service) != nil
      if TI.dataservices().include?(opt(:service))
        valid_service_option = true
      end
    end
    # Look at services on the source and suggest a proper value
    if valid_service_option == false && opt(:source) != nil
      begin
        local_services = TI.dataservices()
        master_services = []
        service_names = []
        raw = TU.ssh_result("#{TI.root()}/#{CURRENT_RELEASE_DIRECTORY}/tungsten-replicator/bin/trepctl services -json", 
          opt(:source), TI.user())
        services = JSON.parse(raw)
        services.each{
          |s|
          svc = s["serviceName"]
          
          # Only suggest this service if it is part of the local replicator
          unless local_services.include?(svc)
            next
          end
          
          service_names << svc
          if s["role"] == "master"
            master_services << svc
          end
        }
        
        if opt(:service) == nil
          error = "The --service option was not given."
        else
          error = "The --service option provided does not exist."
        end
        
        case master_services.size()
        when 0
          TU.error("#{error} You must specify it as one of #{service_names.join(',')}.")
        when 1
          svc = master_services[0]
          TU.error("#{error} You must specify it as #{svc} which is the master on #{opt(:source)}.")
        else
          TU.error("#{error} You must specify it as one of #{master_services.join(',')}.")
        end
      rescue JSON::ParserError => pe
        TU.debug(pe)
        TU.error("Unable to suggest a value for --service due to parsing issues.")
      rescue CommandError => ce
        TU.debug(ce)
        TU.error("Unable to suggest a value for --service due to issues on #{opt(:source)}")
      end
    end
    
    # This section evaluates :mysqldump and :xtrabackup to determine the best
    # mechanism to use. This will need to be more generic as we support
    # more platforms
    if opt(:mysqldump) == true && opt(:xtrabackup) == true
      TU.warning("You have specified --mysqldump and --xtrabackup, the script will use xtrabackup")
    end
    
    # Make sure to unset the :mysqldump value if :xtrabackup has been given
    if opt(:xtrabackup) == true
      opt(:mysqldump, false)
    end
    
    # Inspect the default value for the replication service to identify the 
    # preferred method
    if opt(:service) != nil && opt(:mysqldump) == nil && opt(:xtrabackup) == nil
      if TI.setting(TI.setting_key(REPL_SERVICES, opt(:service), "repl_backup_method")) == "mysqldump"
        opt(:mysqldump, true)
      else
        opt(:mysqldump, false)
      end
    end
    
    if opt(:service) && TI
      topology = TI.setting(TI.setting_key(DATASERVICES, opt(:service), "dataservice_topology"))
    else
      topology = nil
    end
    opt(:topology, topology)
    
    if opt(:topology) == "clustered"
      opt(:is_clustered, true)
    else
      opt(:is_clustered, false)
    end
    
    if opt(:topology) == "direct"
      opt(:mysqldump, true)
      
      if opt(:source) == nil && TI
        opt(:source, TI.setting(TI.setting_key(REPL_SERVICES, opt(:service), "repl_direct_datasource_host")))
      end
    end
    
    # Override the current hostname with the IP address in cases where the user requests it
    if opt(:provision_to_ip_address) == true
      addresses = TU.get_ip_addresses(TI.hostname())
      if addresses == false || addresses.size() == 0
        TU.error("Unable to identify local IP address requested by the --provision-to-ip-address option")
      else
        opt(:target, addresses[0])
      end
    else
      opt(:target, TI.hostname())
    end
    
    # Run validation for super classes after we have determined the backup
    # type. This makes sure that the needed options are loaded
    super()
    
    unless TU.is_valid?()
      return
    end
    
    if @options[:mysqldump] == false
      if sudo_prefix() != ""
        if TI.setting("root_command_prefix") != "true"
          TU.error("The installation at #{TI.root()} is not allowed to use sudo")
        else
          # Test for specific commands
        end
      else
        if ENV['USER'] != @options[:mysqluser] && ENV['USER'] != "root"
          TU.error("The current user is not the #{@options[:mysqluser]} system user or root. You must run the script as #{@options[:mysqluser]} or enable sudo by running `tpm update --enable-sudo-access=true`.")
        end
      end
    
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
    
      @options[:mysqlibdatadir] = get_mysql_option("innodb_data_home_dir")
      @options[:mysqliblogdir] = get_mysql_option("innodb_log_group_home_dir")
      @options[:mysqllogdir] = TI.setting(TI.setting_key(REPL_SERVICES, opt(:service), "repl_datasource_log_directory"))
      @options[:mysqllogpattern] = TI.setting(TI.setting_key(REPL_SERVICES, opt(:service), "repl_datasource_log_pattern"))

      if @options[:mysqldatadir].to_s() == ""
        TU.error "The configuration file at #{@options[:my_cnf]} does not define a datadir value."
      else
        unless TU.cmd("#{sudo_prefix()}test -w #{@options[:mysqldatadir]}")
          TU.error "The MySQL data dir '#{@options[:mysqldatadir]}' is not writeable"
        end
      end

      # Read innodb_log_file_size from the my.cnf file
      @options[:innodb_log_file_size] = get_mysql_option("innodb_log_file_size")
      if @options[:innodb_log_file_size].to_s() == ""
        # The configuration file doesn't have a innodb_log_file_size value
        # Get it from actual file and store it in wrapper config file
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
        
      path = get_innobackupex_path()
      if path == ""
        TU.error("Unable to find the innobackupex script")
      end
          
      # If the InnoDB files are stored somewhere other than datadir we are
      # not able to put them all in the correct position at this time
      # This only matters if we are restoring directly to the data directory
      if @options[:direct] == true
        if @options[:mysqlibdatadir].to_s() != "" || @options[:mysqliblogdir].to_s() != ""
          TU.error("Unable to restore to #{@options[:mysqldatadir]} because #{@options[:my_cnf]} includes a definition for 'innodb_data_home_dir' or 'innodb_log_group_home_dir'")
        end
      end
    else
      # No extra validation needed for mysqldump
    end
    
    if @options[:source].to_s() == ""
      TU.error("The --source argument is required")
    else
      if TU.is_localhost?(opt(:source))
        TU.error("The value provided for --source, #{opt(:source)}, resolves to the local node. You cannot provision a node from itself.")
      end
      
      if opt(:topology) != "direct" && TU.test_ssh(@options[:source], TI.user())
        begin
          TU.forward_cmd_results?(true)
          
          backup_options=[]
          backup_options << TU.get_tungsten_command_options()
          backup_options << "--backup"
          backup_options << "--target=#{opt(:target)}"
          backup_options << "--validate"
          if opt(:is_clustered) == false
            backup_options << "--service=#{@options[:service]}"
          end
          
          if @options[:mysqldump] == false
            TU.ssh_result("#{TI.root()}/#{CURRENT_RELEASE_DIRECTORY}/tungsten-replicator/bin/xtrabackup_to_slave #{backup_options.join(' ')}", 
              @options[:source], TI.user())
          else
            TU.ssh_result("#{TI.root()}/#{CURRENT_RELEASE_DIRECTORY}/tungsten-replicator/bin/mysqldump_to_slave #{backup_options.join(' ')}", 
              @options[:source], TI.user())
          end
          TU.forward_cmd_results?(false)
        rescue CommandError => ce
          TU.debug(ce)
          TU.error("There were errors validating the provision script on #{opt(:source)}")
        end
      end
    end
  end
  
  def configure
    super()
    
    add_option(:mysqldump, {
      :on => "--mysqldump",
      :help => "Use mysqldump instead of xtrabackup"
    })
    
    add_option(:xtrabackup, {
      :on => "--xtrabackup",
      :help => "Use xtrabackup instead of mysqldump"
    })
    
    add_option(:source, {
      :on => "--source String",
      :help => "Server to use as a source for the backup"
    })
    
    add_option(:direct, {
      :on => "--direct",
      :default => false,
      :help => "Use the MySQL data directory for staging and preparation",
      :aliases => ["--restore-to-datadir"]
    })
    
    add_option(:provision_to_ip_address, {
      :on => "--provision-to-ip-address",
      :default => false,
      :help => "Tell the source server to send backup information to the IP address for this server instead of the hostname. This may be needed if DNS entries are not fully propogated when machines are automatically added to a cluster."
    })
    
    # We want the THL and relay logs to be reset with the new data
    set_option_default(:clear_logs, true)
    set_option_default(:offline, true)
    set_option_default(:online, true)
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
    return prefix + Time.now.strftime("%Y-%m-%d_%H-%M") + "_" + rand(100).to_s
  end
  
  def require_local_mysql_service?
    if @options[:mysqldump] == false
      true
    else
      false
    end
  end
  
  def script_name
    "tungsten_provision_slave"
  end
  
  def script_log_path
    if TI
      "#{TI.root()}/service_logs/provision_slave.log"
    else
      super()
    end
  end
end
