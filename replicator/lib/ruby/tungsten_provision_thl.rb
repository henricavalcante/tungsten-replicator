require 'tempfile'

# TODO : Add an option to provision the schema creation into THL as well
# TODO : Include any encryption settings from the installed replicator
class TungstenReplicatorProvisionTHL
  include TungstenScript
  include MySQLServiceScript
  include OfflineSingleServiceScript
  
  # A flag to look for Ctrl-C
  @@interrupted = false
  
  def main
    case command()
    when "provision"
      provision_thl()
    when "generate_schema_file"
      schema = generate_schema_file()
      TU.output(File.read(schema.path()))
      File.unlink(schema.path())
    when "cleanup"
      cleanup_sandbox()
    when "status"
      sandbox_status()
    end
  end
  
  def provision_thl
    # Create a clean working space
    cleanup_sandbox()
    TU.mkdir_if_absent(opt(:working_dir))
    TU.mkdir_if_absent(opt(:software_dir))
    TU.mkdir_if_absent(opt(:replicator_dir))
    TU.mkdir_if_absent(opt(:tmp_dir))
    
    # Move the necessary software packages into our working directory
    begin
      TU.cmd_result("cd #{opt(:software_dir)}; tar zxf #{opt(:tungsten_replicator_package)}")
      TU.cmd_result("cp #{opt(:mysql_package)} #{opt(:software_dir)}")
    rescue CommandError => ce
      TU.debug(ce)
      raise "Unable to prepare software packages"
    end
    
    # Create the MySQL Sandbox instance and the necessary users
    begin
      TU.cmd_result("make_sandbox #{opt(:software_dir)}/#{File.basename(opt(:mysql_package))} -- #{get_sandbox_options().join(' ')}")
      TU.cmd_result("#{opt(:mysql_dir)}/use -u root -e \"grant all on *.* to #{opt(:sandbox_user)} identified by '#{opt(:sandbox_password)}' with grant option\"")
    rescue CommandError => ce
      TU.debug(ce)
      raise "There were issues configure the sandbox MySQL server"
    end
    
    # Calculate the base directory for the binaries so we can include it as
    # a --preferred-path option. This will prevent any version conflict issues
    # in the binaries we use.
    begin
      opt(:mysql_basedir, TU.cmd_result("grep ^BASEDIR #{opt(:mysql_dir)}/use | awk -F= '{print $2}'"))
      
      if opt(:mysql_basedir).to_s() == ""
        raise "Unable to find the binary base directory for the MySQL sandbox"
      end
    rescue CommandError => ce
      TU.debug(ce)
      raise "Unable to find the binary base directory for the MySQL sandbox"
    end
    
    # Install the Tungsten Replicator to the sandbox directory using a
    # modified version of the configuration for this Tungsten Replicator
    f = File.open("#{opt(:tmp_dir)}/tungsten.ini", "w")
    f.puts("[#{opt(:service)}]")
    f.puts(get_tungsten_replicator_options().join("\n"))
    f.close()
    
    begin
      TU.cmd_result("#{opt(:software_dir)}/#{File.basename(opt(:tungsten_replicator_package), '.tar.gz')}/tools/tpm install --ini=#{f.path()} --tty")
    rescue CommandError => ce
      TU.debug(ce)
      raise "There were issues configuring the sandbox Tungsten Replicator"
    end
    
    # Create a TungstenInstall object for the sandbox replicator
    sandbox = TungstenInstall.get(opt(:replicator_dir) + "/tungsten")
    
    if opt(:provision_from_slave) == true
      slave_uri = sandbox.trepctl_value(opt(:service), "masterListenUri")
      TU.notice("Run `trepctl -service #{opt(:service)} setrole -role slave -uri #{slave_uri}` to point at the sandbox replicator")
    end
    
    sandbox_my_cnf = Tempfile.new("cnf")
    sandbox_my_cnf.puts("!include #{opt(:my_cnf)}")
    sandbox_my_cnf.puts('[client]')
    sandbox_my_cnf.puts("user=#{opt(:sandbox_user)}")
    sandbox_my_cnf.puts("password=#{opt(:sandbox_password)}")
    sandbox_my_cnf.close()
    
    mysqldump = "mysqldump --defaults-file=#{@options[:my_cnf]} --host=#{opt(:extraction_host)} --port=#{opt(:extraction_port)}"
    mysql = "mysql --defaults-file=#{sandbox_my_cnf.path()} -h#{TI.hostname()} --port=#{opt(:sandbox_mysql_port)}"
    
    if opt(:override_schema_file) == nil
      schema = generate_schema_file()
      schema_path = schema.path()
    else
      schema_path = opt(:override_schema_file)
    end
    
    begin
      TU.cmd_result("cat #{schema_path} | #{mysql}")
    rescue => ce
      TU.debug(ce)
      raise "Unable to apply MySQL schema to the staging MySQL server"
    end
    
    # Run the mysqldump command and load the contents directly into mysql
    # The output is parsed by egrep to find the CHANGE MASTER statement
    # and write it to another file
    begin      
      cmd = "#{mysqldump} --opt --single-transaction --master-data=2 --no-create-db --no-create-info #{opt(:schemas)}"
      script = File.open("#{opt(:tmp_dir)}/mysqldump.sh", "w")
      script.puts("#!/bin/bash")
      script.puts("#{cmd} | tee >(egrep \"^-- CHANGE MASTER\" > #{opt(:change_master_file)}) | #{mysql}")
      script.close()
      File.chmod(0755, script.path())
      TU.cmd_result("#{script.path()}")
    rescue CommandError => ce
      TU.debug(ce)
      raise "Unable to extract and apply the MySQL data for provisioning"
    end
    
    if opt(:provision_from_slave) == true
      TU.notice("The command(s) below should be used to set replication position on")
      TU.notice("slaves after they have caught up with the sandbox replicator. Only")
      TU.notice("run commands associated with replication services running on the slave.")
      TI.sql_result(@options[:service], "show schemas like 'tungsten_%';").each{
        |row|
        schema = row["Database (tungsten_%)"]
        svc = schema[9, schema.length()]
        sql = "SELECT * FROM #{schema}.trep_commit_seqno;"

        begin
          position = TI.sql_result(opt(:service), sql)
        rescue CommandError => ce
          next
        end

        if position.size() == 0
          TU.notice("For #{svc}: `dsctl -service #{svc} reset`")
        elsif position.size() == 1
          TU.notice("For #{svc}: `#{get_dsctl_set_command(svc, position[0])}`")
        else
          TU.notice("Unable to set a consistent replication position for the #{svc} service. You may try the command below to set it to the lowest committed position.")
          lowest_position = nil
          position.each{
            |pos|
            if pos["seqno"] == "-1"
              next
            end
            
            if lowest_position == nil or lowest_position["seqno"] > pos["seqno"]
              lowest_position = pos.dup()
            end
          }
          
          if lowest_position != nil
            TU.notice("For #{svc}: `#{get_dsctl_set_command(svc, lowest_position)}`")
          end
        end
      }
    else
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
      
        TU.notice("The sandbox has been provisioned to #{binlog_file}:#{binlog_position} on #{opt(:extraction_host)}:#{opt(:extraction_port)}")
      else
        raise "Unable to find CHANGE MASTER data"
      end
    end
    
    old_trap = trap("INT") {
      TungstenReplicatorProvisionTHL.interrupted?(true);
    }
    
    # Watch the sandbox replicator and make sure it finishes ONLINE and
    # caught up with the sandbox MySQL instance
    sandbox_replicator_caught_up = false
    while (sandbox_replicator_caught_up == false && TungstenReplicatorProvisionTHL.interrupted?() == false)
      # Check that the sandbox replicator is still running
      unless sandbox.is_running?("replicator") == true
        raise "The sandbox replicator is no longer running. It may be possible to complete the provisioning process. Resolve the issue and let the replication service in #{opt(:replicator_dir)} catch up before running `#{script_name()} cleanup`."
      end
      
      # Check the sandbox replicator status and position
      status = sandbox.status(sandbox.default_dataservice())
      
      if status.replicator_status(sandbox.hostname()) != "ONLINE"
        raise "The sandbox replicator is no longer ONLINE. It may be possible to complete the provisioning process. Resolve the issue and let the replication service in #{opt(:replicator_dir)} catch up before running `#{script_name()} cleanup`."
      end
      
      currentEventId = status.replicator_value(sandbox.hostname(), "currentEventId")
      # Remove the MySQL thread_id from the end of appliedLastEventId
      appliedLastEventId = status.replicator_value(sandbox.hostname(), "appliedLastEventId").gsub(/;.*/, "")
      
      if currentEventId == appliedLastEventId
        sandbox_replicator_caught_up = true
      else
        TU.debug("Waiting for the sandbox replicator to catch up from #{appliedLastEventId} to #{currentEventId}")
      end
      
      begin
        # Sleep for 1 minute before checking again
        if sandbox_replicator_caught_up == false
          sleep 60
        end
      rescue
        # Ignore any exceptions here because they are probably coming from Ctrl-C
      end
    end
    
    # Throw an error if the script was interrupted so the sandbox services
    # will remain for further inspection.
    if TungstenReplicatorProvisionTHL.interrupted?() == true
      TU.error("The #{script_name()} process was interrupted but the provisioning process may still complete. Check the replicator in #{opt(:replicator_dir)}/tungsten before running `#{script_name()} cleanup`.")
    end
    
    if TU.is_valid?()
      if opt(:provision_from_slave) == true
        appliedLastSeqno = sandbox.trepctl_value(opt(:service), "appliedLastSeqno")
        TU.notice("The final extracted THL event from the sandbox replicator is #{appliedLastSeqno}.")
      end
    end
    
    trap("INT", old_trap);
  end
  
  def generate_schema_file
    TU.mkdir_if_absent(opt(:tmp_dir))
    mysqldump = "mysqldump --defaults-file=#{@options[:my_cnf]} --host=#{opt(:extraction_host)} --port=#{opt(:extraction_port)}"
    
    # Dump and load the schema structure for all entries listed in --schemas
    schema = File.open("#{opt(:tmp_dir)}/schema.sql", "w")
    schema.puts("SET SESSION SQL_LOG_BIN=0;")
    schema.close()
    
    begin
      transforms = []
      transforms << "| perl -pe 's/(myisam|innodb)/Blackhole/i'"
      
      cmd = "#{mysqldump} --no-data --skip-triggers --add-drop-database #{opt(:schemas)} #{transforms.join('')}"
      TU.cmd_result("#{cmd} >> #{schema.path()}")
    rescue CommandError => ce
      TU.debug(ce)
      raise "Unable to extract the MySQL schema for provisioning"
    end
    
    schema
  end

  def configure
    super()
    
    add_option(:sandbox_dir, {
      :on => "--sandbox-directory String",
      :help => "The location to use for storing the temporary replicator and MySQL server"
    })
    
    add_option(:sandbox_user, {
      :on => "--sandbox-user String",
      :help => "The MySQL user to create and use in the MySQL Sandbox",
      :default => "tungsten"
    })
    
    add_option(:sandbox_password, {
      :on => "--sandbox-password String",
      :help => "The password for --sandbox-user",
      :default => "secret"
    })
    
    add_option(:sandbox_mysql_port, {
      :on => "--sandbox-mysql-port String",
      :help => "The listening port for the MySQL Sandbox",
      :parse => method(:parse_integer_option),
      :default => "3307"
    })
    
    add_option(:sandbox_rmi_port, {
      :on => "--sandbox-rmi-port String",
      :help => "The listening port for the temporary Tungsten Replicator",
      :parse => method(:parse_integer_option),
      :default => "10002"
    })
    
    add_option(:sandbox_thl_port, {
      :on => "--sandbox-thl-port String",
      :help => "The THL listening port for the temporary Tungsten Replicator",
      :parse => method(:parse_integer_option),
      :required => false
    })
    
    add_option(:tungsten_replicator_package, {
      :on => "--tungsten-replicator-package String",
      :help => "The location of a fresh Tungsten Replicator tar.gz package",
      :required => true
    })
    
    add_option(:mysql_package, {
      :on => "--mysql-package String",
      :help => "The location of a fresh MySQL tar.gz package",
      :required => true
    })
    
    add_option(:schemas, {
      :on => "--schemas String",
      :help => "The provision process will be limited to these schemas",
      :required => true
    })
    
    add_option(:override_schema_file, {
      :on => "--override-schema-file String",
      :help => "Use this SQL file to generate the schemas in the MySQL Sandbox instead of pulling from the database"
    })
    
    add_option(:cleanup_on_failure, {
      :on => "--cleanup-on-failure String",
      :parse => method(:parse_boolean_option),
      :default => false
    })
    
    add_option(:provision_from_slave, {
      :on => "--provision-from-slave String",
      :parse => method(:parse_boolean_option),
      :default => false
    })
    
    add_command(:provision, {
      :help => "Create THL entries",
      :default => true
    })
    
    add_command(:cleanup, {
      :help => "Cleanup the sandbox environment from a previous run"
    })
    
    add_command(:status, {
      :help => "Show the current sandbox replicator status"
    })
    
    add_command(:generate_schema_file, {
      :help => "Output the SQL to create the listed schemas"
    })
  end
  
  def get_offline_services_list
    if command() == "provision"
      super()
    else
      []
    end
  end
  
  def validate
    case command()
    when "cleanup"
      @option_definitions[:tungsten_replicator_package][:required] = false
      @option_definitions[:mysql_package][:required] = false
      @option_definitions[:schemas][:required] = false
    when "generate_schema_file"
      @option_definitions[:tungsten_replicator_package][:required] = false
      @option_definitions[:mysql_package][:required] = false
    when "status"
      @option_definitions[:tungsten_replicator_package][:required] = false
      @option_definitions[:mysql_package][:required] = false
      @option_definitions[:schemas][:required] = false
    end
    
    super()
    
    unless TU.is_valid?()
      return TU.is_valid?()
    end
    
    if opt(:sandbox_dir) == nil
      opt(:sandbox_dir, TI.setting(TI.setting_key(REPL_SERVICES, opt(:service), "repl_backup_directory")))
    end
    unless File.writable?(opt(:sandbox_dir))
      TU.error("Unable to write to #{opt(:sandbox_dir)}")
    end
    
    if command() == "provision"
      unless TI.is_running?("replicator")
        TU.error("The replicator must be running with the #{opt(:service)} service OFFLINE. Try running `#{TI.service_path("replicator")} start offline`.")
      else
        unless opt(:provision_from_slave) == true
          unless ["master", "direct"].include?(TI.trepctl_value(opt(:service), "role"))
            TU.error("The #{script_name()} script may only be run on a master replication service.")
          end
        end
      end
      
      if opt(:provision_from_slave) == true
        if opt(:sandbox_thl_port) == nil
          TU.error("The --sandbox-thl-port option is required when using --provision-from-slave")
        end
      end
    
      if File.exist?(opt(:tungsten_replicator_package))
        unless opt(:tungsten_replicator_package)[-7,7] == ".tar.gz"
          TU.error("The --tungsten-replicator-package option must be the path to a .tar.gz file")
        end
      else
        TU.error("Unable to find a file at #{opt(:tungsten_replicator_package)}")
      end
    
      if File.exist?(opt(:mysql_package))
        unless opt(:mysql_package)[-7,7] == ".tar.gz"
          TU.error("The --mysql-package option must be the path to a .tar.gz file")
        end
      else
        TU.error("Unable to find a file at #{opt(:mysql_package)}")
      end
      
      if opt(:override_schema_file) != nil
        opt(:override_schema_file, File.expand_path(opt(:override_schema_file)))
        
        unless File.readable?(opt(:override_schema_file))
          TU.error("Unable to read a file at #{opt(:override_schema_file)}")
        end
      end
    end
    
    if TU.which("make_sandbox") == nil
      TU.error("Unable to find the make_sandbox utility from MySQL::Sandbox")
    end
    
    if opt(:schemas) == nil
      opt(:schemas, "--all-databases")
    else
      opt(:schemas, "--databases #{opt(:schemas).split(',').join(' ')}")
    end
    
    opt(:working_dir, "#{opt(:sandbox_dir)}/#{script_name()}")
    opt(:software_dir, "#{opt(:working_dir)}/software")
    opt(:replicator_dir, "#{opt(:working_dir)}/replicator")
    opt(:mysql_dir, "#{opt(:working_dir)}/mysql")
    opt(:tmp_dir, "#{opt(:working_dir)}/tmp")
    
    # File to store the CHANGE MASTER statement from mysqldump
    opt(:change_master_file, "#{opt(:tmp_dir)}/change_master.sql")
    
    opt(:extraction_host, TI.setting(TI.setting_key(REPL_SERVICES, opt(:service), "repl_direct_datasource_host")))
    opt(:extraction_port, TI.setting(TI.setting_key(REPL_SERVICES, opt(:service), "repl_direct_datasource_port")))
  end
  
  def get_mysql_options
    [
      "server-id=10",
      "log-bin=mysql-bin",
      "log-slave-updates",
      "max_allowed_packet=48M",
      "innodb-flush-method=O_DIRECT",
      "innodb-log-file-size=50M",
      "innodb_buffer_pool_size=5G",
      "innodb_buffer_pool_size=1G",
      "max_allowed_packet=48M",
      "max-connections=350",
      "innodb-additional-mem-pool-size=50M",
      "innodb-log-buffer-size=50M",
      "sync_binlog=0",
      "innodb-thread-concurrency=0",
      "binlog-format=ROW",
      "default-storage-engine=innodb"
    ]
  end
  
  def get_tungsten_replicator_options
    # Static properties file for the replication service
    static = TI.setting(TI.setting_key(REPL_SERVICES, opt(:service), "repl_svc_config_file"))
    # Additional --property options for the staging replicator
    additional_properties = []
    
    # Options to read from the existing tpm configuration
    tpm_options = {
      "repl-java-file-encoding" => "repl_java_file_encoding",
      "repl-java-user-timezone" => "repl_java_user_timezone"
    }
    # Settings to read directly from the replication service configuration
    replicator_settings = {
      "replicator.extractor.dbms.usingBytesForString" => "replicator.extractor.dbms.usingBytesForString"
    }
    
    case TI.trepctl_value(opt(:service), "role")
    when "master"
      replicator_settings["replicator.stage.binlog-to-q.filters"] = "replicator.stage.binlog-to-q.filters"
      replicator_settings["replicator.stage.q-to-thl.filters"] = "replicator.stage.q-to-thl.filters"
    when "direct"
      replicator_settings["replicator.stage.binlog-to-q.filters"] = "replicator.stage.d-binlog-to-q.filters"
      replicator_settings["replicator.stage.q-to-thl.filters"] = "replicator.stage.d-q-to-thl.filters"
    end    
    
    # Read settings from tpm and include them using the tpm option
    tpm_options.each{
      |prop,key|
      value = TI.setting(TI.setting_key(REPL_SERVICES, opt(:service), key))
      if value != ""
        additional_properties << "#{prop}=#{value}"
      end
    }
    
    # Read settings from the static properties and include them with --property
    replicator_settings.each{
      |setting,match|
      additional_properties << "--property=#{setting}=#{TI.trepctl_property(opt(:service), match)}"
    }
    
    # Include all filter configuration settings
    TU.cmd_result("egrep '^replicator\.filter\..*\..*=' #{static}").split("\n").each{
      |line|
      additional_properties << "--property=#{line}"
    }
    
    if opt(:sandbox_thl_port) != nil
      thl_listening_port = opt(:sandbox_thl_port)
    else
      # Use the same THL port so slaves can start applying while we provision
      thl_listening_port = TI.setting(TI.setting_key(REPL_SERVICES, opt(:service), 'repl_thl_port'))
    end
    
    if opt(:provision_from_slave) == true
      thl_directory = "#{opt(:replicator_dir)}/thl"
    else
      thl_directory = TI.setting(TI.setting_key(HOSTS, "repl_thl_directory"))
    end
    
    [
      "topology=master-slave",
      "home-directory=#{opt(:replicator_dir)}",
      "master=#{TI.hostname()}",
      "replication-port=#{opt(:sandbox_mysql_port)}",
      "replication-user=#{opt(:sandbox_user)}",
      "replication-password=#{opt(:sandbox_password)}",
      "datasource-mysql-conf=#{opt(:mysql_dir)}/my.sandbox.cnf",
      "rmi-port=#{opt(:sandbox_rmi_port)}",
      "disable-relay-logs=true",
      "repl-java-mem-size=4096",
      "auto-recovery-max-attempts=5",    
      "start=true",
      "thl-port=#{thl_listening_port}",

      # Use the MySQL::Sandbox path for any necessary scripts
      "preferred-path=#{opt(:mysql_basedir)}/bin",

      # Use the same THL directory so the existing replicator will use
      # the new THL entries
      "thl-directory=#{thl_directory}",
      "skip-validation-check=THLStorageCheck",

      # Use a unique source-id for each iteration so the extractor starts
      # from the current binary log position
      "property=replicator.source_id=#{TI.hostname()}.sandbox.#{Process.pid}"
    ] + additional_properties
  end
  
  def get_sandbox_options
    get_mysql_options.map{|x| "-c #{x}"}+[
      "--no_confirm",
      "--no_show",
      "--remote_access=%",
      "--sandbox_port=#{opt(:sandbox_mysql_port)}",
      "--upper_directory=#{File.dirname(opt(:mysql_dir))}",
      "--sandbox_directory=#{File.basename(opt(:mysql_dir))}"
    ]
  end
  
  def cleanup(code = 0)
    cleanup = false
    
    # A succesful run includes a cleanup so the original replicator can
    # come back ONLINE
    if code == 0 || opt(:cleanup_on_failure) == true
      cleanup = true
    end
    
    # Leave running for all attached slaves to finish the provisioning process
    if opt(:provision_from_slave) == true
      cleanup = false
    end
    
    if cleanup == true
      cleanup_sandbox()
    else
      TU.notice("The sandbox services were left in place. Run `#{script_name()} cleanup` to remove them.")
    end
    
    super(code)
  end

  def cleanup_sandbox
    if opt(:working_dir) && File.exists?(opt(:working_dir))
      if File.exists?(opt(:mysql_dir))
        begin
          TU.cmd_result("sbtool -o delete -s #{opt(:mysql_dir)}")
        rescue CommandError
          TU.debug("There were issues removing the sandbox. Trying again before continuing operation.")
          TU.cmd_result("sbtool -o delete -s #{opt(:mysql_dir)}", true)
        end
      end
      
      FileUtils.rmtree(opt(:working_dir))
    end
  end
  
  def sandbox_status
    TU.output(TU.cmd_result("#{opt(:replicator_dir)}/tungsten/tungsten-replicator/bin/trepctl status"))
  end
  
  def script_log_path
    if TI
      "#{TI.root()}/service_logs/provision_thl.log"
    else
      super()
    end
  end
  
  def get_dsctl_set_command(svc, position)
    "dsctl -service #{svc} set -seqno #{position["seqno"]} -epoch #{position["epoch_number"]} -event-id #{position["eventid"].split(";")[0]} -source-id #{position["source_id"]}"
  end
  
  def self.interrupted?(val = nil)
    if val != nil
      @@interrupted = val
    end
    
    (@@interrupted == true)
  end
  
  def script_name
    "tungsten_provision_thl"
  end
end