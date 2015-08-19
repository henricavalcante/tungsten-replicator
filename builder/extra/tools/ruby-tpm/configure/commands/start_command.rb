class StartCommand
  include ConfigureCommand
  include RemoteCommand
  include ClusterCommandModule
  
  unless Configurator.instance.is_locked?()
    include RequireDataserviceArgumentModule
  end

  FROM_EVENT = "from_event"
  FROM_MASTER_BACKUP_EVENT = "from_master_backup_event"
  TYPE_MYSQLDUMP = "mysqldump"
  TYPE_XTRABACKUP = "xtrabackup"
  TYPE_SNAPSHOT = "snapshot"

  def skip_prompts?
    true
  end
  
  def display_alive_thread?
    true
  end
  
  def validate_commit
    super()
   
    include_promotion_setting(FROM_EVENT, @from_event)
    include_promotion_setting(FROM_MASTER_BACKUP_EVENT, @from_master_backup_event)
    
    is_valid?()
  end

  def parsed_options?(arguments)
    arguments = super(arguments)

    if display_help?() && !display_preview?()
      return arguments
    end
  
    # Define extra option to load event.  
    opts=OptionParser.new
    opts.on("--from-event String") { |val| @from_event = val }
    opts.on("--from-master-backup-event String") { |val| @from_master_backup_event = val }
    opts = Configurator.instance.run_option_parser(opts, arguments)

    # Check options.  
    if @from_event != nil && @from_master_backup_event != nil
      error "--from-event and --from-master-backup-event options are incompatible"
    end

    if @from_master_backup_event == TYPE_MYSQLDUMP
      debug "Load event ID from restored mysqldump"
    elsif @from_master_backup_event == TYPE_XTRABACKUP
      debug "Load event ID from restored xtrabackup"
    elsif @from_master_backup_event == TYPE_SNAPSHOT
      debug "Load event ID from restored file system snapshot"
    elsif @from_master_backup_event != nil
      error "Unrecognized backup; only mysqldump, xtrabackup, or snapshot are permitted: #{@from_master_backup_event}"
    end

    # Return options. 
    opts
  end

  def output_command_usage
    super()
    output_usage_line("--from-event", "Start replicator from a specific event ID")
    output_usage_line("--from-master-backup-event", "Start slave replicator from event ID supplied by master backup (values: mysqldump, xtrabackup, or snapshot)")
  end
  
  def get_bash_completion_arguments
    super() + ["--from-event", "--from-master-backup-event"]
  end
 
  def get_validation_checks
    [
      ActiveDirectoryIsRunningCheck.new(),
      CurrentTopologyCheck.new(),
      CurrentCommandCoordinatorCheck.new()
    ]
  end

  def get_deployment_object_modules(config)
    [
      StartClusterDeploymentStep
    ]
  end
  
  def self.display_command
    false
  end

  def self.get_command_name
    'start'
  end
  
  def self.get_command_description
    "Start Tungsten services on the machines specified or this installation."
  end
end

module StartClusterDeploymentStep
  def get_methods
    [
      ConfigureCommitmentMethod.new("start_services_from_event", 1, ConfigureDeploymentStepMethod::FINAL_STEP_WEIGHT),
      ConfigureCommitmentMethod.new("wait_for_manager", 2, -1),
      ConfigureCommitmentMethod.new("report_services", ConfigureDeploymentStepMethod::FINAL_GROUP_ID, ConfigureDeploymentStepMethod::FINAL_STEP_WEIGHT, ConfigureDeploymentStepParallelization::NONE)
    ]
  end
  module_function :get_methods

  # Start services and bring the replicator online from a specific event ID. 
  def start_services_from_event
    # Get information required to configure replicator. 
    dynamic_props = @config.getProperty(REPL_SVC_DYNAMIC_CONFIG)
    from_event = get_additional_property(StartCommand::FROM_EVENT)
    from_master_backup_event = get_additional_property(StartCommand::FROM_MASTER_BACKUP_EVENT)
    debug "dynamic_props: #{dynamic_props}"
    debug "Option from_event: #{from_event}"
    debug "Option from_master_backup_event: #{from_master_backup_event}"

    # If we need to seek the event from a master backup, find it now. 
    if from_master_backup_event == "xtrabackup"
      info "Seeking MySQL event ID from restored Xtrabackup dump"
      xtrabackup_binlog_info = @config.getProperty(REPL_MYSQL_DATADIR) + "/xtrabackup_binlog_info"
      if ! File.exists?(xtrabackup_binlog_info)
        error "Unable to find binlog position from Xtrabackup: #{xtrabackup_binlog_info}"
      end

      # Parse the Xtrabackup file. 
      binlog_info = `cat #{xtrabackup_binlog_info}`
      debug "xtrabackup_binlog_info: #{binlog_info}"
      if binlog_info =~ /\.([0-9]+)\s*([0-9]+)\s*$/
        from_event = $1 + ":" + $2
      else
        error "Unable to locate binlog file name and offset: #{binlog_info}"
      end
      info "Located binlog event ID from Xtrabackup: #{from_event}"
    elsif from_master_backup_event == "mysqldump"
  
      ## The binlog position can be obtained from master.info as
      ## the mysqldump file contained a change master command
      
      info "Seeking MySQL event ID from restored mysqldump" 
      
      master_info = @config.getProperty(REPL_MYSQL_DATADIR) + "/master.info" 
      if ! File.exists?(master_info) 
        error "Unable to find binlog position from master_info: #{master_info}" 
      end 

      # Parse the Xtrabackup file. 
      binlog_info = `cat #{master_info} | head -3 | tail -2 | tr '\n' ' '` 
       
      debug "master_info: #{binlog_info}" 
      if binlog_info =~ /\.([0-9]+)\s*([0-9]+)\s*$/ 
        from_event = $1 + ":" + $2 
      else 
        error "Unable to locate binlog file name and offset: #{binlog_info}" 
      end 
      info "Located binlog event ID from Master_info: #{from_event}" 
    elsif from_master_backup_event == "snapshot"
      # Find the error log and ensure it is readable. 
      info "Seeking MySQL event ID from restored file system snapshot"
      errorLogVar = mysql("show variables like 'log_error'")
      if errorLogVar =~ /log_error\s+(\/.*)$/
        errorLog = $1
      end
      debug "Error log output: spec=#{errorLog} query output=#{errorLogVar}"
      if File.exists?(errorLog)
        debug "Error log found"
      else
        error "Unable to find MySQL error log; check permissions: #{errorLog}"
      end

      # Grep for the last position noted in the log.  
      if @config.getProperty(ROOT_PREFIX) == "true"
        sudo = "sudo -n"
      else
        sudo = ""
      end
      binlog_notice = cmd_result("#{sudo} grep 'Last MySQL binlog file position' #{errorLog}|tail -1")
      if binlog_notice =~ /position\s+[0-9]+\s+([0-9]+), file name.*\.([0-9]+)/
        from_event = $2 + ":" + $1
      else
        error "Unable to locate binlog file name and offset: #{binlog_notice}"
      end
      info "Located last binlog event ID from MySQL log: #{from_event}"
    end

    # If we now have an event ID, prep the replicator by turning off 
    # auto_enable.  
    if from_event != nil
      configure_dynamic_props(dynamic_props, "false")
    end

    # If we have an event ID, start the replicator with the event ID.  
    if from_event != nil
      # Start the replicator. 
      info("Starting the replicator")
      info(cmd_result("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-replicator/bin/replicator start"))
      
      # Put replicator online. 
      info("Bringing replicator online from event ID: #{from_event}")
      trepctl = "#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-replicator/bin/trepctl"
      
      begin
        Timeout.timeout(30) {
          while true
            begin
              cmd_result("#{trepctl} services")
              break
            rescue CommandError
            end
          end
        }
      rescue Timeout::Error
        cmd_result("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-replicator/bin/replicator stop")
        raise "Unable to connect to the replicator to confirm successful start"
      end
      
      begin
        info(cmd_result("#{trepctl} online -from-event #{from_event}"))
      rescue CommandError
        raise "Unable to bring the replicator online at #{from_event}"
      end

      # Restore dynamic.properties if auto_enable is true. 
      auto_enable = @config.getProperty(REPL_AUTOENABLE)
      debug "auto_enable: #{auto_enable}"
      if auto_enable == "true"
        configure_dynamic_props(dynamic_props, "true")
      end
    end
    
    # Start services. 
    info("Starting all services")
    info(cmd_result("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/cluster-home/bin/startall", true))
  end

  # Configure dynamic properties file. 
  def configure_dynamic_props(dynamic_props, auto_enable) 
    set_auto_enable = false
    newprops = []
    # Try to update in place. 
    if File.exists?(dynamic_props) 
      File.open(dynamic_props) do |file|
        while line = file.gets
          if line =~ /replicator.auto_enable/
            set_auto_enable = true
            newprops.insert -1, "replicator.auto_enable=false"
          else
            newprops.insert -1, line
          end
        end
      end
    end

    # Add the property if we didn't set it already. 
    if set_auto_enable != true
      newprops.insert -1, "replicator.auto_enable=false"
    end

    # Write the file.  
    out = File.open(dynamic_props, "w")
    newprops.each { |line| out.puts line }
    out.close
  end

  # Issue a MySQL command and return the output. 
  def mysql(command)
    host = @config.getProperty(REPL_DBHOST)
    port = @config.getProperty(REPL_DBPORT)
    user = @config.getProperty(REPL_DBLOGIN)
    pw = @config.getProperty(REPL_DBPASSWORD)

    begin
      Timeout.timeout(5) {
        return cmd_result("mysql -u#{user} --password=\"#{pw}\" -h#{host} --port=#{port} -e \"#{command}\"")
      }
    rescue Timeout::Error
    rescue RemoteCommandNotAllowed
    rescue => e
    end

    return ""
  end

end
