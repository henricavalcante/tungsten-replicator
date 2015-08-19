class UpdateCommand
  include ConfigureCommand
  
  if Configurator.instance.is_locked?() == false
    include RemoteCommand
    include ResetBasenamePackageModule
    include ProvisionNewSlavesPackageModule
  end
  
  include ClusterCommandModule
  
  def get_command_name
    'update'
  end
  
  def validate_commit
    super()
   
    if @restart_replicators == false
      override_promotion_setting(RESTART_REPLICATORS, false)
    end
    if @restart_managers == false
      override_promotion_setting(RESTART_MANAGERS, false)
    end
    if @restart_connectors == false
      override_promotion_setting(RESTART_CONNECTORS, false)
    end
    
    is_valid?()
  end

  def parsed_options?(arguments)
    @restart_replicators = nil
    @restart_managers = nil
    @restart_connectors = nil
    @replace_release = false
    
    arguments = super(arguments)

    if display_help?() && !display_preview?()
      return arguments
    end
    
    if Configurator.instance.is_locked?()
      command_hosts(@config.getProperty([HOSTS, @config.getProperty([DEPLOYMENT_HOST]), HOST]))
    end
  
    # Define extra option to load event.  
    opts=OptionParser.new
    
    # nil means to bypass all checks and set it to the default
    opts.on("--no-connectors") { @restart_connectors = false }
    
    if Configurator.instance.is_locked?()
      opts.on("--no-restart") { 
        @restart_replicators = false  
        @restart_managers = false
        @restart_connectors = false
      }
    else
      opts.on("--replace-release") { @replace_release = true }
    end
    
    opts = Configurator.instance.run_option_parser(opts, arguments)

    # Return options. 
    opts
  end
  
  def get_default_remote_package_path
    if @replace_release == true
      false
    else
      nil
    end
  end
  
  def enable_log?()
    true
  end

  def output_command_usage
    super()
    output_usage_line("--no-connectors", "Do not restart any connectors running on the server")
    if Configurator.instance.is_locked?()
      output_usage_line("--no-restart", "Do not restart any component on the server")
    else
      output_usage_line("--replace-release", "Replace the current release directory on each host with a copy of this directory")
    end
    
    OutputHandler.queue_usage_output?(true)
    display_cluster_options()
    OutputHandler.flush_usage()
  end
  
  def get_bash_completion_arguments
    super() + ["--no-connectors", "--no-restart", "--replace-release"] + get_cluster_bash_completion_arguments()
  end
  
  def output_completion_text
    output_cluster_completion_text()

    super()
  end
  
  def allow_check_current_version?
    true
  end
  
  def use_remote_package?
    true
  end
  
  def self.get_command_name
    'update'
  end
  
  def self.get_command_aliases
    ['upgrade']
  end
  
  def self.get_command_description
    "Updates an existing installation of Tungsten.  If not arguments are specified, the local configuration is used to install.  If you specify --user, --hosts and --directory; this command will get the current configuration from each host and continue."
  end
end

module NotTungstenUpdatePrompt
  def enabled_for_command_line?
    if [ConfigureDataServiceCommand.name].include?(Configurator.instance.command.class.name) && Configurator.instance.is_locked?()
      return false
    end
    
    if [UpdateCommand.name, ValidateUpdateCommand.name].include?(@config.getNestedProperty([DEPLOYMENT_COMMAND]))
      return false
    else
      return (super() && true)
    end
  end
end

module NotTungstenUpdateCheck
  def enabled?
    if [UpdateCommand.name, ValidateUpdateCommand.name].include?(@config.getNestedProperty([DEPLOYMENT_COMMAND]))
      return false
    end
    
    if Configurator.instance.is_locked?
      return false
    end
    
    return (super() && true)
  end
end

module TungstenUpdateCheck
  def enabled?
    if [UpdateCommand.name, ValidateUpdateCommand.name].include?(@config.getProperty(DEPLOYMENT_COMMAND))
      return (super() && true)
    end
    
    return false
  end
end