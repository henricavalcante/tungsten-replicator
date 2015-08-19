class InstallCommand
  include ConfigureCommand
  include ResetConfigPackageModule
  include ClusterCommandModule
  include ProvisionNewSlavesPackageModule
  
  def output_command_usage()
    super()
    
    OutputHandler.queue_usage_output?(true)
    display_cluster_options()
    OutputHandler.flush_usage()
  end
  
  def output_completion_text
    output_cluster_completion_text()

    super()
  end
  
  def arguments_valid?
    super()
    
    if Configurator.instance.is_locked?()
      error("Unable to install from an installed directory")
    end
    
    return is_valid?()
  end
  
  def enable_log?()
    true
  end
  
  def allow_undefined_dataservice?
    true
  end
  
  def allow_check_current_version?
    true
  end
  
  def use_remote_package?
    true
  end
  
  def self.get_command_name
    'install'
  end
  
  def self.get_command_description
    "Install Tungsten with the current configuration and any options specified at runtime."
  end
end

class InstallerMasterSlaveCheck < ConfigureValidationCheck
  def initialize(config)
    super()
    set_config(config)
  end
  
  def set_vars
    @title = "Check that the master-host is part of the config"
    @fatal_on_error = true
  end
  
  def validate
    @config.getPropertyOr(REPL_SERVICES, {}).keys.each{
      |s_key|
      if @config.getProperty([REPL_SERVICES, s_key, REPL_ROLE]) == REPL_ROLE_S
        found_master = false
        master_host = @config.getProperty([REPL_SERVICES, s_key, REPL_MASTERHOST])
        
        @config.getPropertyOr(HOSTS, {}).keys.each{
          |h_key|
          if @config.getProperty([HOSTS, h_key, HOST]) == master_host
            found_master = true
          end
        }
        
        if found_master == false
          error("Unable to find master host '#{master_host}' for service '#{@config.getProperty([REPL_SERVICES, s_key, DEPLOYMENT_SERVICE])}'")
          help("You can skip this check by adding --skip-validation-check=InstallerMasterSlaveCheck")
        end
      end
    }
  end
end