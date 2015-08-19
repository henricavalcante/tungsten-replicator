class CCTRLCommand
  include ConfigureCommand
  include RemoteCommand
  include ClusterCommandModule
  
  CCTRL_LS = "ls"
  
  def initialize(config)
    super(config)
    @skip_prompts = true
  end
  
  def allow_command_line_cluster_options?
    false
  end
  
  def validate_commit
    super()
    
    @promotion_settings.force_output()
    
    is_valid?()
  end
  
  def output_command_usage
    super()
  end
  
  def get_validation_checks
    [
      ActiveDirectoryIsRunningCheck.new(),
      ClusterStatusCheck.new()
    ]
  end
  
  def get_deployment_object_modules(config)
    [
    ]
  end
  
  def display_alive_thread?
    false
  end
  
  def output_completion_text
  end
  
  def self.display_command
    false
  end
  
  def self.get_command_name
    'cctrl'
  end
  
  def self.get_command_description
    "Get information from cctrl"
  end
end

class ClusterStatusCheck < ConfigureValidationCheck
  include CommitValidationCheck
  
  def set_vars
    @title = "Collect current manager policy"
  end
  
  def validate
    begin
      cctrl = CCTRL.new(Configurator.instance.get_cctrl_path(@config.getProperty(CURRENT_RELEASE_DIRECTORY), @config.getProperty(MGR_RMI_PORT)))
      output_property("cctrl", cctrl.to_a())
    rescue CommandError => ce
      exception(ce)
      error(ce.message)
    end
  end
  
  def enabled?
    current_release_directory = @config.getProperty(CURRENT_RELEASE_DIRECTORY)
    return (File.exists?(current_release_directory) && Configurator.instance.svc_is_running?("#{current_release_directory}/tungsten-manager/bin/manager"))
  end
end