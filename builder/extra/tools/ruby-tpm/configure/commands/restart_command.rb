class RestartCommand
  include ConfigureCommand
  include RemoteCommand
  include ClusterCommandModule
  
  unless Configurator.instance.is_locked?()
    include RequireDataserviceArgumentModule
  end
  
  def skip_prompts?
    true
  end
  
  def display_alive_thread?
    false
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
      RestartClusterDeploymentStep
    ]
  end
  
  def self.display_command
    false
  end
  
  def self.get_command_name
    'restart'
  end
  
  def self.get_command_description
    "Restart Tungsten services on the machines specified or this installation."
  end
end

module RestartClusterDeploymentStep
  def get_methods
    [
      ConfigureCommitmentMethod.new("stop_services", -1, 0),
      ConfigureCommitmentMethod.new("start_services", 1, ConfigureDeploymentStepMethod::FINAL_STEP_WEIGHT),
      ConfigureCommitmentMethod.new("wait_for_manager", 2, -1),
      ConfigureCommitmentMethod.new("report_services", ConfigureDeploymentStepMethod::FINAL_GROUP_ID, ConfigureDeploymentStepMethod::FINAL_STEP_WEIGHT, ConfigureDeploymentStepParallelization::NONE)
    ]
  end
  module_function :get_methods
end