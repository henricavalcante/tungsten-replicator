class StopCommand
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
    []
  end
  
  def get_deployment_object_modules(config)
    [
      StopClusterDeploymentStep
    ]
  end
  
  def self.display_command
    false
  end
  
  def self.get_command_name
    'stop'
  end
  
  def self.get_command_description
    "Stop Tungsten services on the machines specified or this installation."
  end
end

module StopClusterDeploymentStep
  def get_methods
    [
      ConfigureCommitmentMethod.new("stop_services", -1, 0)
    ]
  end
  module_function :get_methods
end