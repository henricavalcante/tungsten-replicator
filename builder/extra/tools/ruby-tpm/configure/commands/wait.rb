class WaitCommand
  include ConfigureCommand
  include ClusterCommandModule
  include RequireDataserviceArgumentModule
  
  def output_command_usage()
    super()
    
    OutputHandler.queue_usage_output?(true)
    display_cluster_options()
    OutputHandler.flush_usage()
  end
  
  def get_validation_checks
    [
      CurrentReleaseDirectoryCheck.new()
    ]
  end
  
  def get_deployment_object_modules(config)
    [
      ClusterWaitDeploymentStep
    ]
  end
  
  def self.display_command
    false
  end
  
  def self.get_command_name
    'wait'
  end
  
  def self.get_command_description
    "Wait for all datasources in the named dataservice to be available in cctrl"
  end
end

module ClusterWaitDeploymentStep
  def get_methods
    [
      ConfigureCommitmentMethod.new("wait_for_manager_and_members", 0, 0)
    ]
  end
  module_function :get_methods
  
  def wait_for_manager_and_members
    wait_for_manager(true)
  end
end