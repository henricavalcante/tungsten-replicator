class PromoteConnectorsCommand
  include ConfigureCommand
  include RemoteCommand
  include ClusterCommandModule
  
  CURRENT_CONNECTOR_DIR = "current_connector_dir"
  
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
      CurrentConnectorCheck.new(),
      CurrentCommandCoordinatorCheck.new()
    ]
  end
  
  def get_deployment_object_modules(config)
    [
      PromoteConnectorDeploymentStep
    ]
  end
  
  def self.get_command_name
    'promote-connector'
  end
  
  def self.get_command_description
    "Stop the currently running connector and start the connector in the active installation.  This command should follow 'tpm promote --no-connectors' or 'tpm upgrade --no-connectors'"
  end
  
  def self.display_command
    if Configurator.instance.is_enterprise?()
      true
    else
      false
    end
  end
end

class CurrentConnectorCheck < ConfigureValidationCheck
  include CommitValidationCheck
  
  def set_vars
    @title = "Identify the currently running connector"
  end
  
  def validate
    found_connector = false
    Dir.glob("#{@config.getProperty(RELEASES_DIRECTORY)}/*") {
      |rel_name|
      
      begin
        if File.exists?("#{rel_name}/tungsten-connector/bin/connector")
          cmd_result("#{rel_name}/tungsten-connector/bin/connector status")        
          found_connector = true
          output_property(PromoteConnectorsCommand::CURRENT_CONNECTOR_DIR, rel_name)
        end
      rescue CommandError
      end
    }
  end
end

module PromoteConnectorDeploymentStep
  def get_methods
    [
      ConfigureCommitmentMethod.new("promote_connector"),
    ]
  end
  module_function :get_methods
  
  def promote_connector
    unless is_connector? == true
      info("Host is not configured as a Connector - skipping.")
      return
    end
    current_connector_dir = get_additional_property(PromoteConnectorsCommand::CURRENT_CONNECTOR_DIR)
    unless current_connector_dir.to_s() == ""
      info("Stopping #{current_connector_dir}/tungsten-connector/bin/connector")
      cmd_result("#{current_connector_dir}/tungsten-connector/bin/connector stop")
    else
      debug("No connector is currently running")
    end
    
    debug("Starting #{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-connector/bin/connector")
    cmd_result("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-connector/bin/connector start")
  end
end