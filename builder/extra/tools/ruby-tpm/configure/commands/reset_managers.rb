class ResetManagersCommand
  include ConfigureCommand
  include RemoteCommand
  include ClusterCommandModule
  include ClusterConfigurationsModule
  
  DELETE_CACHE = "delete_cache"
  
  def initialize(config)
    super(config)
    @skip_prompts = true
  end
  
  def allow_command_line_cluster_options?
    false
  end
  
  def validate_commit
    super()
    
    include_promotion_setting(DELETE_CACHE, delete_cache?().to_s())
    
    is_valid?()
  end
  
  def delete_cache?(val = nil)
    if val != nil
      @delete_cache = val
    end
    
    if @delete_cache == nil
      false
    else
      @delete_cache
    end
  end
  
  def parsed_options?(arguments)
    arguments = super(arguments)
    
    if display_help?() && !display_preview?()
      return arguments
    end
    
    opts=OptionParser.new
    opts.on("--remove-cache") { delete_cache?(true) }
    
    Configurator.instance.run_option_parser(opts, arguments)
  end
  
  def output_command_usage
    super()
  
    output_usage_line("--remove-cache", "Remove cluster metadata cache")
  end
  
  def get_bash_completion_arguments
    super() + ["--remove-cache"]
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
      ResetManagersDeploymentStep
    ]
  end
  
  def self.get_command_name
    'reset-managers'
  end
  
  def self.get_command_description
    "Restart the the Tungsten Manager on each host"
  end
  
  def self.display_command
    if Configurator.instance.is_enterprise?()
      true
    else
      false
    end
  end
end

module ResetManagersDeploymentStep
  def get_methods
    [
      ConfigureCommitmentMethod.new("set_maintenance_policy", ConfigureDeploymentStepMethod::FIRST_GROUP_ID, 0),
      ConfigureCommitmentMethod.new("stop_manager", -1, 0),
      ConfigureCommitmentMethod.new("clear_cluster_status", 0, 0),
      ConfigureCommitmentMethod.new("start_manager", 0, 1, false),
      ConfigureCommitmentMethod.new("wait_for_manager", 2, -1),
      ConfigureCommitmentMethod.new("set_original_policy", 4, 2),
      ConfigureCommitmentMethod.new("report_services", ConfigureDeploymentStepMethod::FINAL_GROUP_ID, ConfigureDeploymentStepMethod::FINAL_STEP_WEIGHT, ConfigureDeploymentStepParallelization::NONE)
    ]
  end
  module_function :get_methods
  
  def clear_cluster_status
    if is_manager?() && get_additional_property(ResetManagersCommand::DELETE_CACHE) == "true"
      Configurator.instance.command.build_topologies(@config)
      @config.getPropertyOr([DATASERVICES], {}).each_key{
        |ds_alias|
        if ds_alias == DEFAULTS
          next
        end
        
        if @config.getProperty([DATASERVICES, ds_alias, DATASERVICE_IS_COMPOSITE]) == "true"
          next
        end
        
        path = "#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/cluster-home/conf/cluster/#{ds_alias}/datasource"
        if File.exists?(path)
          cmd_result("rm -f #{path}/*")
        end
      }
    end
  end
end