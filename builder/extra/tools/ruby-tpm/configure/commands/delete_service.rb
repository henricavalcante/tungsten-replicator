class DeleteReplicationServiceCommand
  include ConfigureCommand
  
  if Configurator.instance.is_locked?() == false
    include RemoteCommand
  end
  
  include ClusterCommandModule
  include RequireDataserviceArgumentModule
  
  def initialize(config)
    super(config)
    @skip_prompts = true
    @confirmation_flag_found = false
  end
  
  def confirmation_flag_found?(v = nil)
    if v != nil
      @confirmation_flag_found = v
    end
    
    return @confirmation_flag_found
  end
  
  def validate_commit
    super()
   
   include_promotion_setting(DELETE_REPLICATION_POSITION, (@keep_position == false))
   
   is_valid?()
  end
  
  def parsed_options?(arguments)
    @keep_position = false
    
    arguments = super(arguments)

    if display_help?() && !display_preview?()
      return arguments
    end
  
    # Define extra option to load event.  
    opts=OptionParser.new
    
    opts.on("--keep-position") { @keep_position = true }
    opts.on("--i-am-sure") { confirmation_flag_found?(true) }
    
    opts = Configurator.instance.run_option_parser(opts, arguments)
    
    unless confirmation_flag_found?()
      error("You must add '--i-am-sure' to the command in order to delete the service")
    end

    # Return options. 
    opts
  end
  
  def output_command_usage
    super()
    output_usage_line("--keep-position", "Do not remove the metadata schema")
  end
  
  def get_bash_completion_arguments
    super() + ["--keep-position"] + get_cluster_bash_completion_arguments()
  end
  
  def allow_command_line_cluster_options?
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
      DeleteReplicationServiceDeploymentStep
    ]
  end
  
  def self.get_command_name
    'delete-service'
  end
  
  def self.get_command_description
    "Delete a replication service from this host"
  end
end

class DeleteServiceIsNotClustering < ConfigureValidationCheck
  include ClusterHostCheck
  include CommitValidationCheck
  include ReplicatorEnabledCheck
  
  def set_vars
    @title = "Check if any of the services use clustering"
  end
  
  def validate
    dataservices = @config.getNestedPropertyOr([DATASERVICES], {})
    Configurator.instance.command.command_dataservices().each{
      |ds_alias|
      
      if dataservices.has_key?(ds_alias)
        topology = Topology.build(ds_alias, @config)
        if topology.is_a?(ClusterTopology)
          error("The 'tpm delete-service' is not supported for clustered services: #{ds_alias}.")
        end
      else
        error("Unable to delete the #{ds_alias} service because it is not in the configuration")
      end
    }
  end
  
  def enabled?
    if [DeleteReplicationServiceCommand.name].include?(@config.getProperty(DEPLOYMENT_COMMAND))
      return (super() && true)
    end
    
    return false
  end
end

module DeleteReplicationServiceDeploymentStep
  def get_methods
    [
      ConfigureCommitmentMethod.new("set_maintenance_policy", ConfigureDeploymentStepMethod::FIRST_GROUP_ID, 0),
      ConfigureCommitmentMethod.new("stop_replication_services", -1, 0),
      ConfigureCommitmentMethod.new("delete_replication_service", 0, 0),
      ConfigureCommitmentMethod.new("start_replication_services", 1, ConfigureDeploymentStepMethod::FINAL_STEP_WEIGHT),
      ConfigureCommitmentMethod.new("set_original_policy", 4, 2),
      ConfigureCommitmentMethod.new("report_services", ConfigureDeploymentStepMethod::FINAL_GROUP_ID, ConfigureDeploymentStepMethod::FINAL_STEP_WEIGHT, ConfigureDeploymentStepParallelization::NONE)
    ]
  end
  module_function :get_methods
  
  def delete_replication_service
    if is_replicator?()
      config_files = {}
      ["#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/#{Configurator::HOST_CONFIG}"].each{
        |path|
        p = Properties.new()
        p.load(path)
        config_files[path] = p
      }

      ds_aliases = Configurator.instance.command.command_dataservices()
      
      @config.getNestedPropertyOr([REPL_SERVICES], {}).each_key{
        |rs_alias|
        unless ds_aliases.include?(@config.getProperty([REPL_SERVICES, rs_alias, DEPLOYMENT_DATASERVICE]))
          next
        end
        
        if svc_is_running?(get_svc_command("#{get_deployment_basedir()}/tungsten-replicator/bin/replicator"))
          cmd_result("echo yes | #{get_deployment_basedir()}/tungsten-replicator/bin/trepctl -port #{@config.getProperty([REPL_SERVICES, rs_alias, REPL_RMI_PORT])} -service #{@config.getProperty([REPL_SERVICES, rs_alias, DEPLOYMENT_SERVICE])} unload")
        end
        
        if File.exist?(@config.getProperty([REPL_SERVICES, rs_alias, REPL_SVC_CONFIG_FILE]))
          FileUtils.rm_f(@config.getProperty([REPL_SERVICES, rs_alias, REPL_SVC_CONFIG_FILE]))
        end
        if File.exist?(WatchFiles.get_original_watch_file(@config.getProperty([REPL_SERVICES, rs_alias, REPL_SVC_CONFIG_FILE])))
          FileUtils.rm_f(WatchFiles.get_original_watch_file(@config.getProperty([REPL_SERVICES, rs_alias, REPL_SVC_CONFIG_FILE])))
        end
        if File.exist?(@config.getProperty([REPL_SERVICES, rs_alias, REPL_SVC_DYNAMIC_CONFIG]))
          FileUtils.rm_f(@config.getProperty([REPL_SERVICES, rs_alias, REPL_SVC_DYNAMIC_CONFIG]))
        end
        
        if File.exist?(@config.getProperty([REPL_SERVICES, rs_alias, REPL_LOG_DIR]))
          FileUtils.rm_rf(@config.getProperty([REPL_SERVICES, rs_alias, REPL_LOG_DIR]))
        end
        if File.exist?(@config.getProperty([REPL_SERVICES, rs_alias, REPL_RELAY_LOG_DIR]))
          FileUtils.rm_rf(@config.getProperty([REPL_SERVICES, rs_alias, REPL_RELAY_LOG_DIR]))
        end
        
        if get_additional_property(DELETE_REPLICATION_POSITION) == true
          ds = get_applier_datasource(rs_alias)
          ds.drop_tungsten_schema(@config.getProperty([REPL_SERVICES, rs_alias, REPL_SVC_SCHEMA]))
        end
        
        ds_alias = @config.getProperty([REPL_SERVICES, rs_alias, DEPLOYMENT_DATASERVICE])
        config_files.each{
          |path,p|
          p.setProperty([SYSTEM, DATASERVICES, ds_alias], nil)
          p.setProperty([SYSTEM, DATASERVICE_HOST_OPTIONS, ds_alias], nil)
          p.setProperty([SYSTEM, DATASERVICE_REPLICATION_OPTIONS, ds_alias], nil)
          p.setProperty([SYSTEM, REPL_SERVICES, rs_alias], nil)
          p.setProperty([DATASERVICES, ds_alias], nil)
          p.setProperty([DATASERVICE_HOST_OPTIONS, ds_alias], nil)
          p.setProperty([DATASERVICE_REPLICATION_OPTIONS, ds_alias], nil)
          p.setProperty([REPL_SERVICES, rs_alias], nil)
        }
      }
      
      config_files.each{
        |path, p|
        p.store(path)
      }
      FileUtils.cp(@config.getProperty(CURRENT_RELEASE_DIRECTORY) + '/' + Configurator::HOST_CONFIG, @config.getProperty(CURRENT_RELEASE_DIRECTORY) + '/.' + Configurator::HOST_CONFIG + '.orig')
    end
  end
end