class CreateSchemaCommand
  include ConfigureCommand
  include ClusterCommandModule
  include RequireDataserviceArgumentModule
  
  SCHEMA_NAME = "schema_name"
  
  def output_command_usage()
    super()
  end
  
  def get_bash_completion_arguments
    super() + ["--schema-name"]
  end
  
  def get_validation_checks
    [
      CurrentReleaseDirectoryCheck.new()
    ]
  end
  
  def validate_commit
    super()
    
    include_promotion_setting(SCHEMA_NAME, @schema_name)
    
    is_valid?()
  end
  
  def parsed_options?(arguments)
    arguments = super(arguments)

    if display_help?() && !display_preview?()
      return arguments
    end
  
    # Define extra option to load event.  
    opts=OptionParser.new
    opts.on("--schema-name String") { |val| @schema_name = val }
    remainder = Configurator.instance.run_option_parser(opts, arguments)

    # Return remaining options. 
    remainder
  end
  
  def get_deployment_object_modules(config)
    [
      CreateSchemaDeploymentStep
    ]
  end
  
  def self.display_command
    false
  end
  
  def self.get_command_name
    'create-schema'
  end
  
  def self.get_command_description
    "Create the Tungsten schema for this service"
  end
end

module CreateSchemaDeploymentStep
  def get_methods
    [
      ConfigureCommitmentMethod.new("create_service_schemas", 0, 0)
    ]
  end
  module_function :get_methods
  
  def create_service_schemas
    Configurator.instance.command.build_topologies(@config)
    Configurator.instance.command.command_dataservices().each{
      |ds_alias|
      @config.getPropertyOr([REPL_SERVICES], {}).each_key{
        |rs_alias|
        if @config.getProperty([REPL_SERVICES, rs_alias, DEPLOYMENT_DATASERVICE]) != ds_alias
          next
        end
        
        create_tungsten_schema(rs_alias, get_additional_property(CreateSchemaCommand::SCHEMA_NAME))
      }
    }
  end
end