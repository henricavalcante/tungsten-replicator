class DeploySingleConfigCommand
  include ConfigureCommand
  include ClusterCommandModule
  
  def initialize(config)
    super(config)
    
    @deployment_method_class_name = nil
    @run_group_id = nil
    @additional_properties_filename = nil
  end
  
  def command_class(val = nil)
    if val != nil
      @command_class = val
      return val
    end
    
    if @command_class
      @command_class
    else
      nil
    end
  end
  
  def get_deployment_object_modules(config)
    if (klass = command_class())
      Module.const_get(klass).new(@config).get_deployment_object_modules(config)
    else
      super()
    end
  end
  
  def parsed_options?(arguments)
    arguments = super(arguments)
    
    if display_help?() && !display_preview?()
      return arguments
    end
    
    opts=OptionParser.new
    opts.on("--deployment-method-class String") { |val|
                                        @deployment_method_class_name = val
                                      }
    opts.on("--run-group-id String") { |val|
                                        @run_group_id = val
                                      }
    opts.on("--additional-properties String") { |val|
                                        @additional_properties_filename = val
                                      }
    opts.on("--command-class String")            { |val|
      command_class(val)
    }
    
    Configurator.instance.run_option_parser(opts, arguments)
  end
    
  def run
    Configurator.instance.debug("Run deployment for #{@deployment_method_class_name}:#{@run_group_id}")
    
    additional_properties = Properties.new
    additional_properties.use_prompt_handler = false
    if @additional_properties_filename != nil && File.exist?(@additional_properties_filename)
      additional_properties.load(@additional_properties_filename)
    end
    
    if @run_group_id
      @run_group_id = @run_group_id.to_i
    end
    result = get_deployment_object(Configurator.instance.get_config()).run(@deployment_method_class_name, @run_group_id, additional_properties)
    
    # Remove the config object so that the dump/load process is faster
    @config = nil
    puts Marshal.dump(result)
  end
  
  def require_all_command_dataservices?
    false
  end
  
  def allow_multiple_tpm_commands?
    true
  end
  
  def self.get_command_name
    'deploy-single-config'
  end
  
  def self.display_command
    false
  end
end