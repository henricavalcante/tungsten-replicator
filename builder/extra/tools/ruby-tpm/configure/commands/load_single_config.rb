class LoadConfigCommand
  include ConfigureCommand
  include ClusterCommandModule
  
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
  
  def get_validation_checks
    if (klass = command_class())
      Module.const_get(klass).new(@config).get_validation_checks()
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
    opts.on("--command-class String")            { |val|
      command_class(val)
    }
    
    Configurator.instance.run_option_parser(opts, arguments)
  end
    
  def run
    reset_errors()
    prompt_handler = ConfigurePromptHandler.new(@config)
    debug("Validate configuration values for #{@config.getProperty(HOST)}")

    # Validate the values in the configuration file against the prompt validation
    prompt_handler.save_system_defaults()
    prompt_handler.validate()
    
    add_remote_result(prompt_handler.get_remote_result())
    output_property('props', @config.props.dup)
    @config.store(Configurator.instance.get_config_filename())
    
    # Remove the config object so that the dump/load process is faster
    @config = nil
    puts Marshal.dump(get_remote_result())
  end
  
  def require_all_command_dataservices?
    false
  end
  
  def get_message_host_key
    @config.getProperty([DEPLOYMENT_CONFIGURATION_KEY])
  end
  
  def allow_multiple_tpm_commands?
    true
  end
  
  def self.get_command_name
    'load-config'
  end
  
  def self.display_command
    false
  end
end