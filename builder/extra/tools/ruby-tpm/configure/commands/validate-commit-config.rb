class ValidateCommitConfigCommand
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
    result = get_validation_handler().validate_commit_config(@config)
    
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
    'validate-commit-config'
  end
  
  def self.display_command
    false
  end
end