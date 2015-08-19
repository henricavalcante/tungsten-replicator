module ResetConfigPackageModule
  def parsed_options?(arguments)
    arguments = super(arguments)
    
    if display_help?() && !display_preview?()
      return arguments
    end
    
    opts=OptionParser.new
    opts.on("--reset") { 
      if is_a?(RemoteCommand) && loaded_remote_config()
        error "Unable to reset the configuration because we have already loaded a remote configuration"
      else
        @config.props = {}
      end
    }

    return Configurator.instance.run_option_parser(opts, arguments)
  end
  
  def output_command_usage
    super()

    output_usage_line("--reset", "Clear the current configuration before processing any arguments")
  end
end