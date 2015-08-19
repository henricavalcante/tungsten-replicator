module ProvisionNewSlavesPackageModule
  def validate_commit
    super()
    
    if @provision_new_slaves != false
      @promotion_settings.props.each_key{
        |key|
        if @promotion_settings.getProperty([key, REPLICATOR_ENABLED]) == "false"
          cfg = get_matching_deployment_configuration(DEPLOYMENT_CONFIGURATION_KEY, key)
          if @config.getProperty([HOSTS, cfg.getProperty([DEPLOYMENT_HOST]), HOST_ENABLE_REPLICATOR]) == "true"
            @promotion_settings.setProperty([key, PROVISION_NEW_SLAVES], @provision_new_slaves.to_s())
          end
        end
      }
    end
    
    is_valid?()
  end
  
  def parsed_options?(arguments)
    @provision_new_slaves = false
    
    arguments = super(arguments)

    if display_help?() && !display_preview?()
      return arguments
    end
    
    # Define extra option to load event.  
    opts=OptionParser.new
    
    unless Configurator.instance.is_locked?()
      opts.on("--provision-new-slaves [String]") {|val|
        if val == nil
          val = true
        else
          # Accepted values are:
          #   true
          #   false
          #   service
          #   service@hostname
          unless val =~ /^([a-zA-Z0-9_]+)(@([a-zA-Z0-9_\.\-]+))?$/
            error("Unable to parse --provision-new-slaves: The value is invalid.")
          end
        end
        
        @provision_new_slaves = val
      }
    end
    
    opts = Configurator.instance.run_option_parser(opts, arguments)

    # Return options.
    opts
  end
  
  def output_command_usage
    super()
    
    unless Configurator.instance.is_locked?()
      # Hide this option until it is fully implemented
      #output_usage_line("--provision-new-slaves", "Use the tungsten_provision_slave script to provision any new replication server")
    end
  end
  
  def get_bash_completion_arguments
    super() + ["--provision-new-slaves"]
  end
end