module ClusterCommandModule
  COMMAND = "command"
  
  def initialize(config)
    super(config)
    reset_cluster_options()
  end
  
  def get_prompts
    [
      ConfigTargetBasenamePrompt.new(),
      DeploymentCommandPrompt.new(),
      DeploymentExternalConfigurationTypePrompt.new(),
      DeploymentExternalConfigurationSourcePrompt.new(),
      DeploymentConfigurationKeyPrompt.new(),
      RemotePackagePath.new(),
      DeploymentHost.new(),
      StagingHost.new(),
      StagingUser.new(),
      StagingDirectory.new(),
      DeploymentServicePrompt.new(),
      ClusterHosts.new(),
      Clusters.new(),
      Managers.new(),
      Connectors.new(),
      ReplicationServices.new(),
      DataserviceHostOptions.new(),
      DataserviceManagerOptions.new(),
      DataserviceReplicationOptions.new(),
      DataserviceConnectorOptions.new()
    ]
  end
  
  def get_validation_checks
    checks = []
    
    ClusterHostCheck.subclasses.each{
      |klass|
      checks << klass.new()
    }
    
    checks << GlobalHostAddressesCheck.new()
    checks << GlobalMatchingPingMethodCheck.new()
    
    return checks
  end
  
  def get_deployment_object_modules(config)    
    modules = []
    
    modules << ConfigureDeploymentStepDeployment
    modules << ConfigureDeploymentStepManager
    modules << ConfigureDeploymentStepReplicator
    modules << ConfigureDeploymentStepReplicationDataservice
    modules << ConfigureDeploymentStepConnector
    modules << ConfigureDeploymentStepServices
  
    DatabaseTypeDeploymentStep.submodules().each{
      |klass|
    
      modules << klass
    }

    modules
  end
  
  def allow_command_line_cluster_options?
    true
  end
  
  def reset_cluster_options
    @general_options = Properties.new()
    @dataservice_options = Properties.new()
    @host_options = Properties.new()
    @manager_options = Properties.new()
    @connector_options = Properties.new()
    @replication_options = Properties.new()
    
    @add_members = nil
    @remove_members = nil
    @add_connectors = nil
    @remove_connectors = nil
    
    @add_fixed_properties = []
    @remove_fixed_properties = []
    @skip_validation_checks = []
    @enable_validation_checks = []
    @skip_validation_warnings = []
    @enable_validation_warnings = []
  end
  
  def parsed_options?(arguments)
    arguments = super(arguments)
    
    if display_help?() && !display_preview?()
      return arguments
    end
    
    unless is_valid?()
      return arguments
    end
    
    unless allow_command_line_cluster_options?()
      return arguments
    end
    
    return parse_cluster_options(arguments, defaults_only?())
  end
  
  def parse_cluster_options(arguments, defaults = false)
    opts = OptionParser.new
    
    each_prompt(nil){
      |prompt|
      
      add_prompt(opts, prompt, @general_options)
    }
    
    each_prompt(Clusters){
      |prompt|
      
      add_prompt(opts, prompt, @dataservice_options, [DATASERVICES])
    }
    
    each_prompt(ClusterHosts){
      |prompt|
      
      add_prompt(opts, prompt, @host_options, [HOSTS])
    }
    
    each_prompt(Managers){
      |prompt|
      
      add_prompt(opts, prompt, @manager_options, [MANAGERS])
    }
    
    each_prompt(Connectors){
      |prompt|
      
      add_prompt(opts, prompt, @connector_options, [CONNECTORS])
    }
    
    each_prompt(ReplicationServices){
      |prompt|

      add_prompt(opts, prompt, @replication_options, [REPL_SERVICES])
    }
    
    unless defaults
      opts.on("--members+ String") {
        |val|
        @add_members = val.split(",")
      }
      opts.on("--members- String") {
        |val|
        @remove_members = val.split(",")
      }
      
      if Configurator.instance.is_enterprise?()
        opts.on("--connectors+ String") {
          |val|
          @add_connectors = val.split(",")
        }
        opts.on("--connectors- String") {
          |val|
          @remove_connectors = val.split(",")
        }
      end
    end
    
    opts.on("--property String")      {|val|
                                        if val.index('=') == nil
                                          error "Invalid value #{val} given for '--property'. There should be a key/value pair joined by a single =."
                                        end
                                        @add_fixed_properties << val
                                      }
    opts.on("--remove-property String") {|val|
                                        @remove_fixed_properties << val
                                      }
    opts.on("--skip-validation-check String")      {|val|
                                        val.split(",").each{
                                          |v|
                                          @skip_validation_checks << v
                                        }
                                      }
    opts.on("--enable-validation-check String")      {|val|
                                        val.split(",").each{
                                          |v|
                                          @enable_validation_checks << v
                                        }
                                      }
    opts.on("--skip-validation-warnings String")      {|val|
                                        val.split(",").each{
                                          |v|
                                          @skip_validation_warnings << v
                                        }
                                      }
    opts.on("--enable-validation-warnings String")      {|val|
                                        val.split(",").each{
                                          |v|
                                          @enable_validation_warnings << v
                                        }
                                      }

    return Configurator.instance.run_option_parser(opts, arguments)
  end
  
  def arguments_valid?
    super()
    
    if use_external_configuration?() && cluster_options_provided?()
      error("Command line arguments are not allowed because configuration is driven by #{external_configuration_summary()}. Update the configuration there and run `tpm update` to apply changes.")
    end
    
    return is_valid?()
  end
  
  def load_external_configurations
    external_configs = []
    external_option_sets = get_external_option_sets()
    
    external_option_sets.each{
      |option_set|
      @config.reset()
      external_options = option_set[:options]
      
      external_options.each{
        |section_options|
        section = section_options[:key]
        arguments = section_options[:arguments]
        reset_cluster_options()

        if section == DEFAULTS
          parse_cluster_options(arguments, true)
          load_cluster_defaults()
        else
          parse_cluster_options(arguments)
          load_cluster_options([to_identifier(section)])
        end
      }
      
      @config.setProperty(DEPLOYMENT_EXTERNAL_CONFIGURATION_TYPE, option_set[:type])
      @config.setProperty(DEPLOYMENT_EXTERNAL_CONFIGURATION_SOURCE, option_set[:source])
      
      if Configurator.instance.is_locked?()
        # Read the staging directory information from the current file
        original_config_file = Configurator.instance.get_base_path() + "/" + Configurator::HOST_CONFIG
        if File.exist?(original_config_file)
          original_config = Properties.new()
          original_config.load(original_config_file)
          @config.setProperty(DEPLOYMENT_HOST, original_config.getProperty(DEPLOYMENT_HOST))
          @config.setProperty(STAGING_HOST, original_config.getProperty(STAGING_HOST))
          @config.setProperty(STAGING_USER, original_config.getProperty(STAGING_USER))
          @config.setProperty(STAGING_DIRECTORY, original_config.getProperty(STAGING_DIRECTORY))
        end
      end
      
      external_configs << @config.dup()
    }
    
    external_configs
  end
  
  def get_external_option_sets
    option_sets = []
    
    if @config_ini_paths.size() != nil
      @config_ini_paths.each{
        |path|
        external_options = get_external_options_from_ini(path)
        external_options.sort!{
          |a,b|

          (a[:weight] <=> b[:weight])
        }

        option_sets << {
          :type => "ini",
          :source => path,
          :options => external_options
        }
      }
    end
    
    option_sets
  end
  
  def get_external_options_from_ini(path)
    external_arguments = []
    hostname = Configurator.instance.hostname()
    natural_order = 0
    
    debug("Load external configuration from #{path}")
    
    ini = IniParse.open(path)
    ini.each{
      |section|
      key = section.key
      
      if section.key == "defaults"
        key = DEFAULTS
      elsif section.key == "defaults.replicator"
        if Configurator.instance.is_enterprise?() == false
          key = DEFAULTS
        else
          debug("Bypassing the defaults.replicator section because this is not a Tungsten Replicator build")
          next
        end
      else
        match = section.key.match(/^([A-Za-z0-9_]+)@([A-Za-z0-9_.\-]+)$/)
        if match != nil
          if match[2] == hostname
            if match[1] == "defaults"
              key = DEFAULTS
            else
              key = match[1]
            end
          else
            debug("Bypassing the #{section.key} section because it does not apply to this host")
            next
          end
        end
      end
      
      args = []
      section.each{
        |line|
        unless line.is_a?(Array)
          values = [line]
        else
          values = line
        end
        
        values.each{
          |value|
          argument = value.key.gsub(/_/, "-")
          unless argument[0,2] == "--"
            argument = "--" + argument
          end
          
          v_string = value.value.to_s()
          if v_string[-2,2].to_s() == " \\"
            v_string = v_string[0, (v_string.length()-2)]
            Configurator.instance.warning("Extra ' \\' characters were found at the end of #{value.key}=#{value.value}. They have been automatically removed. You may wrap the value with double-quotes in order to keep the extra characters.")
          end
          args << "#{argument}=#{v_string}"
        }
      }
      
      external_arguments << {
        :key => key,
        :arguments => args,
        :weight => calculate_external_section_weight(section.key, key, args)
      }
    }
    
    external_arguments
  end
  
  def calculate_external_section_weight(original_key, key, args)
    @natural_order = 0 unless @natural_order
    
    # This is an indication that this section was relabeled to modify another section
    if original_key != key
      is_modification = true
    else
      is_modification = false
    end
    
    # Provide metadata about the arguments for this section
    is_composite = false
    has_relay_source = false
    args.each{
      |arg|
      if arg =~ /^--composite-datasources=|--dataservice-composite-datasources=/
        is_composite = true
        break
      end
      if arg =~ /^--relay-source=|--master-dataservice=|--dataservice-relay-source=/
        has_relay_source = true
        break
      end
    }
    
    # Calculate a weight for this section so we can sort on it
    # DEFAULTS
    # Services
    # Services that reference a relay source
    # Composite services
    # Sections that modify the above specifically for this host
    weight = 0
    if is_modification == true
      weight = 10**12
    elsif is_composite == true
      weight = 10**10
    elsif has_relay_source == true
      weight = 10**8
    elsif key == DEFAULTS
      weight = 10**4
    else
      weight = 10**6
    end
    
    # Add the order from the file as a tie breaker
    weight = weight + @natural_order
    @natural_order = @natural_order+1
    
    weight
  end
  
  def load_prompts
    load_cluster_options()
    
    super()
  end
  
  def defaults_only?
    false
  end
  
  def add_prompt(opts, prompt, config_obj, prefix = [])
    arguments = ["--#{prompt.get_command_line_argument()} [String]"]
    prompt.get_command_line_aliases().each{
      |a|
      arguments << "--#{a} [String]"
    }
    opts.on(*arguments) {
      |val|
      
      if defaults_only?() && prompt.is_a?(GroupConfigurePromptMember)
        unless prompt.allow_group_default()
          error("The \"--#{prompt.get_command_line_argument()}\" argument is not supported in the `tpm configure defaults` command.")
          next
        end
      end
      
      if (av = prompt.get_command_line_argument_value()) != nil
        if val == nil
          val = av
        end
      end
      
      begin
        validated = prompt.accept?(val)
      rescue => e
        debug(e.message + "\n" + e.backtrace.join("\n"))
        error("Unable to parse \"--#{prompt.get_command_line_argument()}\": #{e.message}")
        next
      end
      
      unless use_external_configuration?()
        if Configurator.instance.is_locked?() && prompt.allow_inplace_upgrade?() == false
          error("Unable to accept \"--#{prompt.get_command_line_argument()}\" in an installed directory.  Try running 'tpm update' from a staging directory.")
          next
        end
      end
      
      if prompt.is_a?(GroupConfigurePromptMember) && prompt.allow_group_default()
        config_obj.setProperty(prefix + [DEFAULTS, prompt.name], validated)
      else
        config_obj.setProperty(prefix + [COMMAND, prompt.name], validated)
      end
    }
  end
  
  def load_cluster_defaults
    @config.props = @config.props.merge(@general_options.getPropertyOr([COMMAND], {}))

    @config.override([DATASERVICES, DEFAULTS], @dataservice_options.getProperty([DATASERVICES, DEFAULTS]))
    @config.override([HOSTS, DEFAULTS], @host_options.getProperty([HOSTS, DEFAULTS]))
    @config.override([MANAGERS, DEFAULTS], @manager_options.getProperty([MANAGERS, DEFAULTS]))
    @config.override([CONNECTORS, DEFAULTS], @connector_options.getProperty([CONNECTORS, DEFAULTS]))
    @config.override([REPL_SERVICES, DEFAULTS], @replication_options.getProperty([REPL_SERVICES, DEFAULTS]))
    
    _load_fixed_properties([HOSTS, DEFAULTS, FIXED_PROPERTY_STRINGS])
    _load_fixed_properties([HOSTS, DEFAULTS, FIXED_PROPERTY_STRINGS], @add_fixed_properties, @remove_fixed_properties)
    _load_skipped_validation_classes([HOSTS, DEFAULTS, SKIPPED_VALIDATION_CLASSES])
    _load_skipped_validation_classes([HOSTS, DEFAULTS, SKIPPED_VALIDATION_CLASSES], @skip_validation_checks, @enable_validation_checks)
    _load_skipped_validation_warnings([HOSTS, DEFAULTS, SKIPPED_VALIDATION_WARNINGS])
    _load_skipped_validation_warnings([HOSTS, DEFAULTS, SKIPPED_VALIDATION_WARNINGS], @skip_validation_warnings, @enable_validation_warnings)
  
    clean_cluster_configuration()
    
    if is_valid?()
      unless use_external_configuration?()
        notice("Configuration defaults updated in #{Configurator.instance.get_config_filename()}")
        save_config_file()
      end
    end
  end
  
  def cluster_options_provided?
    options_provided=false
    [
      @dataservice_options,
      @host_options,
      @manager_options,
      @connector_options,
      @replication_options,
      @add_members,
      @remove_members,
      @add_connectors,
      @remove_connectors,
      fixed_properties(),
      removed_properties(),
      ConfigureValidationHandler.get_skipped_validation_classes(),
      ConfigureValidationHandler.get_enabled_validation_classes(),
      ConfigureValidationHandler.get_skipped_validation_warnings(),
      ConfigureValidationHandler.get_enabled_validation_warnings(),
      @add_fixed_properties,
      @remove_fixed_properties,
      @skip_validation_checks,
      @enable_validation_checks,
      @skip_validation_warnings,
      @enable_validation_warnings,
    ].each{
      |opts|
      
      if opts == nil
        next
      end
      unless opts.empty?()
        options_provided = true
      end
    }
    
    return options_provided
  end
  
  def load_cluster_options(dataservices = nil)
    @config.props = @config.props.merge(@general_options.getPropertyOr([COMMAND], {}))
    
    if cluster_options_provided?() == false
      debug("No options given so skipping load_cluster_options")
      return
    end
    
    include_all_dataservices = false
    if dataservices == nil
      dataservices = command_dataservices()
      if dataservices.empty?()
        include_all_dataservices = true
        dataservices = @config.getPropertyOr([DATASERVICES], {}).keys().delete_if{|v| (v == DEFAULTS)}
        if dataservices.size() == 0
          raise "You must specify a dataservice name after the command or by the --dataservice-name argument"
        end
      end
    end
    
    dataservices.each{
      |dataservice_alias|

      @config.setProperty([DATASERVICES, dataservice_alias, DATASERVICENAME], dataservice_alias)
      @config.override([DATASERVICES, dataservice_alias], @dataservice_options.getPropertyOr([DATASERVICES, DEFAULTS]))
      @config.override([DATASERVICES, dataservice_alias], @dataservice_options.getPropertyOr([DATASERVICES, COMMAND]))
      
      unless @add_members == nil
        current_members = @config.getPropertyOr([DATASERVICES, dataservice_alias, DATASERVICE_MEMBERS], "").split(",")
        updated_members = (current_members + @add_members)
        @config.setProperty([DATASERVICES, dataservice_alias, DATASERVICE_MEMBERS], updated_members.join(","))
      end
      unless @remove_members == nil
        current_members = @config.getPropertyOr([DATASERVICES, dataservice_alias, DATASERVICE_MEMBERS], "").split(",")
        updated_members = current_members.delete_if{
          |h|
          @remove_members.include?(h)
        }
        @config.setProperty([DATASERVICES, dataservice_alias, DATASERVICE_MEMBERS], updated_members.join(","))
      end
      unless @add_connectors == nil
        current_connectors = @config.getPropertyOr([DATASERVICES, dataservice_alias, DATASERVICE_CONNECTORS], "").split(",")
        updated_connectors = (current_connectors + @add_connectors)
        @config.setProperty([DATASERVICES, dataservice_alias, DATASERVICE_CONNECTORS], updated_connectors.join(","))
      end
      unless @remove_connectors == nil
        current_connectors = @config.getPropertyOr([DATASERVICES, dataservice_alias, DATASERVICE_CONNECTORS], "").split(",")
        updated_connectors = current_connectors.delete_if{
          |h|
          @remove_connectors.include?(h)
        }
        @config.setProperty([DATASERVICES, dataservice_alias, DATASERVICE_CONNECTORS], updated_connectors.join(","))
      end
      
      topology = Topology.build(dataservice_alias, @config)
      if @config.getProperty([DATASERVICES, dataservice_alias, DATASERVICE_IS_COMPOSITE]) == "true"
        dataservice_hosts = []
        connector_hosts = []
        
        @config.getProperty([DATASERVICES, dataservice_alias, DATASERVICE_COMPOSITE_DATASOURCES]).to_s().split(",").each{
          |composite_ds_member|
          
          dataservice_hosts = dataservice_hosts + @config.getPropertyOr([DATASERVICES, composite_ds_member, DATASERVICE_MEMBERS], "").split(",")
          connector_hosts = connector_hosts + @config.getPropertyOr([DATASERVICES, composite_ds_member, DATASERVICE_CONNECTORS], "").split(",")
        }
      else
        dataservice_hosts = @config.getPropertyOr([DATASERVICES, dataservice_alias, DATASERVICE_MEMBERS], "").split(",")
        connector_hosts = @config.getPropertyOr([DATASERVICES, dataservice_alias, DATASERVICE_CONNECTORS], "").split(",")
      end
    
      (dataservice_hosts+connector_hosts).uniq().each{
        |host|
        h_alias = to_identifier(host)
        if h_alias == ""
          next
        end

        # Check if we are supposed to define this host
        unless include_host?(h_alias)
          next
        end
      
        @config.setDefault([HOSTS, h_alias, HOST], host)
      }
      
      if include_all_hosts?() || (dataservice_hosts+connector_hosts).uniq().size() == 0
        if @config.getProperty([DATASERVICES, dataservice_alias, DATASERVICE_IS_COMPOSITE]) == "true"
          @config.getProperty([DATASERVICES, dataservice_alias, DATASERVICE_COMPOSITE_DATASOURCES]).to_s().split(",").each{
            |composite_ds_member|
            
            _override_dataservice_component_options(composite_ds_member)
          }
        else
          _override_dataservice_component_options(dataservice_alias)
        end
      else
        (dataservice_hosts+connector_hosts).uniq().each{
          |host|
          h_alias = to_identifier(host)
          hs_alias = dataservice_alias + "_" + h_alias
          if h_alias == ""
            next
          end

          # Check if we are supposed to define this host
          unless include_host?(h_alias)
            next
          end

          @config.override([HOSTS, h_alias], @host_options.getProperty([HOSTS, DEFAULTS]))
          @config.override([HOSTS, h_alias], @host_options.getProperty([HOSTS, COMMAND]))

          if include_all_dataservices == true
            _load_fixed_properties([HOSTS, h_alias, FIXED_PROPERTY_STRINGS])
            _load_fixed_properties([HOSTS, h_alias, FIXED_PROPERTY_STRINGS], @add_fixed_properties, @remove_fixed_properties)
          else
            _load_fixed_properties([REPL_SERVICES, hs_alias, FIXED_PROPERTY_STRINGS])
            _load_fixed_properties([REPL_SERVICES, hs_alias, FIXED_PROPERTY_STRINGS], @add_fixed_properties, @remove_fixed_properties)
          end
          
          _load_skipped_validation_classes([HOSTS, h_alias, SKIPPED_VALIDATION_CLASSES])
          _load_skipped_validation_classes([HOSTS, h_alias, SKIPPED_VALIDATION_CLASSES], @skip_validation_checks, @enable_validation_checks)
          _load_skipped_validation_warnings([HOSTS, h_alias, SKIPPED_VALIDATION_WARNINGS])
          _load_skipped_validation_warnings([HOSTS, h_alias, SKIPPED_VALIDATION_WARNINGS], @skip_validation_warnings, @enable_validation_warnings)
        }
        
        dataservice_hosts.each{
          |host|
          h_alias = to_identifier(host)
          hs_alias = dataservice_alias + "_" + h_alias
          if h_alias == ""
            next
          end

          # Check if we are supposed to define this host
          unless include_host?(h_alias)
            next
          end
        
          enable_replicator_service = true
          if topology.use_management?()
            @config.override([MANAGERS, hs_alias], @manager_options.getProperty([MANAGERS, DEFAULTS]))
            @config.override([MANAGERS, hs_alias], @manager_options.getProperty([MANAGERS, COMMAND]))
            
            if @config.getProperty([MANAGERS, hs_alias, MGR_IS_WITNESS]) == "true"
              enable_replicator_service = false
            end
          end
          if enable_replicator_service == true && topology.use_replicator?()
            @config.override([REPL_SERVICES, hs_alias], @replication_options.getProperty([REPL_SERVICES, DEFAULTS]))
            @config.override([REPL_SERVICES, hs_alias], @replication_options.getProperty([REPL_SERVICES, COMMAND]))
          end
        }
        
        connector_hosts.each{
          |host|
          h_alias = to_identifier(host)
          if h_alias == ""
            next
          end

          # Check if we are supposed to define this host
          unless include_host?(h_alias)
            next
          end
        
          if topology.use_connector?()
            @config.override([CONNECTORS, h_alias], @connector_options.getProperty([CONNECTORS, DEFAULTS]))
            @config.override([CONNECTORS, h_alias], @connector_options.getProperty([CONNECTORS, COMMAND]))
          end
        }
      end
    }
    
    clean_cluster_configuration()
    
    if is_valid?()
      dataservices = command_dataservices()
      if dataservices.empty?()
        dataservices = @config.getPropertyOr([DATASERVICES], {}).keys().delete_if{|v| (v == DEFAULTS)}
      end
      
      unless use_external_configuration?()
        notice("Data service(s) #{dataservices.join(',')} updated in #{Configurator.instance.get_config_filename()}")
        save_config_file()
      end
      
      if Configurator.instance.is_locked?() && use_external_configuration?() != true
        warning("Updating individual hosts may cause an inconsistent configuration file on your staging server.  You should refresh the configuration by running `tools/tpm fetch #{dataservices.join(',')}`.")
      end
    end
  end
  
  def _load_fixed_properties(target_key, add = nil, remove = nil)
    if add == nil
      add = fixed_properties()
    end
    if remove == nil
      remove = removed_properties()
    end
    
    # Parse the current fixed properties into the key and value
    current_properties = []
    (@config.getNestedProperty(target_key) || []).each{
      |val|
      unless val =~ /=/
        raise "Invalid value #{val} given for '--property'.  There should be a key/value pair joined by a single =."
      end
      
      parts = val.split("=")
      prop_key = parts.shift()
      current_properties << {:key => prop_key, :value => parts.join("=")}
    }
    
    # Remove any keys that match
    remove.each{
      |remove_key|
      
      current_properties.delete_if{
        |prop|
        if prop[:key] =~ /^#{remove_key}[+~]?/
          true
        else
          false
        end
      }
    }
    
    # Add new fixed properties being sure to update existing entries
    # instead of adding them to the end
    add.each{
      |val|
      unless val =~ /=/
        raise "Invalid value #{val} given for '--property'.  There should be a key/value pair joined by a single =."
      end
      
      parts = val.split("=")
      prop_key = parts.shift()
      
      is_found=false
      current_properties.each{
        |prop|
        
        if prop[:key] == prop_key
          prop[:value] = parts.join("=")
          is_found = true
        end
      }
      
      if is_found == false
        current_properties << {:key => prop_key, :value => parts.join("=")}
      end
    }
    
    # Rebuild the configuration value using the new list
    if current_properties.size() == 0
      @config.setProperty(target_key, nil)
    else
      @config.setProperty(target_key, current_properties.map{
        |prop|
        "#{prop[:key]}=#{prop[:value]}"
      })
    end
  end
  
  def _load_skipped_validation_classes(target_key, skip = nil, enable = nil)
    if skip == nil
      skip = ConfigureValidationHandler.get_skipped_validation_classes()
    end
    if enable == nil
      enable = ConfigureValidationHandler.get_enabled_validation_classes()
    end
    
    @config.append(target_key, skip)
    
    if enable.size() > 0
      klasses = @config.getNestedProperty(target_key)
      if klasses == nil
        return
      end
      
      enable.each{
        |enable_class|
        
        klasses.delete_if{
          |skip_class|
          if skip_class == enable_class
            true
          else
            false
          end
        }
      }  
      @config.setProperty(target_key, klasses)
    end
  end
  
  def _load_skipped_validation_warnings(target_key, skip = nil, enable = nil)
    if skip == nil
      skip = ConfigureValidationHandler.get_skipped_validation_warnings()
    end
    if enable == nil
      enable = ConfigureValidationHandler.get_enabled_validation_warnings()
    end
    
    @config.append(target_key, skip)
    
    if enable.size() > 0
      klasses = @config.getNestedProperty(target_key)
      if klasses == nil
        return
      end
      
      enable.each{
        |enable_class|
        
        klasses.delete_if{
          |skip_class|
          if skip_class == enable_class
            true
          else
            false
          end
        }
      }  
      @config.setProperty(target_key, klasses)
    end
  end
  
  def _override_dataservice_component_options(dataservice_alias)
    topology = Topology.build(dataservice_alias, @config)
    @config.override([DATASERVICE_HOST_OPTIONS, dataservice_alias], @host_options.getProperty([HOSTS, DEFAULTS]))
    @config.override([DATASERVICE_HOST_OPTIONS, dataservice_alias], @host_options.getProperty([HOSTS, COMMAND]))
    
    _load_fixed_properties([DATASERVICE_HOST_OPTIONS, dataservice_alias, FIXED_PROPERTY_STRINGS])
    _load_fixed_properties([DATASERVICE_HOST_OPTIONS, dataservice_alias, FIXED_PROPERTY_STRINGS], @add_fixed_properties, @remove_fixed_properties)
    _load_skipped_validation_classes([DATASERVICE_HOST_OPTIONS, dataservice_alias, SKIPPED_VALIDATION_CLASSES])
    _load_skipped_validation_classes([DATASERVICE_HOST_OPTIONS, dataservice_alias, SKIPPED_VALIDATION_CLASSES], @skip_validation_checks, @enable_validation_checks)
    _load_skipped_validation_warnings([DATASERVICE_HOST_OPTIONS, dataservice_alias, SKIPPED_VALIDATION_WARNINGS])
    _load_skipped_validation_warnings([DATASERVICE_HOST_OPTIONS, dataservice_alias, SKIPPED_VALIDATION_WARNINGS], @skip_validation_warnings, @enable_validation_warnings)
  
    if topology.use_management?()
      @config.override([DATASERVICE_MANAGER_OPTIONS, dataservice_alias], @manager_options.getProperty([MANAGERS, DEFAULTS]))
      @config.override([DATASERVICE_MANAGER_OPTIONS, dataservice_alias], @manager_options.getProperty([MANAGERS, COMMAND]))
    end
    if topology.use_connector?()
      @config.override([DATASERVICE_CONNECTOR_OPTIONS, dataservice_alias], @connector_options.getProperty([CONNECTORS, DEFAULTS]))
      @config.override([DATASERVICE_CONNECTOR_OPTIONS, dataservice_alias], @connector_options.getProperty([CONNECTORS, COMMAND]))
    end
    if topology.use_replicator?()
      @config.override([DATASERVICE_REPLICATION_OPTIONS, dataservice_alias], @replication_options.getProperty([REPL_SERVICES, DEFAULTS]))
      @config.override([DATASERVICE_REPLICATION_OPTIONS, dataservice_alias], @replication_options.getProperty([REPL_SERVICES, COMMAND]))
    end
  end
  
  def clean_cluster_configuration
    # Clear the SYSTEM hash here so that all classes must regenerate their
    # default values. This ensures the values selected here reflect any changes
    # made.
    @config.setProperty([SYSTEM], nil)
    update_deprecated_keys()
    
    # Reduce the component options to remove values that are the same as their defaults
    [DATASERVICES, HOSTS, MANAGERS, CONNECTORS, REPL_SERVICES].each{
      |group_name|
      
      @config.getPropertyOr([group_name], {}).keys().each{
        |m_alias|
        if m_alias == DEFAULTS
          next
        end

        @config.getPropertyOr([group_name, m_alias], {}).each{
          |key, value|
          if @config.getNestedProperty([group_name, DEFAULTS, key]) == value
            @config.setProperty([group_name, m_alias, key], nil)
          end
        }
      }
    }
    
    # Reduce the data service options to remove values that are the same as their defaults
    @config.getPropertyOr([DATASERVICES], {}).keys().each{
      |ds_alias|
      if ds_alias == DEFAULTS
        next
      end
      topology = Topology.build(ds_alias, @config)
      
      option_groups = {
        DATASERVICE_REPLICATION_OPTIONS => REPL_SERVICES,
        DATASERVICE_CONNECTOR_OPTIONS => CONNECTORS,
        DATASERVICE_MANAGER_OPTIONS => MANAGERS,
        DATASERVICE_HOST_OPTIONS => HOSTS,
      }
      unless topology.use_management?()
        option_groups.delete(DATASERVICE_MANAGER_OPTIONS)
      end
      unless topology.use_connector?()
        option_groups.delete(DATASERVICE_CONNECTOR_OPTIONS)
      end
      unless topology.use_replicator?()
        option_groups.delete(DATASERVICE_REPLICATION_OPTIONS)
      end
      option_groups.each{
        |dso_key, group_name|
        
        dso = @config.getPropertyOr([dso_key, ds_alias], {})
        dso.each{
          |key, value|
          if @config.getNestedProperty([group_name, DEFAULTS, key]) == value
            dso.delete(key)
          end
        }
        @config.setProperty([dso_key, ds_alias], dso)
      }
      
      @config.getPropertyOr([DATASERVICES, ds_alias, DATASERVICE_MEMBERS], "").split(",").each{
        |host|
        h_alias = to_identifier(host)
        hs_alias = ds_alias + "_" + h_alias
        if h_alias == ""
          next
        end
        
        # Check if we are supposed to define this host
        unless include_host?(h_alias)
          next
        end
        
        enable_replicator_service = true
        if topology.use_management?()
          @config.setProperty([MANAGERS, hs_alias, DEPLOYMENT_HOST], h_alias)
          @config.setProperty([MANAGERS, hs_alias, DEPLOYMENT_DATASERVICE], ds_alias)
          
          if @config.getProperty([MANAGERS, hs_alias, MGR_IS_WITNESS]) == "true"
            enable_replicator_service = false
          end
        end
        if enable_replicator_service == true && topology.use_replicator?()
          @config.setProperty([REPL_SERVICES, hs_alias, DEPLOYMENT_HOST], h_alias)
          @config.setProperty([REPL_SERVICES, hs_alias, DEPLOYMENT_DATASERVICE], ds_alias)
        end
      }
      
      @config.getPropertyOr([DATASERVICES, ds_alias, DATASERVICE_CONNECTORS], "").split(",").each{
        |host|
        h_alias = to_identifier(host)
        if h_alias == ""
          next
        end
        
        # Check if we are supposed to define this host
        unless include_host?(h_alias)
          next
        end
        
        if topology.use_connector?()
          @config.setProperty([CONNECTORS, h_alias, DEPLOYMENT_HOST], h_alias)
          @config.append([CONNECTORS, h_alias, DEPLOYMENT_DATASERVICE], ds_alias)
        end
      }
    }
    
    # Remove MANAGERS and REPL_SERVICES entries that do not appear in a data service
    [MANAGERS, REPL_SERVICES].each{
      |group_name|
      
      @config.getPropertyOr(group_name, {}).keys().each{
        |m_alias|
        if m_alias == DEFAULTS
          next
        end

        h_alias = @config.getProperty([group_name, m_alias, DEPLOYMENT_HOST])
        hostname = @config.getProperty([HOSTS, h_alias, HOST])
        ds_alias = @config.getProperty([group_name, m_alias, DEPLOYMENT_DATASERVICE])
        
        unless @config.getPropertyOr([DATASERVICES, ds_alias, DATASERVICE_MEMBERS]).include_alias?(h_alias)
          @config.setProperty([group_name, m_alias], nil)
        end
      }
    }
    
    # Remove REPL_SERVICES entries for active witness hosts
    @config.getPropertyOr(REPL_SERVICES, {}).keys().each{
      |m_alias|
      if m_alias == DEFAULTS
        next
      end
      
      h_alias = @config.getProperty([CONNECTORS, m_alias, DEPLOYMENT_HOST])
      hostname = @config.getProperty([HOSTS, h_alias, HOST])
      
      if @config.getProperty([MANAGERS, m_alias, MGR_IS_WITNESS]) == "true"
        @config.setProperty([REPL_SERVICES, m_alias], nil)
      end
    }
    
    # Remove CONNECTORS entries that do not appear in a data service
    @config.getPropertyOr(CONNECTORS, {}).keys().each{
      |m_alias|
      if m_alias == DEFAULTS
        next
      end
      
      h_alias = @config.getProperty([CONNECTORS, m_alias, DEPLOYMENT_HOST])
      hostname = @config.getProperty([HOSTS, h_alias, HOST])
      
      ds_list = @config.getPropertyOr([CONNECTORS, m_alias, DEPLOYMENT_DATASERVICE], []).delete_if{
        |ds_alias|
        (@config.getPropertyOr([DATASERVICES, ds_alias, DATASERVICE_CONNECTORS]).include_alias?(h_alias) != true)
      }
      
      if ds_list.size > 0
        @config.setProperty([CONNECTORS, m_alias, DEPLOYMENT_DATASERVICE], ds_list)
      else
        @config.setProperty([CONNECTORS, m_alias], nil)
      end
    }
    
    # Remove HOSTS entries that do not appear as a manager, replicator or connector
    @config.getPropertyOr(HOSTS, {}).keys().each{
      |h_alias|
      if h_alias == DEFAULTS
        next
      end
      
      is_found = false
      
      [MANAGERS, REPL_SERVICES, CONNECTORS].each{
        |group_name|
        @config.getPropertyOr(group_name, {}).each_key{
          |m_alias|
          if @config.getProperty([group_name, m_alias, DEPLOYMENT_HOST]) == h_alias
            is_found = true
          end
        }
      }
      
      if is_found == false
        @config.setProperty([HOSTS, h_alias], nil)
      end
    }
    
    # Remove data services from these options containers
    [
      DATASERVICE_REPLICATION_OPTIONS,
      DATASERVICE_CONNECTOR_OPTIONS,
      DATASERVICE_MANAGER_OPTIONS,
      DATASERVICE_HOST_OPTIONS,
    ].each{
      |dso_key|
      
      @config.getPropertyOr(dso_key, {}).keys().each{
        |ds_alias|
        if ds_alias == DEFAULTS
          next
        end
        
        if @config.getNestedProperty([DATASERVICES, ds_alias]) == nil
          @config.setProperty([dso_key, ds_alias], nil)
        end
      }
    }
  end
  
  def include_host_by_dataservice?(h_alias)
    v = false
    
    [
      [MANAGERS],
      [CONNECTORS],
      [REPL_SERVICES]
    ].each{
      |path|
      
      @config.getPropertyOr(path, {}).each_key{
        |p_alias|
        if p_alias == DEFAULTS
          next
        end
        
        if @config.getProperty(path + [p_alias, DEPLOYMENT_HOST]) != h_alias
          next
        end
        
        if include_dataservice?(@config.getProperty(path + [p_alias, DEPLOYMENT_DATASERVICE]))
          v = true
        end
      }
    }
    
    return v
  end
  
  def display_cluster_options
    Configurator.instance.write_divider(Logger::ERROR)
    Configurator.instance.output("Data Service options:")
    
    if display_preview?
      hosts = @config.getProperty(HOSTS)
      unless hosts
        host = nil
      else
        host = hosts.keys.at(0)
      end
    end
    
    each_prompt(nil){
      |prompt|

      prompt.output_usage()
    }
    
    each_prompt(Clusters){
      |prompt|
      if display_preview? && host
        prompt.set_member(host)
      end

      prompt.output_usage()
    }
    
    each_prompt(ClusterHosts){
      |prompt|
      if display_preview? && host
        prompt.set_member(host)
      end

      prompt.output_usage()
    }
    
    each_prompt(Managers){
      |prompt|
      if display_preview? && host
        prompt.set_member(host)
      end

      prompt.output_usage()
    }
    
    each_prompt(Connectors){
      |prompt|
      if display_preview? && host
        prompt.set_member(host)
      end

      prompt.output_usage()
    }
    
    each_prompt(ReplicationServices){
      |prompt|
      if display_preview? && host
        prompt.set_member(host)
      end

      prompt.output_usage()
    }
  end

  def get_cluster_bash_completion_arguments
    args = []
    
    each_prompt(nil){
      |prompt|
      prompt.get_bash_completion_arguments().each{
        |arg|
        args << arg
      }
    }
    
    each_prompt(Clusters){
      |prompt|
      prompt.get_bash_completion_arguments().each{
        |arg|
        args << arg
      }
    }
    
    each_prompt(ClusterHosts){
      |prompt|
      prompt.get_bash_completion_arguments().each{
        |arg|
        args << arg
      }
    }
    
    each_prompt(Managers){
      |prompt|
      prompt.get_bash_completion_arguments().each{
        |arg|
        args << arg
      }
    }
    
    each_prompt(Connectors){
      |prompt|
      prompt.get_bash_completion_arguments().each{
        |arg|
        args << arg
      }
    }
    
    each_prompt(ReplicationServices){
      |prompt|
      prompt.get_bash_completion_arguments().each{
        |arg|
        args << arg
      }
    }
    
    args
  end
  
  def use_prompt?(prompt)
    prompt.enabled_for_command_line?()
  end
  
  def each_prompt(klass = nil, &block)
    if klass == nil
      get_prompts().each{
        |prompt|
        
        prompt.set_config(@config)
        exec_prompt(prompt, block)
      }
    else
      ch = klass.new()
      ch.set_config(@config)
      ch.each_prompt{
        |prompt|
        
        exec_prompt(prompt, block)
      }
    end
  end
  
  def exec_prompt(prompt, block)
    if use_prompt?(prompt)
      begin
        block.call(prompt)
      rescue => e
        error(e.message)
      end
    end
  end
  
  # This should return false if you do not want to use any remote package
  # Added to support 'tpm update --replace-release'
  def get_default_remote_package_path
    nil
  end
  
  def get_deployment_configuration(host_alias, config_obj)
    unless include_host?(host_alias)
      return nil
    end
    
    config_obj.setProperty(DEPLOYMENT_HOST, host_alias)
    
    unless Configurator.instance.is_locked?()
      remote_package_path = get_default_remote_package_path()
      # A value of false means that we shouldn't use any remote package. We
      # should skip this check and move on
      unless remote_package_path == false
        [config_obj.getProperty(CURRENT_RELEASE_DIRECTORY), Configurator.instance.get_base_path()].each{|path|      
          begin
            Timeout.timeout(5){        
              val = ssh_result("if [ -f #{path}/tools/tpm ]; then #{path}/tools/tpm query version; else echo \"\"; fi", config_obj.getProperty(HOST), config_obj.getProperty(USERID))
              if val == Configurator.instance.get_release_version()
                remote_package_path = path
                
                # Print a notice if we found a remote path to use since
                # the user may not know it is there and the behavior could
                # cause unexpected behavior.
                if path == Configurator.instance.get_base_path()
                  host = config_obj.getProperty(HOST)
                  
                  Configurator.instance.warning("Using #{path} package on #{host} for local installation")
                end
              end
            }
          rescue Timeout::Error
          rescue RemoteCommandError
          rescue CommandError
          rescue MessageError
          end
        
          if remote_package_path != nil
            break
          end
        }
      
        if remote_package_path
          config_obj.setDefault(REMOTE_PACKAGE_PATH, remote_package_path)
        end
      end
      
      if config_obj.getProperty(REMOTE_PACKAGE_PATH) == config_obj.getProperty(CURRENT_RELEASE_DIRECTORY)
        current_full_path=ssh_result("readlink #{config_obj.getProperty(REMOTE_PACKAGE_PATH)}", config_obj.getProperty(HOST), config_obj.getProperty(USERID))
        config_obj.setProperty([SYSTEM, CONFIG_TARGET_BASENAME], File.basename(current_full_path))
        config_obj.setProperty([CONFIG_TARGET_BASENAME], File.basename(current_full_path))
      else
        config_obj.setProperty([SYSTEM, CONFIG_TARGET_BASENAME], config_obj.getProperty(CONFIG_TARGET_BASENAME))
        config_obj.setProperty([CONFIG_TARGET_BASENAME], config_obj.getProperty(CONFIG_TARGET_BASENAME))
      end
      
      config_obj.setProperty(STAGING_HOST, Configurator.instance.hostname())
      config_obj.setProperty(STAGING_USER, Configurator.instance.whoami())
      config_obj.setProperty(STAGING_DIRECTORY, Configurator.instance.get_base_path())
    end

    [
      [HOSTS]
    ].each{
      |path|
      
      config_obj.getPropertyOr(path, {}).delete_if{
        |h_alias, h_props|

        (h_alias != DEFAULTS && h_alias != config_obj.getProperty([DEPLOYMENT_HOST]))
      }
    }
    
    ds_list = []
    [
      [MANAGERS],
      [CONNECTORS],
      [REPL_SERVICES]
    ].each{
      |path|
      
      config_obj.getPropertyOr(path, {}).delete_if{
        |g_alias, g_props|
        drop = true
        
        if g_alias == DEFAULTS
          drop = false
        end
        
        if g_props[DEPLOYMENT_HOST] == config_obj.getNestedProperty([DEPLOYMENT_HOST])
          Array(config_obj.getProperty(path + [g_alias, DEPLOYMENT_DATASERVICE])).each{
            |ds_alias|
            topology = Topology.build(ds_alias, config_obj)
          
            if topology.enabled?()
              drop = false
              ds_list << ds_alias
            end
          }
        end
        
        drop
      }
    }
    ds_list.uniq!()
    
    # Are any of the data services for this host to be deployed?
    found_included_dataservice = false
    ds_list.each{
      |ds_alias|
      if include_dataservice?(ds_alias)
        found_included_dataservice = true
      end
    }
    if found_included_dataservice == false
      return nil
    end
    
    config_obj.getPropertyOr(DATASERVICES, {}).keys().each{
      |ds_alias|
      
      if ds_alias == DEFAULTS
        next
      end
      
      if config_obj.getProperty([DATASERVICES, ds_alias, DATASERVICEALIAS]) != nil
        config_obj.setProperty([DATASERVICES, ds_alias, DATASERVICENAME], config_obj.getProperty([DATASERVICES, ds_alias, DATASERVICEALIAS]))
      end
      
      if config_obj.getPropertyOr([DATASERVICES, ds_alias, DATASERVICE_IS_COMPOSITE]) == "true"
        comp_ds_list = config_obj.getPropertyOr([DATASERVICES, ds_alias, DATASERVICE_COMPOSITE_DATASOURCES], "").split(",")
        
        ds_list.each{
          |ds|
          
          if comp_ds_list.include?(ds)
            ds_list << ds_alias
            ds_list = ds_list + comp_ds_list
          end
        }
      end
    }
    ds_list.uniq!()
    
    ds_list.each{
      |ds_alias|
      config_obj.getPropertyOr([DATASERVICES, ds_alias, DATASERVICE_RELAY_SOURCE], "").split(",").each{
        |rds_alias|
        unless ds_list.include?(rds_alias)
          ds_list << rds_alias
          
          # If the relay source is a composite service, add the
          # composite datasources to the list
          if @config.getProperty([DATASERVICES, rds_alias, DATASERVICE_IS_COMPOSITE]) == "true"
            @config.getProperty([DATASERVICES, rds_alias, DATASERVICE_COMPOSITE_DATASOURCES]).split(",").each{
              |composite_relay_alias|
              ds_list << composite_relay_alias
            }
          end
        end
      }
      config_obj.getPropertyOr([DATASERVICES, ds_alias, TARGET_DATASERVICE], "").split(",").each{
        |tds_alias|
        unless ds_list.include?(tds_alias)
          ds_list << tds_alias
        end
      }
      config_obj.getPropertyOr([DATASERVICES, ds_alias, DATASERVICE_MASTER_SERVICES], "").split(",").each{
        |mds_alias|
        unless ds_list.include?(mds_alias)
          ds_list << mds_alias
        end
      }
      config_obj.getPropertyOr([DATASERVICES, ds_alias, DATASERVICE_HUB_SERVICE], "").split(",").each{
        |hds_alias|
        unless ds_list.include?(hds_alias)
          ds_list << hds_alias
        end
      }
    }
    
    config_obj.getPropertyOr(DATASERVICES, {}).keys().each{
      |ds_alias|
      
      if ds_alias == DEFAULTS
        next
      end
      
      if ds_list.include?(ds_alias)
        next
      end
        
      config_obj.setProperty([DATASERVICES, ds_alias], nil)
    }
    
    # Remove data services from these options containers
    [
      DATASERVICE_REPLICATION_OPTIONS,
      DATASERVICE_CONNECTOR_OPTIONS,
      DATASERVICE_MANAGER_OPTIONS,
      DATASERVICE_HOST_OPTIONS,
    ].each{
      |dso_key|
      
      config_obj.getPropertyOr(dso_key, {}).keys().each{
        |ds_alias|
        if ds_alias == DEFAULTS
          next
        end
        
        if config_obj.getNestedProperty([DATASERVICES, ds_alias]) == nil
          config_obj.setProperty([dso_key, ds_alias], nil)
        end
      }
    }
    
    [
      [HOSTS],
      [MANAGERS],
      [CONNECTORS],
      [REPL_SERVICES],
      [DATASERVICES]
    ].each{
      |path|
      
      config_obj.getPropertyOr([SYSTEM] + path, {}).delete_if{
        |g_alias, g_props|

        (config_obj.getNestedPropertyOr(path, {}).has_key?(g_alias) != true)
      }
    }
    
    if !(Configurator.instance.is_localhost?(config_obj.getProperty([HOSTS, host_alias, HOST])))
      if config_obj.getProperty([HOSTS, host_alias, REPL_MYSQL_CONNECTOR_PATH])
        config_obj.setProperty([HOSTS, host_alias, GLOBAL_REPL_MYSQL_CONNECTOR_PATH], config_obj.getProperty([HOSTS, host_alias, REPL_MYSQL_CONNECTOR_PATH]))
        config_obj.setProperty([HOSTS, host_alias, REPL_MYSQL_CONNECTOR_PATH], "#{config_obj.getProperty([HOSTS, host_alias, TEMP_DIRECTORY])}/#{config_obj.getProperty(CONFIG_TARGET_BASENAME)}/#{File.basename(config_obj.getProperty([HOSTS, host_alias, REPL_MYSQL_CONNECTOR_PATH]))}")
      end
      
      DeploymentFiles.prompts.each{
        |p|
        config_obj.getPropertyOr(p[:group], {}).keys().each{
          |g_alias|
          if g_alias == DEFAULTS
            next
          end
          
          if config_obj.getProperty([p[:group], g_alias, p[:local]]) != nil
            config_obj.setProperty([p[:group], g_alias, p[:global]], config_obj.getProperty([p[:group], g_alias, p[:local]]))
            config_obj.setProperty([p[:group], g_alias, p[:local]], "#{config_obj.getProperty(TEMP_DIRECTORY)}/#{config_obj.getProperty(CONFIG_TARGET_BASENAME)}/#{File.basename(config_obj.getProperty([p[:group], g_alias, p[:local]]))}")
          end
        }
      }
    end
    
    return config_obj
  end
  
  def build_topologies(config)
    config.getPropertyOr(DATASERVICES, {}).keys().each{
      |ds_alias|
      topology = Topology.build(ds_alias, config)
      topology.build_services()
    }
  end
  
  def output_cluster_completion_text
    if skip_deployment?
      return
    end
    
    output = get_cluster_completion_text()
    if output == ""
      return
    end
    
    write("", Logger::NOTICE)
    write_header("Next Steps", Logger::NOTICE)
    write output, Logger::NOTICE 
  end
  
  def get_cluster_completion_text
    if include_all_hosts?()
      hosts_arg = ""
    else
      hosts_arg = "--hosts=#{command_hosts().join(',')}"
    end
    
    output = []
    
    has_replicator = false
    display_promote_connectors = false
    display_start = true
    root = "$CONTINUENT_ROOT"
    get_deployment_configurations().each{
      |cfg|
      
      c_key = cfg.getProperty(DEPLOYMENT_CONFIGURATION_KEY)
      h_alias = cfg.getProperty(DEPLOYMENT_HOST)

      if cfg.getProperty(HOST_ENABLE_REPLICATOR) == "true"
        has_replicator = true
      end

      if cfg.getProperty(HOST_ENABLE_CONNECTOR) == "true"
        if @promotion_settings.getProperty([c_key, RESTART_CONNECTORS]) == false
          if @promotion_settings.getProperty([c_key, RESTART_CONNECTORS_NEEDED]) == true
            display_promote_connectors = true
          end
        end
      
        if @promotion_settings.getProperty([c_key, ACTIVE_DIRECTORY_PATH]) && @promotion_settings.getProperty([c_key, CONNECTOR_ENABLED]) == "true"
          unless @promotion_settings.getProperty([c_key, CONNECTOR_IS_RUNNING]) == "true"
            display_promote_connectors = true
          end
        elsif cfg.getProperty(SVC_START) != "true"
          display_promote_connectors = true
        end
      end
      
      if cfg.getProperty(SVC_START) == "true" || cfg.getProperty(SVC_REPORT) == "true"
        display_start = false
      end
      
      root = cfg.getProperty(HOME_DIRECTORY)
    }
    if display_promote_connectors == true && Configurator.instance.command.is_a?(UpdateCommand)
      str = <<OUT
The connectors are not running the latest version.  In order to complete 
the process you must promote the connectors.

  #{root}/tungsten/tools/tpm promote-connector #{command_dataservices().join(',')}
OUT
      output << str
    end
    
    if display_start == true
      str = <<OUT
Unless automatically started, you must start the Tungsten services before the 
cluster will be available.

  #{root}/tungsten/cluster-home/bin/startall

Wait a minute for the services to start up and configure themselves.  After 
that you may proceed.
OUT
      output << str
    end
    
    display_profile_info = true
    get_deployment_configurations().each{
      |cfg|
      if display_profile_info == true &&
          cfg.getProperty(PROFILE_SCRIPT) != "" &&
          Configurator.instance.is_localhost?(cfg.getProperty(HOST)) && 
          Configurator.instance.whoami == cfg.getProperty(USERID)
          
        str = <<OUT
We have added Tungsten environment variables to #{cfg.getProperty(PROFILE_SCRIPT)}.
Run `source #{cfg.getProperty(PROFILE_SCRIPT)}` to rebuild your environment.
OUT
        output << str
        
        display_profile_info = false
      end
    }
    
    if has_replicator == true
      if Configurator.instance.is_enterprise?()
        str = <<OUT
Once your services start successfully you may begin to use the cluster.
To look at services and perform administration, run the following command
from any database server.

  #{root}/tungsten/tungsten-manager/bin/cctrl

Configuration is now complete.  For further information, please consult
Tungsten documentation, which is available at docs.continuent.com.
OUT
        output << str
      else
        str = <<OUT
Once your services start successfully replication will begin.
To look at services and perform administration, run the following command
from any database server.

  #{root}/tungsten/tungsten-replicator/bin/trepctl services

Configuration is now complete.  For further information, please consult
Tungsten documentation, which is available at docs.continuent.com.
OUT
        output << str
      end
    end

    return output.join("\n")
  end
end

module ClusterConfigurationsModule
  def build_deployment_configurations
    config_objs = super()

    # If this is a configured directory we will only have a single host 
    # configuration.  Traverse the dataservices to get the other hostnames.
    # Then use that to load the configuration for each host.  Only hosts that 
    # are accessible
    if Configurator.instance.is_locked?()
      threads = []
      mtx = Mutex.new
      additional_hosts = []
    
      config_objs.each{
        |config_obj|
        config_obj.getPropertyOr([DATASERVICES], {}).each_key{
          |ds_alias|
          additional_hosts = additional_hosts + config_obj.getPropertyOr([DATASERVICES, ds_alias, DATASERVICE_MEMBERS]).split(',')
          additional_hosts = additional_hosts + config_obj.getPropertyOr([DATASERVICES, ds_alias, DATASERVICE_CONNECTORS]).split(',')
        }
      }
      additional_hosts.uniq!()

      Configurator.instance.debug("Load configurations from #{@config.getProperty(CURRENT_RELEASE_DIRECTORY)} on #{additional_hosts.join(',')}")
      begin
        additional_hosts.each{
          |host|
          if host == @config.getProperty(HOST)
            next
          end

          threads << Thread.new(host) {
            |host|

            begin
              contents = nil
              Timeout.timeout(15) {
                contents = ssh_result("if [ -f #{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tools/tpm ]; then #{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tools/tpm query config; else echo \"\"; fi", host, @config.getProperty(USERID))
              }
              
              result = result = JSON.parse(contents)
              if result.instance_of?(Hash)
                config_obj = Properties.new()
                config_obj.import(result)

                mtx.synchronize do
                  config_objs << config_obj
                end
              end
            rescue JSON::ParserError
            rescue CommandError
            rescue RemoteCommandError
            rescue MessageError => me
              Configurator.instance.warning(me.message)
            rescue Timeout::Error
            end
          }
        }
        threads.each{|t| t.join() }
      end
    end
  
    return config_objs
  end
end