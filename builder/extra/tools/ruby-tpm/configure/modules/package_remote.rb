module RemoteCommand
  def initialize(config)
    super(config)
    @load_remote_config = false
    @build_members_list = false
  end
  
  def require_remote_config?
    false
  end
  
  def loaded_remote_config?
    @load_remote_config
  end
  
  def parsed_options?(arguments)
    arguments = super(arguments)
    
    if display_help?() && !display_preview?()
      return arguments
    end
    
    deployment_host = @config.getNestedProperty([DEPLOYMENT_HOST])
    if deployment_host.to_s == ""
      deployment_host = DEFAULTS
    end
    
    if has_command_hosts?()
      target_hosts = command_hosts()
    elsif deployment_host == DEFAULTS
      target_hosts = [Configurator.instance.hostname]
    else
      target_hosts = [@config.getProperty([HOSTS, deployment_host, HOST])]
    end
    target_user = @config.getProperty([HOSTS, deployment_host, USERID])
    target_home_directory = @config.getProperty([HOSTS, deployment_host, CURRENT_RELEASE_DIRECTORY])
    default_host = nil
    @override_differences = false
    
    opts=OptionParser.new
    opts.on("--reset") { 
      @config.props = {}
    }
    opts.on("--build-members-list")             { @build_members_list = true }
    opts.on("--default-host String")            { |val|
      if target_hosts.include?(val)
        default_host = val
        @override_differences = true
      else
        error("Unable to find the default host (#{val}) in the list of hosts to be included")
      end
    }
    opts.on("--user String")                    { |val| 
      target_user = val }
    opts.on("--release-directory String", "--directory String")       { |val| 
      @load_remote_config = true
      target_home_directory = val }
    
    arguments = Configurator.instance.run_option_parser(opts, arguments)
    
    unless is_valid?()
      return arguments
    end

    if @load_remote_config == true
      load_remote_config(target_hosts, target_user, target_home_directory, default_host)
    elsif require_remote_config?()
      error "You must provide --user, --hosts and --directory to run the #{self.class.get_command_name()} command"
    end
    
    arguments
  end
  
  def load_remote_config(target_hosts, target_user, target_home_directory, default_host=nil)
    host_configs = {}
    autodetect_hosts = false
    
    target_hosts.each{
      |target_host|
      if default_host == nil
        default_host = target_host
      end
      
      if target_host == "autodetect"
        if default_host == nil
          error("You must specify a hostname before autodetect in the --hosts argument")
        else
          unless Configurator.instance.is_real_hostname?(target_host)
            autodetect_hosts = true
            next
          end
        end
      end

      _add_host_config(target_host, target_user, target_home_directory.dup(), host_configs)
    }
    
    if autodetect_hosts == true
      target_hosts.delete_if{|h| h == "autodetect"}
      
      additional_hosts = []
      host_configs.each_value{
        |config_obj|
        
        if config_obj == false
          next
        end
        
        if config_obj.has_key?(DATASERVICES)
          config_obj[DATASERVICES].each_key{
            |ds_alias|
            additional_hosts = additional_hosts + config_obj[DATASERVICES][ds_alias][DATASERVICE_MASTER_MEMBER].to_s().split(',')
            additional_hosts = additional_hosts + config_obj[DATASERVICES][ds_alias][DATASERVICE_SLAVES].to_s().split(',')
            additional_hosts = additional_hosts + config_obj[DATASERVICES][ds_alias][DATASERVICE_MEMBERS].to_s().split(',')
            additional_hosts = additional_hosts + config_obj[DATASERVICES][ds_alias][DATASERVICE_CONNECTORS].to_s().split(',')
          }
        end
      }
      additional_hosts.uniq!()
      additional_hosts.delete_if{
        |host|
        (host_configs.has_key?(host) == true)
      }
      additional_hosts.each{
        |host|
        target_hosts << host
        _add_host_config(host, target_user, target_home_directory.dup(), host_configs)
      }
    end
    
    unless is_valid?()
      return false
    end
    
    if host_configs.size() == 0
      if has_command_hosts?()
        Configurator.instance.error("No host configurations were found. Verify the value for --hosts, --user and --directory to load configuration information from other servers.")
      else
        Configurator.instance.error("No host configurations were found. Try specifying --hosts, --user and --directory to load configuration information from other servers.")
      end
      return false
    end
    
    if @build_members_list
      build_members_lists(host_configs)
    end
    
    sections_to_merge = [
      DATASERVICES,
      HOSTS,
      MANAGERS,
      CONNECTORS,
      REPL_SERVICES,
      DATASERVICE_HOST_OPTIONS,
      DATASERVICE_MANAGER_OPTIONS,
      DATASERVICE_CONNECTOR_OPTIONS,
      DATASERVICE_REPLICATION_OPTIONS
    ]
    
    final_props = {}
    
    # Initialize the properties to import based on the first host in the list
    sections_to_merge.each{
      |key|
      
      if host_configs[default_host].has_key?(key)
        final_props[key] = host_configs[default_host][key]
        
        # Make sure we have a DEFAULTS value to catch differences with 
        # any possible defaults on other hosts
        unless final_props[key].has_key?(DEFAULTS)
          final_props[key][DEFAULTS] = {}
        end
      else
        final_props[key] = {}
        final_props[key][DEFAULTS] = {}
      end
    }
    
    has_differences = false
    host_configs.each{
      |target_host, host_config_props|
      if target_host == default_host
        next
      end
      
      sections_to_merge.each{
        |key|

        if host_config_props.has_key?(key)
          host_config_props[key].each{
            |g_alias,g_props|
            if final_props[key].has_key?(g_alias)
              # If it already exists in final_props, we need to make sure 
              # there is a match in the values
              unless final_props[key][g_alias] == g_props
                unless @override_differences == true
                  error "The values for #{key}.#{g_alias} do not match between #{target_host} and the other hosts.  Make sure these values match in the #{Configurator::HOST_CONFIG} file on each host."
                  has_differences = true
                end
              end
            else # Add it to final_props because it doesn't exist there yet
              final_props[key][g_alias] = g_props
            end
          }
        end
      }
    }
    
    if has_differences
      error "Try adding --default-host=<host> to resolve any configuration file differences with the settings from that host"
    end

    # Remove the data services we are about to import from the stored config
    if final_props.has_key?(DATASERVICES)
      final_props[DATASERVICES].each_key{
        |ds_alias|
        @config.setProperty([DATASERVICES, ds_alias], nil)
      }
    end
    clean_cluster_configuration()

    # Import the configuration information
    sections_to_merge.each{
      |key|
      unless final_props.has_key?(key)
        next
      end
      final_props[key].each{
        |g_alias,g_props|
        
        if g_alias == DEFAULTS
          # Remove any default values that do not match defaults in the 
          # current configuration
          g_props = g_props.delete_if{
            |d_key,d_value|
            (@config.getNestedProperty([key, DEFAULTS, d_key]) == d_value)
          }
          if g_props.size() > 0
            # Store the remaining default values by including them in the 
            # values for these specific data services
            case key
            when HOSTS
              override_key = DATASERVICE_HOST_OPTIONS
            when MANAGERS
              override_key = DATASERVICE_MANAGER_OPTIONS
            when CONNECTORS
              override_key = DATASERVICE_CONNECTOR_OPTIONS
            when REPL_SERVICES
              override_key = DATASERVICE_REPLICATION_OPTIONS
            else
              # No defaults for the DATASERVICE_ groups
              # The DATASERVICES defaults values will be handled below
              override_key = nil
            end
            
            if override_key != nil
              final_props[DATASERVICES].each_key{
                |ds_alias|
                
                if ds_alias == DEFAULTS
                  next
                end
                
                if @config.getProperty([DATASERVICES, ds_alias, DATASERVICE_IS_COMPOSITE]) == "true"
                  next
                end
                
                @config.include([override_key, ds_alias], g_props)
              }
            end
          end
        else
          @config.override([key, g_alias], g_props)
        end
      }
    }
    
    # Include dataservice defaults in the final configuration
    # These are skipped by the case statement above
    if final_props[DATASERVICES].has_key?(DEFAULTS)
      final_props[DATASERVICES].each_key{
        |ds_alias|

        if ds_alias == DEFAULTS
          next
        end

        if @config.getProperty([DATASERVICES, ds_alias, DATASERVICE_IS_COMPOSITE]) == "true"
          next
        end

        @config.include([DATASERVICES, ds_alias], final_props[DATASERVICES][DEFAULTS])
      }
    end
    
    clean_cluster_configuration()
    update_deprecated_keys()
    
    if is_valid?()
      @load_remote_config = true
      command_hosts(target_hosts.join(','))
      notice("Configuration loaded from #{target_hosts.join(',')}")
    end
  end
  
  def _add_host_config(target_host, target_user, target_home_directory, host_configs)
    begin
      host_configs[target_host] = false
      target_home_directory = validate_home_directory(target_home_directory, target_host, target_user)

      info "Load the current config from #{target_user}@#{target_host}:#{target_home_directory}"
      
      migrate_old_configuration = false
      begin
        if ssh_result("#{target_home_directory}/tools/tpm query dataservices", target_host, target_user) == ""
          migrate_old_configuration = true
        else
          command = "#{target_home_directory}/tools/tpm query config"    
          config_output = ssh_result(command, target_host, target_user)
          parsed_contents = JSON.parse(config_output)
          unless parsed_contents.instance_of?(Hash)
            raise MessageError.new("Unable to read the configuration file from #{target_user}@#{target_host}:#{target_home_directory}")
          end
        end
      rescue CommandError => ce
        migrate_old_configuration = true
      end
      
      if migrate_old_configuration == true
        parent_dir = File.dirname(target_home_directory)
        # This is an indication that tpm may not have been used for installation
        if ssh_result("if [ -f #{parent_dir}/configs/tungsten.cfg ]; then echo 0; else echo 1; fi", target_host, target_user) == "0"
          config_output = ssh_result("cat #{parent_dir}/configs/tungsten.cfg | egrep -v '^#'", target_host, target_user)
          parsed_contents = JSON.parse(config_output)
          unless parsed_contents.instance_of?(Hash)
            raise MessageError.new("Unable to read the configuration file from #{target_user}@#{target_host}:#{target_home_directory}")
          end
          
          migrate_tungsten_installer_configuration(parsed_contents)
          @build_members_list = true
        end
      end
      
      if parsed_contents == nil
        raise MessageError.new("Unable to find configuration for #{target_user}@#{target_host}:#{target_home_directory}")
      else
        host_configs[target_host] = parsed_contents.dup
      end
    rescue JSON::ParserError
      error "Unable to parse the configuration file from #{target_user}@#{target_host}:#{target_home_directory}"
    rescue MessageError => me
      exception(me)
    rescue => e
      exception(e)
      error "Unable to load the current config from #{target_user}@#{target_host}:#{target_home_directory}"
    end
  end
  
  def output_command_usage
    super()
    
    output_usage_line("--user", "The system user that Tungsten runs as")
    output_usage_line("--directory", "The directory to look in for the Tungsten installation")
    output_usage_line("--default-host", "Use the information from this host to resolve any configuration differences")
  end
  
  def get_bash_completion_arguments
    super() + ["--user", "--release-directory", "--directory", "--default-host", "--hosts"]
  end
  
  def validate_home_directory(target_home_directory, target_host, target_user)
    if target_home_directory == "autodetect"
      matching_wrappers = ssh_result("ps aux | grep 'Tungsten [Replicator|Manager|Connector]' | grep -v grep | awk '{print $11}'", target_host, target_user)
      matching_wrappers.each_line{
        |wrapper_path|
        target_home_directory = wrapper_path[0, wrapper_path.rindex("releases")] + "tungsten/"
        break
      }
    end
    
    if ssh_result("if [ -d #{target_home_directory} ]; then if [ -f #{target_home_directory}/#{DIRECTORY_LOCK_FILENAME} ]; then echo 0; else echo 1; fi else echo 1; fi", target_host, target_user) == "0"
      return target_home_directory
    else
      unless target_home_directory =~ /tungsten\/tungsten[\/]?$/
        target_home_directory = target_home_directory + "/tungsten"
        if ssh_result("if [ -d #{target_home_directory} ]; then if [ -f #{target_home_directory}/#{DIRECTORY_LOCK_FILENAME} ]; then echo 0; else echo 1; fi else echo 1; fi", target_host, target_user) == "0"
          return target_home_directory
        end
      end
    end
    
    unless ssh_result("if [ -d #{target_home_directory} ]; then echo 0; else echo 1; fi", target_host, target_user) == "0"
      raise "Unable to find a Tungsten directory at #{target_home_directory}.  Try running with --directory=autodetect."
    else
      raise "Unable to find #{target_home_directory}/#{DIRECTORY_LOCK_FILENAME}.  Try running with --directory=autodetect."
    end
  end
  
  def migrate_tungsten_installer_configuration(cfg)
    cfg[DATASERVICES] = {}
    if cfg.has_key?(REPL_SERVICES)
      cfg[REPL_SERVICES].keys().each{
        |rs_alias|
        if rs_alias == DEFAULTS
          next
        end

        # Determine the proper host alias to use for this replication service
        h_alias = cfg[REPL_SERVICES][rs_alias][DEPLOYMENT_HOST]
        if h_alias == nil
          h_alias = cfg[DEPLOYMENT_HOST]
        end

        dataservice = cfg[REPL_SERVICES][rs_alias][DEPLOYMENT_SERVICE]
        role = cfg[REPL_SERVICES][rs_alias][REPL_ROLE]

        # Start the hash that will be come the dataservice properties
        ds_props = {
          DATASERVICENAME => dataservice,
          DATASERVICE_MEMBERS => cfg[HOSTS][h_alias][HOST]
        }
        
        if role == REPL_ROLE_M
          ds_props[DATASERVICE_MASTER_MEMBER] = cfg[HOSTS][h_alias][HOST]
          cfg[REPL_SERVICES][rs_alias].delete(REPL_ROLE)
        elsif role == REPL_ROLE_S
          ds_props[DATASERVICE_MASTER_MEMBER] = cfg[REPL_SERVICES][rs_alias][REPL_MASTERHOST]
          cfg[REPL_SERVICES][rs_alias].delete(REPL_ROLE)
        elsif role == REPL_ROLE_DI
          ds_alias = cfg[REPL_SERVICES][rs_alias][REPL_MASTER_DATASOURCE]
          ds_props[DATASERVICE_TOPOLOGY] = "direct"
          ds_props[DATASERVICE_MASTER_MEMBER] = cfg[DATASOURCES][ds_alias][REPL_DBHOST]
          cfg[REPL_SERVICES][rs_alias].delete(REPL_ROLE)
        end
        cfg[DATASERVICES][to_identifier(dataservice)] = ds_props
        
        # Merge all datasource properties into the replication service
        ds_alias = cfg[REPL_SERVICES][rs_alias][REPL_DATASOURCE]
        if ds_alias.to_s != ""
          if cfg[DATASOURCES].has_key?(ds_alias)
            cfg[REPL_SERVICES][rs_alias] =
              cfg[REPL_SERVICES][rs_alias].merge(cfg[DATASOURCES][ds_alias])
            cfg[REPL_SERVICES][rs_alias].delete(REPL_DATASOURCE)
          else
            raise "Unable to find a datasource definition for #{ds_alias} in the #{rs_alias} service"
          end
        end
        
        # Merge extractor properties for direct replication into the 
        # replication service
        ds_alias = cfg[REPL_SERVICES][rs_alias][REPL_MASTER_DATASOURCE]
        if ds_alias.to_s != ""
          if cfg[DATASOURCES].has_key?(ds_alias)
            {
              REPL_DBTYPE => EXTRACTOR_REPL_DBTYPE,
              REPL_DBPORT => EXTRACTOR_REPL_DBPORT,
              REPL_DBLOGIN => EXTRACTOR_REPL_DBLOGIN,
              REPL_DBPASSWORD => EXTRACTOR_REPL_DBPASSWORD,
              REPL_MASTER_LOGDIR => EXTRACTOR_REPL_MASTER_LOGDIR,
              REPL_MASTER_LOGPATTERN => EXTRACTOR_REPL_MASTER_LOGPATTERN,
              REPL_DISABLE_RELAY_LOGS => EXTRACTOR_REPL_DISABLE_RELAY_LOGS,
              REPL_ORACLE_SERVICE => EXTRACTOR_REPL_ORACLE_SERVICE,
              REPL_ORACLE_SCAN => EXTRACTOR_REPL_ORACLE_SCAN
            }.each{
              |src,tgt|
              if cfg[DATASOURCES][ds_alias].has_key?(src)
                cfg[REPL_SERVICES][rs_alias][tgt] = cfg[DATASOURCES][ds_alias][src]
              end
            }
            cfg[REPL_SERVICES][rs_alias].delete(REPL_MASTER_DATASOURCE)
          else
            raise "Unable to find a master datasource definition for #{ds_alias} in the #{rs_alias} service"
          end
        end
        
        cfg[REPL_SERVICES][rs_alias][DEPLOYMENT_HOST] = to_identifier(h_alias)
        # Remove unsupported replication service properties
        cfg[REPL_SERVICES][rs_alias].delete(DEPLOYMENT_SERVICE)
        cfg[REPL_SERVICES][rs_alias].delete(REPL_SVC_START)
        cfg[REPL_SERVICES][rs_alias].delete(REPL_SVC_REPORT)
        
        # This replication service must end up with this alias
        new_rs_alias = to_identifier("#{dataservice}_#{cfg[HOSTS][h_alias][HOST]}")
        if rs_alias != new_rs_alias
          cfg[REPL_SERVICES][new_rs_alias] = cfg[REPL_SERVICES][rs_alias].dup()
          cfg[REPL_SERVICES].delete(rs_alias)
        end
      }
      cfg.delete(DATASOURCES)
      
      cfg[HOSTS].keys().each{
        |h_alias|
        if h_alias != to_identifier(cfg[HOSTS][h_alias][HOST])
          cfg[HOSTS][to_identifier(cfg[HOSTS][h_alias][HOST])] = cfg[HOSTS][h_alias].dup()
          cfg[HOSTS].delete(h_alias)
        end
      }
    end
  end
  
  def build_members_lists(cfgs)
    ds_members = {}
    
    cfgs.each{
      |host,cfg|
      
      unless cfg.has_key?(DATASERVICES)
        next
      end
      
      cfg[DATASERVICES].each_key{
        |ds_alias|
        if ds_alias == DEFAULTS
          next
        end
      
        unless ds_members.has_key?(ds_alias)
          ds_members[ds_alias] = []
        end
        
        ds_members[ds_alias] = ds_members[ds_alias] +
          cfg[DATASERVICES][ds_alias][DATASERVICE_MEMBERS].to_s().split(",")
      }
    }
    
    cfgs.each{
      |host,cfg|
      
      unless cfg.has_key?(DATASERVICES)
        next
      end
      
      ds_members.each{
        |ds_alias,members|
        
        if cfg[DATASERVICES].has_key?(ds_alias)
          cfg[DATASERVICES][ds_alias][DATASERVICE_MEMBERS] = members.join(",")
        end
      }
    }
  end
end