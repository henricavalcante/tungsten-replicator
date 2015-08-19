module ConfigureDeploymentCore
  include ConfigureMessages
  include EventHandler
  
  def initialize(config)
    super()
    @config = config
    @additional_properties = Properties.new()
    @additional_properties.use_prompt_handler = false
    @services = []
    @methods = nil
  end
  
  def prepare(modules)
    @methods = {}
    
    modules.each{
      |module_def|
      self.extend(module_def)

      begin
        begin
          module_def.get_methods.each{
            |dm|
            unless @methods.has_key?(dm.class.name)
              @methods[dm.class.name] = {}
            end
            unless @methods[dm.class.name].has_key?(dm.group_id)
              @methods[dm.class.name][dm.group_id] = []
            end
            @methods[dm.class.name][dm.group_id] << dm
          }
        rescue NoMethodError
        end
        
        begin
          module_def.bind_listeners(self, @config)
        rescue NoMethodError
        end
      rescue => e
        exception(e)
      end
    }
  end
  
  def run(class_name, run_group_id = nil, additional_properties = nil)
    if additional_properties
      @additional_properties.import(additional_properties)
      @additional_properties.setProperty(DEPLOYMENT_HOST, @config.getProperty(DEPLOYMENT_HOST))
    else
      @additional_properties.reset()
    end
    
    debug("Run methods configured for #{class_name}:#{run_group_id}")
    if @methods == nil
      raise "You must call prepare before calling run"
    end
    
    begin
      trigger_event("before_#{class_name}")
      
      if run_group_id == nil
        get_group_ids(class_name).each{
          |group_id|
          run_group_methods(@methods[class_name][group_id])
        }
      else
        if @methods[class_name].has_key?(run_group_id)
          run_group_methods(@methods[class_name][run_group_id])
        end
      end
      
      trigger_event("after_#{class_name}")
    rescue => e
      exception(e)
    end
    
    return get_remote_result()
  end
  
  def run_group_methods(group_methods)
    group_methods.each{
      |method|
      trigger_event("before_deployment_step_method", method.method_name)
      self.send(alter_deployment_method_name(method.method_name))
      trigger_event("after_deployment_step_method", method.method_name)
    }
  end
  
  def log_group_methods(class_name = nil)
    @methods.each{
      |method_class_name, method_class_groups|
      if (class_name != nil && method_class_name != class_name)
        next
      end
      
      method_class_groups.keys().sort().each{
        |group_id|
        @methods[method_class_name][group_id].sort!{|a,b| a.weight <=> b.weight}

        debug("Deployment Methods for #{method_class_name}:#{group_id}")
        @methods[method_class_name][group_id].each{
          |group_method|
          debug("    #{group_method.method_name}")
        }
      }
    }
  end
  
  def get_object_methods(class_name )
    if @methods.has_key?(class_name)
      return @methods[class_name]
    else
      return []
    end
  end
  
  def get_group_ids(class_name = nil)
    if class_name
      if @methods.has_key?(class_name)
        return @methods[class_name].keys().sort()
      else
        return []
      end
    else
      group_ids = []
      @methods.each{
        |class_name,group_methods|
        group_methods.keys().each{
          |group_id|
          group_ids << group_id
        }
      }
      
      return group_ids.uniq().sort()
    end
  end
  
  def get_deployment_basedir
    @config.getProperty(PREPARE_DIRECTORY)
  end
  
  def alter_deployment_method_name(method_name)
    method_name
  end
  
  # Create a directory if it is absent. 
  def mkdir_if_absent(dirname)
    if dirname == nil
      return
    end
    
    if File.exists?(dirname)
      if File.directory?(dirname)
        debug("Found directory, no need to create: #{dirname}")
        
        unless File.writable?(dirname)
          raise "Directory already exists but is not writable: #{dirname}"
        end
      else
        raise "Directory already exists as a file: #{dirname}"
      end
    else
      debug("Creating missing directory: #{dirname}")
      FileUtils.mkdir_p(dirname)
    end
  end
  
  def get_root_prefix()
    prefix = @config.getProperty(ROOT_PREFIX)
    if prefix == "true" or prefix == "sudo"
      return "sudo -n"
    else
      return ""
    end
  end
  
  def svc_is_running?(cmd)
    Configurator.instance.svc_is_running?(cmd)
  end
  
  # Update the RUN_AS_USER in a service script.
  def set_run_as_user(script)
    host_transformer(script) {
      |t|
      t.timestamp?(false)
      t.set_template(script)

      # Have to be careful to set first RUN_AS_USER= only or it
      # corrupts the start script.
      t.transform_lines { |line|
        if line =~ /^#RUN_AS_USER=/ then
          "RUN_AS_USER=" + @config.getProperty(USERID)
        else
          line
        end
      }
    }
  end
  
  def get_svc_command(boot_script)
    prefix = get_root_prefix()
    if prefix == ""
      return boot_script
    else
      return prefix + " " + boot_script
    end
  end
  
  # Generate a cluster service properties file for a system service.
  def write_svc_properties(name, applier)
    dataservice = @config.getProperty(DATASERVICENAME)
    props_name = name + ".properties"
    svc_properties_dir = "#{get_deployment_basedir()}/cluster-home/conf/cluster/" + dataservice + "/service/"
    svc_properties = svc_properties_dir + "/" + props_name

    # Ensure services properties directory exists.
    mkdir_if_absent(svc_properties_dir)

    # Create service properties file.
    out = File.open(svc_properties, "w")
    out.puts "# #{props_name}"
    out.puts "name=#{name}"
    
    {
      REPL_DBSERVICE_START => "start",
      REPL_DBSERVICE_STOP => "stop",
      REPL_DBSERVICE_RESTART => "restart",
      REPL_DBSERVICE_STATUS => "status"
    }.each{
      |subcommand_key, subcommand|
      
      key = applier.get_applier_key(subcommand_key)
      shell_command = @config.getProperty(key)
      out.puts "command.#{subcommand}=#{get_svc_command(shell_command)}"
    }
    
    out.puts "# AUTO-CONFIGURED: #{DateTime.now}"
    out.chmod(0755)
    out.close
    info "GENERATED FILE: " + svc_properties
    WatchFiles.watch_file(svc_properties, @config)
  end
  
  # Add an OS service that needs to be started and/or deployed.
  def add_service(start_script)
    @services.insert(-1, start_script)
  end
  
  def add_log_file(log_file)
    basename = File.basename(log_file)
    unless File.exists?("#{@config.getProperty(LOGS_DIRECTORY)}/#{basename}")
      FileUtils.ln_sf("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/#{log_file}", "#{@config.getProperty(LOGS_DIRECTORY)}/#{basename}")
    end
  end
  
  def get_host_alias
    @config.getProperty(DEPLOYMENT_HOST)
  end
  
  def get_host_key(key)
    [HOSTS, @config.getProperty(DEPLOYMENT_HOST), key]
  end
  
  def get_additional_property_key(key)
    [@config.getProperty(DEPLOYMENT_CONFIGURATION_KEY), key]
  end
  
  def get_additional_property(key)
    @additional_properties.getProperty(get_additional_property_key(key))
  end
  
  def get_message_hostname
    @config.getProperty(DEPLOYMENT_HOST)
  end
  
  def get_message_host_key
    @config.getProperty([DEPLOYMENT_CONFIGURATION_KEY])
  end
	
	def transform_host_values(matches)
	  case matches.at(0)
    when "HOST"
      v = @config.getTemplateValue(get_host_key(Kernel.const_get(matches[1])))
    else
      v = @config.getTemplateValue(matches.map{
        |match|
        Kernel.const_get(match)
      })
    end
    
    return v
	end
	
	def transform_service_values(matches)
	  case matches.at(0)
    when "APPLIER"
      v = @config.getTemplateValue(get_service_key(Kernel.const_get(matches[1])))
    when "EXTRACTOR"
      v = @config.getTemplateValue(get_service_key(Kernel.const_get("EXTRACTOR_" + matches[1])))
    when "SERVICE"
      v = @config.getTemplateValue(get_service_key(Kernel.const_get(matches[1])))
    when "HOST"
      v = @config.getTemplateValue(get_host_key(Kernel.const_get(matches[1])))
    else
      v = @config.getTemplateValue(matches.map{
        |match|
        Kernel.const_get(match)
      })
    end
    
    return v
	end
	
	def is_manager?
    (@config.getProperty(HOST_ENABLE_MANAGER) == "true")
  end
  
  def is_connector?
    (@config.getProperty(HOST_ENABLE_CONNECTOR) == "true")
  end
  
  def is_replicator?
    (@config.getProperty(HOST_ENABLE_REPLICATOR) == "true")
  end
  
  def manager_is_running?
    Configurator.instance.svc_is_running?("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-manager/bin/manager")
  end
  
  def replicator_is_running?
    Configurator.instance.svc_is_running?("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-replicator/bin/replicator")
  end
  
  def connector_is_running?
    Configurator.instance.svc_is_running?("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-connector/bin/connector")
  end
  
  def set_maintenance_policy
    if is_manager?() && manager_is_running?() && get_additional_property(IS_COMMAND_COORDINATOR) == true
      cmd_result("echo 'set policy maintenance' | #{get_cctrl_cmd()}")
    end
  end
  
  def set_automatic_policy
    if is_manager?() && manager_is_running?() && get_additional_property(IS_COMMAND_COORDINATOR) == true
      cmd_result("echo 'set policy automatic' | #{get_cctrl_cmd()}")
    end
  end
  
  def set_original_policy
    if is_manager?() && manager_is_running?() && get_additional_property(IS_COMMAND_COORDINATOR) == true
      cmd_result("echo 'set policy #{get_additional_property(MANAGER_POLICY)}' | #{get_cctrl_cmd()}")
    end
  end
  
  def wait_for_manager(require_all_members = nil)
    active_members = nil
    error_message = nil
    
    if is_manager?() && manager_is_running?()
      if require_all_members == nil
        if @config.getProperty(MGR_WAIT_FOR_MEMBERS) == "true"
          require_all_members = true
        else
          require_all_members = false
        end
      end
      
      begin
        Timeout.timeout(120) {
          while true
            begin
              active_members = cmd_result("echo 'members' | #{get_cctrl_cmd()}")
              break
            rescue CommandError
            end
          end
        }
      rescue Timeout::Error
        raise "Unable to connect to the manager to confirm successful start"
      end
      
      unless require_all_members == true
        return
      end
      
      begin
        Timeout.timeout(60) {
          while true
            begin
              missing_members = []
              is_valid = true
              
              ds_alias = @config.getProperty(DEPLOYMENT_DATASERVICE)
              if ds_alias
                topology = Topology::build(ds_alias, @config)
                unless topology.use_management?()
                  break
                end
                
                members = @config.getProperty([DATASERVICES, ds_alias, DATASERVICE_MEMBERS]).to_s().split(",")
              
                members.each{
                  |member|
                
                  debug("Check the active members list for #{member}")
                  ds_output = cmd_result("echo 'ls #{member}' | #{get_cctrl_cmd()}")
                  unless ds_output =~ /MANAGER/
                    missing_members << member

                    is_valid = false
                    error_message = "Unable to find #{missing_members.join(",")} in cctrl"
                  end
                }
              end

              if is_valid == true
                break
              end
            rescue CommandError
            end
          end
        }
      rescue Timeout::Error
        if error_message != nil
          raise error_message
        else
          raise "There was an issue verifying the availability of all datasources"
        end
      end
    end
  end
  
  def stop_services
    info("Stopping all services")
    info(cmd_result("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/cluster-home/bin/stopall", true))
  end
  
  def start_services
    info("Starting all services")
    info(cmd_result("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/cluster-home/bin/startall", true))
  end
  
  def report_services(level = Logger::NOTICE)
    if Configurator.instance.is_enterprise?()
      if is_manager?() && manager_is_running?()
        write("Getting cluster status on #{@config.getProperty(HOST)}", level)
        write(cmd_result("echo 'ls' | #{get_cctrl_cmd()}"), level)
      end
    else
      if is_replicator?() && replicator_is_running?()
        write("Getting replication status on #{@config.getProperty(HOST)}", level)
        write(cmd_result("#{get_trepctl_cmd()} services"), level)
      end
    end
  end
  
  def start_manager
    if is_manager?() && get_additional_property(MANAGER_IS_RUNNING) == "true" && manager_is_running?() != true
      info("Starting the manager")
      info(cmd_result("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-manager/bin/manager start"))
    end
  end
  
  def start_replication_services(offline = false)
    if is_manager?() && get_additional_property(MANAGER_IS_RUNNING) == "true" && manager_is_running?() != true
      info("Starting the manager")
      info(cmd_result("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-manager/bin/manager start"))
    end

    if is_replicator?() && get_additional_property(REPLICATOR_IS_RUNNING) == "true" && replicator_is_running?() != true
      if offline == true
        start_command = "start offline"
      else
        start_command = "start"
      end
      
      info("Starting the replicator")
      info(cmd_result("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-replicator/bin/replicator #{start_command}"))
    end
  end
  
  def start_replication_services_offline
    start_replication_services(true)
  end
  
  def reset_replication_services
    if is_replicator?() && get_additional_property(REPLICATOR_IS_RUNNING) == "true"
      @config.getPropertyOr([REPL_SERVICES], {}).each_key{
        |rs_alias|
        if rs_alias == DEFAULTS
          next
        end

        svc = @config.getProperty([REPL_SERVICES, rs_alias, DEPLOYMENT_SERVICE])
        ds = get_applier_datasource(rs_alias)
        if ds.applier_supports_reset?()
          info("Reset the #{svc} replication service")
          cmd_result("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-replicator/bin/trepctl -service #{svc} offline")
          cmd_result("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-replicator/bin/trepctl -service #{svc} reset -all -y")
          cmd_result("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-replicator/bin/trepctl -service #{svc} online")
        else
          warning("Unable to reset the #{svc} replication service. It will be left OFFLINE.")
        end
      }
    end
  end
  
  def online_replication_services
    if is_replicator?() && replicator_is_running?() == true
      @config.getPropertyOr([REPL_SERVICES], {}).each_key{
        |rs_alias|
        if rs_alias == DEFAULTS
          next
        end

        svc = @config.getProperty([REPL_SERVICES, rs_alias, DEPLOYMENT_SERVICE])
        status = JSON.parse(cmd_result("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-replicator/bin/trepctl -service #{svc} status -json"))
        if status["state"] != "ONLINE"
          info("Put the #{svc} replication service online")
          cmd_result("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-replicator/bin/trepctl -service #{svc} online")
        end
      }
    end
  end
  
  def stop_manager
    if is_manager?() && get_additional_property(MANAGER_IS_RUNNING) == "true" && get_additional_property(RESTART_MANAGERS) != false
      info("Stopping the manager")
      info(cmd_result("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-manager/bin/manager stop"))
    end
  end
  
  def stop_replication_services
    if is_manager?() && get_additional_property(MANAGER_IS_RUNNING) == "true" && get_additional_property(RESTART_MANAGERS) != false
      info("Stopping the manager")
      info(cmd_result("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-manager/bin/manager stop"))
    end
  
    if is_replicator?() && get_additional_property(REPLICATOR_IS_RUNNING) == "true" && get_additional_property(RESTART_REPLICATORS) != false
      if replicator_is_running?()
        # Attempt to put all replication services cleanly offline. This section
        # is wrapped so we can continue even if there are issues during the
        # offline process.
        info("Putting all replication services offline")
        begin
          # Determine if this is before or after integrating Tungsten Replicator 2.0
          version = cmd_result("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-replicator/bin/trepctl version")
          if version =~ /Tungsten Enterprise/
            # This version only supports a single replication service so the
            # `trepctl offline` command is sufficient
            begin
              Timeout::timeout(30) {
                cmd_result("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-replicator/bin/trepctl offline")
              }
            rescue Timeout::Error
              debug("There was a timeout while waiting for the replication service to go offline")
            end
          elsif version =~ /Continuent Tungsten/ || version =~ /Tungsten Replicator/
            # This version may support multiple services. We need to get the
            # list of services and then call `trepctl -service <svc> offline`
            # for each of them
            services = cmd_result("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-replicator/bin/trepctl services | grep serviceName | awk -F \":\" '{print $2}' | tr -d \" \"")
            services.split("\n").each {
              |svc_name|
              begin
                Timeout::timeout(30) {
                  cmd_result("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-replicator/bin/trepctl -service #{svc_name} offline")
                }
              rescue Timeout::Error
                debug("There was a timeout while waiting for the #{svc_name} replication service to go offline")
              end
            }
          end
        rescue CommandError
          notice("Unable to put the replicator cleanly offline. Proceeding with `replicator stop`")
        end
      end
      
      info("Stopping the replicator")
      info(cmd_result("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-replicator/bin/replicator stop"))
    end
  end
  
  def clear_dynamic_properties
    if is_replicator?()
      Dir.glob("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-replicator/conf/dynamic-*.properties") {
        |replicator_dynamic_properties|
        FileUtils.rm_f(replicator_dynamic_properties)
      }
      FileUtils.rm_f("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-manager/conf/dynamic.properties")      
      
      Dir.glob("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/cluster-home/conf/cluster/*").each{
        |ds_name|
        debug("Remove all files in #{ds_name}")
        if File.exist?("#{ds_name}/datasource")
          FileUtils.rm_rf(Dir.glob("#{ds_name}/datasource/*"))
        end
      }
    end
  end
  
  def create_tungsten_schema(rs_alias, schema_name = nil)
    applier = get_applier_datasource(rs_alias)
    applier.create_tungsten_schema(schema_name)
  end
  
  def get_cctrl_cmd
    Configurator.instance.get_cctrl_path(@config.getProperty(CURRENT_RELEASE_DIRECTORY), @config.getProperty(MGR_RMI_PORT))
  end
  
  def get_applier_datasource(rs_alias = nil)
	  if rs_alias == nil
	    rs_alias = @config.getProperty(DEPLOYMENT_SERVICE)
	  end
	  
	  ConfigureDatabasePlatform.build([REPL_SERVICES, rs_alias], @config)
  end
  
  def get_extractor_datasource(rs_alias = nil)
    if rs_alias == nil
	    rs_alias = @config.getProperty(DEPLOYMENT_SERVICE)
	  end
	  
	  if @config.getProperty([REPL_SERVICES, rs_alias, REPL_ROLE]) == REPL_ROLE_DI
	    ConfigureDatabasePlatform.build([REPL_SERVICES, rs_alias], @config, true)
	  else
      get_applier_datasource(rs_alias)
    end
  end
  
  def get_service_key(key)
    svc = @config.getProperty(DEPLOYMENT_SERVICE)
    if svc == nil
      raise MessageError.new("Unable to find a service key for #{Configurator.instance.get_constant_symbol(key)}")
    end
    
    [REPL_SERVICES, svc, key]
  end
  
  def get_applier_key(key)
    get_service_key(key)
  end
  
  def get_extractor_key(key)
    get_service_key(key)
  end
  
  def get_trepctl_cmd
    "#{get_deployment_basedir()}/tungsten-replicator/bin/trepctl -port #{@config.getProperty(REPL_RMI_PORT)}"
  end
  
  def get_thl_cmd
    "#{get_deployment_basedir()}/tungsten-replicator/bin/thl"
  end
  
  def transform_host_template(path, template)
    host_transformer(path) {
      |t|
      t.set_template(template)
    }
  end
  
  def host_transformer(path = nil, &block)
    if path != nil
      path = File.expand_path(path, get_deployment_basedir())
    end
    t = Transformer.new(@config, path)
    t.set_transform_values_method(method(:transform_host_values))
    t.set_fixed_properties(@config.getProperty(get_host_key(FIXED_PROPERTY_STRINGS)))
    
    if block
      block.call(t)
      t.output()
    else
      return t
    end
  end
  
  def transform_service_template(path, template)
    service_transformer(path) {
      |t|
      t.set_template(template)
    }
  end
  
  def service_transformer(path = nil, &block)
    if path != nil
      path = File.expand_path(path, get_deployment_basedir())
    end
    t = Transformer.new(@config, path)
    t.set_transform_values_method(method(:transform_service_values))
    t.set_fixed_properties(@config.getProperty(get_service_key(FIXED_PROPERTY_STRINGS)))
    
    if block
      block.call(t)
      t.output()
    else
      return t
    end
  end
  
  def initiate_composite_dataservices
    prepare_dir = get_deployment_basedir()
    
    @config.getPropertyOr(DATASERVICES, {}).keys().each{
      |comp_ds_alias|

      if comp_ds_alias == DEFAULTS
        next
      end

      if @config.getProperty([DATASERVICES, comp_ds_alias, DATASERVICE_IS_COMPOSITE]) == "false"
        next
      end

      unless include_dataservice?(comp_ds_alias)
        next
      end

      mkdir_if_absent("#{prepare_dir}/cluster-home/conf/cluster/#{comp_ds_alias}/service")
      mkdir_if_absent("#{prepare_dir}/cluster-home/conf/cluster/#{comp_ds_alias}/datasource")
      mkdir_if_absent("#{prepare_dir}/cluster-home/conf/cluster/#{comp_ds_alias}/extension")

      @config.getProperty([DATASERVICES, comp_ds_alias, DATASERVICE_COMPOSITE_DATASOURCES]).to_s().split(",").each{
        |ds_alias|
        
        path = "#{prepare_dir}/cluster-home/conf/cluster/#{comp_ds_alias}/datasource/#{ds_alias}.properties"
        unless File.exist?(path)
          if @config.getProperty([DATASERVICES, ds_alias, DATASERVICE_RELAY_SOURCE]).to_s() != ""
            ds_role = "slave"
          else
            ds_role = "master"
          end
          
          File.open(path, "w") {
            |f|
            f.puts "
appliedLatency=-1.0
precedence=1
name=#{ds_alias}
state=OFFLINE
url=jdbc\:t-router\://#{ds_alias}/${DBNAME}
alertMessage=
isAvailable=true
role=#{ds_role}
isComposite=true
alertStatus=OK
alertTime=#{Time.now().strftime("%s000")}
dataServiceName=#{comp_ds_alias}
vendor=continuent
driver=com.continuent.tungsten.router.jdbc.TSRDriver
host=#{ds_alias}"
          }
        end
      }
    }
  end
end
