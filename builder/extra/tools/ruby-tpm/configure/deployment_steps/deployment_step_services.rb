module ConfigureDeploymentStepServices
  def get_methods
    [
      ConfigureCommitmentMethod.new("set_maintenance_policy", ConfigureDeploymentStepMethod::FIRST_GROUP_ID, ConfigureDeploymentStepMethod::FIRST_STEP_WEIGHT),
      ConfigureCommitmentMethod.new("stop_disabled_services", -1, 0),
      ConfigureCommitmentMethod.new("stop_replication_services", -1, 1),
      ConfigureDeploymentMethod.new("apply_config_services", 0, ConfigureDeploymentStepMethod::FINAL_STEP_WEIGHT),
      ConfigureCommitmentMethod.new("update_metadata", 1, 0),
      ConfigureCommitmentMethod.new("deploy_services", 1, 1),
      ConfigureCommitmentMethod.new("start_replication_services_unless_provisioning", 2, ConfigureDeploymentStepMethod::FINAL_STEP_WEIGHT-1, ConfigureDeploymentStepParallelization::BY_SERVICE),
      ConfigureCommitmentMethod.new("wait_for_manager", 2, ConfigureDeploymentStepMethod::FINAL_STEP_WEIGHT, ConfigureDeploymentStepParallelization::BY_SERVICE),
      ConfigureCommitmentMethod.new("start_connector", 4, 1, ConfigureDeploymentStepParallelization::NONE),
      ConfigureCommitmentMethod.new("set_original_policy", 5, 0),
      ConfigureCommitmentMethod.new("provision_server", 5, 1),
      ConfigureCommitmentMethod.new("report_services", ConfigureDeploymentStepMethod::FINAL_GROUP_ID, ConfigureDeploymentStepMethod::FINAL_STEP_WEIGHT-1, ConfigureDeploymentStepParallelization::NONE),
      ConfigureCommitmentMethod.new("check_ping", ConfigureDeploymentStepMethod::FINAL_GROUP_ID, ConfigureDeploymentStepMethod::FINAL_STEP_WEIGHT)
    ]
  end
  module_function :get_methods
  
  # Set up files and perform other configuration for services.
  def apply_config_services
    Configurator.instance.write_header "Performing services configuration"

    config_wrapper()
  end
  
  # Stop services that are running but have been disabled by
  # a configuration update
  def stop_disabled_services
    # If this value is empty there is no previous configuration
    if get_additional_property(ACTIVE_DIRECTORY_PATH) == nil
      return
    end
    
    unless is_connector?()
      _disable_component(get_additional_property(ACTIVE_DIRECTORY_PATH), "connector")
    end
    
    unless is_replicator?()
      _disable_component(get_additional_property(ACTIVE_DIRECTORY_PATH), "replicator")
    end
    
    unless is_manager?()
      _disable_component(get_additional_property(ACTIVE_DIRECTORY_PATH), "manager")
    end
  end
  
  def _disable_component(root, name)
    cmd = "#{root}/tungsten-#{name}/bin/#{name}"
    if Configurator.instance.svc_is_running?(cmd)
      cmd_result(cmd + " stop", true)
    end
    
    wrapper = "#{root}/tungsten-#{name}/conf/wrapper.conf"
    if File.exist?(wrapper)
      FileUtils.rm_f(wrapper)
    end
  end
  
  def deploy_services
    if get_additional_property(ACTIVE_DIRECTORY_PATH)
      return
    end
    
    unless @config.getProperty(SVC_INSTALL) == "true"
      return
    end
    
    info("Installing services")
    begin
      installed = cmd_result("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/cluster-home/bin/deployall")
      info(installed)
    rescue CommandError => e
      warning("Unable to install the Tungsten services for start on system boot.  This may occur if the services have already been installed.")
      warning("The message returned was: #{e.errors}")
    end
  end
  
  def start_connector
    # We have been told not to restart the connector
    if get_additional_property(RESTART_CONNECTORS) == false
      debug("Don't restart the connector service")
      return
    end
    
    unless is_connector?() == true
      return
    end
    
    if get_additional_property(ACTIVE_DIRECTORY_PATH) && get_additional_property(CONNECTOR_ENABLED) == "true"
      if get_additional_property(CONNECTOR_IS_RUNNING) == "true"
        if get_additional_property(ACTIVE_DIRECTORY_PATH) == File.readlink(@config.getProperty(CURRENT_RELEASE_DIRECTORY))
          if get_additional_property(RECONFIGURE_CONNECTORS_ALLOWED) == true
            # We are updating the existing directory so `connector reconfigure` can be used
            info("Tell the connector to update its configuration")
            begin
              Timeout.timeout(10) {
                info(cmd_result("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-connector/bin/connector reconfigure"))
              }
            rescue Timeout::Error
              # The reconfigure command took too long so we must stop and start the connector
              begin
                info(cmd_result("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-connector/bin/connector graceful-stop 30"))
              ensure
                sleep(1)
                info(cmd_result("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-connector/bin/connector start"))
              end
            end
          else
            # Stop the connector and start it again so that the full configuration, including ports, are reloaded
            begin
              info(cmd_result("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-connector/bin/connector graceful-stop 30"))
            ensure
              sleep(1)
              info(cmd_result("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-connector/bin/connector start"))
            end
          end
        else
          # We are switching to a new active directory. Start the new connector 
          # so it goes into a loop trying to bind the connector port
          info("Starting the new connector")
          info(cmd_result("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-connector/bin/connector start"))
          
          # Attempt to use the graceful-stop command but fail back to a regular stop
          info("Stopping the old connector")
          begin
            info(cmd_result("#{get_additional_property(ACTIVE_DIRECTORY_PATH)}/tungsten-connector/bin/connector graceful-stop 30"))
          rescue CommandError => ce
            if ce.result =~ /Usage:/
              debug("The running version doesn't support graceful-stop")
              # The running version of the connector doesn't support graceful-stop
              info(cmd_result("#{get_additional_property(ACTIVE_DIRECTORY_PATH)}/tungsten-connector/bin/connector stop"))
            else
              # The graceful-stop command failed for another reason so we should bubble that up
              raise ce
            end
          end
        end
      end
    elsif @config.getProperty(SVC_START) == "true"
      info("Starting the connector")
      info(cmd_result("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-connector/bin/connector start"))
    end
  end
  
  def report_services
    if @config.getProperty(SVC_REPORT) == "true"
      super(nil)
    end
  end
  
  def check_ping(level = Logger::NOTICE)
    if @config.getProperty(SVC_REPORT) == "true" && is_manager?() && manager_is_running?()
      cmd_result("echo 'ping' | #{get_cctrl_cmd()}").scan(/HOST ([a-zA-Z0-9\-\.]+)\/[0-9\.]+: NOT REACHABLE/).each{
        |match|
        warning("Unable to ping the host on #{match[0]}")
      }
    end
    
    if is_replicator?() && replicator_is_running?()
      begin
        error_lines = cmd_result("#{get_trepctl_cmd()} services | grep ERROR | wc -l")
        if error_lines.to_i() > 0
          error("At least one replication service has experienced an error")
        end
      rescue CommandError
        error("Unable to check if the replication services are working properly")
      end
    end
  end
  
  def config_wrapper
    # Patch for Ubuntu 64-bit start-up problem.
    if Configurator.instance.distro?() == OS_DISTRO_DEBIAN && Configurator.instance.arch?() == OS_ARCH_64
      wrapper_file = "#{get_deployment_basedir()}/cluster-home/bin/wrapper-linux-x86-32"
      if File.exist?(wrapper_file)
        FileUtils.rm("#{get_deployment_basedir()}/cluster-home/bin/wrapper-linux-x86-32")
      end
    end
  end
  
  def write_startall
    # Create startall script.
    script = "#{get_deployment_basedir()}/cluster-home/bin/startall"
    out = File.open(script, "w")
    out.puts "#!/bin/bash"
    out.puts "# Start all services using local service scripts"
    out.puts "THOME=`dirname $0`/../.."
    out.puts "cd $THOME"
    @services.sort.reverse.each { |svc| out.puts svc + " start" }
    out.puts "# AUTO-CONFIGURED: #{DateTime.now}"
    out.chmod(0755)
    out.close
    info "GENERATED FILE: " + script
  end

  def write_stopall
    # Create stopall script.
    script = "#{get_deployment_basedir()}/cluster-home/bin/stopall"
    out = File.open(script, "w")
    out.puts "#!/bin/bash"
    out.puts "# Stop all services using local service scripts"
    out.puts "THOME=`dirname $0`/../.."
    out.puts "cd $THOME"
    @services.sort.each { |svc| out.puts svc + " stop" }
    out.puts "# AUTO-CONFIGURED: #{DateTime.now}"
    out.chmod(0755)
    out.close
    info "GENERATED FILE: " + script
  end

  def write_deployall
    # Create deployall script.
    if Configurator.instance.can_install_services_on_os?()
      script = "#{get_deployment_basedir()}/cluster-home/bin/deployall"
      out = File.open(script, "w")
      out.puts "#!/bin/bash"
      out.puts "# Install services into /etc directories"
      out.puts "THOME=`dirname $0`/../.."
      out.puts "cd $THOME"
      priority=80
      @services.each { |svc|
        svcname = File.basename svc
        out.puts get_svc_command("ln -fs $PWD/" + svc + " /etc/init.d/t" + svcname)
        if Configurator.instance.distro?() == OS_DISTRO_REDHAT
          out.puts get_svc_command("/sbin/chkconfig --add t" + svcname)
        elsif Configurator.instance.distro?() == OS_DISTRO_DEBIAN
          out.puts get_svc_command("update-rc.d t" + svcname + " defaults  #{priority}")
          priority=priority+1
        end
      }
      out.puts "# AUTO-CONFIGURED: #{DateTime.now}"
      out.chmod(0755)
      out.close
      info "GENERATED FILE: " + script
    end
  end

  def write_undeployall
    # Create undeployall script.
    if Configurator.instance.can_install_services_on_os?()
      script = "#{get_deployment_basedir()}/cluster-home/bin/undeployall"
      out = File.open(script, "w")
      out.puts "#!/bin/bash"
      out.puts "# Remove services from /etc directories"
      out.puts "THOME=`dirname $0`/../.."
      out.puts "cd $THOME"
      @services.each { |svc|
        svcname = File.basename svc
        if Configurator.instance.distro?() == OS_DISTRO_REDHAT
          out.puts get_svc_command("/sbin/chkconfig --del t" + svcname)
          out.puts get_svc_command("rm -f /etc/init.d/t" + svcname)
        elsif Configurator.instance.distro?() == OS_DISTRO_DEBIAN
          out.puts get_svc_command("rm -f /etc/init.d/t" + svcname)
          out.puts get_svc_command("update-rc.d -f  t" + svcname + " remove")
        end
      }
      out.puts "# AUTO-CONFIGURED: #{DateTime.now}"
      out.chmod(0755)
      out.close
      info "GENERATED FILE: " + script
    end
  end
  
  def stop_replication_services
    unless get_additional_property(ACTIVE_DIRECTORY_PATH)
      return
    end
    
    super()
  end
  
  def start_replication_services(offline = false)
    if get_additional_property(ACTIVE_DIRECTORY_PATH) && get_additional_property(REPLICATOR_ENABLED) == "true"
      super(offline)
    elsif @config.getProperty(SVC_START) == "true"
      if is_manager?() && manager_is_running?() != true
        info("Starting the manager")
        info(cmd_result("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-manager/bin/manager start"))
      end

      if is_replicator?() && replicator_is_running?() != true
        if offline == true
          start_command = "start offline"
        else
          start_command = "start"
        end
        
        info("Starting the replicator")
        info(cmd_result("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-replicator/bin/replicator #{start_command}"))
      end
    end
  end
  
  def start_replication_services_unless_provisioning
    unless provision_server?()
      start_replication_services()
    end
  end
  
  def update_metadata
    if get_additional_property(ACTIVE_VERSION) =~ /^1.5.[0-9][\-0-9]+$/
      upgrade_from_1_5()
    else
      if is_replicator?() && get_additional_property(ACTIVE_DIRECTORY_PATH).to_s() != ""
        current_data = get_additional_property(ACTIVE_DIRECTORY_PATH) + "/tungsten-replicator/data"
        if File.exist?(current_data)
          target_data = @config.getProperty(REPL_METADATA_DIRECTORY)
          if File.exist?(target_data)
            Configurator.instance.warning("Position information was found at #{current_data} and #{target_data}. The information at #{target_data} will be kept unchanged.")
          else
            cmd_result("mv #{current_data} #{target_data}")
          end
        end
        
        if Configurator.instance.is_enterprise?
          @config.getProperty([REPL_SERVICES]).keys().each{
            |rs_alias|
            if rs_alias == DEFAULTS
              next
            end

            begin
              key = "repl_services.#{rs_alias}.repl_svc_schema"
              
              schema = @config.getProperty(key)
              result_json = cmd_result("#{get_additional_property(ACTIVE_DIRECTORY_PATH)}/tools/tpm query values #{key}", true)
              result = JSON.parse(result_json)
              
              if result[key] != schema
                # Migrate information to the new schema
                Configurator.instance.debug("Migrate metadata from #{result[key]} to #{schema}")
                
                f = Tempfile.new("tpmmigrate")
                cmd_result("echo \"SET SESSION SQL_LOG_BIN=0;\n\" >> #{f.path()}")
                cmd_result("echo \"CREATE SCHEMA \\`#{schema}\\`;\n\" >> #{f.path()}")
                cmd_result("echo \"USE '#{schema}';\n\" >> #{f.path()}")
                
                cmd = "mysqldump --defaults-file=#{@config.getProperty([REPL_SERVICES, rs_alias, REPL_MYSQL_SERVICE_CONF])} --host=#{@config.getProperty([REPL_SERVICES, rs_alias, REPL_DBHOST])} --port=#{@config.getProperty([REPL_SERVICES, rs_alias, REPL_DBPORT])}"
                cmd_result("#{cmd} --opt --single-transaction #{result[key]} >> #{f.path()}")
                cmd_result("cat #{f.path()} | mysql --defaults-file=#{@config.getProperty([REPL_SERVICES, rs_alias, REPL_MYSQL_SERVICE_CONF])}")
              end
            rescue
            end
          }
        end
      end
    end
  end
  
  def upgrade_from_1_5
    if is_replicator?()
      begin
        thl_directory = @config.getProperty(get_host_key(REPL_LOG_DIR))
        service_thl_directory = nil
        target_dynamic_properties = nil

        @config.getPropertyOr([REPL_SERVICES], {}).each_key{
          |rs_alias|
          
          service_thl_directory = @config.getProperty([REPL_SERVICES, rs_alias, REPL_LOG_DIR])
          target_dynamic_properties = @config.getProperty([REPL_SERVICES, rs_alias, REPL_SVC_DYNAMIC_CONFIG])
        }

        if service_thl_directory == nil
          warning("Unable to upgrade the replicator to the current version")
          raise IgnoreError
        end
        
        source_replicator_properties = "#{get_additional_property(ACTIVE_DIRECTORY_PATH)}/tungsten-replicator/conf/dynamic.properties"
        if File.exists?(source_replicator_properties)
          info("Upgrade the previous replicator dynamic properties")
          FileUtils.mv(source_replicator_properties, target_dynamic_properties)
        end
        
        Dir[thl_directory + "/thl.*"].sort().each do |file| 
          FileUtils.mv(file, service_thl_directory)
        end
      rescue IgnoreError
      end
      
      @config.getPropertyOr([REPL_SERVICES], {}).each_key{
        |rs_alias|
        if rs_alias == DEFAULTS
          next
        end
        
        begin
          create_tungsten_schema(rs_alias)
          
          applier = get_applier_datasource(rs_alias)
          if applier.is_a?(MySQLDatabasePlatform)
            applier.run("SET SQL_LOG_BIN=0; INSERT INTO #{@config.getProperty([REPL_SERVICES, rs_alias, REPL_SVC_SCHEMA])}.trep_commit_seqno (seqno, fragno, last_frag, source_id, epoch_number, eventid, applied_latency, update_timestamp, shard_id, extract_timestamp) SELECT seqno, fragno, last_frag, source_id, epoch_number, eventid, applied_latency, update_timestamp, '#UNKNOWN', update_timestamp FROM tungsten.trep_commit_seqno")
          elsif applier.is_a?(PGDatabasePlatform)
            # Nothing to do here
          end
        rescue => e
          warning(e.message)
        end
      }
    end
  end
  
  def provision_server?()
    # The ProvisionNewSlavesPackageModule module will only set this value
    # if this server was not previously configured as a replicator 
    if ["false", ""].include?(get_additional_property(PROVISION_NEW_SLAVES).to_s())
      return false
    end
    
    unless is_replicator?()
      return false
    end
    
    Configurator.instance.command.build_topologies(@config)    
    @config.getPropertyOr([REPL_SERVICES], {}).each_key{
      |rs_alias|
      if rs_alias == DEFAULTS
        next
      end
      
      # If there is a non-master service, we should provision
      if @config.getProperty([REPL_SERVICES, rs_alias, REPL_ROLE]) != REPL_ROLE_M
        return true
      end
    }
    
    return false
  end
  
  def provision_server
    unless provision_server?()
      return
    end
    
    services = AUTODETECT
    source = AUTODETECT
    if get_additional_property(PROVISION_NEW_SLAVES) == "true"
      info("Provision #{@config.getProperty(HOST)}")
    else
      info("Provision #{@config.getProperty(HOST)} using #{get_additional_property(PROVISION_NEW_SLAVES)}")

      # Identify the service that should be provisioned, a source may
      # optionally be provided in the --provision-new-slaves option
      match = get_additional_property(PROVISION_NEW_SLAVES).to_s().match(/^^([a-zA-Z0-9_]+)(@([a-zA-Z0-9_\.\-]+))?$/)
      if match == nil
        error("Unable to parse the provision source: #{get_additional_property(PROVISION_NEW_SLAVES)}")
        return false
      end
      
      services = [match[1]]
      unless match[3].to_s() == ""
        source = match[3]
      end
    end

    start_replication_services_offline()
    
    # Find all services that should be provisioned. For Fan-In replication, 
    # we may provision from each master server
    if services == AUTODETECT
      services = []
      @config.getPropertyOr([REPL_SERVICES], {}).each_key{
        |rs_alias|
        if rs_alias == DEFAULTS
          next
        end
        
        if @config.getProperty([REPL_SERVICES, rs_alias, REPL_ROLE]) == REPL_ROLE_M
          next
        end
        
        services << @config.getProperty([REPL_SERVICES, rs_alias, DEPLOYMENT_DATASERVICE])
        
        # Stop after finding one until we properly support multi-master topologies
        break
      }
    end

    begin
      is_first = true
      services.each{
        |svc|
        args = []
        args << "--source=#{source}"
        args << "--service=#{svc}"
      
        if is_first == false
          # Do not add this argument until it is supported and we properly support multi-master topologies
          # args << "--logical"
        end
      
        notice("Provision the #{svc} replication service on #{@config.getProperty(HOST)}")
        info(cmd_result("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-replicator/scripts/tungsten_provision_slave #{args.join(' ')}"))
        is_first = false
      }
    
      online_replication_services()
    rescue CommandError => ce
      error("There was a problem running #{ce.command}\n#{ce.result}\nResolve the issue and run the tungsten_provision_slave script on #{@config.getProperty(HOST)}")
    end
  end
end
