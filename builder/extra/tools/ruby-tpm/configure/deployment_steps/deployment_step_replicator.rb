module ConfigureDeploymentStepReplicator
  def get_methods
    [
      ConfigureDeploymentMethod.new("deploy_replicator")
    ]
  end
  module_function :get_methods
  
  def deploy_replicator
    unless is_replicator?()
      info("Tungsten Replicator is not active; skipping configuration")
      return
    end
    
    Configurator.instance.write_header("Perform Tungsten Replicator configuration")    
    
    mkdir_if_absent(@config.getProperty(REPL_BACKUP_DUMP_DIR))
    mkdir_if_absent(@config.getProperty(REPL_BACKUP_STORAGE_DIR))
    mkdir_if_absent(@config.getProperty(REPL_RELAY_LOG_DIR))
    mkdir_if_absent(@config.getProperty(REPL_LOG_DIR))
    
    transform_service_template("tungsten-replicator/conf/services.properties",
      "tungsten-replicator/samples/conf/sample.services.properties")
    
    Configurator.instance.command.build_topologies(@config)
    @config.getPropertyOr(REPL_SERVICES, {}).each_key{
      |hs_alias|
      if hs_alias == DEFAULTS
        next
      end
      
      ds_alias = @config.getProperty([REPL_SERVICES, hs_alias, DEPLOYMENT_DATASERVICE])
      
      @config.setProperty(DEPLOYMENT_SERVICE, hs_alias)
      @config.setProperty(DEPLOYMENT_DATASERVICE, ds_alias)
      
      info("Configure the #{ds_alias} replication service")
      
      if @config.getProperty([REPL_SERVICES, hs_alias, REPL_SVC_CLUSTER_ENABLED]) == "true"
        write_replication_monitor_extension()
      end
      
      trigger_event(:before_deploy_replication_service)
      deploy_replication_dataservice()
      trigger_event(:after_deploy_replication_service)
      
      @config.setProperty(DEPLOYMENT_SERVICE, nil)
      @config.setProperty(DEPLOYMENT_DATASERVICE, nil)
    }
    
    add_log_file("tungsten-replicator/log/trepsvc.log")
    add_log_file("tungsten-replicator/log/xtrabackup.log")
    add_log_file("tungsten-replicator/log/mysqldump.log")
    set_run_as_user("tungsten-replicator/bin/replicator")
    transform_host_template("tungsten-replicator/conf/replicator.env",
	    "tungsten-replicator/samples/conf/replicator.env")
  end
  
  def write_replication_monitor_extension
    svc_properties = "#{get_deployment_basedir()}/cluster-home/conf/cluster/#{@config.getProperty(DATASERVICENAME)}/extension/event.properties"    
    echo_event_sh = get_svc_command("${manager.home}/scripts/echoEvent.sh")
    
    # Create service properties file.
    out = File.open(svc_properties, "w")
    out.puts("# event.properties")
    out.puts("name=event")
    out.puts("command.onResourceStateTransition=#{echo_event_sh}")
    out.puts("command.onDataSourceStateTransition=#{echo_event_sh}")
    out.puts("command.onFailover=#{echo_event_sh}")
    out.puts("command.onPolicyAction=#{echo_event_sh}")
    out.puts("command.onRecovery=#{echo_event_sh}")
    out.puts("command.onDataSourceCreate=#{echo_event_sh}")
    out.puts("command.onResourceNotification=#{echo_event_sh}")
    out.puts "# AUTO-CONFIGURED: #{DateTime.now}"
    out.chmod(0755)
    out.close
    
    info "GENERATED FILE: " + svc_properties
    WatchFiles.watch_file(svc_properties, @config)
  end
  
  def get_dynamic_properties_file()
    @config.getProperty(REPL_SVC_DYNAMIC_CONFIG)
  end
end
