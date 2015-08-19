module ConfigureDeploymentStepManager
  def get_methods
    [
      ConfigureDeploymentMethod.new("deploy_manager"),
    ]
  end
  module_function :get_methods
  
  def deploy_manager
    unless is_manager?()
      info("Tungsten Manager is not active; skipping configuration")
      return
    end
    
    Configurator.instance.write_header("Perform Tungsten Manager configuration")   

    transform_host_template("tungsten-manager/conf/manager.properties",
      "tungsten-manager/samples/conf/manager.properties.tpl")

    transform_host_template("tungsten-manager/conf/mysql.service.properties",
      "tungsten-manager/samples/conf/mysql.service.properties.tpl")
    
    host_transformer("tungsten-manager/conf/mysql_checker_query.sql") {
      |t|
      t.timestamp?(false)
      t.set_template("tungsten-manager/samples/conf/mysql_checker_query.sql.tpl")
    }
    
    group_communication_config = @config.getProperty(MGR_GROUP_COMMUNICATION_CONFIG)
    host_transformer("tungsten-manager/conf/#{group_communication_config}") {
      |t|
      t.timestamp?(false)
      t.set_template("tungsten-manager/samples/conf/#{group_communication_config}.tpl")
    }

    transform_host_template("tungsten-manager/conf/hedera.properties",
      "tungsten-manager/samples/conf/hedera.properties.tpl")

    transform_host_template("tungsten-manager/conf/monitor.properties",
      "tungsten-manager/samples/conf/monitor.properties.tpl")
    
    transform_host_template("tungsten-manager/conf/checker.tungstenreplicator.properties",
      "tungsten-manager/samples/conf/checker.tungstenreplicator.properties.tpl")
    
    transform_service_template("tungsten-manager/conf/checker.mysqlserver.properties",
      "tungsten-manager/samples/conf/checker.mysqlserver.properties.tpl")
    
    unless @config.getProperty(MGR_IS_WITNESS) == "true"
      transform_host_template("tungsten-manager/conf/checker.instrumentation.properties",
	      "tungsten-manager/samples/conf/checker.instrumentation.properties.tpl")
    end
    
    if @config.getProperty(MANAGER_ENABLE_INSTRUMENTATION) == "true"
      FileUtils.cp("#{get_deployment_basedir()}/tungsten-manager/rules-ext/Instrumentation.drl", "#{get_deployment_basedir()}/tungsten-manager/rules/")
    end
    
    add_log_file("tungsten-manager/log/tmsvc.log")
    set_run_as_user("tungsten-manager/bin/manager")
    transform_host_template("tungsten-manager/conf/wrapper.conf",
      "tungsten-manager/samples/conf/wrapper.conf")
  
    FileUtils.cp("#{get_deployment_basedir()}/tungsten-manager/conf/manager.service.properties", 
      "#{get_deployment_basedir()}/cluster-home/conf/cluster/#{@config.getProperty(DATASERVICENAME)}/service/manager.properties")
  end
end