module ConfigureDeploymentStepReplicationDataservice
  def deploy_replication_dataservice()
    mkdir_if_absent(@config.getProperty(get_service_key(REPL_LOG_DIR)))
    
    if @config.getProperty(get_service_key(REPL_RELAY_LOG_DIR))
      mkdir_if_absent(@config.getProperty(get_service_key(REPL_RELAY_LOG_DIR)))
    end
    
    if @config.getProperty(get_service_key(REPL_BACKUP_STORAGE_DIR))
      mkdir_if_absent(@config.getProperty(get_service_key(REPL_BACKUP_STORAGE_DIR)))
    end
    
    # Configure replicator.properties.service.template
    transform_service_template(@config.getProperty(REPL_SVC_CONFIG_FILE),
      get_replication_dataservice_template())
  end
	
	def get_replication_dataservice_template()
    if @config.getProperty(REPL_ROLE) == REPL_ROLE_DI
      "tungsten-replicator/samples/conf/replicator.properties.direct"
	  else
	    begin
	      # This will fail if extraction is not supported meaning this can never be a master
	      extractor_template = get_extractor_datasource().get_extractor_template()
	    rescue
	      if @config.getProperty(REPL_ROLE) == REPL_ROLE_S
	        return "tungsten-replicator/samples/conf/replicator.properties.slave"
	      elsif @config.getProperty(REPL_ROLE) == REPL_ROLE_ARCHIVE
  	      return "tungsten-replicator/samples/conf/replicator.properties.archive"
  	    else
	        raise "Unable to extract from #{get_extractor_datasource.get_connection_summary}"
	      end
	    end
	  
	    if @config.getProperty(REPL_ROLE) == REPL_ROLE_ARCHIVE
	      "tungsten-replicator/samples/conf/replicator.properties.archive"
	    else
	      "tungsten-replicator/samples/conf/replicator.properties.masterslave"
	    end
	  end
	end
end