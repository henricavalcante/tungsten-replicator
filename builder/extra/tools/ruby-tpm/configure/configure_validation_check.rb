class ConfigureValidationCheck
  include ValidationCheckInterface
  
  def warning(message)
    unless skip_class_warnings?()
      super("#{message} (#{self.class.name})")
    end
  end
end

module LocalValidationCheck
end

module CommitValidationCheck
end

module PostValidationCheck
  def get_message_hostname
    "All Hosts"
  end
  
  def enabled?
    Configurator.instance.command.get_deployment_configurations().each{
      |cfg|
      if ConfigureValidationHandler.skip_validation_class?(self.class.name, cfg)
        debug("Skipping validation check '#{self.class.name}'")
        return false
      end
    }
    
    true
  end
end

module PostValidationCommitCheck
  def get_message_hostname
    "All Hosts"
  end
  
  def enabled?
    Configurator.instance.command.get_deployment_configurations().each{
      |cfg|
      if ConfigureValidationHandler.skip_validation_class?(self.class.name, cfg)
        debug("Skipping validation check '#{self.class.name}'")
        return false
      end
    }
    
    true
  end
end

module ClusteringHostCheck
  def enabled?
    unless super()
      return false
    end
    
    h_alias = @config.getProperty(DEPLOYMENT_HOST)
    @config.getPropertyOr([DATASERVICES], {}).each_key{
      |ds_alias|
      if @config.getProperty([DATASERVICES, ds_alias, DEPLOYMENT_HOST]) == h_alias
        if Topology.build(ds_alias, @config).use_connector?()
          return true
        end
      end
    }
    
    return false
  end
end

module ClusteringServiceCheck
  def enabled?
    unless super()
      return false
    end
    
    if get_topology().use_connector?()
      return true
    else
      return false
    end
  end
end

module NotUnlessEnabledCheck
  def enabled?
    enabled_classes = ConfigureValidationHandler.get_enabled_validation_classes()
    if enabled_classes != nil && enabled_classes.include?(self.class.name)
      return super()
    else
      false
    end
  end
end

module ReplicatorEnabledCheck
  def enabled?
    super() && (@config.getProperty(HOST_ENABLE_REPLICATOR) == "true")
  end
end

module ManagerEnabledCheck
  def enabled?
    super() && (@config.getProperty(HOST_ENABLE_MANAGER) == "true")
  end
end

module ConnectorEnabledCheck
  def enabled?
    super() && (@config.getProperty(HOST_ENABLE_CONNECTOR) == "true")
  end
end