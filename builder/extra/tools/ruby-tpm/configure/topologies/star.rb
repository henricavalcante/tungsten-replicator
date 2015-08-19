class StarTopology
  include Topology
  
  def allow_multiple_masters?
    true
  end
  
  def build_services
    host = @config.getProperty(HOST)
    masters = @config.getProperty([DATASERVICES, @ds_alias, DATASERVICE_MASTER_MEMBER]).split(",")
    hub = @config.getProperty([DATASERVICES, @ds_alias, DATASERVICE_HUB_MEMBER])
    services = @config.getProperty([DATASERVICES, @ds_alias, DATASERVICE_MASTER_SERVICES]).split(",")
    hub_service = @config.getProperty([DATASERVICES, @ds_alias, DATASERVICE_HUB_SERVICE])
    
    if masters.size() > services.size()
      raise "Unable to build the #{@ds_alias} star services because not enough --master-services were given"
    end
    
    slaves = @config.getProperty([DATASERVICES, @ds_alias, DATASERVICE_MEMBERS]).split(",")
    
    add_built_service(hub_service, {
      DATASERVICE_MASTER_MEMBER => hub,
      DATASERVICE_MEMBERS => ([hub]+masters).join(",")
    }, {
      REPL_SVC_ENABLE_SLAVE_THL_LISTENER => "false",
      REPL_SVC_APPLIER_FILTERS => "bidiSlave",
      FIXED_PROPERTY_STRINGS => ["replicator.service.comments=true"]
    })
    
    masters.each{
      |master|
      master_service = services.shift()
      
      add_built_service(master_service, {
        DATASERVICE_MASTER_MEMBER => master,
        DATASERVICE_MEMBERS => [master, hub].join(",")
      }, {
        REPL_SVC_ENABLE_SLAVE_THL_LISTENER => "false",
        LOG_SLAVE_UPDATES => "true",
        FIXED_PROPERTY_STRINGS => ["replicator.service.comments=true"]
      })
      
      unless host == master
        next
      end
      
      rs_alias = to_identifier("#{hub_service}_#{master}")
      @config.setProperty([REPL_SERVICES, rs_alias, REPL_SVC_ALLOW_ANY_SERVICE], "true")
      fp = @config.getPropertyOr([REPL_SERVICES, rs_alias, FIXED_PROPERTY_STRINGS], [])
      @config.setProperty([REPL_SERVICES, rs_alias, FIXED_PROPERTY_STRINGS], 
        fp + ["local.service.name=#{master_service}"])
    }
    
    new_services = @config.getProperty([DATASERVICES, @ds_alias, DATASERVICE_MASTER_SERVICES]).split(",")
    new_services << @config.getProperty([DATASERVICES, @ds_alias, DATASERVICE_HUB_SERVICE])
    Configurator.instance.command.replace_command_dataservices(@ds_alias, 
      new_services)
    remove_service(@ds_alias)
  end
  
  def self.get_name
    'star'
  end
end