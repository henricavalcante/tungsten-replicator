class AllMastersTopology
  include Topology
  
  def allow_multiple_masters?
    true
  end
  
  def build_services
    masters = @config.getProperty([DATASERVICES, @ds_alias, DATASERVICE_MASTER_MEMBER]).split(",")
    services = @config.getProperty([DATASERVICES, @ds_alias, DATASERVICE_MASTER_SERVICES]).split(",")
    
    if masters.size() > services.size()
      raise "Unable to build the #{@ds_alias} all-masters services because not enough --master-services were given"
    end
    
    slaves = @config.getProperty([DATASERVICES, @ds_alias, DATASERVICE_MEMBERS]).split(",")
    
    masters.each{
      |master|
      
      add_built_service(services.shift(), {
        DATASERVICE_MASTER_MEMBER => master,
        DATASERVICE_MEMBERS => @config.getProperty([DATASERVICES, @ds_alias, DATASERVICE_MEMBERS]),
      }, {
        REPL_SVC_ENABLE_SLAVE_THL_LISTENER => "false"
      })
    }
    
    new_services = @config.getProperty([DATASERVICES, @ds_alias, DATASERVICE_MASTER_SERVICES]).split(",")
    Configurator.instance.command.replace_command_dataservices(@ds_alias, 
      new_services)
    remove_service(@ds_alias)
  end
  
  def self.get_name
    'all-masters'
  end
end