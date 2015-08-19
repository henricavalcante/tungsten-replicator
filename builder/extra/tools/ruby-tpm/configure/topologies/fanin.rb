class FanInTopology
  include Topology
  
  def allow_multiple_masters?
    true
  end
  
  def build_services
    masters = @config.getProperty([DATASERVICES, @ds_alias, DATASERVICE_MASTER_MEMBER]).split(",")
    services = @config.getProperty([DATASERVICES, @ds_alias, DATASERVICE_MASTER_SERVICES]).split(",")
    
    if masters.size() > services.size()
      raise "Unable to build the #{@ds_alias} fan-in services because not enough --master-services were given"
    end
    
    # Remove masters from the list of members to determine slaves
    slaves = @config.getProperty([DATASERVICES, @ds_alias, DATASERVICE_MEMBERS]).split(",")
    slaves = slaves - masters
    
    masters.each{
      |master|
      
      add_built_service(services.shift(), {
        DATASERVICE_MASTER_MEMBER => master,
        DATASERVICE_MEMBERS => ([master]+slaves).join(","),
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
    'fan-in'
  end
end