class ClusterSlaveTopology
  include Topology
  
  def get_master_thl_uri(h_alias)
    values = []
    
    unless @config.getProperty([DATASERVICES, @ds_alias, DATASERVICE_MASTER_MEMBER]).to_s().include_alias?(h_alias)
      rs_alias = @ds_alias + "_" + h_alias
      hosts = @config.getTemplateValue([REPL_SERVICES, rs_alias, REPL_MASTERHOST]).to_s().split(",")
      port = @config.getTemplateValue([REPL_SERVICES, rs_alias, REPL_MASTERPORT])
      values << _splice_hosts_port(hosts, port, @config.getProperty([REPL_SERVICES, rs_alias, REPL_THL_PROTOCOL]))
    end
    
    relay_sources = []
    relay_source = @config.getProperty([DATASERVICES, @ds_alias, DATASERVICE_RELAY_SOURCE])
    relay_source.split(",").each{
      |relay_alias|
      # If the relay source is a composite service, replace the service name
      # with the composite-datasources
      if @config.getProperty([DATASERVICES, relay_alias, DATASERVICE_IS_COMPOSITE]) == "true"
        @config.getProperty([DATASERVICES, relay_alias, DATASERVICE_COMPOSITE_DATASOURCES]).split(",").each{
          |composite_relay_alias|
          relay_sources << composite_relay_alias
        }
      else
        relay_sources << relay_alias
      end
    }

    relay_sources.each{
      |relay_alias|
      hosts = @config.getTemplateValue([DATASERVICES, relay_alias, DATASERVICE_MEMBERS]).to_s().split(",")
      port = @config.getTemplateValue([DATASERVICES, relay_alias, DATASERVICE_THL_PORT])
      values << _splice_hosts_port(hosts, port, @config.getProperty([REPL_SERVICES, relay_alias, REPL_THL_PROTOCOL]))
    }
    
    return values.join(",")
  end
  
  def get_dataservice_alias
    first_ds_alias = nil
    cluster_ds_alias = nil
    ms_ds_alias = nil
    
    relay_source = @config.getProperty([DATASERVICES, @ds_alias, DATASERVICE_RELAY_SOURCE])
    relay_source.split(",").each{
      |relay_alias|
      
      unless first_ds_alias == nil
        first_ds_alias = relay_source
      end
      
      topology = Topology.build(relay_source, @config)
      if [ClusterTopology, ClusterAliasTopology].include?(topology.class) && cluster_ds_alias == nil
        cluster_ds_alias = relay_alias
      end
      if [MasterSlaveTopology].include?(topology.class) && ms_ds_alias == nil
        ms_ds_alias = relay_alias
      end
    }
    
    if cluster_ds_alias != nil
      return cluster_ds_alias
    elsif ms_ds_alias != nil
      return ms_ds_alias
    elsif first_ds_alias != nil
      return first_ds_alias
    else
      return nil
    end
  end
  
  def get_replication_schema
    relay_source = @config.getProperty([DATASERVICES, @ds_alias, DATASERVICE_RELAY_SOURCE])
    return @config.getProperty([DATASERVICES, relay_source, DATASERVICE_SCHEMA])
  end
  
  def get_role(hostname)
    return REPL_ROLE_S
  end
  
  def master_preferred_role
    return REPL_ROLE_S
  end
  
  def self.get_name
    'cluster-slave'
  end
end