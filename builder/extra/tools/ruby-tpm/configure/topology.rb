module Topology
  @@classes = false
  
  def initialize(ds_alias, config)
    @ds_alias = ds_alias
    @config = config
  end
  
  def allow_multiple_masters?
    false
  end
  
  def use_replicator?
    true
  end
  
  def use_management?
    false
  end
  
  def use_connector?
    false
  end
  
  def enabled?
    if Configurator.instance.is_enterprise?()
      enable_all_topologies = @config.getProperty([DATASERVICES, @ds_alias, DATASERVICE_ENABLE_ALL_TOPOLOGIES])
      if enable_all_topologies == "true"
        return true
      end
    end
    
    if use_management?()
      # Managed topologies are only enabled if this isn't a 
      # clustering build
      if Configurator.instance.is_enterprise?()
        true
      else
        false
      end
    else
      # Non-Management topologies are only enabled if this isn't a 
      # clustering build
      if Configurator.instance.is_enterprise?()
        false
      else
        true
      end
    end
  end
  
  def get_master_thl_uri(h_alias)
    rs_alias = @ds_alias + "_" + h_alias
    hosts = @config.getTemplateValue([REPL_SERVICES, rs_alias, REPL_MASTERHOST]).to_s().split(",")
    port = @config.getTemplateValue([REPL_SERVICES, rs_alias, REPL_MASTERPORT])
    
    return _splice_hosts_port(hosts, port, @config.getProperty([REPL_SERVICES, rs_alias, REPL_THL_PROTOCOL]))
  end
  
  def get_role(h_alias)
    if h_alias == nil || @ds_alias == nil
      rs_alias = DEFAULTS
    else
      rs_alias = @ds_alias + "_" + h_alias
    end
    
    if @config.getPropertyOr([DATASERVICES, @ds_alias, DATASERVICE_MASTER_MEMBER]).include_alias?(h_alias)
      relay_source = @config.getProperty([DATASERVICES, @ds_alias, DATASERVICE_RELAY_SOURCE])
      
      if relay_source.to_s == ""
        return REPL_ROLE_M
      else
        return REPL_ROLE_R
      end
    elsif @config.getProperty([REPL_SERVICES, rs_alias, RELAY_ENABLED]) == "true"
      return REPL_ROLE_S_RELAY
    else
      return REPL_ROLE_S
    end
  end
  
  def get_dataservice_alias
    nil
  end
  
  def get_replication_schema
    nil
  end
  
  def master_preferred_role
    ""
  end
  
  def disable_relay_logs?
    "false"
  end
  
  def build_services
  end
  
  def add_built_service(service, ds_props, rs_props)
    host = @config.getProperty(DEPLOYMENT_HOST)
    unless ds_props[DATASERVICE_MEMBERS].to_s().include_alias?(host)
      return
    end
    
    service = to_identifier(service)
    ds_props[DATASERVICENAME] = service
    rs_alias = "#{service}_#{host}"
    
    @config.include([DATASERVICES, service], ds_props)
    @config.include([REPL_SERVICES, rs_alias], rs_props)
    @config.include([REPL_SERVICES, rs_alias], @config.getPropertyOr([REPL_SERVICES, "#{@ds_alias}_#{host}"], {}))
    @config.setProperty([REPL_SERVICES, rs_alias, DEPLOYMENT_DATASERVICE], service)
    @config.setProperty([REPL_SERVICES, rs_alias, DEPLOYMENT_HOST], host)
    @config.include([DATASERVICE_HOST_OPTIONS, service], @config.getPropertyOr([DATASERVICE_HOST_OPTIONS, @ds_alias], {}))
    @config.include([DATASERVICE_REPLICATION_OPTIONS, service], @config.getPropertyOr([DATASERVICE_REPLICATION_OPTIONS, @ds_alias], {}))
  end
  
  def remove_service(service)
    host = @config.getProperty(DEPLOYMENT_HOST)
    
    @config.setProperty([REPL_SERVICES, to_identifier("#{service}_#{host}")], nil)
    @config.setProperty([DATASERVICES, service], nil)
    @config.setProperty([DATASERVICE_HOST_OPTIONS, service], nil)
    @config.setProperty([DATASERVICE_REPLICATION_OPTIONS, service], nil)
  end
  
  def _splice_hosts_port(hosts, default_port, protocol)
    values = []
    
    hosts.each{
      |host|
      
      if host.index(':') == nil
        values << "#{protocol}://#{host}:#{default_port}/"
      else
        values << "#{protocol}://#{host}"
      end
    }
    
    return values.join(",")
  end
  
  def self.build(ds_alias, config)
    klass = Topology.get_class(config.getProperty([DATASERVICES, ds_alias, DATASERVICE_TOPOLOGY]))
    return klass.new(ds_alias, config)
  end
  
  def self.get_classes
    unless @@classes
      @@classes = {}

      self.subclasses.each{
        |klass|
        begin
          @@classes[klass.get_name()] = klass
        rescue NoMethodError
        end
      }
    end
    
    @@classes
  end
  
  def self.get_types
    return self.get_classes().keys().delete_if{
      |key|
      key.to_s == ""
    }
  end
  
  def self.get_class(name)
    if name.to_s() == ""
      return self.get_default_class()
    end
    
    get_classes().each{
      |klass_name,klass|
      
      if klass_name == name
        return klass
      end
    }
    
    raise "Unable to find a topology class for #{name}"
  end
  
  def self.get_default_class
    self.get_classes().each{
      |klass_name,klass|
      begin
        if klass.is_default?() == true
          return klass
        end
      rescue NoMethodError
      end
    }
  end
  
  def self.included(subclass)
    @subclasses ||= []
    @subclasses << subclass
  end
  
  def self.subclasses
    @subclasses
  end
end