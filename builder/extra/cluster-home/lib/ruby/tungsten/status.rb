class TungstenStatus
  include TungstenAPI

  DATASERVICE = "dataservice"
  COORDINATOR = "coordinator"
  ROUTERS = "routers"
  DATASOURCES = "datasources"
  REPLICATORS = "replicators"
  MANAGER = "manager"
  REPLICATOR = "replicator"
  MASTER = "master"
  DATASERVER = "dataserver"
  STATUS = "status"
  HOSTNAME = "hostname"
  ROLE = "role"
  SEQNO = "seqno"
  LATENCY = "latency"
  
  SERVICE_TYPE = "service_type"
  REPLICATION_SERVICE = :replication
  PHYSICAL_SERVICE = :physical
  COMPOSITE_SERVICE = :composite

  def initialize(install, dataservice = nil)
    @install = install
    @dataservice = dataservice
    @service_type = nil
    @props = nil
  end

  def parse
    if @props != nil
      return
    end
    
    if @dataservice == nil
      if @install.dataservices().size > 1
        raise "Unable to parse trepctl because there are multiple dataservices defined for this replicator. Try specifying a specific replication service."
      end
      @dataservice = @install.dataservices()[0]
    end
    
    if @install.is_manager?()
      unless @install.is_running?("manager")
        raise "Unable to provide status for #{@dataservice} from #{@install.hostname()}:#{@install.root()} because the manager is not running"
      end
      parse_manager()
    elsif @install.is_replicator?()
      unless @install.is_running?("replicator")
        raise "Unable to provide status for #{@dataservice} from #{@install.hostname()}:#{@install.root()} because the replicator is not running"
      end
      parse_replicator()
    else
      raise "Unable to provide status for #{@dataservice} from #{@install.hostname()}:#{@install.root()} because it is not a database server"
    end
  end
  
  def parse_manager
    @props = Properties.new()
    
    mgr = TungstenDataserviceManager.new(@install.mgr_api_uri())
    result = mgr.get(@dataservice, 'status')
    result = result["outputPayload"]["dataServiceState"]

    @props.setProperty(DATASERVICE, @dataservice)
    if result["composite"] == true
      @props.setProperty(SERVICE_TYPE, COMPOSITE_SERVICE)
    else
      @props.setProperty(SERVICE_TYPE, PHYSICAL_SERVICE)
    end
    @props.setProperty(COORDINATOR, {
      "host" => result["coordinator"],
      "mode" => result["policyManagerMode"]
    })
    @props.setProperty(DATASOURCES, result["dataSources"])
    @props.setProperty(REPLICATORS, result["replicators"])
  end
  
  def parse_replicator
    @props = Properties.new()

    @props.setProperty(DATASERVICE, @dataservice)
    @props.setProperty(SERVICE_TYPE, REPLICATION_SERVICE)
    r_props = JSON.parse(TU.cmd_result("#{@install.trepctl(@dataservice)} status -json"))
    @props.setProperty([REPLICATORS, @install.hostname()], r_props)
  end
  
  def name
    self.parse()
    return @props.getProperty(DATASERVICE)
  end
  
  def coordinator
    self.parse()
    return @props.getProperty(['coordinator','host'])
  end
  
  def policy
    self.parse()
    return @props.getProperty(['coordinator','mode'])
  end
  
  def datasources
    self.parse()
    return @props.getPropertyOr([DATASOURCES], {}).keys()
  end
  
  def replicators
    self.parse()
    return @props.getPropertyOr([REPLICATORS], {}).keys()
  end
  
  def datasource_role(hostname)
    datasource_value(hostname, 'role')
  end
  
  def datasource_status(hostname)
    datasource_value(hostname, 'state')
  end
  
  def replicator_role(hostname)
    replicator_value(hostname, 'role')
  end
  
  def replicator_status(hostname)
    replicator_value(hostname, 'state')
  end
  
  def replicator_latency(hostname)
    replicator_value(hostname, 'appliedLatency').to_f()
  end
  
  def datasource_value(hostname, argument)
    self.parse()
    return @props.getProperty([DATASOURCES, hostname, argument])
  end
  
  def replicator_value(hostname, argument)
    self.parse()
    return @props.getProperty([REPLICATORS, hostname, argument])
  end
  
  def is_replication?
    self.parse()
    return @props.getProperty(SERVICE_TYPE) == REPLICATION_SERVICE
  end
  
  def is_physical?
    self.parse()
    return @props.getProperty(SERVICE_TYPE) == PHYSICAL_SERVICE
  end
  
  def is_composite?
    self.parse()
    return @props.getProperty(SERVICE_TYPE) == COMPOSITE_SERVICE
  end
  
  def to_s
    self.parse()
    return @props.to_s()
  end

  def to_hash
    self.parse()
    return @props.props
  end

  def output()
    self.parse()
    TU.output(self.to_s)
  end

  def force_output()
    self.parse()
    TU.force_output(self.to_s)
  end
end