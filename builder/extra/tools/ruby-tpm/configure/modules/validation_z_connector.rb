class ConnectorChecks < GroupValidationCheck
  include ClusterHostCheck
  
  def initialize
    super(CONNECTORS, "connector", "connectors")
    
    ConnectorCheck.submodules().each{
      |klass|
      
      self.add_check(klass.new())
    }
  end
  
  def set_vars
    @title = "Connector checks"
  end
end

module ConnectorCheck
  include GroupValidationCheckMember
  include ConnectorEnabledCheck
  
  def get_host_key(key)
    [HOSTS, @config.getProperty(get_member_key(DEPLOYMENT_HOST)), key]
  end
  
  def get_dataservice_key(key)
    ds_aliases = @config.getPropertyOr(get_member_key(DEPLOYMENT_DATASERVICE), [])
    return [DATASERVICES, ds_aliases.at(0), key]
  end
  
  def get_dataservice_alias
    if self.is_a?(HashPromptMemberModule)
      get_member()
    else
      ds_aliases = @config.getPropertyOr(get_member_key(DEPLOYMENT_DATASERVICE), [])
      return ds_aliases.at(0)
    end
  end
  
  def get_topology
    Topology.build(get_dataservice_alias(), @config)
  end
  
  def self.included(subclass)
    @submodules ||= []
    @submodules << subclass
  end

  def self.submodules
    @submodules || []
  end
end

class ConnectorSmartScaleAllowedCheck < ConfigureValidationCheck
  include ConnectorCheck
  
  def set_vars
    @title = "Connector SmartScale allowed check"
  end
  
  def validate
    if (@config.getProperty(get_member_key(CONN_SMARTSCALE)) == "true" &&
        @config.getProperty(get_member_key(CONN_RWSPLITTING)) == "true"
    )
      error("Both SmartScale and R/W Splitting are enabled.  You must disable one of them.")
    end
  end
end

class ConnectorListenerAddressCheck < ConfigureValidationCheck
  include ConnectorCheck
  
  def set_vars
    @title = "Connector listener address check"
  end
  
  def validate
    addr = @config.getProperty(get_member_key(CONN_LISTEN_ADDRESS))
    if addr.to_s() == ""
      error("Unable to determine the listening address for the connector")
    end
  end
end

class ConnectorRWROAddressesCheck < ConfigureValidationCheck
  include ConnectorCheck
  
  def set_vars
    @title = "Check that the connector r/w addresses and r/o addresses are different"
  end

  def validate
    rw_addresses = @config.getProperty(get_member_key(CONN_RW_ADDRESSES)).split(",")
    ro_addresses = @config.getProperty(get_member_key(CONN_RO_ADDRESSES)).split(",")

    rw_addresses.each{
      |address|
      if address == ""
        next
      end
      
      if ro_addresses.include?(address)
        error("#{address} appears in --connector-rw-addresses and --connector-ro-addresses")
      end
    }
    
    unless is_valid?()
      help("Redefine the --connector-rw-addresses and --connector-ro-addresses for this host or data service so that the values are unique")
    end
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(CONN_RW_ADDRESSES)).to_s != "" && @config.getProperty(get_member_key(CONN_RO_ADDRESSES)).to_s != ""
  end
end

class ConnectorUserCheck < ConfigureValidationCheck
  include ConnectorCheck
  
  def set_vars
    @title = "Connector User check"
  end
  
  def validate
    conuser = @config.getProperty('connector_user')
    repluser = @config.getProperty('repl_datasource_user')
   
    if conuser == repluser
      error("Connector User must be different from Datasource User")
      help("The Connector user and the Datasource user must be different users - Ensure the --application-user parameter is specified")
    end
  end
  
  def enabled?
    super() && @config.getProperty(ENABLE_CONNECTOR_BRIDGE_MODE) != "true"
  end
end

class ConnectorDBVersionCheck < ConfigureValidationCheck
  include ConnectorCheck

  def set_vars
    @title = "Connector DB Version check"
  end

  def validate
    version = @config.getProperty(CONN_DB_VERSION)
    if version != "autodetect" && version[0,1] =~ /[A-Za-z]/
      error("connector-db-version must start with a numeric value - current value is #{@config.getProperty('connector_db_version')}")
    end
  end
end

class RouterKeepAliveTimeoutCheck < ConfigureValidationCheck
  include ConnectorCheck

  def set_vars
    @title = "Router Keep Alive Timeout Check"
  end

  def validate
    if @config.getProperty('connector_keepalive_timeout').to_i <= 0 or   @config.getProperty('connector_keepalive_timeout').to_i > 300000
      error("connector_keepalive_timeout must start greater than 0 and less than 300000  - current value is #{@config.getProperty('connector_keepalive_timeout').to_i}")
    end
  end
end

class RouterDelayBeforeOfflineCheck < ConfigureValidationCheck
  include ConnectorCheck

  def set_vars
    @title = "Router Delay Before Offline Check"
  end

  def validate
    if @config.getProperty('connector_delay_before_offline').to_i <= 0 or @config.getProperty('connector_delay_before_offline').to_i > 60
      error("connector_delay_before_offline must start greater than 0 and less than 60  - current value is #{@config.getProperty('connector_delay_before_offline').to_i}")
    end
  end
end

class RouterAffinityCheck < ConfigureValidationCheck
  include ConnectorCheck
  
  def set_vars
    @title = "Connector affinity validity check"
  end
  
  def validate
    # The combination of --connector-affinity and --connector-bridge-mode
    # will only work on an RO_RELAXED connection. This is only accomplished
    # by adding --connector-readonly or --application-readonly-port
    if @config.getProperty(get_member_key(ENABLE_CONNECTOR_BRIDGE_MODE)) == "true"
      if @config.getProperty(get_member_key(CONN_AFFINITY)).to_s() != ""
        # See if --connector-readonly or --application-readonly-port is given
        has_read_only = false
        if @config.getProperty(get_member_key(ENABLE_CONNECTOR_RO)) == "true"
          has_read_only = true
        elsif @config.getProperty(get_member_key(CONN_RO_LISTEN_PORT)).to_s() != ""
          has_read_only = true
        end
        
        if has_read_only == false
          error("The `--connector-affinity` option is only supported when `--connector-bridge-mode` is not given; or if either `--connector-readonly` or `--application-readonly-port` is provided.")
        end
      end
    end
  end
end