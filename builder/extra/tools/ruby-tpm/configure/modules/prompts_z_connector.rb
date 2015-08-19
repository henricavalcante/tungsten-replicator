CONNECTORS = "connectors"
HOST_ENABLE_CONNECTOR = "host_enable_connector"
CONN_LISTEN_INTERFACE = "connector_listen_interface"
CONN_LISTEN_ADDRESS = "connector_listen_address"
CONN_LISTEN_PORT = "connector_listen_port"
CONN_RO_LISTEN_PORT = "connector_readonly_listen_port"
CONN_CLIENTLOGIN = "connector_user"
CONN_CLIENTPASSWORD = "connector_password"
CONN_CLIENTDEFAULTDB = "connector_default_schema"
CONN_DB_PROTOCOL = "connector_db_protocol"
CONN_DB_VERSION = "connector_db_version"
CONN_DELETE_USER_MAP = "connector_delete_user_map"
CONN_AUTORECONNECT = "connector_autoreconnect"
CONN_RWSPLITTING = "connector_rwsplitting"
CONN_SMARTSCALE = "connector_smartscale"
CONN_SMARTSCALE_SESSIONID = "connector_smartscale_sessionid"
CONN_DRIVEROPTIONS = "connector_driver_options"
CONN_SLAVE_STATUS_IS_RELATIVE = "connector_relative_slave_status"
CONN_PASSWORD_LINES = "connector_password_lines"
CONN_DIRECT_LINES = "connector_direct_lines"
CONN_RW_ADDRESSES = "connector_rw_addresses"
CONN_RO_ADDRESSES = "connector_ro_addresses"
ROUTER_JMX_PORT = "router_jmx_port"
ROUTER_GATEWAY_PORT = "router_gateway_port"
ROUTER_GATEWAY_RETURN_PORT = "router_gateway_return_port"
ROUTER_WAITFOR_DISCONNECT_TIMEOUT = "connector_disconnect_timeout"
ROUTER_KEEP_ALIVE_TIMEOUT = "connector_keepalive_timeout"
ROUTER_DELAY_BEFORE_OFFLINE = "connector_delay_before_offline"
CONN_JAVA_MEM_SIZE = "conn_java_mem_size"
CONN_JAVA_ENABLE_CONCURRENT_GC = "conn_java_enable_concurrent_gc"
CONN_RR_INCLUDE_MASTER = "conn_round_robin_include_master"
ENABLE_CONNECTOR_SSL = "enable_connector_ssl"
ENABLE_CONNECTOR_CLIENT_SSL = "enable_connector_client_ssl"
ENABLE_CONNECTOR_SERVER_SSL = "enable_connector_server_ssl"
JAVA_CONNECTOR_KEYSTORE_PASSWORD = "java_connector_keystore_password"
JAVA_CONNECTOR_TRUSTSTORE_PASSWORD = "java_connector_truststore_password"
JAVA_CONNECTOR_TRUSTSTORE_PATH = "java_connector_truststore_path"
GLOBAL_JAVA_CONNECTOR_TRUSTSTORE_PATH = "global_java_connector_truststore_path"
JAVA_CONNECTOR_KEYSTORE_PATH = "java_connector_keystore_path"
GLOBAL_JAVA_CONNECTOR_KEYSTORE_PATH = "global_java_connector_keystore_path"
ENABLE_CONNECTOR_RO = "enable_connector_readonly"
ENABLE_CONNECTOR_BRIDGE_MODE = "enable_connector_bridge_mode"
CONN_AFFINITY = "connector_affinity"
CONN_RO_PROPERTIES_EXISTS = "connector_ro_properties_exists"
CONN_MAX_SLAVE_LATENCY = "connector_max_slave_latency"
CONN_MAX_CONNECTIONS = "connector_max_connections"
CONN_DROP_AFTER_MAX_CONNECTIONS = "connector_drop_after_max_connections"

class Connectors < GroupConfigurePrompt
  def initialize
    super(CONNECTORS, "Enter connector information for @value", 
      "connector", "connectors", "CONNECTOR")
      
    ConnectorPrompt.subclasses().each{
      |klass|
      self.add_prompt(klass.new())
    }
  end
end

module ConnectorPrompt
  include GroupConfigurePromptMember
  include HashPromptDefaultsModule
  include NoReplicatorRestart
  include NoManagerRestart
  include CommercialPrompt
  
  def self.included(subclass)
    @subclasses ||= []
    @subclasses << subclass
  end

  def self.subclasses
    @subclasses || []
  end
  
  def allow_group_default
    true
  end
  
  def get_dataservice()
    if self.is_a?(HashPromptMemberModule)
      get_member()
    else
      ds_aliases =@config.getPropertyOr(get_member_key(DEPLOYMENT_DATASERVICE), [])
      if ! ds_aliases.kind_of?(Array)
         ds_aliases=Array(ds_aliases);
      end  
      ds_aliases.at(0)
    end
  end
  
  def get_repl_service_key(dataservice, key)
    repl_service_alias = "#{dataservice}_#{@config.getProperty(get_member_key(DEPLOYMENT_HOST))}"
    unless @config.getPropertyOr([REPL_SERVICES], {}).keys().include?(repl_service_alias)
      return nil
    end
    
    [REPL_SERVICES, repl_service_alias, key]
  end
  
  def get_dataservice_key(key)
    return [DATASERVICES, get_dataservice(), key]
  end
  
  def get_host_alias
    @config.getProperty(get_member_key(DEPLOYMENT_HOST))
  end
  
  def get_host_key(key)
    [HOSTS, @config.getProperty(get_member_key(DEPLOYMENT_HOST)), key]
  end
  
  def get_hash_prompt_key
    return [DATASERVICE_CONNECTOR_OPTIONS, get_dataservice(), @name]
  end
end

class ConnectorDeploymentHost < ConfigurePrompt
  include ConnectorPrompt
  include ConstantValueModule
  include NoTemplateValuePrompt

  def initialize
    super(DEPLOYMENT_HOST, 
      "On what host would you like to deploy this connector?", 
      PV_IDENTIFIER)
  end
  
  def load_default_value
    @default = @config.getProperty(DEPLOYMENT_HOST)
  end
  
  def is_valid?
    super()
    
    unless @config.getProperty(HOSTS).has_key?(get_value())
      raise ConfigurePromptError.new(self, "Host #{get_value()} does not exist in the configuration file", get_value())
    end
  end
end

class ConnectorDataservice < ConfigurePrompt
  include ConnectorPrompt
  include ConstantValueModule
  include NoTemplateValuePrompt
  
  def initialize
    super(DEPLOYMENT_DATASERVICE, "The dataaservice(s) used by this connector",
      PV_ANY)
  end
  
  def get_default_value
    @config.getProperty(DEPLOYMENT_DATASERVICE)
  end
  
  def is_valid?
    super()
    
    get_value().to_a().each{
      |ds_alias|
      
      unless @config.getProperty(DATASERVICES).has_key?(ds_alias)
        raise ConfigurePromptError.new(self, "Data service #{ds_alias} does not exist in the configuration file", get_value().to_a().join(","))
      end
    }
  end
end

class ConnectorLogin < ConfigurePrompt
  include ConnectorPrompt
  
  def initialize
    super(CONN_CLIENTLOGIN, "Database username for the connector", PV_ANY)
    override_command_line_argument("application-user")
  end
  
  def load_default_value
    repl_service_key = get_repl_service_key(get_dataservice(), REPL_DBLOGIN)
    
    if repl_service_key != nil
      @default = @config.getPropertyOr(repl_service_key, nil)
    else
      @default = nil
    end
    
    if @default.to_s == ""
      @default = @config.getNestedProperty([DATASERVICE_REPLICATION_OPTIONS, get_dataservice(), REPL_DBLOGIN])
    end
    
    if @default.to_s == ""
      @default = @config.getProperty([REPL_SERVICES, DEFAULTS, REPL_DBLOGIN])
    end
  end
end

class ConnectorPassword < ConfigurePrompt
  include ConnectorPrompt
  include PrivateArgumentModule
  
  def initialize
    super(CONN_CLIENTPASSWORD, "Database password for the connector", PV_ANY)
    override_command_line_argument("application-password")
  end
  
  def load_default_value
    repl_service_key = get_repl_service_key(get_dataservice(), REPL_DBPASSWORD)
    
    if repl_service_key != nil
      @default = @config.getPropertyOr(repl_service_key, nil)
    else
      @default = nil
    end
    
    if @default.to_s == ""
      @default = @config.getNestedProperty([DATASERVICE_REPLICATION_OPTIONS, get_dataservice(), REPL_DBPASSWORD])
    end

    if @default.to_s == ""
      @default = @config.getProperty([REPL_SERVICES, DEFAULTS, REPL_DBPASSWORD])
    end
  end
end

class ConnectorListenInterface < ConfigurePrompt
  include ConnectorPrompt
  include AdvancedPromptModule
  
  def initialize
    super(CONN_LISTEN_INTERFACE, "Listen interface to use for the connector", 
      PV_ANY)
  end
  
  def required?
    false
  end
end

class ConnectorListenAddress < ConfigurePrompt
  include ConnectorPrompt
  include ConstantValueModule
  include NoSystemDefault
  
  def initialize
    super(CONN_LISTEN_ADDRESS, "Listen address to use for the connector", 
      PV_ANY)
  end
  
  def load_default_value
    if (iface = @config.getProperty(get_member_key(CONN_LISTEN_INTERFACE))) != nil
      @default = Configurator.instance.get_interface_address(iface)
    else
      @default = "0.0.0.0"
    end
  end
end

class ConnectorListenPort < ConfigurePrompt
  include ConnectorPrompt
  include NoConnectorReconfigure
  
  def initialize
    super(CONN_LISTEN_PORT, "Port for the connector to listen on", PV_INTEGER, "9999")
    override_command_line_argument("application-port")
  end
  
  def get_template_value
    if @config.getTemplateValue(get_member_key(ENABLE_CONNECTOR_RO)) == "true"
      @config.getPropertyOr(get_member_key(CONN_RO_LISTEN_PORT), get_value())
    else
      get_value()
    end
  end
  
  PortForUsers.register(CONNECTORS, CONN_LISTEN_PORT)
end

class ConnectorReadOnlyListenPort < ConfigurePrompt
  include ConnectorPrompt
  include NoConnectorReconfigure
  
  def initialize
    super(CONN_RO_LISTEN_PORT, "Port for the connector to listen on for read-only connections", PV_INTEGER)
    override_command_line_argument("application-readonly-port")
  end
  
  def required?
    false
  end 
  
  PortForUsers.register(CONNECTORS, CONN_RO_LISTEN_PORT)
end

class ConnectorDefaultSchema < ConfigurePrompt
  include ConnectorPrompt
  
  def initialize
    super(CONN_CLIENTDEFAULTDB, "Default schema for the connector to use", PV_ANY, "none")
    add_command_line_alias("connector-forced-schema")
  end
end

class ConnectorDBProtocol < ConfigurePrompt
  include ConnectorPrompt
  include ConstantValueModule
  
  def initialize
    super(CONN_DB_PROTOCOL, "DB protocol for the connector to use", PV_ANY)
  end
  
  def load_default_value
    ds_alias = get_dataservice()
    @config.getPropertyOr([REPL_SERVICES], {}).each_key{
      |rs_alias|
      
      if @config.getProperty([REPL_SERVICES, rs_alias, DEPLOYMENT_DATASERVICE]) == ds_alias
        dbtype = @config.getProperty([REPL_SERVICES, rs_alias, REPL_DBTYPE])
        case dbtype
        when DBMS_POSTGRESQL_WAL
          @default = "postgresql"
        else
          @default = dbtype
        end
        return
      end
    }
    
    @default = @config.getProperty([REPL_SERVICES, DEFAULTS, REPL_DBTYPE]).to_s()
  end
end

class ConnectorDBVersion < ConfigurePrompt
  include ConnectorPrompt
  include HiddenValueModule
  
  def initialize
    super(CONN_DB_VERSION, "DB version for the connector to display", PV_ANY, "autodetect")
  end
end

class ConnectorOverwriteUserMap < ConfigurePrompt
  include ConnectorPrompt
  include AdvancedPromptModule
  include NoStoredConfigValue
  
  def initialize
    super(CONN_DELETE_USER_MAP, "Overwrite an existing user.map file", PV_BOOLEAN, "false")
  end
end

class ConnectorUserMapPasswordLines < ConfigurePrompt
  include ConnectorPrompt
  include ConstantValueModule
  
  def initialize
    super(CONN_PASSWORD_LINES, "Connector user.map password lines", PV_ANY, "")
  end
  
  def get_template_value
    ds_alias = get_dataservice()
    composite_ds_alias = nil
    @config.getPropertyOr(DATASERVICES, {}).keys().each{
      |ds|
      
      unless include_dataservice?(ds)
        next
      end
      
      unless @config.getProperty([DATASERVICES, ds, DATASERVICE_IS_COMPOSITE]) == "true"
        next
      end
      
      if @config.getProperty([DATASERVICES, ds, DATASERVICE_COMPOSITE_DATASOURCES]).to_s().split(",").include?(ds_alias)
        composite_ds_alias = ds
      end
    }
    
    lines = []
    
    if composite_ds_alias
      lines << "#{@config.getProperty(CONN_CLIENTLOGIN)} #{@config.getPropertyOr(CONN_CLIENTPASSWORD, "-")} #{composite_ds_alias} #{ds_alias}"
    else
      lines << "#{@config.getProperty(CONN_CLIENTLOGIN)} #{@config.getPropertyOr(CONN_CLIENTPASSWORD, "-")} #{ds_alias}"
    end
    
    return lines.join("\n")
  end
end

class ConnectorUserMapDirectLines < ConfigurePrompt
  include ConnectorPrompt
  include ConstantValueModule
  
  def initialize
    super(CONN_DIRECT_LINES, "Connector user.map @direct lines", PV_ANY, "")
  end
  
  def get_template_value
    if @config.getProperty(get_member_key(CONN_RWSPLITTING)) == "true"
      prefix = ""
    else
      prefix = "#"
    end
    
    lines = []
    
    lines << "#{prefix}@direct #{@config.getProperty(CONN_CLIENTLOGIN)}"
    
    return lines.join("\n")
  end
end

class ConnectorRWAddresses < ConfigurePrompt
  include ConnectorPrompt
  
  def initialize
    super(CONN_RW_ADDRESSES, "Connector addresses that should receive a r/w connection", PV_ANY, "")
  end
  
  def get_template_value
    lines = []
    
    get_value().to_s().split(",").each{
      |rw_address|
      if rw_address == ""
        next
      end
      
      lines << "@hostoption #{rw_address} qos=RW_STRICT"
    }
    
    return lines.join("\n")
  end
end

class ConnectorROAddresses < ConfigurePrompt
  include ConnectorPrompt

  def initialize
    super(CONN_RO_ADDRESSES, "Connector addresses that should receive a r/o connection", PV_ANY, "")
  end
  
  def get_template_value
    lines = []
    
    get_value().to_s().split(",").each{
      |ro_address|
      if ro_address == ""
        next
      end
      
      lines << "@hostoption #{ro_address} qos=RO_RELAXED"
    }
    
    return lines.join("\n")
  end
end

class ConnectorRWSplitting < ConfigurePrompt
  include ConnectorPrompt
  
  def initialize
    super(CONN_RWSPLITTING, "Enable DirectReads R/W splitting in the connector", PV_BOOLEAN, "false")
  end
end

class ConnectorSmartScale < ConfigurePrompt
  include ConnectorPrompt
  
  def initialize
    super(CONN_SMARTSCALE, "Enable SmartScale R/W splitting in the connector", PV_BOOLEAN, "false")
  end
  
  def get_template_value
    if @config.getTemplateValue(get_member_key(ENABLE_CONNECTOR_RO)) == "true"
      "false"
    else
      super()
    end
  end
end

class ConnectorSmartScaleSession < ConfigurePrompt
  include ConnectorPrompt

  def initialize
    super(CONN_SMARTSCALE_SESSIONID, "The default session ID to use with smart scale", PropertyValidator.new("DATABASE|USER|CONNECTION|PROVIDED_IN_DBNAME", 
        "Value must be either of DATABASE, USER, CONNECTION, PROVIDED_IN_DBNAME"), "DATABASE")
  end
  
  def accept?(raw_value)
    if (@config.getProperty(get_member_key(CONN_SMARTSCALE)) == "true")
      @validator = PropertyValidator.new("DATABASE|USER|CONNECTION|PROVIDED_IN_DBNAME", 
        "Value must be either of DATABASE, USER, CONNECTION, PROVIDED_IN_DBNAME")
    else
      @validator = PropertyValidator.new("DATABASE|USER|CONNECTION|PROVIDED_IN_DBNAME|", 
        "Value must be either of DATABASE, USER, CONNECTION, PROVIDED_IN_DBNAME")
    end
    
    return super(raw_value)
  end
end

class ConnectorAutoReconnect < ConfigurePrompt
  include ConnectorPrompt
  
  def initialize
    super(CONN_AUTORECONNECT, "Enable auto-reconnect in the connector", PV_BOOLEAN, "true")
  end
end

class ConnectorRelativeSlaveStatus < ConfigurePrompt
  include ConnectorPrompt
  include HiddenValueModule
  
  def initialize
    super(CONN_SLAVE_STATUS_IS_RELATIVE, "Display slave status using relative time", PV_BOOLEAN)
  end
  
  def load_default_value
    @default = @config.getProperty(get_dataservice_key(DATASERVICE_USE_RELATIVE_LATENCY))
  end
end

class ConnectorDisconnectTimeout < ConfigurePrompt
  include ConnectorPrompt
  include HiddenValueModule
  
  def initialize
    super(ROUTER_WAITFOR_DISCONNECT_TIMEOUT, "Time to wait for active connection to disconnect before forcing them closed", PV_INTEGER, "5")
  end
end

class ConnectorKeepaliveTimeout < ConfigurePrompt
  include ConnectorPrompt
  include HiddenValueModule
  
  def initialize
    super(ROUTER_KEEP_ALIVE_TIMEOUT, "Time to wait for a manager to respond to a keep-alive request", PV_INTEGER)
  end
  
  def load_default_value
    host = @config.getProperty(get_member_key(DEPLOYMENT_HOST))
    
    @default = 30000
    @config.getPropertyOr(MANAGERS, {}).keys().each{
      |m_alias|
      if (@config.getProperty([MANAGERS, m_alias, DEPLOYMENT_HOST]) == host)
        mgr_interval = @config.getPropertyOr([MANAGERS, m_alias, MGR_MONITOR_INTERVAL], 0).to_i
        @default = [@default, 3*mgr_interval].max()
      end
    }
  end
end

class ConnectorDelayBeforeOffline < ConfigurePrompt
  include ConnectorPrompt
  include HiddenValueModule
  
  def initialize
    super(ROUTER_DELAY_BEFORE_OFFLINE, "Time to wait to take a router offline after losing the connection to a manager", PV_INTEGER, "30")
  end
end

class RouterGatewayPort < ConfigurePrompt
  include ConnectorPrompt
  include AdvancedPromptModule
  include NoConnectorReconfigure
  
  def initialize
    super(ROUTER_GATEWAY_PORT, "The router gateway port", PV_INTEGER, "11999")
  end
  
  PortForManagers.register(CONNECTORS, ROUTER_GATEWAY_PORT, ROUTER_GATEWAY_RETURN_PORT)
  PortForConnectors.register(CONNECTORS, ROUTER_GATEWAY_PORT, ROUTER_GATEWAY_RETURN_PORT)
end

class RouterGatewayReturnPort < ConfigurePrompt
  include ConnectorPrompt
  include HiddenValueModule
  include NoConnectorReconfigure
  
  def initialize
    super(ROUTER_GATEWAY_RETURN_PORT, "The router gateway return port", PV_INTEGER)
  end
  
  def load_default_value
    @default = (@config.getProperty(get_member_key(ROUTER_GATEWAY_PORT)).to_i() + 1).to_s
  end
end

class RouterJMXPort < ConfigurePrompt
  include ConnectorPrompt
  include AdvancedPromptModule
  include NoConnectorReconfigure
  
  def initialize
    super(ROUTER_JMX_PORT, "The router jmx port", PV_INTEGER, "10999")
  end
  
  PortForManagers.register(CONNECTORS, ROUTER_JMX_PORT)
end

class ConnectorJavaMemorySize < ConfigurePrompt
  include ConnectorPrompt
  include AdvancedPromptModule
  include NoConnectorReconfigure
  
  def initialize
    super(CONN_JAVA_MEM_SIZE, "Connector Java heap memory size in Mb (min 128)",
      PV_INTEGER, 256)
  end
end

class ConnectorJavaGarbageCollection < ConfigurePrompt
  include ConnectorPrompt
  include AdvancedPromptModule
  include NoConnectorReconfigure
  
  def initialize
    super(CONN_JAVA_ENABLE_CONCURRENT_GC, "Connector Java uses concurrent garbage collection",
      PV_BOOLEAN, "false")
  end
  
  def get_template_value
    if get_value() == "true"
      ""
    else
      "#"
    end
  end
end

class ConnectorRoundRobinIncludeMaster < ConfigurePrompt
  include ConnectorPrompt
  include AdvancedPromptModule
  
  def initialize
    super(CONN_RR_INCLUDE_MASTER, "Should the Connector include the master in round-robin load balancing",
      PV_BOOLEAN, "false")
  end
end

class ConnectorDriverOptions < ConfigurePrompt
  include ConnectorPrompt
  include HiddenValueModule
  
  def initialize
    super(CONN_DRIVEROPTIONS, "JDBC options for connector JDBC connections to the database", PV_ANY)
  end
  
  def load_default_value
    opts = []
    
    # Each prompt that has registered via ConnectorDriverOptions.register
    # will be called and can add options to the array
    self.class.prompts().each{
      |name|
      p = @config.getPromptHandler().find_prompt(get_member_key(name))
      if p
        p.add_jdbc_driver_options(opts)
      end
    }
    
    if opts.size() == 0
      @default = ""
    else
      @default = "?#{opts.join('&')}"
    end
  end
  
  def get_template_value
    v = get_value()
    if @config.getTemplateValue(get_member_key(ENABLE_CONNECTOR_RO)) == "true"
      if v == ""
        return "?qos=RO_RELAXED"
      else
        return "#{v}&qos=RO_RELAXED"
      end
    else
      return v
    end
  end
  
  def self.register(klass)
    @driver_prompts ||= []
    @driver_prompts << klass
  end
  
  def self.prompts
    @driver_prompts || []
  end
end

class ConnectorEnableSSL < ConfigurePrompt
  include ConnectorPrompt
  
  def initialize
    super(ENABLE_CONNECTOR_SSL, "Enable SSL encryption of connector traffic to the database", PV_BOOLEAN, "false")
    add_command_line_alias("connector-ssl")
  end
end

class ConnectorEnableClientSSL < ConfigurePrompt
  include ConnectorPrompt
  
  def initialize
    super(ENABLE_CONNECTOR_CLIENT_SSL, "Enable SSL encryption of traffic from the client to the connector", PV_BOOLEAN)
    add_command_line_alias("connector-client-ssl")
  end
  
  def load_default_value
    if @config.getProperty(get_member_key(ENABLE_CONNECTOR_SSL)) == "true"
      @default = "true"
    else
      super()
    end
  end
  
  def get_template_value
    value = "false"
    
    if @config.getProperty(get_member_key(ENABLE_CONNECTOR_CLIENT_SSL)) == "true"
      value = "true"
    end
    
    if @config.getProperty(get_member_key(ENABLE_CONNECTOR_SERVER_SSL)) == "true"
      value = "true"
    end
    
    value
  end
  
  def required?
    false
  end
end

class ConnectorEnableServerSSL < ConfigurePrompt
  include ConnectorPrompt
  
  def initialize
    super(ENABLE_CONNECTOR_SERVER_SSL, "Enable SSL encryption of traffic from the connector to the database", PV_BOOLEAN)
    add_command_line_alias("connector-server-ssl")
  end
  
  def load_default_value
    if @config.getProperty(get_member_key(ENABLE_CONNECTOR_SSL)) == "true"
      @default = "true"
    else
      super()
    end
  end
  
  def required?
    false
  end
  
  def add_jdbc_driver_options(opts)
    if get_value() == "true"
      opts << "useSSL=true"
    end
  end
  
  ConnectorDriverOptions.register(ENABLE_CONNECTOR_SERVER_SSL)
end

class ConnectorJavaKeystorePassword < ConfigurePrompt
  include ConnectorPrompt
  include PrivateArgumentModule
  
  def initialize
    super(JAVA_CONNECTOR_KEYSTORE_PASSWORD, "The password for unlocking the tungsten_connector_keystore.jks file in the security directory", PV_ANY)
  end
  
  def load_default_value
    @default = @config.getProperty(get_host_key(JAVA_KEYSTORE_PASSWORD))
  end
end

class ConnectorJavaTruststorePassword < ConfigurePrompt
  include ConnectorPrompt
  include PrivateArgumentModule
  
  def initialize
    super(JAVA_CONNECTOR_TRUSTSTORE_PASSWORD, "The password for unlocking the tungsten_connector_truststore.jks file in the security directory", PV_ANY)
  end
  
  def load_default_value
    @default = @config.getProperty(get_host_key(JAVA_TRUSTSTORE_PASSWORD))
  end
end

class ConnectorJavaKeystorePath < ConfigurePrompt
  include ConnectorPrompt
  include NoStoredServerConfigValue
  
  def initialize
    super(JAVA_CONNECTOR_KEYSTORE_PATH, "Local path to the Java Connector Keystore file.", PV_FILENAME)
  end
  
  def get_template_value
    @config.getProperty(get_host_key(SECURITY_DIRECTORY)) + "/tungsten_connector_keystore.jks"
  end
  
  def required?
    false
  end
  
  def validate_value(value)
    super(value)
    if is_valid?() && value != ""
      unless File.exists?(value)
        error("The file #{value} does not exist")
      end
    end
    
    is_valid?()
  end
  
  DeploymentFiles.register(JAVA_CONNECTOR_KEYSTORE_PATH, GLOBAL_JAVA_CONNECTOR_KEYSTORE_PATH, CONNECTORS)
end

class GlobalConnectorJavaKeystorePath < ConfigurePrompt
  include ConnectorPrompt
  include ConstantValueModule
  include NoStoredServerConfigValue
  
  def initialize
    super(GLOBAL_JAVA_CONNECTOR_KEYSTORE_PATH, "Staging path to the Java Connector Keystore file", 
      PV_FILENAME)
  end
end

class ConnectorJavaTruststorePath < ConfigurePrompt
  include ConnectorPrompt
  include NoStoredServerConfigValue
  
  def initialize
    super(JAVA_CONNECTOR_TRUSTSTORE_PATH, "Local path to the Java Connector Truststore file.", PV_FILENAME)
  end
  
  def get_template_value
    @config.getProperty(get_host_key(SECURITY_DIRECTORY)) + "/tungsten_connector_truststore.ts"
  end
  
  def required?
    false
  end
  
  def validate_value(value)
    super(value)
    if is_valid?() && value != ""
      unless File.exists?(value)
        error("The file #{value} does not exist")
      end
    end
    
    is_valid?()
  end
  
  DeploymentFiles.register(JAVA_CONNECTOR_TRUSTSTORE_PATH, GLOBAL_JAVA_CONNECTOR_TRUSTSTORE_PATH, CONNECTORS)
end

class GlobalConnectorJavaTruststorePath < ConfigurePrompt
  include ConnectorPrompt
  include ConstantValueModule
  include NoStoredServerConfigValue
  
  def initialize
    super(GLOBAL_JAVA_CONNECTOR_TRUSTSTORE_PATH, "Staging path to the Java Connector Truststore file", 
      PV_FILENAME)
  end
end

class ConnectorEnableReadOnly < ConfigurePrompt
  include ConnectorPrompt
  
  def initialize
    super(ENABLE_CONNECTOR_RO, "Enable the Tungsten Connector read-only mode", PV_BOOLEAN, "false")
    override_command_line_argument("connector-readonly")
  end
  
  def get_template_value
    if get_value() == "true" || ConfigureDeploymentStepConnector.connector_ro_mode?() == true
      "true"
    else
      "false"
    end
  end
end

class ConnectorEnableBridgeMode < ConfigurePrompt
  include ConnectorPrompt
  
  def initialize
    super(ENABLE_CONNECTOR_BRIDGE_MODE, "Enable the Tungsten Connector bridge mode", PV_BOOLEAN, "false")
    override_command_line_argument("connector-bridge-mode")
  end
  
  def get_template_value
    if get_value() == "true"
      if @config.getTemplateValue(get_member_key(ENABLE_CONNECTOR_RO)) == "true"
        "RO_RELAXED"
      else
        "RW_STRICT"
      end
    else
      "OFF"
    end
  end
end

class ConnectorReadOnlyPropertiesExists < ConfigurePrompt
  include ConnectorPrompt
  include ConstantValueModule
  include NoStoredServerConfigValue
  
  def initialize
    super(CONN_RO_PROPERTIES_EXISTS, "Enable the second Tungsten Connector listeners", PV_BOOLEAN, "false")
  end
  
  def get_template_value
    if File.exist?("#{@config.getProperty(PREPARE_DIRECTORY)}/tungsten-connector/conf/connector.ro.properties") == true
      "true"
    else
      ""
    end
  end
end

class ConnectorMaxSlaveLatency < ConfigurePrompt
  include ConnectorPrompt
  
  def initialize
    super(CONN_MAX_SLAVE_LATENCY, "The maximum applied latency for slave connections", PV_INTEGER)
    add_command_line_alias("connector-max-applied-latency")
  end
  
  def required?
    false
  end
end

class ConnectorAffinity < ConfigurePrompt
  include ConnectorPrompt
  
  def initialize
    super(CONN_AFFINITY, "The default affinity for all connections", PV_ANY)
  end
  
  def required?
    false
  end
  
  def add_jdbc_driver_options(opts)
    if get_value() != nil
      opts << "affinity=#{get_value}"
    end
  end
  
  def get_template_value
    if @config.getTemplateValue(get_member_key(ENABLE_CONNECTOR_RO)) == "true"
      super()
    else
      ""
    end
  end
  
  ConnectorDriverOptions.register(CONN_AFFINITY)
end

class ConnectorMaxConnections < ConfigurePrompt
  include ConnectorPrompt
  
  def initialize
    super(CONN_MAX_CONNECTIONS, "The maximum number of connections the connector should allow at any time", PV_ANY_INTEGER, "-1")
  end
end

class ConnectorDropAfterMaxConnections < ConfigurePrompt
  include ConnectorPrompt
  
  def initialize
    super(CONN_DROP_AFTER_MAX_CONNECTIONS, "Instantly drop connections that arrive after --connector-max-connections has been reached", PV_BOOLEAN, "false")
  end
end