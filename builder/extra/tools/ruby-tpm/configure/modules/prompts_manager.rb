MANAGERS = "managers"
MGR_LISTEN_INTERFACE = "mgr_listen_interface"
MGR_LISTEN_ADDRESS = "mgr_listen_address"
MGR_POLICY_MODE = "mgr_policy_mode"
MGR_REPLICATOR_PROXY = "mgr_replicator_proxy"
MGR_VIP_ENABLED = "mgr_vip_enabled"
MGR_VIP_DEVICE = "mgr_vip_device"
MGR_VIP_IPADDRESS = "mgr_vip_ipaddress"
MGR_VIP_NETMASK = "mgr_vip_netmask"
MGR_VIP_IPADDRESS_NETMASK = "mgr_vip_ipaddress_netmask"
MGR_VIP_IFCONFIG_PATH = "mgr_vip_ifconfig_path"
MGR_VIP_ARP_PATH = "mgr_vip_arp_path"
MGR_RO_SLAVE = "mgr_ro_slave"
MGR_DB_PING_TIMEOUT = "mgr_db_ping_timeout"
MON_DB_QUERY_TIMEOUT = "mon_db_query_timeout"
MGR_HOST_PING_TIMEOUT = "mgr_host_ping_timeout"
MGR_POLICY_LIVENESS_SAMPLE_PERIOD_SECS = "mgr_policy_liveness_sample_period_seconds"
MGR_POLICY_LIVENESS_SAMPLE_PERIOD_THRESHOLD = "mgr_policy_liveness_sample_period_threshold"
MGR_POLICY_FENCE_SLAVE_REPLICATOR = "mgr_policy_fence_slave"
MGR_POLICY_FENCE_MASTER_REPLICATOR = "mgr_policy_fence_master"
MGR_POLICY_NOTIFICATION_ADJUST_AUTO = "mgr_policy_notification_auto_adjust"
MGR_POLICY_NOTIFICATION_ADJUST_BACKOFF = "mgr_policy_notification_adjust_backoff"
MGR_POLICY_NOTIFICATION_MAX_TIMEOUT = "mgr_policy_notification_max_timeout"
MGR_POLICY_NOTIFICATION_ADJUST_THRESHOLD = "mgr_policy_notification_adjust_threshold"
MGR_POLICY_SUCCESSFUL_NOTIFICATION_ADJUST_THRESHOLD = "mgr_policy_successful_notification_adjust_threshold"
MGR_POLICY_NOTIFICATION_BACKOFF_TRY_THRESHOLD = "mgr_policy_notification_backoff_try_threshold"
MGR_NOTIFICATIONS_TIMEOUT = "mgr_notifications_timeout"
MGR_NOTIFICATIONS_SEND = "mgr_notifications_send"
MGR_IDLE_ROUTER_TIMEOUT = "mgr_idle_router_timeout"
MGR_POLICY_FAIL_THRESHOLD = "mgr_policy_fail_threshold"
MGR_ROUTER_STATUS_TIMEOUT = "mgr_router_status_timeout"
MGR_GROUP_PROTOCOL = "mgr_group_communication_protocol"
MGR_GROUP_PROTOCOL_PING = "ping"
MGR_GROUP_COMMUNICATION_PORT = "mgr_group_communication_port"
MGR_GROUP_COMMUNICATION_NUM_INITIAL_HOSTS = "mgr_group_communication_num_initial_hosts"
MGR_GROUP_COMMUNICATION_INITIAL_HOSTS = "mgr_group_communication_initial_hosts"
MGR_GROUP_COMMUNICATION_CONFIG = "mgr_group_communication_config"
MGR_RMI_PORT = "mgr_rmi_port"
MGR_RMI_REMOTE_PORT = "mgr_rmi_remote_port"
MGR_MONITOR_INTERVAL = "mgr_monitor_interval"
MGR_WAIT_FOR_MEMBERS = "mgr_wait_for_members"
MANAGER_ENABLE_INSTRUMENTATION = "manager_enable_instrumentation"
MGR_JAVA_MEM_SIZE = "mgr_java_mem_size"
MGR_JAVA_ENABLE_CONCURRENT_GC = "mgr_java_enable_concurrent_gc"
MGR_HEAP_THRESHOLD = "mgr_heap_threshold"
MGR_API = "mgr_api"
MGR_API_PORT = "mgr_api_port"
MGR_API_ADDRESS = "mgr_api_address"
MGR_VALIDATE_WITNESS = "mgr_validate_witness"
MGR_IS_WITNESS = "mgr_is_witness"
MGR_REPL_DBLOGIN = "mgr_repl_user"
MGR_REPL_DBPASSWORD = "mgr_repl_password"
MGR_REPL_DBPORT = "mgr_repl_port"
MGR_REPL_SCHEMA = "mgr_repl_schema"
MGR_REPL_RMI_PORT = "mgr_repl_rmi_port"
MGR_REPL_JDBC_DRIVER = "mgr_repl_jdbc_driver"
MGR_PING_METHOD = "mgr_ping_method"

class Managers < GroupConfigurePrompt
  def initialize
    super(MANAGERS, "Enter manager information for @value", 
      "manager", "managers", "MANAGER")
      
    ManagerPrompt.subclasses().each{
      |klass|
      self.add_prompt(klass.new())
    }
  end
end

module ManagerPrompt
  include GroupConfigurePromptMember
  include HashPromptDefaultsModule
  include NoConnectorRestart
  include NoReplicatorRestart
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
  
  def get_host_alias
    @config.getProperty(get_member_key(DEPLOYMENT_HOST))
  end
  
  def get_host_key(key)
    [HOSTS, @config.getProperty(get_member_key(DEPLOYMENT_HOST)), key]
  end
  
  def get_dataservice
    if self.is_a?(HashPromptMemberModule)
      get_member()
    else
      @config.getProperty(get_member_key(DEPLOYMENT_DATASERVICE))
    end
  end
  
  def get_dataservice_key(key)
    [DATASERVICES, get_dataservice(), key]
  end
  
  def get_topology
    Topology.build(get_dataservice(), @config)
  end
  
  def get_hash_prompt_key
    return [DATASERVICE_MANAGER_OPTIONS, get_dataservice(), @name]
  end
end

class ManagerDeploymentHost < ConfigurePrompt
  include ManagerPrompt
  include ConstantValueModule
  include NoTemplateValuePrompt

  def initialize
    super(DEPLOYMENT_HOST, 
      "On what host would you like to deploy this manager?", 
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

class ManagerDataservice < ConfigurePrompt
  include ManagerPrompt
  include ConstantValueModule
  include NoTemplateValuePrompt
  
  def initialize
    super(DEPLOYMENT_DATASERVICE, "The dataaservice used by this manager",
      PV_ANY)
  end
  
  def load_default_value
    @default = @config.getProperty(DEPLOYMENT_DATASERVICE)
  end
  
  def is_valid?
    super()
    
    unless @config.getProperty(DATASERVICES).has_key?(get_value())
      raise ConfigurePromptError.new(self, "Data service #{get_value()} does not exist in the configuration file", get_value())
    end
  end
end

class ManagerVIPEnabled < ConfigurePrompt
  include ManagerPrompt
  include ConstantValueModule
  
  def initialize
    super(MGR_VIP_ENABLED, "Is VIP management enabled?", PV_BOOLEAN)
  end
  
  def load_default_value
    @default = @config.getProperty(get_dataservice_key(DATASERVICE_VIP_ENABLED))
  end
end

class ManagerVIPIPAddress < ConfigurePrompt
  include ManagerPrompt
  include ConstantValueModule
  
  def initialize
    super(MGR_VIP_IPADDRESS, "VIP IP address", PV_ANY)
  end
  
  def enabled?
    super() && (@config.getProperty(get_member_key(MGR_VIP_ENABLED)) == "true")
  end
  
  def load_default_value
    @default = @config.getProperty(get_dataservice_key(DATASERVICE_VIP_IPADDRESS))
  end
end

class ManagerVIPNetmask < ConfigurePrompt
  include ManagerPrompt
  include ConstantValueModule
  
  def initialize
    super(MGR_VIP_NETMASK, "VIP netmask", PV_ANY, "255.255.255.0")
  end
  
  def enabled?
    super() && (@config.getProperty(get_member_key(MGR_VIP_ENABLED)) == "true")
  end
  
  def load_default_value
    @default = @config.getProperty(get_dataservice_key(DATASERVICE_VIP_NETMASK))
  end
end

class ManagerVIPDevice < ConfigurePrompt
  include ManagerPrompt
  include AdvancedPromptModule
    
  def initialize
    super(MGR_VIP_DEVICE, "VIP network device", PV_ANY)
  end
  
  def enabled?
    super() && (@config.getProperty(get_member_key(MGR_VIP_ENABLED)) == "true")
  end
end

class ManagerVIPIPAddressNetmask < ConfigurePrompt
  include ManagerPrompt
  include ConstantValueModule
  
  def initialize
    super(MGR_VIP_IPADDRESS_NETMASK, "VIP IP address and netmask together", PV_ANY)
  end
  
  def load_default_value
    if (@config.getProperty(get_member_key(MGR_VIP_ENABLED)) == "true")
      @default = "#{@config.getProperty(get_member_key(MGR_VIP_IPADDRESS))} netmask #{@config.getProperty(get_member_key(MGR_VIP_NETMASK))}"
    end
  end
  
  def enabled?
    super() && (@config.getProperty(get_member_key(MGR_VIP_ENABLED)) == "true")
  end
end

class ManagerIfconfigPath < ConfigurePrompt
  include ManagerPrompt
  
  def initialize
    super(MGR_VIP_IFCONFIG_PATH, "Path to the ifconfig binary", PV_ANY)
  end
  
  def enabled?
    super() && (@config.getProperty(get_member_key(MGR_VIP_ENABLED)) == "true")
  end
  
  def load_default_value
    begin
      path = ssh_result("which ifconfig 2>/dev/null", @config.getProperty(get_host_key(HOST)), @config.getProperty(get_host_key(USERID)))
    rescue CommandError
      path = ""
    end
    
    if path == ""
      path = find_command(["/sbin/ifconfig", "/usr/sbin/ifconfig", "/usr/local/sbin/ifconfig"],
        @config.getProperty(get_host_key(HOST)),
        @config.getProperty(get_host_key(USERID))).to_s
    end
    
    if path == ""
      path = "/sbin/ifconfig"
    end
    
    @default = path
  end
end

class ManagerArpPath < ConfigurePrompt
  include ManagerPrompt
  
  def initialize
    super(MGR_VIP_ARP_PATH, "Path to the arp binary", PV_ANY)
  end
  
  def enabled?
    super() && (@config.getProperty(get_member_key(MGR_VIP_ENABLED)) == "true")
  end
  
  def load_default_value
    begin
      path = ssh_result("which arp 2>/dev/null", @config.getProperty(get_host_key(HOST)), @config.getProperty(get_host_key(USERID)))
    rescue CommandError
      path = ""
    end
    
    if path == ""
      path = find_command(["/sbin/arp", "/usr/sbin/arp", "/usr/local/sbin/arp"],
        @config.getProperty(get_host_key(HOST)),
        @config.getProperty(get_host_key(USERID))).to_s
    end
    
    if path == ""
      path = "/sbin/arp"
    end
    
    @default = path
  end
end

class ManagerListenInterface < ConfigurePrompt
  include ManagerPrompt
  include AdvancedPromptModule
  
  def initialize
    super(MGR_LISTEN_INTERFACE, "Listen interface to use for the manager", 
      PV_ANY)
  end
  
  def required?
    false
  end
end

class ManagerListenAddress < ConfigurePrompt
  include ManagerPrompt
  include ConstantValueModule
  include NoSystemDefault
  
  def initialize
    super(MGR_LISTEN_ADDRESS, "Listen address to use for the manager", 
      PV_ANY)
  end
  
  def load_default_value
    if (iface = @config.getProperty(get_member_key(MGR_LISTEN_INTERFACE))) != nil
      @default = Configurator.instance.get_interface_address(iface)
    else
      if (host = @config.getProperty(get_host_key(HOST)))
        begin
          ip_addresses = Configurator.instance.get_ip_addresses(host)
          if ip_addresses && ip_addresses.size() > 0
            @default = ip_addresses[0]
          else
            @default = nil
          end
        rescue
          @default = nil
        end
      else
        @default = nil
      end
    end
  end
end

class ManagerReplicatorProxy < ConfigurePrompt
  include ManagerPrompt
  include ConstantValueModule
  
  def initialize
    super(MGR_REPLICATOR_PROXY, "", PV_ANY, "com.continuent.tungsten.manager.resource.proxy.ReplicatorManagerProxyImplOSS")
  end
end

class ManagerReadOnlySlaves < ConfigurePrompt
  include ManagerPrompt
  
  def initialize
    super(MGR_RO_SLAVE, "Make slaves read-only", PV_BOOLEAN, "true")
  end
end

class ManagerDBPingTimeout < ConfigurePrompt
  include ManagerPrompt
  include HiddenValueModule
  
  def initialize
    super(MGR_DB_PING_TIMEOUT, "Datasource ping timeout", PV_INTEGER, "15")
  end
end

class ManagerDBQueryTimeout < ConfigurePrompt
  include ManagerPrompt
  include HiddenValueModule
  
  def initialize
    super(MON_DB_QUERY_TIMEOUT, "Datasource query timeout", PV_INTEGER, "5")
  end
end

class ManagerHostPingTimeout < ConfigurePrompt
  include ManagerPrompt
  include HiddenValueModule
  
  def initialize
    super(MGR_HOST_PING_TIMEOUT, "Host ping timeout", PV_INTEGER, "2")
  end
end

class ManagerLivenessSamplePeriod < ConfigurePrompt
  include ManagerPrompt
  include HiddenValueModule
  
  def initialize
    super(MGR_POLICY_LIVENESS_SAMPLE_PERIOD_SECS, "How often to sample the policy manager", PV_INTEGER, "2")
  end
end

class ManagerLivenessSamplePeriodThreshold < ConfigurePrompt
  include ManagerPrompt
  include HiddenValueModule
  
  def initialize
    super(MGR_POLICY_LIVENESS_SAMPLE_PERIOD_THRESHOLD, "How many consecutive sample periods can pass without progress", PV_INTEGER, "30")
  end
end

class ManagerFenceSlave < ConfigurePrompt
  include ManagerPrompt
  include HiddenValueModule
  
  def initialize
    super(MGR_POLICY_FENCE_SLAVE_REPLICATOR, "Fence failed slave datasources", PV_BOOLEAN, "false")
  end
end

class ManagerFenceMaster < ConfigurePrompt
  include ManagerPrompt
  include HiddenValueModule
  
  def initialize
    super(MGR_POLICY_FENCE_MASTER_REPLICATOR, "Fency failed master datasources", PV_BOOLEAN, "false")
  end
end

class ManagerNotificationAutoAdjust < ConfigurePrompt
  include ManagerPrompt
  include HiddenValueModule
  
  def initialize
    super(MGR_POLICY_NOTIFICATION_ADJUST_AUTO, "Automatically determine optimal level for router notification timeouts", PV_BOOLEAN, "true")
  end
end

class ManagerNotificationAutoAdjustBackoff < ConfigurePrompt
  include ManagerPrompt
  include HiddenValueModule
  
  def initialize
    super(MGR_POLICY_NOTIFICATION_ADJUST_BACKOFF, "Interval to use in order to backoff router notification timeout", PV_INTEGER, "3600000")
  end
end

class ManagerNotificationMaxTimeout < ConfigurePrompt
  include ManagerPrompt
  include HiddenValueModule
  
  def initialize
    super(MGR_POLICY_NOTIFICATION_MAX_TIMEOUT, "The maximum possible timeout for notifications", PV_INTEGER, "5000")
  end
end

class ManagerNotificationAdjustThreshold < ConfigurePrompt
  include ManagerPrompt
  include HiddenValueModule
  
  def initialize
    super(MGR_POLICY_NOTIFICATION_ADJUST_THRESHOLD, "The number of notification delivery failures that will trigger an adjustment", PV_INTEGER, "5")
  end
end

class ManagerSuccessfulNotificationAdjustThreshold < ConfigurePrompt
  include ManagerPrompt
  include HiddenValueModule
  
  def initialize
    super(MGR_POLICY_SUCCESSFUL_NOTIFICATION_ADJUST_THRESHOLD, "The number of successful notification deliveries that will trigger a backoff", PV_INTEGER, "10")
  end
end

class ManagerNotificationBackoffThreshold < ConfigurePrompt
  include ManagerPrompt
  include HiddenValueModule
  
  def initialize
    super(MGR_POLICY_NOTIFICATION_BACKOFF_TRY_THRESHOLD, "The number of backoff attempts before the timeout will be leveled", PV_INTEGER, "10")
  end
end

class ManagerNotificationTimeout < ConfigurePrompt
  include ManagerPrompt
  include HiddenValueModule
  
  def initialize
    super(MGR_NOTIFICATIONS_TIMEOUT, "How long to wait for a router notification to be delivered", PV_INTEGER, "100")
  end
end

class ManagerForwardReplicatorNotifications < ConfigurePrompt
  include ManagerPrompt
  include HiddenValueModule
  
  def initialize
    super(MGR_NOTIFICATIONS_SEND, "Forward replicator notifications to routers", PV_BOOLEAN, "true")
  end
end

class ManagerIdleRouterTimeout < ConfigurePrompt
  include ManagerPrompt
  include HiddenValueModule
  
  def initialize
    super(MGR_IDLE_ROUTER_TIMEOUT, "Maximum number of seconds to wait for router answers", PV_INTEGER, "20")
  end
end

class ManagerFailThreshold < ConfigurePrompt
  include ManagerPrompt
  include HiddenValueModule
  
  def initialize
    super(MGR_POLICY_FAIL_THRESHOLD, "Number of allowed failed dispatch periods", PV_INTEGER, "0")
  end
end

class ManagerRouterTimeout < ConfigurePrompt
  include ManagerPrompt
  include HiddenValueModule
  
  def initialize
    super(MGR_ROUTER_STATUS_TIMEOUT, "Maximum number of seconds to wait for interactive router answers", PV_INTEGER, "2000")
  end
end

class ManagerMonitorInterval < ConfigurePrompt
  include ManagerPrompt
  include HiddenValueModule
  
  def initialize
    super(MGR_MONITOR_INTERVAL, "Time between monitor checking calls", PV_INTEGER, "3000")
  end
end



class ManagerGroupCommunicationProtocol < ConfigurePrompt
  include ManagerPrompt
  include ConstantValueModule
  
  def initialize
    super(MGR_GROUP_PROTOCOL, "Manager group communication protocol", PV_ANY, MGR_GROUP_PROTOCOL_PING)
  end
end

class ManagerPolicy < ConfigurePrompt
  include ManagerPrompt
  include AdvancedPromptModule
  
  def initialize
    super(MGR_POLICY_MODE, "Manager policy mode (manual|automatic|maintenance)", PropertyValidator.new("manual|automatic|maintenance", 
      "Value must be manual, maintenance or automatic"), "automatic")
  end
end

class ManagerGroupCommunicationPort < ConfigurePrompt
  include ManagerPrompt
  include AdvancedPromptModule
  
  def initialize
    super(MGR_GROUP_COMMUNICATION_PORT, "Port to use for manager group communication", PV_INTEGER, "7800")
  end
  
  PortForManagers.register(MANAGERS, MGR_GROUP_COMMUNICATION_PORT)
end

class ManagerGroupCommunicationNumHosts < ConfigurePrompt
  include ManagerPrompt
  include ConstantValueModule
  
  def initialize
    super(MGR_GROUP_COMMUNICATION_NUM_INITIAL_HOSTS, "Number of managers in the physical data service", PV_INTEGER)
  end
  
  def load_default_value
    @default = @config.getProperty(get_dataservice_key(DATASERVICE_MEMBERS)).split(',').size().to_s()
  end
end

class ManagerRMIPort < ConfigurePrompt
  include ManagerPrompt
  include AdvancedPromptModule
  
  def initialize
    super(MGR_RMI_PORT, "Port to use for the manager RMI server", PV_INTEGER, "9997")
  end
  
  PortForManagers.register(MANAGERS, MGR_RMI_PORT)
end

class ManagerRMIRemotePort < ConfigurePrompt
  include ManagerPrompt
  include AdvancedPromptModule
  
  def initialize
    super(MGR_RMI_REMOTE_PORT, "Port to use for calling the remote manager RMI server", PV_ANY_INTEGER, "12000")
  end
end

class ManagerGroupCommunicationInitialHosts < ConfigurePrompt
  include ManagerPrompt
  include ConstantValueModule
  
  def initialize
    super(MGR_GROUP_COMMUNICATION_INITIAL_HOSTS, "Initial hosts for seeding group communication", PV_ANY)
  end
  
  def get_template_value
    fill_ports_near_hosts(@config.getProperty(get_dataservice_key(DATASERVICE_MEMBERS)), @config.getProperty(get_member_key(MGR_GROUP_COMMUNICATION_PORT)))
  end
end

class ManagerGroupCommunicationConfig < ConfigurePrompt
  include ManagerPrompt
  include ConstantValueModule
  
  def initialize
    super(MGR_GROUP_COMMUNICATION_CONFIG, "Config file for the group communication protocol", PV_ANY)
  end
  
  def load_default_value
    case @config.getProperty(get_member_key(MGR_GROUP_PROTOCOL))
    when MGR_GROUP_PROTOCOL_PING
      @default = "/jgroups_tcp_ping.xml"
    end
  end
end

class ManagerJavaMemorySize < ConfigurePrompt
  include ManagerPrompt
  include AdvancedPromptModule
  
  def initialize
    super(MGR_JAVA_MEM_SIZE, "Manager Java heap memory size in MB",
      PV_INTEGER, 80)
  end
end

class ManagerJavaGarbageCollection < ConfigurePrompt
  include ManagerPrompt
  include AdvancedPromptModule
  
  def initialize
    super(MGR_JAVA_ENABLE_CONCURRENT_GC, "Manager Java uses concurrent garbage collection",
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

class ManagerHeapThreshold < ConfigurePrompt
  include ManagerPrompt
  include AdvancedPromptModule
  
  def initialize
    super(MGR_HEAP_THRESHOLD, "Java memory usage (MB) that will force a Manager restart",
      PV_INTEGER, 60)
  end
  
  def load_default_value
    @default = (@config.getProperty(get_member_key(MGR_JAVA_MEM_SIZE)).to_i() * 0.75).to_i()
  end
  
  def required?
    false
  end
end

class ManagerWaitForMembers < ConfigurePrompt
  include ManagerPrompt
  include AdvancedPromptModule
  
  def initialize
    super(MGR_WAIT_FOR_MEMBERS, "Wait for all datasources to be available before completing installation", 
      PV_BOOLEAN, "false")
  end
end

class ManagerEnableInstrumentation < ConfigurePrompt
  include ManagerPrompt
  include HiddenValueModule
  
  def initialize
    super(MANAGER_ENABLE_INSTRUMENTATION, 
      "Should we log instrumentation values for this manager?",
      PV_BOOLEAN)
  end
  
  def load_default_value
    @default = @config.getProperty(get_dataservice_key(DATASERVICE_ENABLE_INSTRUMENTATION))
  end
end

class ManagerAPI < ConfigurePrompt
  include ManagerPrompt
  include AdvancedPromptModule
  
  def initialize
    super(MGR_API, "Enable the Manager API", PV_BOOLEAN, "true")
  end
end

class ManagerAPIPort < ConfigurePrompt
  include ManagerPrompt
  include AdvancedPromptModule
  
  def initialize
    super(MGR_API_PORT, "Port for the Manager API", PV_INTEGER, "8090")
  end
  
  PortForManagers.register(MANAGERS, MGR_API_PORT)
end

class ManagerAPIAddress < ConfigurePrompt
  include ManagerPrompt
  include AdvancedPromptModule
  
  def initialize
    super(MGR_API_ADDRESS, "Address for the Manager API", PV_ANY, "127.0.0.1")
  end
end

class ManagerPingMethod < ConfigurePrompt
  include ManagerPrompt
  
  def initialize
    validator = PropertyValidator.new("^echo|ping$", 
      "Value must be echo or ping")
    super(MGR_PING_METHOD, "Mechanism to use when identifying the liveness of other datasources (ping, echo)", validator)
  end
  
  def load_default_value
    successful_ping = []
    successful_echo = []
    
    # Generate a list of nodes that the manager may attempt to ping
    hosts = @config.getProperty(get_dataservice_key(DATASERVICE_MEMBERS)).split(",")
    if @config.getProperty(get_dataservice_key(ENABLE_ACTIVE_WITNESSES)) == "true"
      hosts = hosts + @config.getProperty(get_dataservice_key(DATASERVICE_WITNESSES)).split(",")
    end
    hosts = hosts.uniq()
    
    tping = "#{Configurator.instance.get_base_path()}/cluster-home/bin/tping"
    hosts.each{
      |h|
      begin
        Timeout.timeout(2) {
          ping_result = cmd_result("ping -c1 #{h} 2>/dev/null >/dev/null ; echo $?", true)
          if ping_result.to_i == 0
            successful_ping << h
          end
        }
      rescue Timeout::Error
      end
      
      begin
        Timeout.timeout(2) {
          echo_result = cmd_result("#{tping} #{h} 7 2000 2>/dev/null >/dev/null ; echo $?", true)
          if echo_result.to_i == 0
            successful_echo << h
          end
        }
      rescue Timeout::Error
      end
    }
    
    if successful_ping.size() == hosts.size()
      @default = "ping"
    elsif successful_echo.size() == hosts.size()
      @default = "echo"
    else
      raise "Unable to identify an available manager ping method. An ICMP Ping works with #{successful_ping.size()} hosts and TCP Echo works with #{successful_echo.size()} hosts."
    end
  end
end

class ManagerValidateWitness < ConfigurePrompt
  include ManagerPrompt
  include HiddenValueModule
  
  def initialize
    super(MGR_VALIDATE_WITNESS, "Validate the subnet for dataservice witnesses", PV_BOOLEAN, "true")
  end
end


class ManagerIsWitness < ConfigurePrompt
  include ManagerPrompt
  include HiddenValueModule
  
  def initialize
    super(MGR_IS_WITNESS, "Manager is an active witness", PV_BOOLEAN, "false")
  end
  
  def load_default_value
    if @config.getProperty(get_dataservice_key(ENABLE_ACTIVE_WITNESSES)) == "true"
      if @config.getPropertyOr(get_dataservice_key(DATASERVICE_WITNESSES)).include_alias?(get_host_alias())
        @default = "true"
      else
        @default = "false"
      end
    else
      @default = "false"
    end
  end
end

class ManagerReplicationDBUser < ConfigurePrompt
  include ManagerPrompt
  include HiddenValueModule
  
  def initialize
    super(MGR_REPL_DBLOGIN, "Database login for the manager to check replication hosts", PV_IDENTIFIER)
  end
  
  def load_default_value
    master = @config.getProperty(get_dataservice_key(DATASERVICE_MASTER_MEMBER)).split(",")[0]
    rs_alias = to_identifier("#{get_dataservice()}_#{master}")
    @default = @config.getProperty([REPL_SERVICES, rs_alias, REPL_DBLOGIN])
  end
end

class ManagerReplicationDBPassword < ConfigurePrompt
  include ManagerPrompt
  include HiddenValueModule
  
  def initialize
    super(MGR_REPL_DBPASSWORD, "Database password for the manager to check replication hosts", PV_ANY)
  end
  
  def load_default_value
    master = @config.getProperty(get_dataservice_key(DATASERVICE_MASTER_MEMBER)).split(",")[0]
    rs_alias = to_identifier("#{get_dataservice()}_#{master}")
    @default = @config.getProperty([REPL_SERVICES, rs_alias, REPL_DBPASSWORD])
  end
end

class ManagerReplicationDBPort < ConfigurePrompt
  include ManagerPrompt
  include HiddenValueModule
  
  def initialize
    super(MGR_REPL_DBPORT, "Database port for the manager to check replication hosts", PV_INTEGER)
  end
  
  def load_default_value
    master = @config.getProperty(get_dataservice_key(DATASERVICE_MASTER_MEMBER)).split(",")[0]
    rs_alias = to_identifier("#{get_dataservice()}_#{master}")
    @default = @config.getProperty([REPL_SERVICES, rs_alias, REPL_DBPORT])
  end
end

class ManagerReplicationDBSchema < ConfigurePrompt
  include ManagerPrompt
  include HiddenValueModule
  
  def initialize
    super(MGR_REPL_SCHEMA, "Database schema for the manager to check replication hosts", PV_IDENTIFIER)
  end
  
  def load_default_value
    master = @config.getProperty(get_dataservice_key(DATASERVICE_MASTER_MEMBER)).split(",")[0]
    rs_alias = to_identifier("#{get_dataservice()}_#{master}")
    @default = @config.getProperty([REPL_SERVICES, rs_alias, REPL_SVC_SCHEMA])
  end
end

class ManagerReplicationRMIPort < ConfigurePrompt
  include ManagerPrompt
  include HiddenValueModule
  
  def initialize
    super(MGR_REPL_RMI_PORT, "Replication RMI port for the manager to check replication hosts", PV_INTEGER)
  end
  
  def load_default_value
    master = @config.getProperty(get_dataservice_key(DATASERVICE_MASTER_MEMBER)).split(",")[0]
    h_alias = to_identifier(master)
    @default = @config.getProperty([HOSTS, h_alias, REPL_RMI_PORT])
  end
end

class ManagerReplicationJDBCDriver < ConfigurePrompt
  include ManagerPrompt
  include HiddenValueModule
  
  def initialize
    super(MGR_REPL_JDBC_DRIVER, "Replication JDBC driver for the manager to check replication hosts", PV_ANY)
  end
  
  def load_default_value
    master = @config.getProperty(get_dataservice_key(DATASERVICE_MASTER_MEMBER)).split(",")[0]
    rs_alias = to_identifier("#{get_dataservice()}_#{master}")
    @default = @config.getTemplateValue([REPL_SERVICES, rs_alias, REPL_DBJDBCDRIVER])
  end
end