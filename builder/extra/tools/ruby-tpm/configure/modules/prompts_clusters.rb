DATASERVICES = "dataservices"
DATASERVICE_HOST_OPTIONS = "dataservice_host_options"
DATASERVICE_REPLICATION_OPTIONS = "dataservice_replication_options"
DATASERVICE_CONNECTOR_OPTIONS = "dataservice_connector_options"
DATASERVICE_MANAGER_OPTIONS = "dataservice_manager_options"
DATASERVICE_MEMBERS = "dataservice_hosts"
DATASERVICE_REPLICATION_MEMBERS = "dataservice_replication_members"
DATASERVICE_MASTER_MEMBER = "dataservice_master_host"
DATASERVICE_SLAVES = "dataservice_slaves"
DATASERVICE_RELAY_ENABLED = "dataservice_relay_enabled"
DATASERVICE_RELAY_SOURCE = "dataservice_relay_source"
DATASERVICE_CONNECTORS = "dataservice_connectors"
DATASERVICE_WITNESSES = "dataservice_witnesses"
ENABLE_ACTIVE_WITNESSES = "enable_active_witnesses"
DATASERVICE_THL_PORT = "dataservice_thl_port"
DATASERVICE_VIP_ENABLED = "dataservice_vip_enabled"
DATASERVICE_VIP_IPADDRESS = "dataservice_vip_ipaddress"
DATASERVICE_VIP_NETMASK = "dataservice_vip_netmask"
DATASERVICE_USERS = "dataservice_users"
DATASERVICE_TOPOLOGY = "dataservice_topology"
DATASERVICE_PARENT_DATASERVICE = "dataservice_parent_dataservice"
DATASERVICE_IS_COMPOSITE = "dataservice_is_composite"
DATASERVICE_COMPOSITE_DATASOURCES = "dataservice_composite_datasources"
DATASERVICE_SCHEMA = "dataservice_schema"
DATASERVICE_ENABLE_INSTRUMENTATION = "dataservice_enable_instrumentation"
DATASERVICE_MASTER_SERVICES = "dataservice_master_services"
DATASERVICE_HUB_MEMBER = "dataservice_hub_host"
DATASERVICE_HUB_SERVICE = "dataservice_hub_service"
TARGET_DATASERVICE = "target_dataservice"
DATASERVICE_ENABLE_ALL_TOPOLOGIES = "dataservice_enable_all_topologies"
DATASERVICE_USE_RELATIVE_LATENCY = "dataservice_use_relative_latency"

class Clusters < GroupConfigurePrompt
  def initialize
    super(DATASERVICES, "Enter dataservice information for @value", 
      "dataservice", "dataservices", "DATASERVICE")
      
    ClusterPrompt.subclasses().each{
      |klass|
      self.add_prompt(klass.new())
    }
  end
end

module ClusterPrompt
  include GroupConfigurePromptMember

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
  
  def enabled?
    if enabled_for_dataservice?()
      super()
    else
      false
    end
  end
  
  def enabled_for_config?
    if enabled_for_dataservice?()
      super()
    else
      false
    end
  end
  
  def enabled_for_dataservice?
    (@config.getProperty(get_member_key(DATASERVICE_IS_COMPOSITE)) == "false")
  end
  
  def get_dataservice_alias
    get_member()
  end
  
  def get_topology
    Topology.build(get_dataservice_alias(), @config)
  end
end

module CompositeDataservicePrompt
  def enabled_for_dataservice?
    (@config.getProperty(get_member_key(DATASERVICE_IS_COMPOSITE)) == "true")
  end
end

class DataServiceName < ConfigurePrompt
  include ClusterPrompt
  include ConstantValueModule
  
  def initialize
    super(DATASERVICENAME, "Name of this dataservice", PV_IDENTIFIER, DEFAULT_SERVICE_NAME)
    self.extend(NotTungstenUpdatePrompt)
  end
  
  def allow_group_default
    false
  end
  
  def enabled_for_dataservice?
    true
  end
end

class DataServiceAlias < ConfigurePrompt
  include ClusterPrompt
  
  def initialize
    super(DATASERVICEALIAS, "Replication alias of this dataservice", PV_IDENTIFIER)
    self.extend(NotTungstenUpdatePrompt)
    override_command_line_argument("service-alias")
  end
  
  def validate_value(value)
    topology = get_topology()
    if topology.is_a?(ClusterTopology)
      error("The --service-alias argument is not supported for clustered dataservices")
    end
  end
  
  def allow_group_default
    false
  end
  
  def enabled_for_dataservice?
    true
  end
  
  def required?
    false
  end
  
  def load_default_value
    @default = get_topology().get_dataservice_alias()
  end
end

class ClusterName < ConfigurePrompt
  include ClusterPrompt
  include ConstantValueModule
  
  def initialize
    super(CLUSTERNAME, "Name of this dataservice", PV_IDENTIFIER)
    self.extend(NotTungstenUpdatePrompt)
  end
  
  def load_default_value
    @default = @config.getProperty(get_member_key(DATASERVICENAME))
  end
  
  def allow_group_default
    false
  end
  
  def enabled_for_command_line?()
    false
  end
  
  def enabled_for_dataservice?
    true
  end
end

class ClusterTopologyPrompt < ConfigurePrompt
  include ClusterPrompt
  include AdvancedPromptModule
  
  def initialize
    validator = PropertyValidator.new(Topology.get_types().join("|"), 
      "Value must be #{Topology.get_types().join(',')}")
      
    default = nil
    Topology.get_classes().each{
      |klass_name,klass|
      begin
        if klass.is_default?() == true
          default = klass_name
        end
      rescue NoMethodError
      end
    }
      
    super(DATASERVICE_TOPOLOGY, "Replication topology for the dataservice", validator, default)
    self.extend(NotTungstenUpdatePrompt)
    override_command_line_argument("topology")
  end
  
  def allow_group_default
    false
  end
  
  def get_prompt_description
    "Valid values are " + Topology.get_types().join(',')
  end
end

class TargetDataservice < ConfigurePrompt
  include ClusterPrompt
  include NewDirectoryUpdate
  
  def initialize
    super(TARGET_DATASERVICE, "Dataservice to use to determine the value of --master, --slaves, --members, --connectors and --witnesses", PV_IDENTIFIER)
    add_command_line_alias("slave-dataservice")
  end
  
  def allow_group_default
    false
  end
  
  def accept?(raw_value)
    v = super(raw_value)
    unless v == nil
      v = to_identifier(v)
    end
    
    return v
  end
  
  def validate_value(value)
    if value.to_s() == ""
      return
    end
    
    if @config.getProperty([DATASERVICES, value]) == nil
      error "Unable to find the target dataservice '#{value}'"
    end
  end
  
  def required?
    false
  end
  
  module TargetDataserviceDefaultValue
    def load_default_value
      td = @config.getProperty(get_member_key(TARGET_DATASERVICE))
      if td != nil
        @default = @config.getProperty([@parent_group.name, td, @name])
      else
        super()
      end
    end
  end
end

class ClusterMembers < ConfigurePrompt
  include ClusterPrompt
  include NoReplicatorRestart
  include NoConnectorRestart
  
  def initialize
    super(DATASERVICE_MEMBERS, "Hostnames for the dataservice members", PV_ANY)
    override_command_line_argument("members")
    self.extend(TargetDataservice::TargetDataserviceDefaultValue)
  end
  
  def allow_group_default
    false
  end
  
  def load_default_value
    members = @config.getProperty(get_member_key(DATASERVICE_MASTER_MEMBER)).to_s().split(",")
    members = members + @config.getProperty(get_member_key(DATASERVICE_HUB_MEMBER)).to_s().split(",")
    members = members + @config.getProperty(get_member_key(DATASERVICE_SLAVES)).to_s().split(",")
    if @config.getProperty(get_member_key(ENABLE_ACTIVE_WITNESSES)) == "true"
      members = members + @config.getProperty(get_member_key(DATASERVICE_WITNESSES)).to_s().split(",")
    end
    
    @default = members.uniq().join(",")
  end
  
  def validate_value(value)
    value_parts = value.to_s.split(',')
    
    value_parts.each{
      |member|
      
      if member =~ /_/
        error("The hostname for '#{member}' includes an underscore.  Modify the system to use a hostname that does not include underscores.")
      end
    }
    
    if get_topology().use_management?()
      @parent_group.each_member{
        |ds_alias|
        if ds_alias == get_member()
          next
        end
        
        if ds_alias == DEFAULTS
          next
        end
        
        if @config.getProperty([DATASERVICES, ds_alias, DATASERVICE_IS_COMPOSITE]) == "true"
          next
        end
        
        begin
          unless Topology.build(ds_alias, @config).use_management?()
            next
          end
        rescue => e
          error("Unable to check the topology '" + 
            @config.getPropertyOr([DATASERVICES, ds_alias, DATASERVICE_TOPOLOGY], "None") + 
            "' for " + ds_alias)
          next
        end
      
        dataservice_members = (
          @config.getPropertyOr([@parent_group.name, ds_alias, DATASERVICE_MEMBERS], "").split(",") +
          @config.getPropertyOr([@parent_group.name, ds_alias, DATASERVICE_CONNECTORS], "").split(",")
        ).uniq()
      
        matches = dataservice_members & value_parts
        if matches.length > 0
          error("#{matches.join(',')} appears in dataservice #{@config.getProperty([DATASERVICES, ds_alias, DATASERVICENAME])} and #{@config.getProperty([DATASERVICES, get_member(), DATASERVICENAME])}")
        end
      }
    end
  end
end

class ClusterReplicationHosts < ConfigurePrompt
  include ClusterPrompt
  include HiddenValueModule
  
  def initialize
    super(DATASERVICE_REPLICATION_MEMBERS, "Hostnames for the dataservice replication members", PV_ANY)
    self.extend(TargetDataservice::TargetDataserviceDefaultValue)
  end
  
  def load_default_value
    members = @config.getProperty(get_member_key(DATASERVICE_MEMBERS)).to_s().split(",")
    
    if @config.getProperty(get_member_key(ENABLE_ACTIVE_WITNESSES)) == "true"
      members = members - @config.getProperty(get_member_key(DATASERVICE_WITNESSES)).to_s().split(",")
    end
    
    @default = members.uniq().join(",")
  end
end

class ClusterMasterHost < ConfigurePrompt
  include ClusterPrompt
  include NoReplicatorRestart
  include NoManagerRestart
  include NoConnectorRestart
  
  def initialize
    super(DATASERVICE_MASTER_MEMBER, "What is the master host for this dataservice?", 
      PV_ANY)
    self.extend(NotTungstenUpdatePrompt)
    self.extend(TargetDataservice::TargetDataserviceDefaultValue)
    override_command_line_argument("master")
    add_command_line_alias("masters")
    add_command_line_alias("relay")
  end
    
  def enabled?
    super() && @config.getProperty(get_member_key(DATASERVICE_MEMBERS)).to_s != ""
  end
  
  def allow_group_default
    false
  end
  
  def validate_value(value)
    value_parts = value.to_s.split(',')
    topology = get_topology()
    
    if value_parts.length > 1 && topology.allow_multiple_masters?() != true
      error("Unable to deploy the #{@config.getProperty(get_member_key(DATASERVICENAME))} dataservice with multiple masters")
    end
    
    if topology.is_a?(ClusterTopology)
      value_parts.each{
        |master|
        unless @config.getPropertyOr(get_member_key(DATASERVICE_REPLICATION_MEMBERS), "").split(",").include?(master)
          error("Unable to deploy the #{@config.getProperty(get_member_key(DATASERVICENAME))} dataservice because the master '#{master}' is not a member.")
        end
      }
    end
  end
  
  def required?
    # This value is required if this is a replicator host and --master-thl-host isn't given
    rs_alias = to_identifier("#{get_member()}_#{@config.getProperty([DEPLOYMENT_HOST])}")
    super() && (@config.getProperty(HOST_ENABLE_REPLICATOR) == "true") &&
      (@config.getProperty([REPL_SERVICES, rs_alias, REPL_MASTERHOST]) == nil)
  end
  
  def build_command_line_argument(member, v, public_argument = false)
    relay_source = @config.getProperty([DATASERVICES, member, DATASERVICE_RELAY_SOURCE])
    
    if relay_source.to_s() != ""
      ["--relay=#{v}"]
    elsif v.to_s().split(",").size() > 1
      ["--masters=#{v}"]
    else
      ["--master=#{v}"]
    end
  end
end

class ClusterSlaves < ConfigurePrompt
  include ClusterPrompt
  include NoReplicatorRestart
  include NoManagerRestart
  include NoConnectorRestart
  
  def initialize
    super(DATASERVICE_SLAVES, "What are the slaves for this dataservice?", 
      PV_ANY)
    self.extend(NotTungstenUpdatePrompt)
    self.extend(TargetDataservice::TargetDataserviceDefaultValue)
    override_command_line_argument("slaves")
  end
  
  def allow_group_default
    false
  end
  
  def required?
    false
  end
end

class ClusterConnectors < ConfigurePrompt
  include ClusterPrompt
  include NoReplicatorRestart
  include NoManagerRestart
  include NoConnectorRestart
  include CommercialPrompt
  
  def initialize
    super(DATASERVICE_CONNECTORS, "Hostnames for the dataservice connectors", PV_ANY, "")
    self.extend(TargetDataservice::TargetDataserviceDefaultValue)
    override_command_line_argument("connectors")
  end
  
  def allow_group_default
    false
  end
  
  def validate_value(value)
    topology = get_topology()
    
    if topology.use_management?()
      if value.to_s() == ""
        warning("You have not specified any connectors for the #{@config.getProperty(get_member_key(DATASERVICENAME))} data service")
      end
    end
  end
end

class ClusterWitnesses < ConfigurePrompt
  include ClusterPrompt
  include NoReplicatorRestart
  include NoConnectorRestart
  include CommercialPrompt
  
  def initialize
    super(DATASERVICE_WITNESSES, "Witness hosts for the dataservice", PV_ANY)
    self.extend(TargetDataservice::TargetDataserviceDefaultValue)
    override_command_line_argument("witnesses")
  end
  
  def allow_group_default
    false
  end
  
  def validate_value(value)
    active_witnesses = @config.getProperty(get_member_key(ENABLE_ACTIVE_WITNESSES))

    value.to_s().split(",").each{
      |witness|
      found_member = @config.getProperty(get_member_key(DATASERVICE_MEMBERS)).to_s().split(",").include?(witness)
      if active_witnesses == "true"
        if found_member == false
          error("The witness host '#{witness}' is not a member of the #{@config.getProperty(get_member_key(DATASERVICENAME))} dataservice. You should specify a witness that is one of the dataservice members or add the witness to --members.")
        end
      else
        if found_member == true
          error("The witness host '#{witness}' is a member of the #{@config.getProperty(get_member_key(DATASERVICENAME))} dataservice. You should specify a witness that is not one of the dataservice members or specify --active-witnesses=true.")
        end
      end
    }
  end
  
  def required?
    if @config.getProperty(get_member_key(ENABLE_ACTIVE_WITNESSES)) == "true"
      true
    else
      false
    end
  end
  
  def get_template_value
    if @config.getProperty(get_member_key(ENABLE_ACTIVE_WITNESSES)) == "true"
      return ""
    else
      super()
    end
  end
end

class ClusterActiveWitnesses < ConfigurePrompt
  include ClusterPrompt
  
  def initialize
    super(ENABLE_ACTIVE_WITNESSES, "Enable active witness hosts", PV_BOOLEAN, "false")
    self.extend(TargetDataservice::TargetDataserviceDefaultValue)
    add_command_line_alias("active-witnesses")
  end
end

class ClusterRelayEnabled < ConfigurePrompt
  include ClusterPrompt
  include NewDirectoryUpdate
  
  def initialize
    super(DATASERVICE_RELAY_ENABLED, "Make this dataservice the slave of another", PV_BOOLEAN, "false")
  end
  
  def load_default_value
    if @config.getPropertyOr(get_member_key(DATASERVICE_RELAY_SOURCE), "") == ""
      @default = "false"
    else
      @default = "true"
    end
  end
  
  def allow_group_default
    false
  end
end

class ClusterRelaySource < ConfigurePrompt
  include ClusterPrompt
  include NewDirectoryUpdate
  
  def initialize
    super(DATASERVICE_RELAY_SOURCE, "Dataservice name to use as a relay source", PV_MULTIIDENTIFIER)
    override_command_line_argument("relay-source")
    add_command_line_alias("master-dataservice")
  end
  
  def allow_group_default
    false
  end
  
  def accept?(raw_value)
    v = super(raw_value)
    unless v == nil
      v = to_identifier(v)
    end
    
    return v
  end
  
  def validate_value(value)
    if value.to_s() == ""
      return
    end
    
    if to_identifier(value) == get_member()
      error "Unable to configure a service to use itself as a source"
      return
    end
    
    found_composite_datasource = false
    
    topology = Topology.build(get_member(), @config)
    unless topology.use_management?()
      return
    end
    
    @parent_group.each_member{
      |ds_alias|
      if ds_alias == get_member()
        next
      end
      
      if @config.getProperty([DATASERVICES, ds_alias, DATASERVICE_IS_COMPOSITE]) == "true"
        composite_datasources = @config.getProperty([DATASERVICES, ds_alias, DATASERVICE_COMPOSITE_DATASOURCES]).split(",")
        
        if composite_datasources.include?(get_member()) && composite_datasources.include?(value)
          found_composite_datasource = true
        end
      end
    }
    
    if found_composite_datasource == false
      error "Unable to find a composite data service that includes the '#{get_member()}' and '#{value}' data services.  Try running 'tools/tpm configure <global data service name> --dataservice-composite-datasources=#{value},#{get_member()}'"
    end
  end
  
  def required?
    super() && (@config.getProperty(get_member_key(DATASERVICE_RELAY_ENABLED)) == "true")
  end
end

class ClusterVIPEnabled < ConfigurePrompt
  include ClusterPrompt
  include NewDirectoryUpdate
  include CommercialPrompt
  
  def initialize
    super(DATASERVICE_VIP_ENABLED, "Is VIP management enabled?", PV_BOOLEAN, "false")
  end
end

class ClusterVIPIPAddress < ConfigurePrompt
  include ClusterPrompt
  include NewDirectoryUpdate
  include CommercialPrompt
  
  def initialize
    super(DATASERVICE_VIP_IPADDRESS, "VIP IP address", PV_ANY)
  end
  
  def enabled?
    super() && (@config.getProperty(get_member_key(DATASERVICE_VIP_ENABLED)) == "true")
  end
  
  def allow_group_default
    false
  end
end

class ClusterVIPNetmask < ConfigurePrompt
  include ClusterPrompt
  include NewDirectoryUpdate
  include CommercialPrompt
  
  def initialize
    super(DATASERVICE_VIP_NETMASK, "VIP netmask", PV_ANY, "255.255.255.0")
  end
  
  def enabled?
    super() && (@config.getProperty(get_member_key(DATASERVICE_VIP_ENABLED)) == "true")
  end
end

class ClusterTHLPort < ConfigurePrompt
  include ClusterPrompt
  include NoConnectorRestart
  include NoManagerRestart
  
  def initialize
    super(DATASERVICE_THL_PORT, 
      "Port to use for THL operations", PV_INTEGER, "2112")
  end
end

class DataserviceSchema < ConfigurePrompt
  include ClusterPrompt
  include NoConnectorRestart
  
  def initialize
    super(DATASERVICE_SCHEMA, "The db schema to hold dataservice details", PV_IDENTIFIER)
  end
  
  def load_default_value
    parent_dataservice = @config.getProperty(get_member_key(DATASERVICE_PARENT_DATASERVICE), true)
    
    if parent_dataservice == nil
      @default = "tungsten_#{@config.getProperty(get_member_key(DATASERVICENAME))}"
    else
      @default = "tungsten_#{@config.getProperty([DATASERVICES, parent_dataservice, DATASERVICENAME])}"
    end
  end
end

class DataserviceParentDataservice < ConfigurePrompt
  include ClusterPrompt
  include ConstantValueModule
  
  def initialize
    super(DATASERVICE_PARENT_DATASERVICE, "The composite dataservice, if any, that uses this dataservice", PV_ANY)
  end
  
  def load_default_value
    if @config.getProperty(get_member_key(DATASERVICE_IS_COMPOSITE)) == "true"
      @default = nil
    else
      @config.getPropertyOr([DATASERVICES], {}).each_key{
        |ds_alias|
        
        if ds_alias == DEFAULTS
          next
        end

        if @config.getProperty([DATASERVICES, ds_alias, DATASERVICE_IS_COMPOSITE]) == "false"
          next
        end
        
        composite_datasources = @config.getPropertyOr([DATASERVICES, ds_alias, DATASERVICE_COMPOSITE_DATASOURCES], "").split(",")
        if composite_datasources.include?(get_member()) == true
          @default = ds_alias
        end
      }
    end
  end
end

class DataserviceIsComposite < ConfigurePrompt
  include ClusterPrompt
  include ConstantValueModule
  
  def initialize
    super(DATASERVICE_IS_COMPOSITE, "Is this a composite data service", PV_BOOLEAN)
  end
  
  def load_default_value
    if @config.getProperty(get_member_key(DATASERVICE_COMPOSITE_DATASOURCES)).to_s == ""
      @default = "false"
    else
      @default = "true"
    end
  end
  
  def enabled_for_dataservice?
    true
  end
end

class DataserviceCompositeDatasources < ConfigurePrompt
  include ClusterPrompt
  include CompositeDataservicePrompt
  include NewDirectoryUpdate
  
  def initialize
    super(DATASERVICE_COMPOSITE_DATASOURCES, 
      "Data services that should be added to this composite data service",
      PV_ANY)
    override_command_line_argument("composite-datasources")
  end
  
  def accept?(raw_value)
    v = super(raw_value)
    unless v == nil
      v = v.split(",").map!{|ds| to_identifier(ds)}.join(",")
    end
    
    return v
  end
  
  def validate_value(value)
    if value.to_s() == ""
      warning("You have not specified any camposite data sources for the #{get_member()} data service")
    else
      ds_keys = @config.getPropertyOr(DATASERVICES, {}).keys().delete_if{|k| (k == DEFAULTS)}
      value.to_s().split(",").each{
        |ds|
        
        if ds_keys.include?(ds)
          if @config.getProperty([DATASERVICES, ds, DATASERVICE_IS_COMPOSITE]) == "true"
            error("The #{ds} data service is already a composite data service and cannot be used for #{get_member()}")
          end
        else
          error("The #{ds} data service does not exist")
        end
      }
    end
  end
end

class DataserviceMasterServices < ConfigurePrompt
  include ClusterPrompt
  
  def initialize
    super(DATASERVICE_MASTER_SERVICES, 
      "Data service names that should be used on each master",
      PV_ANY)
    override_command_line_argument("master-services")
  end
  
  def accept?(raw_value)
    v = super(raw_value)
    unless v == nil
      v = v.split(",").map!{|ds| to_identifier(ds)}.join(",")
    end
    
    return v
  end
  
  def enabled_for_dataservice?
    (get_topology().allow_multiple_masters?() == true)
  end
end

class ClusterHubHost < ConfigurePrompt
  include ClusterPrompt
  include NoReplicatorRestart
  include NoManagerRestart
  include NoConnectorRestart
  
  def initialize
    super(DATASERVICE_HUB_MEMBER, "What is the hub host for this all-masters dataservice?", 
      PV_ANY)
    self.extend(NotTungstenUpdatePrompt)
    override_command_line_argument("hub")
  end
    
  def allow_group_default
    false
  end
  
  def enabled_for_dataservice?
    (get_topology().is_a?(StarTopology))
  end
end

class DataserviceHubService < ConfigurePrompt
  include ClusterPrompt
  
  def initialize
    super(DATASERVICE_HUB_SERVICE, 
      "The data service to use for the hub of a star topology",
      PV_IDENTIFIER)
    self.extend(NotTungstenUpdatePrompt)
    override_command_line_argument("hub-service")
  end
  
  def enabled_for_dataservice?
    (get_topology().is_a?(StarTopology))
  end
end

class DataserviceEnableInstrumentation < ConfigurePrompt
  include ClusterPrompt
  include HiddenValueModule
  include NoReplicatorRestart
  include NoConnectorRestart
  
  def initialize
    super(DATASERVICE_ENABLE_INSTRUMENTATION, 
      "Should we log instrumentation values for this dataservice?",
      PV_BOOLEAN, "false")
  end
  
  def get_command_line_aliases
    ["enable-instrumentation"]
  end
end

class DataserviceEnableAllTopologies < ConfigurePrompt
  include ClusterPrompt
  include HiddenValueModule
  include NoReplicatorRestart
  include NoConnectorRestart
  
  def initialize
    super(DATASERVICE_ENABLE_ALL_TOPOLOGIES, 
      "Should we deploy all topologies that are defined?", PV_BOOLEAN, "false")
    override_command_line_argument("enable-all-topologies")
  end
end

class ClusterRelativeLatency < ConfigurePrompt
  include ClusterPrompt
  
  def initialize
    super(DATASERVICE_USE_RELATIVE_LATENCY, "Enable the cluster to operate on relative latency", PV_BOOLEAN, "false")
    add_command_line_alias("use-relative-latency")
  end
end

class DataserviceGlobalProperties < ConfigurePrompt
  include ClusterPrompt
  include ConstantValueModule
  
  def initialize
    super(FIXED_PROPERTY_STRINGS, "Fixed properties for this dataservice")
  end
  
  def get_value(allow_default = true, allow_disabled = false)
    if allow_default && (enabled_for_config?() || allow_disabled)
      value = get_default_value()
    else
      value = super()
    end
        
    value
  end
  
  def load_default_value
    @default = []
  end
  
  def get_default_value
    if get_member() == DEFAULTS
      return super()
    end
    
    values = []
    
    defaults = get_group_default_value()
    if defaults.is_a?(Array)
      values = values + defaults
    end
    
    ds_value = @config.getNestedProperty(get_name())
    if ds_value.is_a?(Array)
      values = values + ds_value
    end
    
    return values
  end
  
  def required?
    false
  end
  
  def build_command_line_argument(member, values, public_argument = false)
    args = []
    
    if values.is_a?(Array)
      values.each{
        |v|
        args << "--property=#{v}"
      }
    elsif values.to_s() != ""
      args << "--property=#{values}"
    end
    
    if args.length == 0
      raise IgnoreError
    else
      return args
    end
  end
end

class DataserviceHostOptions < GroupConfigurePrompt
  include ConstantValueModule
  include HashPromptModule
  
  def initialize
    super(DATASERVICE_HOST_OPTIONS, "Enter options for the @value dataservice", 
      "dataservice host option", "dataservice host options", "DATASERVICE_OPTION")
      
    ClusterHostPrompt.subclasses().each{
      |klass|
      
      if klass == ManagerDeploymentHost
      else
        o = klass.new()
        if o.allow_group_default()
          self.add_prompt(o)
        end
      end
    }
  end
end

class DataserviceManagerOptions < GroupConfigurePrompt
  include ConstantValueModule
  include HashPromptModule
  
  def initialize
    super(DATASERVICE_MANAGER_OPTIONS, "Enter options for the @value dataservice", 
      "dataservice manager option", "dataservice manager options", "DATASERVICE_OPTION")
      
    ManagerPrompt.subclasses().each{
      |klass|
      
      if klass == ManagerDeploymentHost
      elsif klass == ManagerDataservice
      else
        self.add_prompt(klass.new())
      end
    }
  end
end

class DataserviceReplicationOptions < GroupConfigurePrompt
  include ConstantValueModule
  include HashPromptModule
  
  def initialize
    super(DATASERVICE_REPLICATION_OPTIONS, "Enter options for the @value dataservice", 
      "dataservice replication option", "dataservice replication options", "DATASERVICE_OPTION")
      
    ReplicationServicePrompt.subclasses().each{
      |klass|
      
      if klass == ReplicationServiceDeploymentHost
      else
        self.add_prompt(klass.new())
      end
    }
    
    DatasourcePrompt.subclasses().each{
      |klass|
      
      self.add_prompt(klass.new())
    }
  end
end

class DataserviceConnectorOptions < GroupConfigurePrompt
  include ConstantValueModule
  include HashPromptModule

  def initialize
    super(DATASERVICE_CONNECTOR_OPTIONS, "Enter options for the @value dataservice", 
      "dataservice connector option", "dataservice connector options", "DATASERVICE_OPTION")
      
    ConnectorPrompt.subclasses().each{
      |klass|
      
      if klass == ConnectorDataservice
      elsif klass == ConnectorDeploymentHost
      else
        self.add_prompt(klass.new())
      end
    }
  end
end