module DatasourcePrompt
  include GroupConfigurePromptMember
  include HashPromptDefaultsModule
  include NoManagerRestart
  include NoConnectorRestart
  
  def self.included(subclass)
    @subclasses ||= []
    @subclasses << subclass
  end

  def self.subclasses
    @subclasses || []
  end
  
  def get_datasource
    ConfigureDatabasePlatform.build([@parent_group.name, get_member()], @config)
  end
  
  def get_extractor_datasource
    if @config.getProperty(get_member_key(REPL_ROLE)) == REPL_ROLE_DI
      ConfigureDatabasePlatform.build([@parent_group.name, get_member()], @config, true)
    else
      get_datasource()
    end
  end
  
  def get_command_line_argument()
    super.gsub("repl-", "")
  end
  
  def get_command_line_aliases
    super() + super().collect{|al| al.gsub("repl-", "")} + [@name.gsub("_", "-")]
  end
  
  def get_host_alias
    @config.getProperty(get_member_key(DEPLOYMENT_HOST))
  end
  
  def get_host_key(key)
    if get_member() == DEFAULTS
      return [HOSTS, DEFAULTS, key]
    end
    
    host_alias = get_host_alias()
    if host_alias == nil
      raise "Unable to find the host alias for this replication service (#{get_member()}:#{get_name()})"
    end
    
    [HOSTS, host_alias, key]
  end
  
  def get_dataservice_alias
    @config.getProperty(get_member_key(DEPLOYMENT_DATASERVICE))
  end
  
  def get_dataservice_key(key)
    [DATASERVICES, get_dataservice_alias(), key]
  end
  
  def get_topology
    Topology.build(get_dataservice_alias(), @config)
  end
  
  def get_userid
    host_alias = get_host_alias()
    if host_alias == nil
      super
    end
    
    @config.getProperty([HOSTS, host_alias, USERID])
  end
  
  def get_hostname
    host_alias = get_host_alias()
    if host_alias == nil
      super
    end
    
    @config.getProperty([HOSTS, host_alias, HOST])
  end
  
  def allow_group_default
    true
  end
  
  def get_hash_prompt_key
    return [DATASERVICE_REPLICATION_OPTIONS, @config.getProperty(get_member_key(DEPLOYMENT_DATASERVICE)), @name]
  end
  
  def enabled_for_command_dataservice?
    host_alias = get_host_alias()
    @config.getPropertyOr(REPL_SERVICES, []).keys().each{
      |rs_alias|
      
      if @config.getProperty([REPL_SERVICES, rs_alias, DEPLOYMENT_HOST]) == host_alias
        if Configurator.instance.command.include_dataservice?(@config.getProperty([REPL_SERVICES, rs_alias, DEPLOYMENT_DATASERVICE]))
          return true
        end
      end
    }
    
    return false
  end
  
  def get_display_member
    "Host #{get_hostname()}"
  end
end

class DatasourceDBHost < ConfigurePrompt
  include DatasourcePrompt
  
  def initialize
    super(REPL_DBHOST, "Database server hostname", PV_HOSTNAME)
    override_command_line_argument("replication-host")
  end
  
  def load_default_value
    @default = @config.getPropertyOr(get_host_key(HOST), Configurator.instance.hostname())
  end
  
  def allow_group_default
    false
  end
end

class DatasourceDBPort < ConfigurePrompt
  include DatasourcePrompt
  
  def initialize
    super(REPL_DBPORT, "Database server port", PV_INTEGER)
    override_command_line_argument("replication-port")
  end
  
  def load_default_value
    @default = get_datasource().get_default_port()
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_dbport'))
    super()
  end
  
  def required?
    super() && get_datasource().get_default_port() != nil
  end
  
  PortForConnectors.register(REPL_SERVICES, REPL_DBPORT)
  PortForManagers.register(REPL_SERVICES, REPL_DBPORT)
end

class DatasourceDBUser < ConfigurePrompt
  include DatasourcePrompt
  
  def initialize
    super(REPL_DBLOGIN, "Database login for Tungsten", 
      PV_IDENTIFIER, Configurator.instance.whoami())
    override_command_line_argument("replication-user")
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_admin_login'))
    super()
  end
end

class DatasourceDBPassword < ConfigurePrompt
  include DatasourcePrompt
  include ManagerRestart
  include PrivateArgumentModule
  
  def initialize
    super(REPL_DBPASSWORD, "Database password", 
      PV_ANY, "")
    override_command_line_argument("replication-password")
  end
    
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_admin_password'))
    super()
  end
end

class DatasourceEnableDBSSL < ConfigurePrompt
  include DatasourcePrompt

  def initialize
    super(REPL_ENABLE_DBSSL, "Enable SSL connection to DBMS server",
      PV_BOOLEAN, "false")
  end

  def required?
    false
  end
end

class DatasourceInitScript < ConfigurePrompt
  include DatasourcePrompt
  include AdvancedPromptModule
  include OptionalPromptModule

  def initialize
    super(REPL_BOOT_SCRIPT, "Database start script", PV_FILENAME)
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_boot_script'))
    super()
  end
end

class DatasourceSystemctlService < ConfigurePrompt
  include DatasourcePrompt
  include AdvancedPromptModule
  include OptionalPromptModule

  def initialize
    super(REPL_SYSTEMCTL_SERVICE, "Database systemctl script", PV_ANY)
  end
end

class DatasourceServiceStartCommand < ConfigurePrompt
  include DatasourcePrompt
  include ConstantValueModule
  include OptionalPromptModule
  
  def initialize
    super(REPL_DBSERVICE_START, "Database service start command", PV_ANY)
  end
  
  def load_default_value
    @default = get_datasource.get_start_command()
  end
end

class DatasourceServiceStopCommand < ConfigurePrompt
  include DatasourcePrompt
  include ConstantValueModule
  include OptionalPromptModule
  
  def initialize
    super(REPL_DBSERVICE_STOP, "Database service stop command", PV_ANY)
  end
  
  def load_default_value
    @default = get_datasource.get_stop_command()
  end
end

class DatasourceServiceRestartCommand < ConfigurePrompt
  include DatasourcePrompt
  include ConstantValueModule
  include OptionalPromptModule
  
  def initialize
    super(REPL_DBSERVICE_RESTART, "Database service restart command", PV_ANY)
  end
  
  def load_default_value
    @default = get_datasource.get_restart_command()
  end
end

class DatasourceServiceStatusCommand < ConfigurePrompt
  include DatasourcePrompt
  include ConstantValueModule
  include OptionalPromptModule
  
  def initialize
    super(REPL_DBSERVICE_STATUS, "Database service status command", PV_ANY)
  end
  
  def load_default_value
    @default = get_datasource.get_status_command()
  end
end

# This prompt isn't used any more but it's left in place to prevent
# upgrade issues
class DatasourceInitServiceName< ConfigurePrompt
  include DatasourcePrompt
  include ConstantValueModule

  def initialize
    super(REPL_BOOT_SERVICE_NAME, "Database service name", PV_ANY)
  end
  
  def required?
    false
  end
end

class DatasourceVersion < ConfigurePrompt
  include DatasourcePrompt
  include HiddenValueModule
  
  def initialize
    super(REPL_DBVERSION, "DB version for the replication database", PV_ANY)
  end
  
  def load_default_value
    if (ds = get_datasource())
      @default = ds.getVersion()
    else
      @default = nil
    end
  end
end

class DatasourceMasterLogDirectory < ConfigurePrompt
  include DatasourcePrompt
  
  def initialize
    super(REPL_MASTER_LOGDIR, "Master log directory", 
      PV_FILENAME)
  end
  
  def load_default_value
    @default = get_datasource().get_default_master_log_directory()
  end
  
  def required?
    (get_default_value() != nil)
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_mysql_binlog_dir'))
    super()
  end
end

class DatasourceMasterLogPattern < ConfigurePrompt
  include DatasourcePrompt
  
  def initialize
    super(REPL_MASTER_LOGPATTERN, "Master log filename pattern", PV_ANY, "mysql-bin")
  end
  
  def load_default_value
    @default = get_datasource().get_default_master_log_pattern()
  end

  def required?
    (get_default_value() != nil)
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_mysql_binlog_pattern'))
    super()
  end
end

class DatasourceDisableRelayLogs < ConfigurePrompt
  include DatasourcePrompt
  
  def initialize
    super(REPL_DISABLE_RELAY_LOGS, "Disable the use of relay-logs?",
      PV_BOOLEAN)
  end
  
  def load_default_value
    topology = get_topology()
    
    begin
      @default = topology.disable_relay_logs?()
    rescue
      @default = "true"
    end
  end
  
  def get_template_value
    v = super()
    
    if v == "false"
      "true"
    else
      "false"
    end
  end
end

class DirectDatasourceDBType < ConfigurePrompt
  include DatasourcePrompt
  
  def initialize
    validator = PropertyValidator.new(ConfigureDatabasePlatform.get_types().join("|"), 
      "Value must be #{ConfigureDatabasePlatform.get_types().join(',')}")
      
    super(EXTRACTOR_REPL_DBTYPE, "Database type (#{ConfigureDatabasePlatform.get_types().join(',')})", 
        validator)
  end
  
  def get_default_value
    case Configurator.instance.whoami()
    when "postgres"
      return "postgresql"
    when "enterprisedb"
      return "postgresql"
    else
      return "mysql"
    end
  end
end

class DirectDatasourceDBHost < ConfigurePrompt
  include DatasourcePrompt

  def initialize
    super(EXTRACTOR_REPL_DBHOST, "Database server hostname", PV_HOSTNAME)
    override_command_line_argument("direct-replication-host")
  end

  def load_default_value
    if get_topology().is_a?(DirectTopology)
      @default = @config.getProperty(get_dataservice_key(DATASERVICE_MASTER_MEMBER))
    else
      @default = @config.getProperty(get_member_key(REPL_DBHOST))
    end
  end

  def allow_group_default
    false
  end
end

class DirectDatasourceDBPort < ConfigurePrompt
  include DatasourcePrompt

  def initialize
    super(EXTRACTOR_REPL_DBPORT, "Database server port", PV_INTEGER)
    override_command_line_argument("direct-replication-port")
  end

  def load_default_value
    @default = @config.getProperty(get_member_key(REPL_DBPORT))
  end
  
  def required?
    super() && get_datasource().get_default_port() != nil
  end

  PortForConnectors.register(REPL_SERVICES, EXTRACTOR_REPL_DBPORT)
  PortForManagers.register(REPL_SERVICES, EXTRACTOR_REPL_DBPORT)
end

class DirectDatasourceDBUser < ConfigurePrompt
  include DatasourcePrompt

  def initialize
    super(EXTRACTOR_REPL_DBLOGIN, "Database login for Tungsten", 
      PV_IDENTIFIER, Configurator.instance.whoami())
    override_command_line_argument("direct-replication-user")
  end
  
  def load_default_value
    @default = @config.getProperty(get_member_key(REPL_DBLOGIN))
  end
end

class DirectDatasourceDBPassword < ConfigurePrompt
  include DatasourcePrompt
  include PrivateArgumentModule

  def initialize
    super(EXTRACTOR_REPL_DBPASSWORD, "Database password", 
      PV_ANY, "")
    override_command_line_argument("direct-replication-password")
  end
  
  def load_default_value
    @default = @config.getProperty(get_member_key(REPL_DBPASSWORD))
  end
end

class DirectDatasourceMasterLogDirectory < ConfigurePrompt
  include DatasourcePrompt
  
  def initialize
    super(EXTRACTOR_REPL_MASTER_LOGDIR, "Master log directory", 
      PV_FILENAME)
  end
  
  def load_default_value
    @default = get_datasource().get_default_master_log_directory()
  end
  
  def required?
    (get_default_value() != nil)
  end
end

class DirectDatasourceMasterLogPattern < ConfigurePrompt
  include DatasourcePrompt
  
  def initialize
    super(EXTRACTOR_REPL_MASTER_LOGPATTERN, "Master log filename pattern", PV_ANY)
  end
  
  def load_default_value
    @default = get_datasource().get_default_master_log_pattern()
  end

  def required?
    (get_default_value() != nil)
  end
end

class DirectDatasourceDisableRelayLogs < ConfigurePrompt
  include DatasourcePrompt
  include ConstantValueModule
  
  def initialize
    super(EXTRACTOR_REPL_DISABLE_RELAY_LOGS, "Disable the use of relay-logs?",
      PV_BOOLEAN)
  end
  
  def get_template_value
    @config.getTemplateValue(get_member_key(REPL_DISABLE_RELAY_LOGS))
  end
end

class DatasourceTHLURL < ConfigurePrompt
  include DatasourcePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_DBTHLURL, "Datasource THL URL")
  end
  
  def get_template_value
    get_datasource().get_thl_uri()
  end
end

class DatasourceJDBCURL < ConfigurePrompt
  include DatasourcePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_DBJDBCURL, "Datasource JDBC URL")
  end
  
  def get_template_value
    get_datasource().getJdbcUrl()
  end
end

class DatasourceJDBCSSLOptions < ConfigurePrompt
  include DatasourcePrompt
  include ConstantValueModule
 
  def initialize
    super(REPL_DBJDBCURLSSLOPTIONS, "Datasource JDBC URL SSL options")
  end
 
  def get_template_value
    get_datasource().getJdbcUrlSSLOptions()
  end
end

class DatasourceExtractorJDBCURL < ConfigurePrompt
  include DatasourcePrompt
  include ConstantValueModule
  
  def initialize
    super(EXTRACTOR_REPL_DBJDBCURL, "Datasource Extraction JDBC URL")
  end
  
  def get_template_value
    get_datasource().getExtractorJdbcUrl()
  end
end

class DatasourceJDBCQueryURL < ConfigurePrompt
  include DatasourcePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_DBJDBCQUERYURL, "Datasource JDBC Query URL")
  end
  
  def load_default_value
    @default = get_datasource().getJdbcQueryUrl()
  end
end

class DatasourceExtractorJDBCQueryURL < ConfigurePrompt
  include DatasourcePrompt
  include ConstantValueModule
  
  def initialize
    super(EXTRACTOR_REPL_DBJDBCQUERYURL, "Datasource Extraction JDBC Query URL")
  end
  
  def load_default_value
    @default = get_extractor_datasource().getJdbcQueryUrl()
  end
end

class DatasourceJDBCDriver < ConfigurePrompt
  include DatasourcePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_DBJDBCDRIVER, "Datasource JDBC Driver")
  end
  
  def get_template_value
    get_datasource().getJdbcDriver()
  end
end

class DatasourceVendor < ConfigurePrompt
  include DatasourcePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_DBJDBCVENDOR, "Datasource Vendor")
  end
  
  def get_template_value
    get_datasource().getVendor()
  end
end

class DatasourceJDBCScheme < ConfigurePrompt
  include DatasourcePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_DBJDBCSCHEME, "Datasource JDBC Scheme")
  end
  
  def get_template_value
    get_datasource().getJdbcScheme()
  end
end

class DatasourceBackupAgents < ConfigurePrompt
  include DatasourcePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_DBBACKUPAGENTS, "Datasource Backup Agents")
  end
  
  def get_template_value
    get_datasource().get_backup_agents().join(",")
  end
end

class DatasourceDefaultBackupAgent < ConfigurePrompt
  include DatasourcePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_DBDEFAULTBACKUPAGENT, "Datasource Default Backup Agent")
  end
  
  def get_template_value
    get_datasource().get_default_backup_agent().split(",").at(0).to_s()
  end
end
