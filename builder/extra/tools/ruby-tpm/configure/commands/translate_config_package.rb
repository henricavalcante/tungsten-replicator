class TranslateConfigCommand
  include ConfigureCommand
  include ResetConfigPackageModule
  include ClusterCommandModule
  include DisabledForExternalConfiguration
  
  def get_validation_checks
    []
  end
  
  def allow_undefined_dataservice?
    true
  end
  
  def parsed_options?(arguments)
    arguments = super(arguments)
    
    if display_help?() && !display_preview?()
      return true
    end
    
    @config_file = nil
    
    opts = OptionParser.new
    opts.on("--import-config String") {|val| @config_file=val}
    remainder = Configurator.instance.run_option_parser(opts, arguments)
    
    if @config_file == nil
      error "You must specify the file to translate with the --translate-config argument"
    end
    
    remainder
  end
  
  def output_command_usage
    super()
  
    output_usage_line("--import-config", "The Tungsten 1.3 config file to import")
  end
  
  def get_bash_completion_arguments
    super() + ["--import-config"]
  end
    
  def run
    import_props = Properties.new()
    import_props.force_json = false
    import_props.load(@config_file)
    
    dbtype = import_props.getProperty(TungstenParameterNames_1_3::GLOBAL_DBMS_TYPE)
    
    if command_dataservices().empty?()
      command_dataservices(import_props.getProperty(TungstenParameterNames_1_3::GLOBAL_DSNAME))
    end
    
    add_to_config(@dataservice_options, [DATASERVICES, DEFAULTS], {
      DATASERVICE_CONNECTORS => import_props.getProperty(TungstenParameterNames_1_3::GLOBAL_HOSTS),
      DATASERVICE_MASTER_MEMBER => import_props.getProperty(TungstenParameterNames_1_3::REPL_MASTERHOST),
      DATASERVICE_MEMBERS => import_props.getProperty(TungstenParameterNames_1_3::GLOBAL_HOSTS),
      DATASERVICE_VIP_ENABLED => (((v = import_props.getProperty(TungstenParameterNames_1_3::REPL_MASTER_VIP)) != "none") ? "true" : nil),
      DATASERVICE_VIP_IPADDRESS => (((v = import_props.getProperty(TungstenParameterNames_1_3::REPL_MASTER_VIP)) != "none") ? v : nil),
      DATASERVICE_VIP_NETMASK => ((import_props.getProperty(TungstenParameterNames_1_3::REPL_MASTER_VIP) != "none") ? import_props.getProperty(TungstenParameterNames_1_3::REPL_MASTER_VIP_NETMASK) : nil),
      DATASERVICE_WITNESSES => import_props.getProperty(TungstenParameterNames_1_3::GLOBAL_WITNESSES)
    })
    
    root_prefix = "false"
    if import_props.getProperty(TungstenParameterNames_1_3::REPL_BACKUP_COMMAND_PREFIX) == "true"
      root_prefix = "true"
    end
    if import_props.getProperty(TungstenParameterNames_1_3::GLOBAL_ROOT_PREFIX) == "true"
      root_prefix = "true"
    end
    
    add_to_config(@host_options, [HOSTS, DEFAULTS], {
      USERID => import_props.getProperty(TungstenParameterNames_1_3::GLOBAL_USERID),
      ROOT_PREFIX => root_prefix,
      SVC_INSTALL => import_props.getProperty(TungstenParameterNames_1_3::GLOBAL_SVC_INSTALL),
      SVC_START => import_props.getProperty(TungstenParameterNames_1_3::GLOBAL_SVC_START),
    })
    
    add_to_config(@replication_options, [REPL_SERVICES, DEFAULTS], {
      REPL_DBTYPE => dbtype,
      REPL_AUTOENABLE => import_props.getProperty(TungstenParameterNames_1_3::REPL_AUTOENABLE),
      REPL_BACKUP_DUMP_DIR => import_props.getProperty(TungstenParameterNames_1_3::REPL_BACKUP_DUMP_DIR),
      REPL_BACKUP_METHOD => import_props.getProperty(TungstenParameterNames_1_3::REPL_BACKUP_METHOD),
      REPL_BACKUP_ONLINE => import_props.getProperty(TungstenParameterNames_1_3::REPL_BACKUP_ONLINE),
      REPL_BACKUP_RETENTION => import_props.getProperty(TungstenParameterNames_1_3::REPL_BACKUP_RETENTION),
      REPL_BACKUP_SCRIPT => import_props.getProperty(TungstenParameterNames_1_3::REPL_BACKUP_SCRIPT),
      REPL_BACKUP_STORAGE_DIR => import_props.getProperty(TungstenParameterNames_1_3::REPL_BACKUP_STORAGE_DIR),
      REPL_BOOT_SCRIPT => import_props.getProperty(TungstenParameterNames_1_3::REPL_BOOT_SCRIPT),
      REPL_BUFFER_SIZE => import_props.getProperty(TungstenParameterNames_1_3::REPL_BUFFER_SIZE),
      REPL_CONSISTENCY_POLICY => import_props.getProperty(TungstenParameterNames_1_3::REPL_CONSISTENCY_POLICY),
      REPL_DBLOGIN => import_props.getProperty(TungstenParameterNames_1_3::REPL_DBLOGIN),
      REPL_DBPASSWORD => import_props.getProperty(TungstenParameterNames_1_3::REPL_DBPASSWORD),
      REPL_DBPORT => import_props.getProperty(TungstenParameterNames_1_3::REPL_DBPORT),
      REPL_DISABLE_RELAY_LOGS => ((import_props.getProperty(TungstenParameterNames_1_3::REPL_EXTRACTOR_USE_RELAY_LOGS) == "true") ? "false" : "true"),
      REPL_JAVA_MEM_SIZE => import_props.getProperty(TungstenParameterNames_1_3::REPL_JAVA_MEM_SIZE),
      REPL_LOG_DIR=> import_props.getProperty(TungstenParameterNames_1_3::REPL_LOG_DIR),
      REPL_RELAY_LOG_DIR => import_props.getProperty(TungstenParameterNames_1_3::REPL_RELAY_LOG_DIR),
      REPL_THL_DO_CHECKSUM => import_props.getProperty(TungstenParameterNames_1_3::REPL_THL_DO_CHECKSUM),
      REPL_THL_LOG_CONNECTION_TIMEOUT => import_props.getProperty(TungstenParameterNames_1_3::REPL_THL_LOG_CONNECTION_TIMEOUT),
      REPL_THL_LOG_FILE_SIZE => import_props.getProperty(TungstenParameterNames_1_3::REPL_THL_LOG_FILE_SIZE),
      REPL_THL_LOG_RETENTION => import_props.getProperty(TungstenParameterNames_1_3::REPL_THL_LOG_RETENTION),
    })
    
    add_to_config(@manager_options, [MANAGERS, DEFAULTS], {
      MGR_DB_PING_TIMEOUT => import_props.getProperty(TungstenParameterNames_1_3::MGR_DB_PING_TIMEOUT),
      MGR_HOST_PING_TIMEOUT => import_props.getProperty(TungstenParameterNames_1_3::MGR_HOST_PING_TIMEOUT),
      MGR_MONITOR_INTERVAL => import_props.getProperty(TungstenParameterNames_1_3::MON_DB_CHECK_FREQUENCY),
      MGR_NOTIFICATIONS_SEND => import_props.getProperty(TungstenParameterNames_1_3::MGR_NOTIFICATIONS_SEND),
      MGR_NOTIFICATIONS_TIMEOUT => import_props.getProperty(TungstenParameterNames_1_3::MGR_NOTIFICATIONS_TIMEOUT),
      MGR_POLICY_FAIL_THRESHOLD => import_props.getProperty(TungstenParameterNames_1_3::MGR_POLICY_FAIL_THRESHOLD),
      MGR_POLICY_FENCE_MASTER_REPLICATOR => import_props.getProperty(TungstenParameterNames_1_3::MGR_POLICY_FENCE_MASTER_REPLICATOR),
      MGR_POLICY_FENCE_SLAVE_REPLICATOR => import_props.getProperty(TungstenParameterNames_1_3::MGR_POLICY_FENCE_SLAVE_REPLICATOR),
      MGR_POLICY_LIVENESS_SAMPLE_PERIOD_SECS => import_props.getProperty(TungstenParameterNames_1_3::MGR_POLICY_LIVENESS_SAMPLE_PERIOD_SECS),
      MGR_POLICY_LIVENESS_SAMPLE_PERIOD_THRESHOLD => import_props.getProperty(TungstenParameterNames_1_3::MGR_POLICY_LIVENESS_SAMPLE_PERIOD_THRESHOLD),
      MGR_POLICY_MODE => import_props.getProperty(TungstenParameterNames_1_3::POLICY_MGR_MODE),
      MGR_POLICY_NOTIFICATION_ADJUST_BACKOFF => import_props.getProperty(TungstenParameterNames_1_3::MGR_POLICY_NOTIFICATION_ADJUST_BACKOFF),
      MGR_POLICY_NOTIFICATION_ADJUST_THRESHOLD=> import_props.getProperty(TungstenParameterNames_1_3::MGR_POLICY_NOTIFICATION_ADJUST_THRESHOLD),
      MGR_POLICY_NOTIFICATION_MAX_TIMEOUT => import_props.getProperty(TungstenParameterNames_1_3::MGR_POLICY_NOTIFICATION_MAX_TIMEOUT),
      MGR_POLICY_SUCCESSFUL_NOTIFICATION_ADJUST_THRESHOLD => import_props.getProperty(TungstenParameterNames_1_3::MGR_POLICY_SUCCESSFUL_NOTIFICATION_ADJUST_THRESHOLD),
      MGR_ROUTER_STATUS_TIMEOUT => import_props.getProperty(TungstenParameterNames_1_3::MGR_ROUTER_STATUS_TIMEOUT),
      MGR_VIP_ARP_PATH => import_props.getProperty(TungstenParameterNames_1_3::REPL_MASTER_VIP_ARP),
      MGR_VIP_IFCONFIG_PATH => import_props.getProperty(TungstenParameterNames_1_3::REPL_MASTER_VIP_IFCONFIG),
      MGR_VIP_DEVICE => import_props.getProperty(TungstenParameterNames_1_3::REPL_MASTER_VIP_DEVICE),
      MON_DB_QUERY_TIMEOUT => import_props.getProperty(TungstenParameterNames_1_3::MON_DB_QUERY_TIMEOUT),
      MGR_IDLE_ROUTER_TIMEOUT => import_props.getProperty(TungstenParameterNames_1_3::MGR_IDLE_ROUTER_TIMEOUT),
      MGR_POLICY_NOTIFICATION_BACKOFF_TRY_THRESHOLD => import_props.getProperty(TungstenParameterNames_1_3::MGR_POLICY_NOTIFICATION_BACKOFF_TRY_THRESHOLD)
    })
    
    add_to_config(@connector_options, [CONNECTORS, DEFAULTS], {
      CONN_AUTORECONNECT => import_props.getProperty(TungstenParameterNames_1_3::CONN_RECONNECT),
      CONN_CLIENTDEFAULTDB => import_props.getProperty(TungstenParameterNames_1_3::CONN_CLIENTDEFAULTDB),
      CONN_CLIENTLOGIN => import_props.getProperty(TungstenParameterNames_1_3::CONN_CLIENTLOGIN),
      CONN_CLIENTPASSWORD => import_props.getProperty(TungstenParameterNames_1_3::CONN_CLIENTPASSWORD),
      CONN_DELETE_USER_MAP => import_props.getProperty(TungstenParameterNames_1_3::CONN_DELETE_USER_MAP),
      CONN_LISTEN_PORT => import_props.getProperty(TungstenParameterNames_1_3::CONN_LISTEN_PORT),
      CONN_RWSPLITTING => import_props.getProperty(TungstenParameterNames_1_3::CONN_RWSPLITTING),
      CONN_SLAVE_STATUS_IS_RELATIVE => import_props.getProperty(TungstenParameterNames_1_3::CONN_SLAVE_STATUS_IS_RELATIVE),
      ROUTER_WAITFOR_DISCONNECT_TIMEOUT => import_props.getProperty(TungstenParameterNames_1_3::ROUTER_WAITFOR_DISCONNECT),
      
      # Keep These?
      ROUTER_DELAY_BEFORE_OFFLINE => import_props.getProperty(TungstenParameterNames_1_3::SQLR_DELAY_BEFORE_OFFLINE),
      ROUTER_KEEP_ALIVE_TIMEOUT => import_props.getProperty(TungstenParameterNames_1_3::SQLR_KEEP_ALIVE_TIMEOUT)
    })
    
    if dbtype == DBMS_MYSQL
      add_to_config(@host_options, [HOSTS, DEFAULTS], {
        REPL_MYSQL_CONNECTOR_PATH => import_props.getProperty(TungstenParameterNames_1_3::GLOBAL_MYSQL_CONNECTOR_PATH),
      })
      add_to_config(@replication_options, [REPL_SERVICES, DEFAULTS], {
        REPL_MASTER_LOGDIR => import_props.getProperty(TungstenParameterNames_1_3::REPL_MYSQL_BINLOGDIR),
        REPL_MASTER_LOGPATTERN => import_props.getProperty(TungstenParameterNames_1_3::REPL_MYSQL_BINLOGPATTERN),
        REPL_MYSQL_CONF => import_props.getProperty(TungstenParameterNames_1_3::REPL_MYSQL_MYCNF),
        REPL_MYSQL_DATADIR => import_props.getProperty(TungstenParameterNames_1_3::REPL_MYSQL_DATADIR),
        REPL_MYSQL_RO_SLAVE => import_props.getProperty(TungstenParameterNames_1_3::REPL_MYSQL_RO_SLAVE),
        REPL_MYSQL_USE_BYTES_FOR_STRING => import_props.getProperty(TungstenParameterNames_1_3::REPL_USE_BYTES),
        REPL_MYSQL_XTRABACKUP_DIR => import_props.getProperty(TungstenParameterNames_1_3::REPL_MYSQL_XTRABACKUP_DIR)
      })
    elsif dbtype == DBMS_POSTGRESQL_WAL
      add_to_config(@replication_options, [REPL_SERVICES, DEFAULTS], {
        REPL_PG_ARCHIVE => import_props.getProperty(TungstenParameterNames_1_3::REPL_PG_ARCHIVE),
        REPL_PG_ARCHIVE_TIMEOUT => import_props.getProperty(TungstenParameterNames_1_3::REPL_PG_ARCHIVE_TIMEOUT),
        REPL_PG_CONF => import_props.getProperty(TungstenParameterNames_1_3::REPL_PG_POSTGRESQL_CONF),
        REPL_PG_HOME => import_props.getProperty(TungstenParameterNames_1_3::REPL_PG_HOME),
        REPL_PG_ROOT => import_props.getProperty(TungstenParameterNames_1_3::REPL_PG_ROOT)
      })
    end
    
    load_cluster_options()
    
    is_valid?()
  end
  
  def add_to_config(obj, prefix, properties = {})
    properties.each{
      |k,v|
      key = prefix + [k]
      
      if v == ""
        next
      end
      begin
        value = obj.getNestedProperty(key)
        if value == nil
          obj.setProperty(key, v)
        end
      rescue => e
        obj.setProperty(key, v)
      end
    }
  end
  
  def self.get_command_name
    'translate-config'
  end
  
  def self.display_command
    false
  end
end

module TungstenParameterNames_1_3
  # Generic parameters that control the entire installation.
  GLOBAL_DSNAME = "global_data_service_name"
  GLOBAL_HOST = "global_host_name"
  GLOBAL_IP_ADDRESS = "global_ip_address"
  GLOBAL_USERID = "global_userid"
  GLOBAL_DBMS_TYPE = "global_dbms_type"

  # Operating system service parameters.
  GLOBAL_SVC_INSTALL = "global_install_svc_scripts"
  GLOBAL_SVC_START = "global_start_svc_scripts"
  GLOBAL_ROOT_PREFIX = "global_root_command_prefix"
  GLOBAL_RESTART_DBMS = "global_restart_dbms"

  # Network control parameters.
  GLOBAL_GC_MEMBERSHIP = "global_gc_membership_protocol"
  GLOBAL_GOSSIP_PORT = "global_gossip_port"
  GLOBAL_GOSSIP_HOSTS = "global_gossip_hosts"
  GLOBAL_HOSTS = "global_hosts"
  GLOBAL_WITNESSES = "global_witnesses"

  # Generic replication parameters.
  REPL_ACTIVE = "repl_is_active"
  REPL_MONITOR_ACTIVE = "repl_is_monitor_active"
  REPL_AUTOENABLE = "repl_auto_enable"
  REPL_ROLE = "repl_role"
  REPL_MASTERHOST = "repl_master_host"
  REPL_MASTERPORT = "repl_master_port"
  REPL_SLAVEHOST = "repl_slave_host"
  REPL_BACKUP_METHOD = "repl_backup_method"
  REPL_BACKUP_DUMP_DIR = "repl_backup_dump_dir"
  REPL_BACKUP_STORAGE_DIR = "repl_backup_storage_dir"
  REPL_BACKUP_RETENTION = "repl_backup_retention"
  REPL_BACKUP_SCRIPT = "repl_backup_script"
  REPL_BACKUP_ONLINE = "repl_backup_online"
  REPL_BACKUP_COMMAND_PREFIX = "repl_backup_command_prefix"
  REPL_BOOT_SCRIPT = "repl_boot_script"
  REPL_DBPORT = "repl_dbport"
  REPL_LOG_TYPE = "repl_log_type"
  REPL_LOG_DIR = "repl_log_dir"
  REPL_DBLOGIN = "repl_admin_login"
  REPL_DBPASSWORD = "repl_admin_password"
  REPL_MONITOR_INTERVAL = "repl_monitor_interval_millisecs"
  REPL_JAVA_MEM_SIZE = "repl_java_mem_size"
  REPL_BUFFER_SIZE = "repl_buffer_size"
  REPL_REMOTE_MASTER_VIP  = "repl_remote_master_vip"
  REPL_MASTER_VIP  = "repl_master_vip"
  REPL_MASTER_VIP_NETMASK = "repl_master_vip_netmask"
  REPL_MASTER_VIP_DEVICE = "repl_master_vip_device"
  REPL_MASTER_VIP_IFCONFIG = "repl_master_vip_ifconfig"
  REPL_MASTER_VIP_ARP = "repl_master_vip_arp"
  REPL_EXTRACTOR_USE_RELAY_LOGS = "repl_extractor_use_relay_logs"
  REPL_RELAY_LOG_DIR = "repl_relay_log_dir"
  REPL_THL_DO_CHECKSUM = "repl_thl_do_checksum"
  REPL_THL_LOG_CONNECTION_TIMEOUT = "repl_thl_log_connection_timeout"
  REPL_THL_LOG_FILE_SIZE = "repl_thl_log_file_size"

  REPL_THL_LOG_RETENTION = "repl_thl_log_retention"
  REPL_CONSISTENCY_POLICY = "repl_consistency_policy"

  REPL_USE_BYTES = "repl_use_bytes"
  REPL_USE_DRIZZLE = "repl_use_drizzle"

  # MySQL-specific parameters
  GLOBAL_MYSQL_CONNECTOR_PATH = "global_mysql_connector_path"
  REPL_MYSQL_BINLOGDIR = "repl_mysql_binlog_dir"
  REPL_MYSQL_MYCNF = "repl_mysql_mycnf"
  REPL_MYSQL_BINLOGPATTERN = "repl_mysql_binlog_pattern"
  REPL_MYSQL_RO_SLAVE = "repl_mysql_ro_slave"
  REPL_MYSQL_DATADIR = "repl_mysql_data_dir"
  REPL_MYSQL_XTRABACKUP_DIR = "repl_mysql_xtrabackup_dir"

  # Oracle-specific parameters.
  REPL_ORACLE_SERVICE = "repl_oracle_service"
  REPL_ORACLE_DSPORT = "repl_oracle_dslisten_port"
  REPL_ORACLE_HOME = "repl_oracle_home"
  REPL_ORACLE_LICENSE = "repl_oracle_license"
  REPL_ORACLE_SCHEMA = "repl_oracle_schema"
  REPL_ORACLE_LICENSED_SLAVE = "repl_oracle_licensed_slave"

  # PostgreSQL-specific parameters.
  REPL_PG_REPLICATOR = "repl_pg_replicator"
  REPL_PG_STREAMING = "repl_pg_streaming"
  REPL_PG_LOND_DATABASE = "repl_pg_lond_database"
  REPL_PG_HOME = "repl_pg_home"
  REPL_PG_ROOT = "repl_pg_root"
  REPL_PG_POSTGRESQL_CONF = "repl_pg_postgresql_conf"
  REPL_PG_ARCHIVE = "repl_pg_archive"
  REPL_PG_ARCHIVE_TIMEOUT = "repl_pg_archive_timeout"

  ROUTER_WAITFOR_DISCONNECT = "router_waitfor_disconnect"

  CONN_ACTIVE = "conn_is_active"
  CONN_CLIENTLOGIN = "conn_client_login"
  CONN_CLIENTPASSWORD = "conn_client_password"
  CONN_CLIENTDEFAULTDB = "conn_client_default_db"
  CONN_RWSPLITTING = "conn_rw_splitting"
  CONN_LISTEN_PORT = "conn_listen_port"
  CONN_DELETE_USER_MAP = "conn_delete_user_map"
  CONN_RECONNECT = "conn_reconnect"
  CONN_SLAVE_STATUS_IS_RELATIVE = "conn_slave_status_is_relative"

  POLICY_MGR_MODE = "policy_mgr_mode"

  SQLR_ACTIVE = "sqlr_is_active"
  SQLR_USENEWPROTOCOL = "sqlr_use_new_protocol"
  SQLR_MANAGER_LIST = "sql_manager_list"
  SQLR_DELAY_BEFORE_OFFLINE = "sqlr_delay_before_offline"
  SQLR_KEEP_ALIVE_TIMEOUT = "sqlr_keep_alive_timeout"

  MGR_ACTIVE = "mgr_is_active"
  MGR_DB_PING_TIMEOUT = "mgr_db_ping_timeout"
  MGR_HOST_PING_TIMEOUT = "mgr_host_ping_timeout"
  MGR_IDLE_ROUTER_TIMEOUT = "mgr_idle_router_timeout"
  MGR_POLICY_LIVENESS_SAMPLE_PERIOD_SECS = "mgr_policy_liveness_sample_period_secs"
  MGR_POLICY_LIVENESS_SAMPLE_PERIOD_THRESHOLD = "mgr_policy_liveness_sample_period_threshold"
  MGR_POLICY_FENCE_SLAVE_REPLICATOR = "mgr_policy_fence_slave_replicator"
  MGR_POLICY_FENCE_MASTER_REPLICATOR = "mgr_policy_fence_master_replicator"
  MGR_POLICY_FAIL_THRESHOLD = "mgr_policy_fail_threshold"
  MGR_POLICY_NOTIFICATION_ADJUST_AUTO = "mgr_policy_notification_adjust_auto"
  MGR_POLICY_NOTIFICATION_ADJUST_BACKOFF = "mgr_policy_notification_adjust_backoff"
  MGR_POLICY_NOTIFICATION_MAX_TIMEOUT                 = "mgr_policy_max_timeout"
  MGR_POLICY_NOTIFICATION_ADJUST_THRESHOLD            = "mgr_policy_notification_adjust_threshold"
  MGR_POLICY_SUCCESSFUL_NOTIFICATION_ADJUST_THRESHOLD = "mgr_policy_successful_notification_adjust_threshold"
  MGR_POLICY_NOTIFICATION_BACKOFF_TRY_THRESHOLD       = "mgr_policy_notification_backoff_try_threshold"
  MGR_NOTIFICATIONS_TIMEOUT = "mgr_notifications_timeout"
  MGR_NOTIFICATIONS_SEND = "mgr_notifications_send"
  MGR_POLICY_SLAVE_PROMOTION_LATENCY_THRESHOLD = "mgr_policy_slave_promotion_latency_threshold"
  MGR_ROUTER_STATUS_TIMEOUT = "mgr_router_status_timeout"

  MON_REPLICATOR_CHECK_FREQUENCY = "mon_replicator_check_frequency"
  MON_DB_CHECK_FREQUENCY = "mon_db_check_frequency"
  MON_DB_QUERY_TIMEOUT = "mon_db_query_timeout"

end