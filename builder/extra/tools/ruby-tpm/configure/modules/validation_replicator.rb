require 'socket'

class ReplicatorChecks < GroupValidationCheck
  include ClusterHostCheck
  
  def initialize
    super(REPL_SERVICES, "replication service", "replication services")
    
    ReplicationServiceValidationCheck.submodules().each{
      |klass|
      
      self.add_check(klass.new())
    }
  end
  
  def set_vars
    @title = "Replication service checks"
  end
end

module ReplicationServiceValidationCheck
  include GroupValidationCheckMember
  include ReplicatorEnabledCheck
  
  def get_applier_datasource
    ConfigureDatabasePlatform.build([@parent_group.name, get_member()], @config)
  end
  
  def get_extractor_datasource
    get_applier_datasource()
  end
  
  def get_host_key(key)
    [HOSTS, @config.getProperty(get_member_key(DEPLOYMENT_HOST)), key]
  end
  
  def get_dataservice
    if self.is_a?(HashPromptMemberModule)
      get_member()
    else
      @config.getPropertyOr(get_member_key(DEPLOYMENT_DATASERVICE), nil)
    end
  end
  
  def get_topology
    Topology.build(get_dataservice(), @config)
  end
  
  def get_dataservice_key(key)
    return [DATASERVICES, get_dataservice(), key]
  end
  
  def get_applier_key(key)
    get_member_key(key)
  end
  
  def get_extractor_key(key)
    get_member_key(key)
  end
  
  def self.included(subclass)
    @submodules ||= []
    @submodules << subclass
  end

  def self.submodules
    @submodules || []
  end
end

module CreateServiceCheck
end

module ModifyServiceCheck
  def enabled?
    false
  end
end

class THLDirectoryWriteableCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  
  def set_vars
    @title = "THL directory writeable check"
  end
  
  def validate
    dir = @config.getProperty(get_member_key(REPL_LOG_DIR))
    unless File.writable?(dir)
      if File.exists?(dir)
        if File.directory?(dir)
          error("Unable to write to the THL directory (#{dir})")
        else
          error("The THL directory (#{dir}) is a file")
        end
      else
        begin
          FileUtils.mkdir_p(dir)
          FileUtils.rmdir(dir)
        rescue => e
          error("Unable to create the THL directory (#{dir})")
          error(e.message)
        end
      end
    end
  end
end

class RelayDirectoryWriteableCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  
  def set_vars
    @title = "Relay directory writeable check"
  end
  
  def validate
    dir = @config.getProperty(get_member_key(REPL_RELAY_LOG_DIR))
    unless File.writable?(dir)
      if File.exists?(dir)
        if File.directory?(dir)
          error("Unable to write to the relay directory (#{dir})")
        else
          error("The relay directory (#{dir}) is a file")
        end
      else
        begin
          FileUtils.mkdir_p(dir)
          FileUtils.rmdir(dir)
        rescue => e
          error("Unable to create the relay directory (#{dir})")
          error(e.message)
        end
      end
    end
  end
end

class BackupDumpDirectoryWriteableCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  
  def set_vars
    @title = "Backup temp directory writeable check"
  end
  
  def validate
    dir = @config.getProperty(get_member_key(REPL_BACKUP_DUMP_DIR))
    unless File.writable?(dir)
      if File.exists?(dir)
        if File.directory?(dir)
          error("Unable to write to the backup temp directory (#{dir})")
        else
          error("The backup temp directory (#{dir}) is a file")
        end
      else
        begin
          FileUtils.mkdir_p(dir)
          FileUtils.rmdir(dir)
        rescue => e
          error("Unable to create the backup temp directory (#{dir})")
          error(e.message)
        end
      end
    end
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_BACKUP_METHOD)) != "none"
  end
end

class BackupDirectoryWriteableCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  
  def set_vars
    @title = "Backup storage directory writeable check"
  end
  
  def validate
    dir = @config.getProperty(get_member_key(REPL_BACKUP_STORAGE_DIR))
    unless File.writable?(dir)
      if File.exists?(dir)
        if File.directory?(dir)
          error("Unable to write to the backup storage directory (#{dir})")
        else
          error("The backup storage directory (#{dir}) is a file")
        end
      else
        begin
          FileUtils.mkdir_p(dir)
          FileUtils.rmdir(dir)
        rescue => e
          error("Unable to create the backup storage directory (#{dir})")
          error(e.message)
        end
      end
    end
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_BACKUP_METHOD)) != "none"
  end
end

class BackupScriptAvailableCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  
  def set_vars
    @title = "Backup script availability check"
  end
  
  def validate    
    if File.executable?(@config.getProperty(get_member_key(REPL_BACKUP_SCRIPT)))
      info("The backup script is executable")
    else
      if File.exists?(@config.getProperty(get_member_key(REPL_BACKUP_SCRIPT)))
        error("The backup script (#{@config.getProperty(get_member_key(REPL_BACKUP_SCRIPT))}) is not executable")
      else
        error("The backup script (#{@config.getProperty(get_member_key(REPL_BACKUP_SCRIPT))}) does not exist")
      end
    end
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_BACKUP_METHOD)) == "script"
  end
end

class THLStorageCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  
  def set_vars
    @title = "THL storage check"
    self.extend(NotTungstenUpdateCheck)
  end
  
  def validate
    repl_log_dir = @config.getProperty(get_member_key(REPL_LOG_DIR))
    if repl_log_dir
      if File.exists?(repl_log_dir) && !File.directory?(repl_log_dir)
        error("Replication log directory #{repl_log_dir} already exists as a file")
      elsif File.exists?(repl_log_dir)
        dir_file_count = cmd_result("ls #{repl_log_dir} | wc -l")
        if dir_file_count.to_i() > 0
          error("Replication log directory #{repl_log_dir} already contains log files")
        end
      end
    end
    
    begin
      get_applier_datasource.check_thl_schema(@config.getProperty(get_member_key(REPL_SVC_SCHEMA)))
    rescue => e
      error(e.message)
    end
  end
  
  def enabled?
    super() && (get_topology().is_a?(ClusterSlaveTopology) != true)
  end
end

class THLSchemaChangeCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  
  def set_vars
    @title = "THL schema change check"
    self.extend(TungstenUpdateCheck)
  end
  
  def validate
    schema_key = get_member_key(REPL_SVC_SCHEMA).join('.')
    begin
      raw = cmd_result(@config.getProperty(CURRENT_RELEASE_DIRECTORY) + "/tools/tpm query values #{HOST_ENABLE_REPLICATOR} #{schema_key}")
    rescue CommandError => ce
      return
    end
    
    begin
      result = JSON.parse(raw)
      if result[HOST_ENABLE_REPLICATOR] == "true"
        if result[schema_key] != @config.getProperty(schema_key)
          begin
            get_applier_datasource.check_thl_schema(@config.getProperty(get_member_key(REPL_SVC_SCHEMA)))
          rescue => e
            error(e.message)
          end
        end
      end
    rescue JSON::ParserError
      return
    end
  end
  
  def enabled?
    super() && (get_topology().is_a?(ClusterTopology) == true)
  end
end

class RowBasedBinaryLoggingCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck

  def set_vars
    @title = "Row Based Binary Logging Check"
  end
  
  def validate
    if get_extractor_datasource.get_default_binlog_format().upcase !="ROW"
      error("The MySQL datasource binlog_format must be set to 'ROW' for heterogenous replication. The MySQL configuration file does not include binlog_format=row")
    elsif get_extractor_datasource.get_value("show variables like 'binlog_format'", "Value") !="ROW"
      error("The MySQL datasource binlog_format must be set to 'ROW' for heterogenous replication.")
    end
  end

  def enabled?
    # If this service is configured to be a master by default
    if @config.getProperty(get_member_key(REPL_ROLE)) == REPL_ROLE_M
      is_master = true
    else
      is_master = false
    end
    
    # If this is part of a cluster then it may become a master
    if get_topology().class == ClusterTopology
      is_master = true
    end
    
    super() \
      && get_extractor_datasource().class == MySQLDatabasePlatform \
        && (is_master == true) \
          && @config.getProperty(get_member_key(ENABLE_HETEROGENOUS_MASTER))  == "true"
  end
end

class SwappinessCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck

  def set_vars
    @title = "Linux Swappiness Check"
  end

  def validate
    swappiness = cmd_result("cat /proc/sys/vm/swappiness", true)
    reboot_swappiness = cmd_result("grep -rs '^vm.swappiness' /etc/sysctl.conf /etc/sysctl.d/*.conf | sed -E 's/[[:space:]]+//g' | cut -f2 -d'=' | tail -n1", true)
    if reboot_swappiness.to_s() == ""
      reboot_swappiness = 60
    end
    if swappiness.to_s() == ""
      swappiness = 60
    end
    if reboot_swappiness.to_i() > 10 || swappiness.to_i() > 10
      warning("Linux swappiness is currently set to #{swappiness}, on restart it will be #{reboot_swappiness}, consider setting this to 10 or under to avoid swapping.")
    elsif reboot_swappiness.to_i() != swappiness.to_i()
      warning("Linux swappiness will change after a restart. Current setting is #{swappiness}, on restart it will be #{reboot_swappiness}.")
    end
  end

  def enabled?
    super() && File.exists?("/proc/sys/vm/swappiness") && File.exists?("/etc/sysctl.conf")
  end
end


class ServiceTransferredLogStorageCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include CreateServiceCheck
  
  def set_vars
    @title = "Service transferred log storage check"
    self.extend(NotTungstenUpdateCheck)
  end
  
  def validate
    if File.exists?(@config.getProperty(get_member_key(REPL_RELAY_LOG_DIR)))
      dir_file_count = cmd_result("ls #{@config.getProperty(get_member_key(REPL_RELAY_LOG_DIR))} | wc -l")
      if dir_file_count.to_i() > 0
        error("Transferred log directory #{@config.getProperty(get_member_key(REPL_RELAY_LOG_DIR))} already contains log files")
      end
    end
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_RELAY_LOG_DIR)).to_s != ""
  end
end

class DifferentMasterSlaveCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  
  def set_vars
    @title = "Different master/slave datasource check"
  end
  
  def validate
    if (extractor = get_extractor_datasource())
      if extractor == get_applier_datasource()
        error("Service '#{@config.getProperty(get_member_key(DEPLOYMENT_SERVICE))}' uses the same datasource for extracting and applying events")
      end
    end
  end
end

class RMIListenerAddressCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  
  def set_vars
    @title = "RMI listener address check"
  end
  
  def validate
    addr = @config.getProperty(get_member_key(REPL_RMI_ADDRESS))
    if addr.to_s() == ""
      error("Unable to determine the listening address for RMI operations")
    end
  end
end

class THLListenerAddressCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  
  def set_vars
    @title = "THL listener address check"
  end
  
  def validate
    addr = @config.getProperty(get_member_key(REPL_SVC_THL_ADDRESS))
    if addr.to_s() == ""
      error("Unable to determine the listening address for THL operations")
    end
  end
end

class ParallelReplicationCountCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  
  def set_vars
    @title = "Parallel replication count check"
  end
  
  def validate
    p = @config.getPromptHandler().find_prompt(get_member_key(REPL_SVC_CHANNELS))
    host_channels = @config.getNestedProperty(get_member_key(REPL_SVC_CHANNELS))
    ds_channels = p.get_default_value()
    
    if host_channels != nil && host_channels != ds_channels
      error("You are trying to configure this host with a custom replication channels setting.  That is not currently supported.  Please update the host configuration with --channels=#{ds_channels}")
    end
  end
  
  def enabled?
    if get_topology().is_a?(ClusterTopology)
      super()
    else
      false
    end
  end
end

class DatasourceBootScriptCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  
  def set_vars
    @title = "Datasource boot script check"
  end
  
  def validate
    command = @config.getProperty(get_applier_key(REPL_DBSERVICE_STATUS))
    if command.to_s() == ""
      error("Unable to identify a command to control the datasource process")
      return
    end
    
    if @config.getProperty(get_host_key(ROOT_PREFIX)) == "true"
      begin
        # Test if this script can be run via sudo w/o actually running it
        cmd_result("sudo -n -l #{command}")
      rescue CommandError
        error("Unable to run 'sudo #{command}'")
        help("Update the /etc/sudoers file or disable sudo by adding --enable-sudo-access=false")
      end
      
      if is_valid?()
        begin
          cmd_result("sudo #{command}")
        rescue CommandError
          error("Unable to run 'sudo #{command}' or the database server is not running")
          help("Update the /etc/sudoers file or disable sudo by adding --enable-sudo-access=false")
        end
      end
    else
      begin
        cmd_result("#{command}")
      rescue CommandError
        warning("Unable to run '#{command}' or the database server is not running")
      end
    end
  end
  
  def enabled?
    return (super() && (get_topology().use_management?() || 
      (@config.getPropertyOr(get_member_key(REPL_BACKUP_METHOD), "") =~ /xtrabackup/))
    )
  end
end

class ParallelReplicationCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  
  def set_vars
    @title = "Parallel replication consistency check"
  end
  
  def validate
    ptype = @config.getProperty(get_member_key(REPL_SVC_PARALLELIZATION_TYPE))
    channels = @config.getProperty(get_member_key(REPL_SVC_CHANNELS))
    
    if ptype == "none" and channels != "1"
      error("Parallelization type is set to 'none' but channels are set to #{channels}; either set parallelization type to 'disk' or 'memory' using --svc-parallelization-type or set channels to 1 using --channels")
    elsif ptype == "memory"
      warning("The 'memory' parallelization type is not recommended for production use; use 'disk' instead")
    end
  end
end

class ConsistentReplicationCredentialsCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  
  def set_vars
    @title = "Consistent replication credentials check"
  end
  
  def validate
    p = @config.getPromptHandler().find_prompt(get_member_key(REPL_DBLOGIN))
    host_value = @config.getNestedProperty(get_member_key(REPL_DBLOGIN))
    ds_value = p.get_default_value()
    if host_value != nil && host_value != ds_value
      error("You are trying to configure this host with a custom --replication-user.  That is not currently supported.")
    end
    
    p = @config.getPromptHandler().find_prompt(get_member_key(REPL_DBPASSWORD))
    host_value = @config.getNestedProperty(get_member_key(REPL_DBPASSWORD))
    ds_value = p.get_default_value()
    if host_value != nil && host_value != ds_value
      error("You are trying to configure this host with a custom --replication-password.  That is not currently supported.")
    end
    
    p = @config.getPromptHandler().find_prompt(get_member_key(REPL_DBPORT))
    host_value = @config.getNestedProperty(get_member_key(REPL_DBPORT))
    ds_value = p.get_default_value()
    if host_value != nil && host_value != ds_value
      error("You are trying to configure this host with a custom --replication-port.  That is not currently supported.")
    end
  end
  
  def enabled?
    if get_topology().is_a?(ClusterTopology)
      super()
    else
      false
    end
  end
end
