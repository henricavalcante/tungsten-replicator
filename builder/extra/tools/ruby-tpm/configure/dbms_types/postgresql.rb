DBMS_POSTGRESQL = "postgresql"

class PostgreSQLDatabasePlatform < ConfigureDatabasePlatform
  def get_uri_scheme
    DBMS_POSTGRESQL
  end
  
  def get_default_backup_method
    "pg_dump"
  end
  
  def get_valid_backup_methods
    "none|pg_dump|script"
  end
  
  def run(command)
    begin
      ssh_result("echo '#{command}' | psql -q -A -t -p #{@port}", @host, @username)
    rescue RemoteError
      return ""
    end
  end
  
  def get_variable(name)
    run("show #{name}").chomp.strip;
  end
  
  def get_thl_uri
	  "jdbc:postgresql://${replicator.global.db.host}:${replicator.global.db.port}/${replicator.extractor.dbms.database}"
	end
  
  def get_default_port
    "5432"
  end
  
  def get_default_start_script
    "/etc/init.d/postgres"
  end
  
  def get_thl_filters()
    ["dropcomments"] + super()
  end
  
  def get_applier_filters()
    if @config.getProperty(REPL_POSTGRESQL_ENABLE_MYSQL2PGDDL) == "true"
      ["pgddl"] + super()
    else
      super()
    end
  end
  
  def get_default_master_log_directory
    nil
  end
  
  def get_default_master_log_pattern
    nil
  end
  
  def getBasicJdbcUrl()
    "jdbc:postgresql://${replicator.global.db.host}:${replicator.global.db.port}/"
  end
  
  def getJdbcUrl()
    "jdbc:postgresql://${replicator.global.db.host}:${replicator.global.db.port}/${replicator.extractor.dbms.database}"
  end
  
  def getJdbcDriver()
    "org.postgresql.Driver"
  end
  
  def getVendor()
    "postgresql"
  end
end

#
# Prompts
#

module PostgreSQLDatasourcePrompt
  def enabled?
    super() && (get_datasource().is_a?(PostgreSQLDatabasePlatform))
  end
  
  def enabled_for_config?
    super() && (get_datasource().is_a?(PostgreSQLDatabasePlatform))
  end
  
  def get_default_value
    begin
      if Configurator.instance.display_help? && !Configurator.instance.display_preview?
        raise ""
      end
      
      get_postgresql_default_value()
    rescue => e
      super()
    end
  end
  
  def get_postgresql_default_value
    raise "Undefined function"
  end
end

REPL_POSTGRESQL_DBNAME = "repl_postgresql_dbname"
class PostgreSQLDatabaseName < ConfigurePrompt
 include ReplicationServicePrompt
 
 def initialize
   super(REPL_POSTGRESQL_DBNAME, "Name of the database to replicate",
     PV_ANY)
 end
   
 def enabled?
   super() && get_extractor_datasource().is_a?(PostgreSQLDatabasePlatform)
 end
 
 def enabled_for_config?
   super() && get_extractor_datasource().is_a?(PostgreSQLDatabasePlatform)
 end
end

REPL_POSTGRESQL_SLONIK = "repl_postgresql_slonik"
class PostgreSQLSlonikPath < ConfigurePrompt
 include ReplicationServicePrompt
 
 def initialize
   super(REPL_POSTGRESQL_SLONIK, "Path to the slonik executable",
     PV_FILENAME)
 end
   
 def enabled?
   super() && get_extractor_datasource().is_a?(PostgreSQLDatabasePlatform) &&
     @config.getProperty(get_member_key(REPL_ROLE)) == REPL_ROLE_M
 end
 
 def enabled_for_config?
   super() && get_extractor_datasource().is_a?(PostgreSQLDatabasePlatform) &&
     @config.getProperty(get_member_key(REPL_ROLE)) == REPL_ROLE_M
 end
end

REPL_POSTGRESQL_TABLES = "repl_postgresql_tables"
class PostgreSQLTables < ConfigurePrompt
 include ReplicationServicePrompt
 
 def initialize
   super(REPL_POSTGRESQL_TABLES, "Tables to replicate in form: schema1.table1,schema2.table2,...",
     PV_ANY)
 end
   
 def enabled?
   super() && get_extractor_datasource().is_a?(PostgreSQLDatabasePlatform) &&
     @config.getProperty(get_member_key(REPL_ROLE)) == REPL_ROLE_M
 end
 
 def enabled_for_config?
   super() && get_extractor_datasource().is_a?(PostgreSQLDatabasePlatform) &&
     @config.getProperty(get_member_key(REPL_ROLE)) == REPL_ROLE_M
 end
end

REPL_POSTGRESQL_ENABLE_MYSQL2PGDDL = "repl_postgresql_enable_mysql2pgddl"
class PostgreSQLDDL < ConfigurePrompt
 include ReplicationServicePrompt
 
 def initialize
   super(REPL_POSTGRESQL_ENABLE_MYSQL2PGDDL, "Enable MySQL -> PostgreSQL DDL dialect converting filter placeholder",
     PV_ANY, "false")
 end
   
 def enabled?
   super() &&
     get_applier_datasource().is_a?(PostgreSQLDatabasePlatform) &&
     @config.getProperty(get_member_key(REPL_ROLE)) == REPL_ROLE_S
 end
 
 def enabled_for_config?
   super() &&
     get_applier_datasource().is_a?(PostgreSQLDatabasePlatform) &&
     @config.getProperty(get_member_key(REPL_ROLE)) == REPL_ROLE_S
 end
end

#
# Validation
#

module PostgreSQLApplierCheck
  def enabled?
    super() && (get_applier_datasource().is_a?(PostgreSQLDatabasePlatform))
  end
end

module SlonyExtractorCheck
  def enabled?
    super() && (get_extractor_datasource().is_a?(PostgreSQLDatabasePlatform))
  end
end

#
# Deployment
#

module ConfigureDeploymentStepPostgreSQL
  include DatabaseTypeDeploymentStep
  
  def bind_listeners(obj, config_obj)
    obj.listen_event(:before_deploy_replication_service, obj, "postgresql_before_deploy_replication_service")
    obj.listen_event(:after_deploy_replication_service, obj, "postgresql_after_deploy_replication_service")
  end
  module_function :bind_listeners
  
  def postgresql_before_deploy_replication_service
    if (get_applier_datasource().is_a?(PostgreSQLDatabasePlatform) &&
     @config.getProperty(get_service_key(REPL_ROLE)) == REPL_ROLE_S)
      # Do Stuff
    end
  end
  
  def postgresql_after_deploy_replication_service
    if (get_extractor_datasource().is_a?(PostgreSQLDatabasePlatform) &&
     @config.getProperty(get_service_key(REPL_ROLE)) == REPL_ROLE_M)
      # Do Stuff
    end
  end
  
  def deploy_replication_dataservice()
    info("deploy_slony")
    if (get_extractor_datasource().is_a?(PostgreSQLDatabasePlatform) &&
     @config.getProperty(get_service_key(REPL_ROLE)) == REPL_ROLE_M)
      deploy_slony()
    end
    if (get_applier_datasource().is_a?(PostgreSQLDatabasePlatform) &&
     @config.getProperty(get_service_key(REPL_ROLE)) == REPL_ROLE_S)
      deploy_postgresql()
    end
    
    super()
  end
  
  def deploy_slony()
    Configurator.instance.write_divider()

    cmd_prefix = "#{@config.getProperty(get_service_key(REPL_POSTGRESQL_SLONIK))} <<_EOF_\n" +
           "cluster name = #{@config.getProperty(get_service_key(DEPLOYMENT_SERVICE))};\n" +
           "node 1 admin conninfo = 'dbname=#{@config.getProperty(get_service_key(REPL_POSTGRESQL_DBNAME))} host=#{@config.getProperty(get_extractor_key(REPL_DBHOST))} port=#{@config.getProperty(get_extractor_key(REPL_DBPORT))} user=#{@config.getProperty(get_extractor_key(REPL_DBLOGIN))}';\n"
    
    warning("Running procedure to clean (uninstall) any available Slony triggers...")
    cmd1 = cmd_prefix +
           "uninstall node ( id = 1 );\n" +
           "_EOF_\n"    
    warning(cmd1)
    cmd_result(cmd1, true)
    
    warning("Running procedure to deploy Slony triggers...")
    cmd2 = cmd_prefix +
          "init cluster ( id=1, comment = 'Tungsten service #{@config.getProperty(get_service_key(DEPLOYMENT_SERVICE))}' );\n" +
          "create set ( id=1, origin=1, comment='' );\n"
    @config.getProperty(get_service_key(REPL_POSTGRESQL_TABLES)).split(",").each_with_index { |table, i|     
      cmd2 += "set add table ( set id=1, origin=1, id=#{i}, fully qualified name = '#{table.strip}', comment='' );\n"
    }
    cmd2 += "_EOF_\n"
    warning(cmd2)
    cmd_result(cmd2, false)
    
    Configurator.instance.write_divider()
  end
  
  def deploy_postgresql()
  end
end