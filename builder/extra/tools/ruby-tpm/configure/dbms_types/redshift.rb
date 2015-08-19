DBMS_REDSHIFT = "redshift"

class RedshiftDatabasePlatform < ConfigureDatabasePlatform
  def get_uri_scheme
    DBMS_REDSHIFT
  end
 
  def get_thl_uri
	  "jdbc:postgresql://${replicator.global.db.host}:${replicator.global.db.port}/#{@config.getProperty(REPL_REDSHIFT_DBNAME)}?tcpKeepAlive=true"
  end
  
  def get_default_port
    "5439"
  end
  
  def get_default_start_script
    nil
  end
  
  def get_default_master_log_directory
    nil
  end
  
  def get_default_master_log_pattern
    nil
  end
  
  def enable_applier_filter_pkey?
    false
  end
  
  def enable_applier_filter_bidiSlave?
    false
  end
  
  def enable_applier_filter_colnames?
    false
  end
  
  def getBasicJdbcUrl()
    "jdbc:postgresql://${replicator.global.db.host}:${replicator.global.db.port}/"
  end
  
  def getJdbcUrl()
    "jdbc:postgresql://${replicator.global.db.host}:${replicator.global.db.port}/#{@config.getProperty(REPL_REDSHIFT_DBNAME)}?tcpKeepAlive=true"
  end
  
  def getJdbcQueryUrl
    "jdbc:postgresql://#{@host}:#{@port}/#{@config.getProperty(REPL_REDSHIFT_DBNAME)}?tcpKeepAlive=true"
  end
  
  def getJdbcDriver()
    "org.postgresql.Driver"
  end
  
  def getVendor()
    "redshift"
  end

  # Redshift does not have an extractor. 
  def get_extractor_template
    raise "Unable to use RedshiftDatabasePlatform as an extractor"
  end
end

#
# Prompts
#

REPL_REDSHIFT_DBNAME = "repl_redshift_dbname"
class RedshiftDatabaseName < ConfigurePrompt
 include ReplicationServicePrompt
 
 def initialize
   super(REPL_REDSHIFT_DBNAME, "Name of the database to replicate into",
     PV_ANY)
 end
   
 def enabled?
   super() && get_applier_datasource().is_a?(RedshiftDatabasePlatform)
 end
 
 def enabled_for_config?
   super() && get_applier_datasource().is_a?(RedshiftDatabasePlatform)
 end
end