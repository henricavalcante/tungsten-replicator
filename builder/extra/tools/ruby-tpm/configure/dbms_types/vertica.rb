DBMS_VERTICA = "vertica"

class VerticaDatabasePlatform < ConfigureDatabasePlatform
  def get_uri_scheme
    DBMS_VERTICA
  end
 
  def get_thl_uri
	  "jdbc:vertica://${replicator.global.db.host}:${replicator.global.db.port}/#{@config.getProperty(REPL_VERTICA_DBNAME)}"
  end
  
  def get_default_port
    "5433"
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
    "jdbc:vertica://${replicator.global.db.host}:${replicator.global.db.port}/"
  end
  
  def getJdbcUrl()
    "jdbc:vertica://${replicator.global.db.host}:${replicator.global.db.port}/#{@config.getProperty(REPL_VERTICA_DBNAME)}"
  end
  
  def getJdbcDriver()
    "com.vertica.Driver"
  end
  
  def getVendor()
    "postgresql"
  end

  # Vertica does not have an extractor. 
  def get_extractor_template
    raise "Unable to use VerticaDatabasePlatform as an extractor"
  end
end

#
# Prompts
#

REPL_VERTICA_DBNAME = "repl_vertica_dbname"
class VerticaDatabaseName < ConfigurePrompt
 include ReplicationServicePrompt
 
 def initialize
   super(REPL_VERTICA_DBNAME, "Name of the database to replicate into",
     PV_ANY)
 end
   
 def enabled?
   super() && get_applier_datasource().is_a?(VerticaDatabasePlatform)
 end
 
 def enabled_for_config?
   super() && get_applier_datasource().is_a?(VerticaDatabasePlatform)
 end
end