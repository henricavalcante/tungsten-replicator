require "#{File.dirname(__FILE__)}/backup"

class TungstenSnapshotBackup < TungstenBackupScript
  def backup
    timestamp = build_timestamp_id()
    path = "#{TI.setting(TI.setting_key(REPL_SERVICES, opt(:service), "repl_backup_dump_directory"))}/#{timestamp}"
    
    begin
      ds = TI.datasource(opt(:service))
      
      # Store the original state of the dataserver for later
      ds_is_running = ds.is_running?()
    
      if require_stopped_dataserver_for_backup?
        if ds_is_running == true
          TU.debug("Stop the datasource before taking the snapshot")
          ds.stop()
        end
      elsif opt(:lock_tables) == true
        TU.debug("Lock tables for the datasource")
        ds.lock_tables()
      end
    
      # Call the script specific method to create snapshots
      snapshot_ids = create_snapshot(timestamp)
    
      # Store the snapshot ids as a JSON object to make it easy to parse later
      File.open(path, "w") {
        |f|
        f.puts(JSON.pretty_generate(snapshot_ids))
      }
    rescue => e
      TU.debug(e)
      raise e
    ensure
      # Be sure to restart the dataserver even if there was an error
      # Do not start the dataserver if it wasn't running at the beginning
      if require_stopped_dataserver_for_backup?
        if ds_is_running == true
          TU.debug("Make sure the datasource is running after taking the snapshot")
          ds.start()
        end
      elsif opt(:lock_tables) == true
        TU.debug("Unlock tables for the datasource")
        ds.unlock_tables()
      end
    end
    
    # Look at all existing snapshots and remove the ones that have not
    # been retained by the replicator or were just created
    begin
      delete_unretained_snapshots(find_retained_backups() + [snapshot_ids])
    rescue => e
      TU.exception(e)
    end
  
    return path
  end
  
  def create_snapshot(timestamp)
    raise "Undefined function: #{self.class.name()}.create_snapshot"
  end
  
  def restore
    begin
      ds = TI.datasource(opt(:service))
      
      if ds.is_running?()
        TU.debug("Stop the datasource before restoring the snapshot")
        ds.stop()
      end
    
      snapshot_ids = nil
      snapshot_file = TU.cmd_result(". #{@options[:properties]}; echo $file")
      begin
        File.open(snapshot_file, "r") {
          |f|
          snapshot_ids = JSON.parse(f.readlines().join())
        }
      rescue
        raise "Unable to read snapshot ids from #{snapshot_file}"
      end
      
      # Check for differences between the paths in the snapshot
      # and the paths that the datasource expects
      snapshot_paths = snapshot_ids.keys()
      ds_paths = ds.snapshot_paths()
      (snapshot_paths - ds_paths).each{
        |extra_path|
        TU.error("The snapshot includes data for #{extra_path} that is not expected for the #{opt(:service)} replication service")
      }
      (ds_paths - snapshot_paths).each{
        |missing_path|
        TU.error("The snapshot is missing data for #{missing_path} that is expected for the #{opt(:service)} replication service")
      }
      unless TU.is_valid?()
        return false
      end
      
      restore_snapshot(snapshot_ids)
      
      TU.debug("Start the datasource after restoring the snapshot")
      ds.start()
    end
  end
  
  def restore_snapshot(snapshot_ids)
    raise "Undefined function: #{self.class.name()}.restore_snapshot"
  end
  
  def delete_unretained_snapshots(retained_backups)
  end
  
  def find_retained_backups
    backups = []
    
    Dir.glob("#{TI.setting(TI.setting_key(REPL_SERVICES, opt(:service), "repl_backup_directory"))}/*#{self.class.name()}*").each {
      |path|
      begin
        if File.directory?(path)
          next
        end
        
        File.open(path, "r") {
          |f|
          backups << JSON.parse(f.readlines().join())
        }
      rescue => e
        TU.debug(e)
        TU.error("Unable to load backup descriptions from #{path}")
      end
    }
    
    backups
  end
  
  def validate
    super()
    
    unless TU.is_valid?()
      return
    end
    
    ds = TI.datasource(opt(:service))
    
    if opt(:lock_tables) == true
      if ds.can_lock_tables?() != true
        TU.error("Unable to use --lock-tables because the #{opt(:service)} datasource is unable to lock tables")
      end
    end
    
    begin
      ds.snapshot_paths()
    rescue
      TU.error("Unable to use snapshots with the datasource for #{opt(:service)} because it does not define snapshot paths.")
    end
    
    case opt(:action)
    when ACTION_BACKUP
      if require_stopped_dataserver_for_backup?() 
        if master_backup?()
          TU.error("Unable to backup using #{script_name()} because this service is a master and requires the dataserver to be stopped")
        elsif ds.can_manage_service?() == false
          TU.error("Unable to backup using #{script_name()} because this datasource service cannot be managed")
        end
      end
    when ACTION_RESTORE
      if require_stopped_dataserver_for_backup?() 
        if ds.can_manage_service?() == false
          TU.error("Unable to restore using #{script_name()} because this datasource service cannot be managed")
        end
      end
    end
  end
  
  def configure
    super()
    
    add_option(:lock_tables, {
      :on => "--lock-tables [String]",
      :default => false,
      :parse => method(:parse_boolean_option_blank_is_true),
      :help => "Lock database tables instead of stop/starting the DBMS"
    })
  end
  
  def require_stopped_dataserver_for_backup?
    false
  end
  
  def build_timestamp_id()
    return "#{self.class.name()}_" + Time.now.strftime("%Y-%m-%d_%H-%M") + "_" + rand(100).to_s
  end
end