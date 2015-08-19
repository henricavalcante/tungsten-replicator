require "#{File.dirname(__FILE__)}/snapshot_backup"

# TODO: Optimize to do file copy is parallel threads

class FileCopySnapshotBackup < TungstenSnapshotBackup
  def create_snapshot(timestamp)
    snapshot_ids = {}
    ds = TI.datasource(opt(:service))
    basedir = "#{TI.setting(TI.setting_key(REPL_SERVICES, opt(:service), "repl_backup_directory"))}/#{self.class.name}"
    
    ds.snapshot_paths.each{
      |path|
      target_directory = "#{basedir}/#{timestamp}/#{path}"
      TU.debug("Copy files from #{path} to #{target_directory}")
      TU.mkdir_if_absent(target_directory)
      TU.cmd_result("#{TI.sudo_prefix()}cp -rf #{path}/* #{target_directory}/")
      
      snapshot_ids[path] = timestamp
    }  
    
    # Make sure the Tungsten system user owns these files
    TU.cmd_result("#{TI.sudo_prefix()}chown -RL #{TI.user()}: #{basedir}")
    
    snapshot_ids
  end
  
  def restore_snapshot(snapshot_ids)
    ds = TI.datasource(opt(:service))
    basedir = "#{TI.setting(TI.setting_key(REPL_SERVICES, opt(:service), "repl_backup_directory"))}/#{self.class.name}"
    
    unless File.exists?(basedir)
      raise "Unable to restore from #{snapshot_id} because #{basedir} does not exist"
    end
    
    snapshot_ids.each{
      |path, id|
      target_directory = "#{basedir}/#{id}/#{path}"
      TU.debug("Copy files from #{target_directory} to #{path}")
      TU.cmd_result("#{TI.sudo_prefix()}find #{path}/ -mindepth 1 -delete")
      TU.cmd_result("#{TI.sudo_prefix()}cp -rf #{target_directory}/* #{path}/")
      TU.cmd_result("#{TI.sudo_prefix()}chown -RL #{ds.get_system_user()}: #{path}/*")
    }
  end
  
  def validate
    super()
  end
  
  def require_stopped_dataserver_for_backup?
    if opt(:lock_tables) == true
      false
    else
      true
    end
  end
  
  def script_name
    "file_copy_snapshot.sh"
  end
end