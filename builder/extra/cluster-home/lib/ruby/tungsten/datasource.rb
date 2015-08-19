class TungstenScriptDatasource
  def initialize(ti, service, is_direct = false)
    @ti = ti
    @service = service
    @is_direct = is_direct
  end
  
  def can_manage_service?
    false
  end
  
  def is_running?
    raise "Undefined function: #{self.class.name()}.is_running?"
  end
  
  def stop
    if @is_direct == true
      raise "Unable to stop the direct datasource for #{@service} replication service"
    end
    
    unless can_manage_service?()
      raise "Unable to stop the datasource for #{@service} replication service"
    end
    
    if is_running?() == false
      return
    end
    
    TU.notice("Stop the #{self.class.name()} service")
    _stop_server()
  end
  
  def _stop_server
    raise "Undefined function: #{self.class.name()}._stop_server"
  end
  
  def start
    if @is_direct == true
      raise "Unable to start the direct datasource for #{@service} replication service"
    end
    
    unless can_manage_service?()
      raise "Unable to start the datasource for #{@service} replication service"
    end
    
    if is_running?() == true
      return
    end
    
    TU.notice("Start the #{self.class.name()} service")
    _start_server()
  end
  
  def _stop_server
    raise "Undefined function: #{self.class.name()}._start_server"
  end
  
  def can_lock_tables?
    false
  end
  
  def lock_tables
    raise "Undefined function: #{self.class.name()}.lock_tables"
  end
  
  def unlock_tables
    raise "Undefined function: #{self.class.name()}.unlock_tables"
  end
  
  def snapshot_paths
    raise "Undefined function: #{self.class.name()}.snapshot_paths"
  end
  
  def can_sql?
    @ti.can_sql?(@service, @is_direct)
  end
  
  def sql_results(sql)
    @ti.sql_results(@service, sql, @is_direct)
  end
  
  def sql_result(sql)
    @ti.sql_result(@service, sql, @is_direct)
  end
end