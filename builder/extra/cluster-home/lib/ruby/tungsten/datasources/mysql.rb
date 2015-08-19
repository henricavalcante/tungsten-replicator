class TungstenScriptMySQLDatasource < TungstenScriptDatasource
  def is_running?
    begin
      sql_result("SELECT 1")
      return true
    rescue
      return false
    end
  end
  
  def _stop_server
    begin
      pid_file = get_variable("pid_file")
      pid = TU.cmd_result("#{@ti.sudo_prefix()}cat #{pid_file}")
    rescue CommandError
      pid = ""
    end
    
    begin
      stop_command = @ti.setting(TI.setting_key(REPL_SERVICES, @service, "repl_datasource_service_stop"))
      TU.cmd_result("#{@ti.sudo_prefix()}#{stop_command}")
    rescue CommandError
    end
    
    # Raise an error if we got a response to the previous command
    if is_running?() == true
      raise "Unable to properly shutdown the MySQL service"
    end
    
    # We saw issues where MySQL would not close completely. This will
    # watch the PID and make sure it does not appear
    unless pid.to_s() == ""
      begin
        TU.debug("Verify that the MySQL pid has gone away")
        Timeout.timeout(30) {
          pid_missing = false
          
          while pid_missing == false do
            begin
              TU.cmd_result("#{@ti.sudo_prefix()}ps -p #{pid}")
              sleep 5
            rescue CommandError
              pid_missing = true
            end
          end
        }
      rescue Timeout::Error
        raise "Unable to verify that MySQL has fully shutdown"
      end
    end
  end
  
  def _start_server
    begin
      start_command = @ti.setting(TI.setting_key(REPL_SERVICES, @service, "repl_datasource_service_start"))
      TU.cmd_result("#{@ti.sudo_prefix()}#{start_command}")
    rescue CommandError
    end
    
    # Wait 30 seconds for the MySQL service to be responsive
    begin
      Timeout.timeout(30) {
        while true
          if is_running?()
            break
          else
            # Pause for a second before running again
            sleep 1
          end
        end
      }
    rescue Timeout::Error
      raise "The MySQL server has taken too long to start"
    end
  end
  
  # Read the configured value for a mysql variable
  def get_option(opt)
    begin
      cnf = @ti.setting(@ti.setting_key(REPL_SERVICES, @service, "repl_datasource_mysql_service_conf"))
      val = TU.cmd_result("my_print_defaults --config-file=#{cnf} mysqld | grep -e'^--#{opt.gsub(/[\-\_]/, "[-_]")}='")
    rescue CommandError => ce
      return nil
    end

    return val.split("\n")[0].split("=")[1]
  end
  
  # Read the current value for a mysql variable
  def get_variable(var)
    begin
      sql_result("SHOW VARIABLES LIKE '#{var}'")[0]["Value"]
    rescue => e
      TU.debug(e)
      return nil
    end
  end
  
  def get_system_user
    if @mysql_user == nil
      @mysql_user = get_option("user")
      if @mysql_user.to_s() == ""
        @mysql_user = "mysql"
      end
    end
    
    @mysql_user
  end
  
  def can_lock_tables?
    true
  end
  
  def lock_tables
    if @lock_thread != nil
      TU.debug("Unable to lock tables because they are already locked")
      return
    end
    
    TU.debug("Run FLUSH TABLES WITH READ LOCK")
    @lock_thread = Thread.new(get_mysql_command()) {
      |mysql_command|
      status = Open4::popen4("export LANG=en_US; #{mysql_command}") do |pid, stdin, stdout, stderr|
        stdin.puts("SET wait_timeout=30;\n")
        stdin.puts("SET lock_wait_timeout=30;\n")
        stdin.puts("FLUSH TABLES WITH READ LOCK;\n")
        
        # Infinite loop to keep this thread alive until it is killed by the 
        # unlock_tables method
        while true
          sleep 60
        end
      end
    }
  end
  
  def unlock_tables
    if @lock_thread != nil
      begin
        Thread.kill(@lock_thread)
      rescue TypeError
      end

      @lock_thread = nil
    end
  end
  
  def snapshot_paths
    paths = []
    
    # The datadir may not exist in my.cnf but we need the value
    # If we don't see it in my.cnf get the value from the dbms
    val = get_option("datadir")
    if val == nil
      val = get_variable("datadir")
    end
    paths << val
    
    # These values must appear in my.cnf if they are to be used
    val = get_option("innodb_data_home_dir")
    if val != nil
      paths << val
    end
    val = get_option("innodb_log_group_home_dir")
    if val != nil
      paths << val
    end
    
    # Only return a unique set of paths
    paths.uniq()
  end
  
  def can_manage_service?
    true
  end
  
  def get_mysql_command
    host = @ti.setting(@ti.setting_key(REPL_SERVICES, @service, "repl_datasource_host"))
    port = @ti.setting(@ti.setting_key(REPL_SERVICES, @service, "repl_datasource_port"))
    my_cnf = @ti.setting(@ti.setting_key(REPL_SERVICES, @service, "repl_datasource_mysql_service_conf"))
    
    "mysql --defaults-file=#{my_cnf} -h#{host} --port=#{port}"
  end
end