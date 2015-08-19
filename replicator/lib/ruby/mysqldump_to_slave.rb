require "#{File.dirname(__FILE__)}/backup"

class TungstenMysqldumpToSlaveScript < TungstenBackupScript
  include MySQLServiceScript
  
  def backup
    begin
      TU.notice("Create mysqldump in #{@options[:storage_file]}")
      TU.cmd_result("echo \"-- Tungsten database dump - should not be logged on restore\n\" #{get_ssh_pipe_command(false)}");
      TU.cmd_result("echo \"SET SESSION SQL_LOG_BIN=0;\n\" #{get_ssh_pipe_command()}");
      TU.cmd_result("echo \"/*!50112 SET @OLD_SLOW_QUERY_LOG=@@SLOW_QUERY_LOG */;\n\" #{get_ssh_pipe_command()}");
      TU.cmd_result("echo \"/*!50112 SET GLOBAL SLOW_QUERY_LOG=0 */;\n\" #{get_ssh_pipe_command()}");
      TU.cmd_result("#{get_mysqldump_command()} #{get_ssh_pipe_command()}")
      TU.cmd_result("echo \"/*!50112 SET GLOBAL SLOW_QUERY_LOG=@OLD_SLOW_QUERY_LOG */;\n\" #{get_ssh_pipe_command()}");
      
      # Find the CHANGE MASTER line so we can read the current binlog position
      # unzip the file before sending it through the head command
      change_master_line = TU.ssh_result("gzip -cd #{@options[:storage_file]} | head -n50 | grep \"CHANGE MASTER\"",
        @options[:target], TI.user())
      
      # Parse the CHANGE MASTER information for the file number and position
      if change_master_line != nil
        m = change_master_line.match(/MASTER_LOG_FILE='([a-zA-Z0-9\.\-\_]*)', MASTER_LOG_POS=([0-9]*)/)
        if m
          @binlog_file = m[1]
          @binlog_position = m[2]
        end
      end
    rescue => e
      raise e
    end
  end
  
  def validate
    super()
    
    unless TU.is_valid?()
      return
    end
    
    if TI.is_replicator?()
      if TI.is_running?("replicator")
        state = TI.trepctl_value(opt(:service), "state")
        unless ["ONLINE", "OFFLINE:NORMAL"].include?(state)
          TU.error("The #{opt(:service)} replication service is not ONLINE or OFFLINE:NORMAL")
        end
      else
        TU.error("The replicator process is not running")
      end
    else
      TU.error("This server is not configured for replication")
    end
    
    unless TU.is_valid?()
      return
    end
    
    if @options[:target].to_s() == ""
      TU.error("The --target argument is required")
    else
      TU.test_ssh(@options[:target], TI.user())
    end
    
    path = TU.cmd_result("which mysqldump 2>/dev/null", true)
    if path == ""
      TU.error("Unable to find the mysqldump script")
    end
  end
  
  def configure
    super()
    
    add_option(:target, {
      :on => "--target String",
      :help => "Server to send the backup to"
    })
    
    add_option(:storage_file, {
      :on => "--storage-file String",
      :help => "File to place the mysqldump output in"
    })
  end
  
  def store_master_position_sql(sql, storage_file)
    TU.notice("Write master backup position information to #{@options[:target]}:#{@options[:storage_file]}")
    sql.each{
      |line|
      TU.ssh_result("echo \"#{line}\" | gzip -c >> #{@options[:storage_file]}", @options[:target], TI.user())
    }
  end
  
  def get_ssh_pipe_command(append = true)
    if append == true
      chev = ">>"
    else
      chev = ">"
    end
    
    "| gzip -c | ssh #{TU.get_ssh_command_options()} #{@options[:target]} \"tee #{chev} #{@options[:storage_file]}\""
  end
  
  def script_name
    "mysqldump_to_slave.sh"
  end
end