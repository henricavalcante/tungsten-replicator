require "#{File.dirname(__FILE__)}/backup"

class TungstenMySQLdumpScript < TungstenBackupScript
  include MySQLServiceScript
  
  def backup
    begin
      id = build_timestamp_id()

      if @options[:gz] == "true"
        mysqldump_file = @options[:tungsten_backups] + "/" + id + ".sql.gz"

        TU.output("Create mysqldump in #{mysqldump_file}")
        TU.cmd_result("echo \"-- Tungsten database dump - should not be logged on restore\n\" | gzip -c > #{mysqldump_file}");
        TU.cmd_result("echo \"SET SESSION SQL_LOG_BIN=0;\n\" | gzip -c >> #{mysqldump_file}");
        TU.cmd_result("echo \"/*!50112 SET @OLD_SLOW_QUERY_LOG=@@SLOW_QUERY_LOG */;\n\" | gzip -c >> #{mysqldump_file}");
        TU.cmd_result("echo \"/*!50112 SET GLOBAL SLOW_QUERY_LOG=0 */;\n\" | gzip -c >> #{mysqldump_file}");
        TU.cmd_result("#{get_mysqldump_command()} | gzip -c >> #{mysqldump_file}")
        TU.cmd_result("echo \"/*!50112 SET GLOBAL SLOW_QUERY_LOG=@OLD_SLOW_QUERY_LOG */;\n\" | gzip -c >> #{mysqldump_file}");
        
        # Find the CHANGE MASTER line so we can read the current binlog position
        # unzip the file before sending it through the head command
        change_master_line = TU.cmd_result("gzip -cd #{mysqldump_file} | head -n50 | grep \"CHANGE MASTER\"")
      else
        mysqldump_file = @options[:tungsten_backups] + "/" + id + ".sql"

        TU.output("Create mysqldump in #{mysqldump_file}")
        TU.cmd_result("echo \"-- Tungsten database dump - should not be logged on restore\n\" > #{mysqldump_file}");
        TU.cmd_result("echo \"SET SESSION SQL_LOG_BIN=0;\n\" >> #{mysqldump_file}");
        TU.cmd_result("echo \"/*!50112 SET @OLD_SLOW_QUERY_LOG=@@SLOW_QUERY_LOG */;\n\" >> #{mysqldump_file}");
        TU.cmd_result("echo \"/*!50112 SET GLOBAL SLOW_QUERY_LOG=0 */;\n\" >> #{mysqldump_file}");
        TU.cmd_result("#{get_mysqldump_command()} >> #{mysqldump_file}")
        TU.cmd_result("echo \"/*!50112 SET GLOBAL SLOW_QUERY_LOG=@OLD_SLOW_QUERY_LOG */;\n\" >> #{mysqldump_file}");
        
        # Find the CHANGE MASTER line so we can read the current binlog position
        change_master_line = TU.cmd_result("head -n50 #{mysqldump_file} | grep \"CHANGE MASTER\"")
      end
      
      # Parse the CHANGE MASTER information for the file number and position
      if change_master_line != nil
        m = change_master_line.match(/MASTER_LOG_FILE='([a-zA-Z0-9\.\-\_]*)', MASTER_LOG_POS=([0-9]*)/)
        if m
          @binlog_file = m[1]
          @binlog_position = m[2]
        end
      end

      # Change the directory ownership if run with sudo
      if ENV.has_key?('SUDO_USER') && TU.whoami() == "root"
        TU.cmd_result("chown -R #{ENV['SUDO_USER']}: #{mysqldump_file}")
      end

      return mysqldump_file
    rescue => e
      TU.error(e.message)

      if mysqldump_file && File.exists?(mysqldump_file)
        TU.output("Remove #{mysqldump_file} due to the error")
        TU.cmd_result("rm #{mysqldump_file}")
      end

      raise e
    end
  end
  
  def store_master_position_sql(sql, storage_file)
    # Append the UPDATE trep_commit_seqno statement to the end of the file
    if @options[:gz] == "true"
      # The contents must be zipped while added
      sql.each{
        |line|
        TU.cmd_result("echo \"#{line}\" | gzip -c >> #{storage_file}");
      }
    else
      sql.each{
        |line|
        TU.cmd_result("echo \"#{line}\" >> #{storage_file}");
      }
    end
  end

  def restore
    begin
      storage_file = TU.cmd_result(". #{@options[:properties]}; echo $file")
      TU.output("Restore from #{storage_file}")

      begin
        TU.cmd_result("echo \"SET @OLD_SLOW_QUERY_LOG=@@SLOW_QUERY_LOG;SET GLOBAL SLOW_QUERY_LOG=0;SET GLOBAL SLOW_QUERY_LOG=@OLD_SLOW_QUERY_LOG;\" | #{get_mysql_command()}")
        rescue => ignored
        TU.debug("Ignoring error caused by workaround: #{ignored}")
      end
      if File.extname(storage_file) == ".gz"
        TU.cmd_result("gunzip -c #{storage_file} | #{get_mysql_command()}")
      else
        TU.cmd_result("cat #{storage_file} | #{get_mysql_command()}")
      end
    rescue => e
      TU.error(e.message)
      raise e
    end
  end
  
  def validate
    super()
    
    if @options[:tungsten_backups] == nil
      TU.error "You must specify the Tungsten backups storage directory"
    end

    unless File.exist?(@options[:tungsten_backups])
      TU.mkdir_if_absent(@options[:tungsten_backups])
    end
    
    unless File.writable?(@options[:tungsten_backups])
      TU.error "The directory '#{@options[:tungsten_backups]}' is not writeable"
    end
  end
  
  def build_timestamp_id()
    return "mysqldump_" + Time.now.strftime("%Y-%m-%d_%H-%M") + "_" + rand(100).to_s
  end
  
  def script_name
    "mysqldump.sh"
  end
end