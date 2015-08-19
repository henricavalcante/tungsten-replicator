require "cgi"
class TungstenBackupScript
  include TungstenScript
  
  ACTION_BACKUP = "backup"
  ACTION_RESTORE = "restore"
  ACTION_DELETE = "delete"
  
  def main
    @binlog_file = nil
    @binlog_position = nil
    
    if @options[:action] == ACTION_BACKUP
      # Determine the sequence number before and after taking the backup. If
      # this is a master, we will use that information to determine the correct 
      # sequence number that should be stored in trep_commit_seqno
      start_thl_seqno = TI.trepctl_value(@options[:service], 'maximumStoredSeqNo')
      storage_file = backup()
      end_thl_seqno = TI.trepctl_value(@options[:service], 'maximumStoredSeqNo')

      if @options[:properties].to_s() != ""
        # Store the path to the backup file in the properties file provided
        # by the replicator
        TU.cmd_result("echo \"file=#{storage_file}\" > #{@options[:properties]}")
      end
      
      if @master_backup == true
        # A master backup requires the binlog file and position in order to 
        # store the proper sequence number. If that information isn't available
        # we don't have a useable backup.
        if @binlog_file == nil
          raise "Unable to find the binlog position information for this backup. Please try again or take the backup from a slave"
        end
        
        binlog_regex = Regexp.new("#{@binlog_file}:[0]+#{@binlog_position}")
        master_position_thl_record = nil
        
        if start_thl_seqno == end_thl_seqno
          # Search the THL records for one that matches the binlog position
          # of the backup
          TU.debug("Compare seqno #{end_thl_seqno} to #{@binlog_file}:#{@binlog_position}")
          
          # Get the headers for all THL events that have passed
          thl_result = TU.cmd_result("#{TI.thl(@options[:service])} list -headers -json -seqno #{end_thl_seqno}")
          thl_records = JSON.parse(thl_result)
          unless thl_records.instance_of?(Array)
            TU.error("Unable to read the THL record for seqno #{end_thl_seqno}")
          end
          
          # Find the THL record that matches the binlog position of the backup
          thl_record = thl_records[0]
          if thl_record["eventId"] =~ binlog_regex
            TU.debug("Use #{thl_record['seqno']} and #{thl_record['eventId']}")
            master_position_thl_record = thl_record
          end
        else
          # If the starting and stopping sequence numbers are the same then
          # we compare the binlog position of the backup matches the THL
          # record
          TU.debug("Search thl from #{start_thl_seqno} to #{end_thl_seqno} for #{@binlog_file}:#{@binlog_position}")
          
          TU.log_cmd_results?(false)
          thl_result = TU.cmd_result("#{TI.thl(@options[:service])} list -headers -json -low #{start_thl_seqno} -high #{end_thl_seqno}")
          TU.log_cmd_results?(true)
          thl_records = JSON.parse(thl_result)
          unless thl_records.instance_of?(Array)
            TU.error("Unable to read the THL record for seqno #{end_thl_seqno}")
          end
          
          thl_records.each{
            |thl_record|
            if thl_record["eventId"] =~ binlog_regex
              TU.debug("Use #{thl_record['seqno']} and #{thl_record['eventId']}")
              master_position_thl_record = thl_record
            end
          }
        end
        
        # If we can't find a valid THL record then there is something 
        # wrong with the backup, the reported binlog position or THL
        if master_position_thl_record == nil
          raise "Unable to find a THL record to reflect the backup position"
        end
        
        # Let the backup method chose how to store the information
        store_master_position(master_position_thl_record, storage_file)
      end
    elsif @options[:action] == ACTION_RESTORE
      restore()
    else
      raise "Unable to determine the appropriate action for #{self.class.name}"
    end
  end
  
  def backup
    raise "You must define the #{self.class.name}.backup method"
  end
  
  def restore
    raise "You must define the #{self.class.name}.backup method"
  end
  
  def store_master_position(thl_record, storage_file)
    service_schema = TI.trepctl_property(@options[:service], "replicator.schema")
    sql = ["-- Reset the Tungsten service schema with the proper position",
      "SET SESSION SQL_LOG_BIN=0;DELETE FROM #{service_schema}.trep_commit_seqno; INSERT INTO #{service_schema}.trep_commit_seqno (task_id, seqno, fragno, last_frag, epoch_number, eventid, source_id, update_timestamp, extract_timestamp) VALUES (0, #{thl_record['seqno']}, #{thl_record['frag']}, #{thl_record['lastFrag'] == true ? 1 : 0}, #{thl_record['epoch']}, '#{thl_record['eventId']}', '#{thl_record['sourceId']}', NOW(), NOW());"]
    store_master_position_sql(sql, storage_file)
  end
  
  def store_master_position_sql(sql, storage_file)
    raise "You must define the #{self.class.name}.store_master_position_sql method"
  end
  
  def validate
    super()
    
    if TI.trepctl_value(@options[:service], 'role') == "master"
      @master_backup = true
    else
      @master_backup = false
      
      TI.replication_services().each{
        |repl_service|
        if TI.trepctl_value(repl_service, 'role') == "master"
          if repl_service != opt(:service)
            TU.error("Unable to backup #{TI.hostname()} using the #{opt(:service)} service because there is a master service available. Use the #{repl_service} service instead.")
          end
        end
      }
    end
    
    if @options[:action] == ACTION_BACKUP
      if @master_backup == true && TI.trepctl_value(@options[:service], 'state') != "ONLINE"
        TU.error("Unable to backup a master host unless it is ONLINE. Try running `trepctl -service #{@options[:service]} online`.")
      end
    end
  end

  def configure
    super()
    
    TU.remaining_arguments.map!{ |arg|
      # The backup agent sends single dashes instead of double dashes
      if arg[0,1] == "-" && arg[0,2] != "--"
        "-" + arg
      else
        arg
      end
    }
    
    add_option(:backup, {
      :on => "--backup"
    }) {
      @options[:action] = ACTION_BACKUP
      
      nil
    }
    
    add_option(:restore, {
      :on => "--restore"
    }) {
      @options[:action] = ACTION_RESTORE
      
      nil
    }
    
    add_option(:properties, {
      :on => "--properties String"
    })
    
    add_option(:options, {
      :on => "--options String"
    }) {|val|
      CGI::parse(val).each{
        |key,value|
        sym = key.to_sym
        if value.is_a?(Array)
          @options[sym] = value.at(0).to_s
        else
          @options[sym] = value.to_s
        end
      }
      
      nil
    }
  end
  
  def master_backup?(val = nil)
    if val != nil
      @master_backup = val
    end
    
    (@master_backup == true)
  end
end