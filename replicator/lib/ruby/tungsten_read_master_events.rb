class TungstenReplicatorReadMasterEvents
  include TungstenScript
  include MySQLServiceScript
  
  def main
    begin
      topology = TI.topology(opt(:service))
      datasource_type = TI.datasource_type(opt(:service), (topology.type == "direct"))
      case datasource_type
      when "mysql"
        if topology.type == "direct"
          opt(:my_cnf, TI.setting(TI.setting_key(REPL_SERVICES, @options[:service], "repl_direct_datasource_mysql_service_conf")))
        end
        
        if @options[:after] != nil
          mysqlbinlog_after(@options[:after])
        elsif @options[:high] == nil
          mysqlbinlog_after(@options[:low]-1)
        else
          mysqlbinlog_between(@options[:low]-1, @options[:high])
        end
      else
        TU.error("Unable to support tungsten_read_master_events for #{datasource_type} datasources")
      end
    rescue => e
      raise e
    end
  end
  
  def mysqlbinlog_after(low)
    begin
      thl_record = get_thl_record(low)
      event_info = thl_record["eventId"].split(":")
      if event_info.size() != 2
        raise "Unable to parse the THL eventId for the starting file name"
      end
      start_file = event_info[0]
      event_position_info = event_info[1].split(";")
      if event_position_info.size() > 0
        start_position = event_position_info[0]
      else
        raise "Unable to parse the THL eventId for the starting position"
      end
      
      begin
        TU.cmd_result("mysql --defaults-file=#{@options[:my_cnf]} --port=#{@options[:mysqlport]} -h#{thl_record['sourceId']} -e'SELECT 1'")
      rescue CommandError => ce
        TU.debug(ce)
        raise "Unable to connect to #{thl_record['sourceId']}:#{@options[:mysqlport]} try running the command from #{thl_record['sourceId']}"
      end
      
      TU.log_cmd_results?(false)
      TU.cmd_stdout("mysqlbinlog --defaults-file=#{@options[:my_cnf]} --port=#{@options[:mysqlport]} --base64-output=DECODE-ROWS --verbose -R -t -h#{thl_record['sourceId']} --start-position=#{start_position} #{start_file}") {
        |line|
        TU.output(line)
      }
      TU.log_cmd_results?(true)
    rescue CommandError => ce
      TU.exception(ce)
    rescue => e
      raise e
    end
  end
  
  def mysqlbinlog_between(low, high)
    begin
      low_thl_record = get_thl_record(low)
      high_thl_record = get_thl_record(high)
      
      if low_thl_record["sourceId"] != high_thl_record["sourceId"]
        raise "Unable to display mysqlbinlog events between seqno #{low} and #{high} because the sourceId does not stay the same"
      end
      
      event_info = low_thl_record["eventId"].split(":")
      if event_info.size() != 2
        raise "Unable to parse the THL eventId for the starting file name"
      end
      start_file = event_info[0]
      event_position_info = event_info[1].split(";")
      if event_position_info.size() > 0
        start_position = event_position_info[0]
      else
        raise "Unable to parse the THL eventId for the starting position"
      end
      
      event_info = high_thl_record["eventId"].split(":")
      if event_info.size() != 2
        raise "Unable to parse the THL eventId for the ending file name"
      end
      stop_file = event_info[0]
      event_position_info = event_info[1].split(";")
      if event_position_info.size() > 0
        stop_position = event_position_info[0]
      else
        raise "Unable to parse the THL eventId for the ending position"
      end
      
      # Build the list of mysqlbinlog files to read from
      files = []
      start_file_info = start_file.split(".")
      stop_file_info = stop_file.split(".")
      file_number = start_file_info[1].to_i()
      while file_number <= stop_file_info[1].to_i()
        files << sprintf("%s.%06d", start_file_info[0], file_number)
        file_number = file_number+1
      end
      
      begin
        TU.cmd_result("mysql --defaults-file=#{@options[:my_cnf]} --port=#{@options[:mysqlport]} -h#{low_thl_record['sourceId']} -e'SELECT 1'")
      rescue CommandError => ce
        TU.debug(ce)
        raise "Unable to connect to #{low_thl_record['sourceId']}:#{@options[:mysqlport]} try running the command from #{low_thl_record['sourceId']}"
      end
      
      TU.log_cmd_results?(false)
      TU.cmd_stdout("mysqlbinlog --defaults-file=#{@options[:my_cnf]} --port=#{@options[:mysqlport]} --base64-output=DECODE-ROWS --verbose -R -h#{low_thl_record['sourceId']} --start-position=#{start_position} --stop-position=#{stop_position} #{files.join(' ')}") {
        |line|
        TU.output(line)
      }
      TU.log_cmd_results?(true)
    rescue CommandError => ce
      TU.exception(ce)
    rescue => e
      raise e
    end
  end
  
  def get_thl_record(seqno)
    begin
      TU.info("Load seqno #{seqno} from #{@options[:source]}")
      cmd = "#{TI.thl(@options[:service])} list -seqno #{seqno} -headers -json"
      thl_record_content = TU.ssh_result(cmd, @options[:source], TI.user())
    rescue CommandError => ce
      TU.debug(ce)
      raise "There was an error running the thl command on #{@options[:source]}"
    end
  
    thl_records = JSON.parse(thl_record_content)
    unless thl_records.instance_of?(Array) && thl_records.size() == 1
      raise "Unable to read the THL record for seqno #{seqno} on #{@options[:source]}"
    end
  
    return thl_records[0]
  end

  def configure
    super()
    description("Read mysqlbinlog events from the given THL event forward<br>
Examples:<br>
$> tungsten_read_master_events.sh --after=35
$> tungsten_read_master_events.sh --low=10 --high=20")
    
    add_option(:after, {
      :on => "--after String",
      :help => "Display all events starting after this sequence number",
      :parse => method(:parse_integer_option),
    })
    
    add_option(:low, {
      :on => "--low String",
      :help => "Display events starting with this sequence number",
      :parse => method(:parse_integer_option),
    })
    
    add_option(:high, {
      :on => "--high String",
      :help => "Display events ending with this sequence number",
      :parse => method(:parse_integer_option),
    })
    
    add_option(:source, {
      :on => "--source String",
      :help => "Determine metadata for the --after, --low, --high statements from this host",
      :default => (TI == nil ? nil : TI.hostname()),
      :hidden => true
    })
  end
  
  def validate
    super()
    
    if @options[:low] != nil && @options[:high] != nil
      lowhigh = true
    else
      lowhigh = false
    end
    
    if @options[:after] == nil && lowhigh == false
      TU.error("You must specify the --after argument or the --low and --high arguments")
    end
    
    if @options[:source] != TI.hostname()
      TU.error("The --source argument is not supported at this time. Remove it or run this command on #{@options[:source]}")
      
      # This section is commented out until we add support back in for --source
      #if opt(:service).to_s() != "" && @options[:source] != TI.hostname()
      #  cmd = "egrep \"^service.name\" #{TI.root()}/#{CURRENT_RELEASE_DIRECTORY}/tungsten-replicator/conf/static-* | awk -F \"=\" '{print $2}'"
      #  services = TU.ssh_result(cmd, @options[:source], TI.user()).split("\n")
      #  unless services.include?(@options[:service])
      #    TU.error("The #{@options[:service]} service was not found in the replicator at #{@options[:source]}:#{TI.root()}")
      #  end
      #end
    end
  end
  
  def script_name
    "tungsten_read_master_events"
  end
end