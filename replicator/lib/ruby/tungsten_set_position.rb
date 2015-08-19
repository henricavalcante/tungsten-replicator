
class TungstenReplicatorSetPosition
  include TungstenScript
  include OfflineSingleServiceScript
  
  def main
    begin
      if @options[:source] != nil
        begin
          TU.info("Load seqno #{@options[:seqno]} from #{@options[:source]}")
          if TI.is_commercial?()
            # Do not include a service name when using the clustering software
            # because the source service name be different but the replicator
            # will only have a single service defined. This enables proper
            # operation for SOR deployments
            cmd = "#{TI.thl()} list -seqno #{@options[:seqno]} -headers -json"
          else
            cmd = "#{TI.thl(@options[:service])} list -seqno #{@options[:seqno]} -headers -json"
          end
          
          thl_record_content = TU.ssh_result(cmd, @options[:source], TI.user())
        rescue CommandError => ce
          TU.debug(ce)
          raise "There was an error running the thl command on #{@options[:source]}"
        end
      
        thl_records = JSON.parse(thl_record_content)
        unless thl_records.instance_of?(Array) && thl_records.size() == 1
          raise "Unable to read the THL record for seqno #{@options[:seqno]} from #{@options[:source]}"
        end
      
        thl_record = thl_records[0]
        columns = ['task_id', 'seqno', 'fragno', 'last_frag', 'epoch_number', 
          'eventid', 'source_id', 'update_timestamp', 'extract_timestamp']
        values = [0, thl_record['seqno'], thl_record['frag'], 
          (thl_record['lastFrag'] == true ? 1 : 0), thl_record['epoch'], 
          "'#{thl_record['eventId']}'", "'#{thl_record['sourceId']}'", 
          timestamp(), timestamp()]
      else
        columns = ['task_id', 'seqno', 'fragno', 'last_frag', 'epoch_number', 
          'update_timestamp', 'extract_timestamp']
        values = [0, @options[:seqno], 0, 1, @options[:epoch],  
          timestamp(), timestamp()]
          
        if @options[:event_id] != nil
          columns << "eventid"
          values << "'#{@options[:event_id]}'"
        end
        if @options[:source_id] != nil
          columns << "source_id"
          values << "'#{@options[:source_id]}'"
        end
      end
      
      if TI.can_sql?(opt(:service))
        service_schema = TI.setting(TI.setting_key(REPL_SERVICES, @options[:service], "repl_svc_schema"))
        set_position_in_schema(service_schema, columns, values)
      else
        TU.error("Unable to run #{script_name()} because the #{TI.datasource_type(opt(:service))} datasource for #{opt(:service)} is not supported.")
      end
    rescue => e
      raise e
    end
  end
  
  def set_position_in_schema(service_schema, columns, values)
    sql = []
    
    sql << "DELETE FROM #{service_schema}.trep_commit_seqno;"
    sql << "INSERT INTO #{service_schema}.trep_commit_seqno (#{columns.join(",")}) VALUES (#{values.join(",")});"
    
    if @options[:sql] == true
      TU.output(sql.join(""))
    else
      pre_sql = []
      
      if opt(:replicate_statements) == false
        case TI.datasource_type(opt(:service))
        when "mysql"
          pre_sql << "SET SESSION SQL_LOG_BIN=0;"
        else
          TU.warning("Unable to prevent replication of #{script_name()} statements on a #{TI.datasource_type(opt(:service))} datasource.")
        end
      end
      
      begin
        position = TI.sql_result(@options[:service], "SELECT * FROM #{service_schema}.trep_commit_seqno;")
      rescue CommandError => ce
        TU.debug(ce)
        TU.warning "Unable to check the '#{service_schema}.trep_commit_seqno' table. This may be an indication of problems with the database."
      end

      if position != nil && position.size() > 1
        if TU.force?() == true
          TU.warning("The '#{service_schema}.trep_commit_seqno' table contains more than 1 row. All rows will be replaced.")
        else
          raise "Unable to update '#{service_schema}.trep_commit_seqno' because it currrently has more than one row. Add '--force' to bypass this warning."
        end
      end
      
      # We can ignore failures here since they would likely be due to
      # the schema not existing. If there is another issue, it will be caught
      # in the next section.
      begin
        drop_schema = pre_sql.dup()
        
        case TI.datasource_type(opt(:service))
        when "mysql"
          drop_schema << "DROP SCHEMA #{service_schema};"
        when "redshift"
          drop_schema << "DROP SCHEMA #{service_schema} CASCADE;"
        end
        
        TI.check_sql_results(TI.sql_results(opt(:service), drop_schema))
      rescue CommandError => ce
        TU.debug(ce)
      end
      
      # Create the schema contents
      TU.notice("Create the #{service_schema} schema")
      begin
        schema = pre_sql.dup()
        
        case TI.datasource_type(opt(:service))
        when "mysql"
          schema << "CREATE SCHEMA #{service_schema};"
          schema << "USE #{service_schema};"
        when "redshift"
          schema << "CREATE SCHEMA #{service_schema};"
          schema << "SET search_path TO #{service_schema};"
        end
        
        # Load the schema declaration. We have to modify it so that each 
        # statement exists on a single line.
        tungsten_schema_sql_file = "#{TI.root()}/#{CURRENT_RELEASE_DIRECTORY}/tools/ruby-tpm/configure/sql/tungsten_schema.#{TI.datasource_type(opt(:service))}.sql"
        unless File.exist?(tungsten_schema_sql_file)
          tungsten_schema_sql_file = "#{TI.root()}/#{CURRENT_RELEASE_DIRECTORY}/tools/ruby-tpm/configure/sql/tungsten_schema.sql"
        end
        File.open(tungsten_schema_sql_file, "r") {
          |f|
          schema << f.read().tr("\n", "").gsub(/;/, ";\n")
        }
        
        TI.check_sql_results(TI.sql_results(opt(:service), schema))
      rescue CommandError => ce
        TU.debug(ce)
        raise "Unable to create the #{service_schema} "
      end
      
      # Apply the trep_commit_seqno information
      TU.notice("Update the #{service_schema}.trep_commit_seqno table")
      begin
        TI.check_sql_results(TI.sql_results(opt(:service), pre_sql + sql))
      rescue CommandError => ce
        TU.debug(ce)
        raise "Unable to update #{service_schema}.trep_commit_seqno"
      end
    end
  end

  def configure
    super()
    description("Update the trep_commit_seqno table with metadata for the given sequence number.<br>
Examples:<br>
$> tungsten_set_position.sh --source=db1 --seqno=35<br>
$> tungsten_set_position.sh --seqno=35 --epoch=23")

    add_option(:epoch, {
      :on => "--epoch String",
      :help => "The epoch number to use for updating the trep_commit_seqno table"
    })
    
    add_option(:event_id, {
      :on => "--event-id String",
      :help => "The event id to use for updating the trep_commit_seqno table"
    })
    
    add_option(:source, {
      :on => "--source String",
      :help => "Determine metadata for the --seqno statement from this host"
    })
    
    add_option(:seqno, {
      :on => "--seqno String",
      :help => "The sequence number to use for updating the trep_commit_seqno table"
    })
    
    add_option(:source_id, {
      :on => "--source-id String",
      :help => "The source id to use for updating the trep_commit_seqno table"
    })
    
    add_option(:replicate_statements, {
      :on => "--replicate-statements String",
      :default => false,
      :parse => method(:parse_boolean_option),
      :help => "Execute the events so they will be replicated if the service is a master"
    })
    
    add_option(:sql, {
      :on => "--sql",
      :default => false,
      :help => "Only output the SQL statements needed to update the schema",
      :aliases => ["--dry-run"]
    })
  end
  
  def validate
    super()
    
    if @options[:seqno] == nil
      TU.error("The --seqno argument is required")
    end
    
    if @options[:clear_logs] == true && @options[:sql] == true
      TU.error("Unable to clear logs when the --sql argument is given")
    end
    
    if @options[:source] == nil && @options[:epoch] == nil
      TU.error("You must provide the --source or --epoch argument")
    end
    
    if @options[:source] != nil && @options[:epoch] != nil
      TU.error("You may not provide the --source or --epoch arguments together")
    end
  end
  
  def timestamp
    case TI.datasource_type(opt(:service))
    when "mysql"
      "NOW()"
    when "redshift"
      "'" + DateTime.now().strftime("%Y-%m-%d %H:%M:%S.%L") + "'"
    else
      raise "Unable to run #{script_name()} against the #{TI.datasource_type(opt(:service))} datasource for #{opt(:service)}."
    end
  end
  
  def require_offline_services?
    if @options[:sql] == true
      false
    else
      super()
    end
  end
  
  def allow_service_state_change?
    if @options[:sql] == true
      false
    else
      super()
    end
  end
  
  def script_name
    "tungsten_set_position"
  end
end