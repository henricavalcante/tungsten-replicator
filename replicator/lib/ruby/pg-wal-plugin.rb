# TUNGSTEN SCALE-OUT STACK
# Copyright (C) 2009 Continuent, Inc.
# All rights reserved
#

# Tungsten libraries.
require 'plugin'
require 'tungsten/properties'
require 'tungsten/exception'
require 'tungsten/transformer'

# Ruby extension libraries. 
require 'fileutils'
require 'uri'
require 'time'

# Implementation class for PostgreSQL WAL shipping. 
class PgWalPlugin < Plugin
  # GLOBAL VARIABLES
  PG_WAL_ARCHIVE   = "/bin/pg/pg-wal-archive"
  PG_WAL_RESTORE   = "/bin/pg/pg-wal-restore"
  PG_WAL_CONF_DIR  = "/conf/pg-wal"
  PG_WAL_PROPERTIES = "/pg-wal.properties"
  PG_TUNGSTEN_CONF = "/postgresql.tungsten.conf"
  PG_TUNGSTEN_CONF_S = "/postgresql.tungsten.conf.tpl"
  INITIALIZED      = "/initialized"
  GOING_ONLINE     = "/going_online"
  ONLINE           = "/online"
  ARCHIVING_ACTIVE = "/archiving_active"
  ARCHIVING_ACTIVE_TEMPLATE = "/archiving_active_template"
  
  # Initialize configuration arguments. 
  def initialize(cmd, argv)
    # Loads the config file. 
    super(cmd, argv)
    
    # Compute useful file names.  These may not depend on the official
    # configuration file as it has not been loaded yet. 
    @replicator_home    = getReplicatorHome()
    @pg_wal_archive     = @replicator_home + PG_WAL_ARCHIVE
    @pg_wal_restore     = @replicator_home + PG_WAL_RESTORE
    @pg_wal_conf        = @replicator_home + PG_WAL_CONF_DIR
    @pg_wal_when        = @replicator_home + PG_WAL_CONF_DIR + "/when"
    @pg_tungsten_conf_s = @replicator_home + "/samples/conf" + PG_TUNGSTEN_CONF_S
    @pg_tungsten_conf   = @pg_wal_conf + PG_TUNGSTEN_CONF
    @pg_wal_properties  = @pg_wal_conf + PG_WAL_PROPERTIES
    @initialized        = @pg_wal_conf + INITIALIZED
    @archiving_active   = @pg_wal_conf + ARCHIVING_ACTIVE
    @archiving_active_template = @pg_wal_conf + ARCHIVING_ACTIVE_TEMPLATE
    @archiving_active_standby = @pg_wal_conf + ARCHIVING_ACTIVE + "." + `hostname`.chomp
    @going_online       = @pg_wal_conf + GOING_ONLINE
    @online             = @pg_wal_conf + ONLINE
    @recovery_conf      = @pg_wal_conf + "/recovery.conf"
    @wal_archived       = @pg_wal_conf + "/wal_archived"
    @wal_restored       = @pg_wal_conf + "/wal_restored"
    @wal_restored_when  = @pg_wal_conf + "/wal_restored.when"
    
    # Maximum time to wait for a master to startup (in seconds).
    @master_max_startup_time = 120
    # Name of system 'tungsten' database in PostgreSQL instance.
    @tungsten_db = "tungsten"
    # Default port for PostgreSQL if not defined.
    @default_pg_port = 5432
  end
  
  # IMPLEMENTATION METHODS. 
  
  # Prepare the plugin for use.  This is issued when the replicator
  # goes online. 
  def plugin_prepare
    log "Plug-in is prepared for use"
  end
  
  # Prepare the plugin for use.  This is issued when the replicator
  # goes online. 
  def plugin_release
    log "Plug-in resources are released"
  end
  
  # Checks whether the given command exists. Linux only. Not silent.
  def command_exists? (cmd, root)
    begin
      check_cmd = "which " + cmd
      if root
        check_cmd = get_root_cmd(check_cmd)
      end
      exec_cmd2(check_cmd, false)
      return true
    rescue SystemError
      return false
    end
  end
  
  # Install infrastructure and update PostgreSQL configuration files
  # to support replication.  Note:  Installation is designed to be identical
  # for primary and standby.  
  def plugin_install
    # Fetch properties and define variables used for initialization.
    master_host         = @config.getProperty("postgresql.master.host")
    master_port         = @config.getProperty("postgresql.master.port")
    pg_data             = @config.getProperty("postgresql.data")
    archive_timeout     = @config.getProperty("postgresql.archive_timeout")
    pg_standby          = @config.getProperty("postgresql.pg_standby")
    pg_archive          = @config.getProperty("postgresql.archive")
    trigger             = @config.getProperty("postgresql.pg_standby.trigger")
    postgresql_conf     = @config.getProperty("postgresql.conf")
    pg_archivecleanup   = @config.getProperty("postgresql.pg_archivecleanup")
    tungsten_user       = ENV['USER']
    
    # Check for usage description:
    log "Checking pg_standby availability"
    if not command_exists?(pg_standby, false)
      log "ERROR: pg_standby is not available in the given path:"
      log "       " + pg_standby
      log "       Replication won't work without pg_standby."
      log "Please consult Replicator Guide for installation details."
      log ""
      print_batch_mode_suggestion
      log ""
      raise SystemError, "pg_standby is not available in: " + pg_standby
    end
    if not command_exists?(pg_standby, true)
      log "ERROR: pg_standby is not available for root user:"
      log "       " + pg_standby
      log "       Replication won't work without pg_standby."
      log "Please consult Replicator Guide for installation details."
      log ""
      print_batch_mode_suggestion
      log ""
      raise SystemError, "pg_standby is not available for root: " + pg_standby
    end
    
    # If we are already initialized, nothing to do. 
    if File.exist?(@initialized)
      log("Already initialized; nothing to do")
      return
    end
    log("Starting installation for PostgreSQL WAL shipping")
    
    # Create conf/postgresql-wal directory if not already present.
    log("Creating conf dir: " + @pg_wal_conf)
    FileUtils.mkdir_p(@pg_wal_conf)
    
    log("Creating WAL generation times' dir: " + @pg_wal_when)
    FileUtils.mkdir_p(@pg_wal_when)
    
    # Generate postgressql.conf include file from template. 
    log("Updating postgresql.tungsten.conf")
    transformer = Transformer.new(@pg_tungsten_conf_s, @pg_tungsten_conf,
      "#");
    transformer.transform { |line|
      if line =~ /archive_mode/ then
        "archive_mode = on"
      elsif line =~ /archive_command/ then
        "archive_command ='" + @pg_wal_archive + " %p %f'"
      elsif line =~ /archive_timeout/
        "archive_timeout = " + archive_timeout
      elsif line =~ /#max_wal_senders/ and is_streaming_replication
        line[1..-1]
      elsif line =~ /#wal_level/ and is_streaming_replication
        line[1..-1]
      elsif line =~ /#hot_standby/ and is_streaming_replication
        line[1..-1]
      else
        line
      end
    }
    
    # Update include statement in postgresql.conf if there is one to 
    # point to postgresql.tungsten.conf. 
    no_include = true
    transformer = Transformer.new(postgresql_conf, postgresql_conf, nil);
    transformer.transform { |line|
      if line =~ /^include.*postgresql.tungsten.conf/ then
        no_include = false
        "include '" + @pg_tungsten_conf + "'"
      else
        line
      end
    }
    # If we didn't find an include, add it now. 
    if no_include
      exec_cmd("echo \"\" >> #{postgresql_conf}")
      exec_cmd("echo \"# Include for properties required by Tungsten.\" >> #{postgresql_conf}")
      exec_cmd("echo \"include '#{@pg_tungsten_conf}'\" >> #{postgresql_conf}")
    end
    
    # Generate local archiving_active template in case we are a standby. 
    hostname = `hostname`.chomp
    arch_active_props = Properties.new
    arch_active_props.setProperty("postgresql.standby.host", hostname)
    arch_active_props.setProperty("postgresql.standby.archive", pg_archive)
    arch_active_props.store(@archiving_active_template) 
    
    # Create initial copy of current properties. 
    ckeys = ["postgresql.role", 
             "postgresql.port",
             "postgresql.master.host",
             "postgresql.master.port",
             "postgresql.archive",
             "postgresql.pg_standby",
             "postgresql.pg_standby.trigger",
             "postgresql.pg_archivecleanup"]
    cprops = Properties.new  
    ckeys.each do | key |
      val = @config.getProperty(key)
      cprops.setProperty(key, val)
    end
    cprops.store(@pg_wal_properties)
    
    # Determine role and prepare accordingly. 
    role = @config.getProperty("postgresql.role")
    if role == "slave"
      install_standby(pg_data, postgresql_conf, pg_archive)
    elsif role == "master"
      if @options.skip_restart
        log "Skipping PostgreSQL server restart as requested by user."
      else
        # Ensure the server is started. 
        stop_pg_server(true)
        status_pg_server(false) # Check PostgreSQL is stopped.
        start_pg_server(false)
        # Bide a few seconds; If this PG instance was a slave, it might not be
        # in continuous recovery mode yet, thus trigger file might get eaten.
        # TODO: wait for SQLSTATE=57P03 (eg. develop a `./monitorctl checkdb`). 
        sleep(10)
        status_pg_server(true) # Check PostgreSQL is started.
        exec_cmd("touch #{trigger}") # End recovery (needed if installing on previously a standby).
        wait_until_login_works
      end
      create_tungsten_db(true)
      exec_cmd2("psql -p#{@config.getProperty("postgresql.port")} -t -c \"CREATE TABLE progress_timestamp (wal varchar(32), segment varchar(12), datetime timestamp)\" #{@tungsten_db};", false)
    else
      raise UserError, "Unknown role: #{role}"
    end
    
    generate_recovery_conf(master_host, master_port)
    
    # Note the happy outcome.  
    tag_file_write(@initialized, role)
    log("Installation completed successfully")
  end 
  
  # Generate recovery.conf so we can go online later.
  def generate_recovery_conf(master_host, master_port)
    config = getConfig()
    cprops = getPgWalProperties()
    
    master_user = config.getProperty("postgresql.master.user")
    trigger = cprops.getProperty("postgresql.pg_standby.trigger")
    pg_archive  = @config.getProperty("postgresql.archive")
    
    out = File.open(@recovery_conf, "w")
    if is_streaming_replication
      pg_archivecleanup = @config.getProperty("postgresql.pg_archivecleanup")
      
      out.puts("# Specifies whether to start the server as a standby. In streaming replication,")
      out.puts("# this parameter must to be set to on.")
      out.puts("standby_mode = 'on'")
      out.puts
      out.puts("# Specifies a connection string which is used for the standby server to connect")
      out.puts("# with the primary.")
      out.puts("primary_conninfo = 'host=#{master_host} port=#{master_port} user=#{master_user}'")
      out.puts
      out.puts("# Specifies a trigger file whose presence should cause streaming replication to")
      out.puts("# end (i.e., failover).")
      out.puts("trigger_file = '#{trigger}'")
      out.puts
      out.puts("# In SR, we only move the WAL file to PG, if asked.")
      out.puts("restore_command = 'echo \"Restoring %f\"; mv #{pg_archive}/%f %p'")
      out.puts
      out.puts("# We also need to take care about removing not needed WAL files under SR.") 
      out.puts("archive_cleanup_command = '#{pg_archivecleanup} -d #{pg_archive} %r 2>>cleanup.log'")
    else
      out.puts("restore_command = '#{@pg_wal_restore} %f %p %r'")
    end
    #out.puts("restore_command = '#{pg_standby} -l -d -s 2 -t #{trigger} #{pg_archive} %f %p %r 2>>standby.log'")  
    out.close    
  end
  
  # Uninstall infrastructure to update PostgreSQL configuration files. 
  def plugin_uninstall
    # Clean all files in pg-wal configuration directory. 
    log("Performing uninstall")
    role = @config.getProperty("postgresql.role")
    if role == "master"
      create_tungsten_db(false) # Delete the 'tungsten' database.
    end
    if File.exists?(@pg_wal_conf)
      Dir.foreach(@pg_wal_conf) { | file |
        fpath = @pg_wal_conf + "/" + file
        if File.file?(fpath)
          File.delete(fpath)
          log("Deleted file: " + fpath)
        end
      }
    end
  end 
  
  # Provision a standby server. 
  def plugin_provision()
    # Make sure we are a standby.
    if ! is_standby()
      raise UserError, "You can only provision a standby"
    end
    
    # Perform provisioning operation. 
    provision
  end
  
  # Tries login to PostgreSQL until success or @master_max_startup_time reached.
  def wait_until_login_works
    sleep_time = 0
    sleep_timeout = @master_max_startup_time
    while true
      psql_out = `echo "select 'UP'" |psql -p#{@config.getProperty("postgresql.port")} 2>&1`
      if psql_out =~ /UP/
        puts "Master is up and responding"
        break;
      else # TENT-161, can't depend on localized messages: psql_out =~ /the database system is starting up/
        puts psql_out
        if sleep_time <= sleep_timeout
          sleep_time += 5
          puts "Master database might still be starting up; sleeping #{sleep_time} of #{sleep_timeout} seconds..."
          sleep 5
        else
          raise SystemError, "Master database did not come up within #{sleep_timeout} secons"
        end
      end
    end    
  end
  
  # Turn PG replication online.  This requires a restart. 
  def plugin_online
    # Make sure we are online. 
    if tag_file_exist?(@online)
      log("Node is already online")
    else
      tag_file_write(@going_online, get_role())
      
      # Branch on the role. 
      if is_master() 
        # Ensure the server is started.
        if not status_pg_server(nil)
          start_pg_server(false)
        end
        
        # Write the trigger file.  This will terminate recovery if in effect.
        log("Performing master online procedure")
        trigger = @config.getProperty("postgresql.pg_standby.trigger")
        exec_cmd("touch #{trigger}")
        
        # Wait until we can login.  Otherwise we get race conditions when 
        # trying to start standby instance(s).
        wait_until_login_works
      elsif is_standby()
        log("Performing standby online procedure")
        # Provision standby data.  This stops a live server. 
        provision
        
        # Start the server. 
        start_pg_server(true)
        # Bide a few seconds; PG might not be in continuous recovery mode yet.
        # TODO: wait for SQLSTATE=57P03 (eg. develop a `./monitorctl checkdb`). 
        sleep(10)
        status_pg_server(true) # Check PostgreSQL is started.
      else
        raise UserError, "Illegal role value"
      end
      
      # Mark online.  
      tag_file_write(@online, get_role())
      if tag_file_exist?(@going_online)
        tag_file_remove(@going_online)
      end
    end 
  end
  
  # Set the replicator role. 
  def plugin_setrole
    # Make sure we are offline. 
    if tag_file_exist?(@online)
      raise UserError, "Must be offline to set role"
    end
    
    # Find current properties and fetch current role/host values. 
    cprops = getPgWalProperties()
    current_role = cprops.getProperty("postgresql.role")
    current_host = cprops.getProperty("postgresql.master.host")
    current_port = cprops.getProperty("postgresql.master.port")
    
    # Fetch arguments and ensure they are valid and different from current values. 
    role = @args.getProperty("role")
    if ! role
      raise UserError, "Must specify a value for role property"
    end
    
    raw_uri = @args.getProperty("uri")
    if raw_uri
      uri = URI.parse(raw_uri)
      host = uri.host
      port = uri.port
    end
    if ! host 
      host = "none"
    end
    if ! port
      # Use default PG port if none specified.
      port = @default_pg_port
    end
    
    log("Processing set role operation: role=#{role} master.host=#{host} master.port=#{port}")
    if role == current_role && host == current_host && port == current_port
      log("Role value is unchanged; no action taken")
    end
    
    # Process the role shift. 
    case
      when role == "master"
      # Have to write trigger file if we are currently a standby. 
      if current_role == "slave"
        trigger = cprops.getProperty("postgresql.pg_standby.trigger")
        log("Writing trigger file to ensure recovery ends: #{trigger}")
        exec_cmd("touch #{trigger}")
      end
      when role == "slave"
      if ! raw_uri || host == "none"
        raise UserError, "Role shift to slave requires valid URI"
      end
      
      # If we have a current master, turn off updates.  
      # TODO:  Sometimes it appears properties lost host name, which is why
      # we check for the empty string. 
      if current_host != nil && current_host != "none" && current_host.strip() != ""
        log("Removing archiving file on master #{current_host}")
        log("WAL files will no longer be sent to this standby")
        exec_cmd2("ssh #{current_host} rm -f #{@archiving_active_standby}", true)
      end
    else
      raise UserError, "Unrecognized role: #{role}"
    end
    
    # Update properties.  
    log("Updating pg-wal properties: role=#{role} master.host=#{host} master.port=#{port}")
    cprops.setProperty("postgresql.role", role)
    cprops.setProperty("postgresql.master.host", host)
    cprops.setProperty("postgresql.master.port", port)
    cprops.store(@pg_wal_properties)
    
    generate_recovery_conf(host, port)
  end
  
  # Turns the replicator offline, which stops replication. 
  def plugin_offline
    # Make sure we are online. 
    if tag_file_exist?(@online)
      if is_master()
        log("Stopping archiving on master")
      else
        # Stop sending WAL files from the master to this slave.
        dynamicConfig = getPgWalProperties()
        master_host = dynamicConfig.getProperty("postgresql.master.host")
        exec_cmd2("ssh #{master_host} rm -f #{@archiving_active_standby}", true)  
      end 
      
      tag_file_remove(@online)
      log("Node is now offline")
    else
      log("Node is already offline")
    end
  end
  
  def plugin_offline_deferred
    log("offlineDeferred call mapped to offline")
    plugin_offline
  end
  
  # Query PG for one field and one row result.
  def psql_atom_query(sql)
    `echo "#{sql}" |psql -p#{@config.getProperty("postgresql.port")} -q -A -t 2>&1`
  end
  
  def pg_current_xlog_location()
    psql_atom_query "select pg_current_xlog_location();"
  end
  
  def pg_last_xlog_replay_location()
    psql_atom_query "select pg_last_xlog_replay_location();"
  end
  
  def pg_last_xlog_receive_location()
    psql_atom_query "select pg_last_xlog_receive_location();"
  end
  
  # Emits current WAL replication status. 
  def plugin_status
    # See if we have a file to which we can write.  
    if @out_params_file
      out = File.new(@out_params_file, "w")
    else
      out = STDOUT
    end
    
    # Write the values. 
    if is_master()
      out.puts("role=master")
      #out.puts("errmsg=")
      out.puts("pendingError=")
      #out.puts("last-sent=" + read_status_file(@wal_archived))
      if is_streaming_replication
        save_progress_timestamp
        out.puts("appliedLastSeqno=" + pg_current_xlog_location())
        out.puts("appliedLastEventId=" + pg_current_xlog_location())
        out.puts("currentEventId=" + pg_current_xlog_location())
      else
        out.puts("appliedLastSeqno=" + read_status_file(@wal_archived))
        out.puts("appliedLastEventId=" + read_status_file(@wal_archived))
        out.puts("currentEventId=" + read_status_file(@wal_archived))
      end
      #out.puts("last-applied=") 
      #out.puts("last-received=")
      out.puts("minimumStoredSeqNo=") # TODO: oldest stored WAL file.
      out.puts("maximumStoredSeqNo=")
      #out.puts("applied-latency=0.0")
      out.puts("appliedLatency=0.0")
    else
      out.puts("role=slave")
      #out.puts("errmsg=")
      out.puts("pendingError=")
      #out.puts("last-sent=")
      #out.puts("last-applied=" + read_status_file(@wal_restored))
      if is_streaming_replication
        out.puts("appliedLastSeqno=" + pg_last_xlog_replay_location())
        out.puts("appliedLastEventId=" + pg_last_xlog_replay_location())
        out.puts("minimumStoredSeqNo=") # TODO: oldest stored WAL file.
        out.puts("maximumStoredSeqNo=" + pg_last_xlog_receive_location())
        out.puts("currentEventId=" + pg_last_xlog_replay_location())
        out.puts("appliedLatency=" + get_streaming_replication_latency())
      else
        out.puts("appliedLastSeqno=" + read_status_file(@wal_restored))
        out.puts("appliedLastEventId=" + read_status_file(@wal_restored))
        #out.puts("last-received=" + read_status_file(@wal_archived))
        out.puts("minimumStoredSeqNo=") # TODO: oldest stored WAL file.
        out.puts("maximumStoredSeqNo=" + read_status_file(@wal_archived))
        out.puts("currentEventId=" + read_status_file(@wal_restored))
        out.puts("appliedLatency=" + get_wal_shipping_latency().to_s)
      end
    end
    
    # Close file only if we are using output params. 
    if @out_params_file
      out.close
    end
  end
  
  # Lists plugin capabilities. 
  def plugin_capabilities
    # See if we have a file to which we can write.  
    if @out_params_file
      out = File.new(@out_params_file, "w")
    else
      out = STDOUT
    end
    
    # Write capabilities. 
    out.puts("roles=master,slave")
    out.puts("model=push")
    out.puts("consistency=false")
    out.puts("heartbeat=false")
    out.puts("flush=true")
    out.puts("provision=joiner") 
    
    # Close file only if we are using output params. 
    if @out_params_file
      out.close
    end
  end
  
  # Ensures that a database for tungsten is created in PG.
  # If create == true, the database is created, otherwise - dropped.
  def create_tungsten_db(create)
    if create
      log "Ensuring that database '#{@tungsten_db}' is created"
      exec_cmd2("psql -p#{@config.getProperty("postgresql.port")} -t -c \"CREATE DATABASE #{@tungsten_db}\";", true)
    else
      log "Dropping database '#{@tungsten_db}'"
      exec_cmd2("psql -p#{@config.getProperty("postgresql.port")} -t -c \"DROP DATABASE #{@tungsten_db}\";", true)
    end
  end
  
  # Flushes master events into replication stream. 
  def plugin_flush
    # Make sure we are a master.
    if ! is_master()
      raise UserError, "You can only flush from a master"
    end
    
    # Switch logs on master.
    # Ensure tungsten database is created.
    create_tungsten_db(true)
    # Create table might fail, if it already exists, eg. after a failed flush
    # from the previous time.
    exec_cmd2("psql -p#{@config.getProperty("postgresql.port")} -t -c \"CREATE TABLE force_log_switch (i int)\" #{@tungsten_db};", true)
    # Drop table should always succeed, unless we failed to create table in the
    # previous step, which would indicate a problem.
    exec_cmd2("psql -p#{@config.getProperty("postgresql.port")} -t -c \"DROP TABLE force_log_switch\" #{@tungsten_db};", false)
    switch_sql = "SELECT pg_switch_xlog()"
    log switch_sql
    pg_switch_xlog = psql_atom_query(switch_sql)
    log "Logs flushed at: #{pg_switch_xlog}"
    
    # Bide a few seconds; PG will flush the log if there are committed
    # data.  This will get us the most recently sent data. 
    sleep(10)
    
    # See if we have a file to which we can write.
    if @out_params_file
      out = File.new(@out_params_file, "w")
    else
      out = STDOUT
    end
    
    # Write the last archived file.
    if is_streaming_replication
      out.puts("appliedLastSeqno=" + pg_switch_xlog)
    else
      out.puts("appliedLastSeqno=" + read_status_file(@wal_archived))
    end
    
    # Close file only if we are using output params.
    if @out_params_file
      out.close
    end
  end
  
  # Ties current replication position to the current time, so slave latency
  # could be calculated later.
  def save_progress_timestamp
    # Make sure we are online. 
    if tag_file_exist?(@online)
      # We don't need old records.
      exec_cmd2("psql -p#{@config.getProperty("postgresql.port")} -t -c \"DELETE FROM progress_timestamp\" #{@tungsten_db};", true, true)
      # Link current WAL file and SR offset to current time.
      exec_cmd2("psql -p#{@config.getProperty("postgresql.port")} -t -c \"INSERT INTO progress_timestamp VALUES (pg_xlogfile_name(pg_current_xlog_location()), pg_current_xlog_location(), CURRENT_TIMESTAMP)\" #{@tungsten_db};", true, true)
    end
  end
  
  def get_streaming_replication_latency()
    # Current time minus time of the last applied segment = approx. delay in replication.
    latency = `psql -p#{@config.getProperty("postgresql.port")} -t -c \"SELECT TRUNC(CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP-MAX(datetime)) AS DECIMAL),1) AS latency FROM progress_timestamp\" #{@tungsten_db}`.strip()
    if latency == ""
      return "-1.0"
    else
      return latency
    end
  end

  def get_wal_shipping_latency()
    # Current time minus time of when the WAL file was archived on a master = latency.
    archived_when = read_status_file(@wal_restored_when)
    if archived_when != "UNKNOWN"
      return (Time.now - Time.parse(archived_when)).to_i
    else
      return "?"
    end
  end

  # Waits for an event to arrive on the standby.  
  def plugin_waitevent
    # Make sure we are a standby.
    if ! is_standby()
      raise UserError, "You can only wait for events on a standby"
    end
    
    # We take timeout and event ID arguments. 
    event = @args.getProperty("event")
    timeout = @args.getProperty("timeout")
    if ! event
      raise UserError, "You must supply an event to wait for"
    end
    if ! timeout 
      timeout = 0
    else
      # Have to convert type as we are holding a string, not a Fixnum.  
      timeout = timeout.to_i
    end
    
    # Write the values. 
    waited = 0
    while timeout == 0 || waited < timeout
      latest_event = read_status_file(@wal_restored)
      if latest_event >= event
        puts "Found matching event: #{latest_event}"
        break
      end
      sleep 1
      waited += 1
    end
  end
  
  ################################
  # SUPPORT ROUTINES             #
  ################################
  # Configure standby.  
  def install_standby(pg_data, postgresql_conf, pg_archive)
    # Save postgresql.conf including permissions and ownership. 
    trace("Saving postgresql.conf: #{postgresql_conf}")
    exec_cmd("cp -p #{postgresql_conf} #{@pg_wal_conf}")
    
    # Ensure standby won't activate immediately on start-up. 
    trigger = @config.getProperty("postgresql.pg_standby.trigger")
    exec_cmd("rm -f #{pg_data}/trigger")   
    #####################################################
    # The following lines were commented out because for both
    # the 8.4 and 9.0 installs, removing these files causes
    # a failure in the server startup because, somewhere,
    # probably in a database, there's a checkpoint record that
    # points at log files in these directories and if you just
    # remove the files, the server can't restart from the
    # checkpoint record.  Perhaps a new checkpoint should
    # be taken after deleting these files if you really
    # need to do this.
    ######################################################
    #exec_cmd("rm -rf #{pg_data}/pg_xlog")
    #exec_cmd("mkdir -p #{pg_data}/pg_xlog/archive_status")
  
    
    # Create archive directory. 
    exec_cmd("mkdir -p #{pg_archive}")
  end
  
  # Provision a standby server.  This shuts down the server. 
  def provision()
    # Make sure we are a standby.
    if ! is_standby()
      raise UserError, "You can only provision a standby"
    end
    
    # Get static properties. 
    config = getConfig()
    pg_data     = config.getProperty("postgresql.data")
    pg_archive  = config.getProperty("postgresql.archive")
    master_user = config.getProperty("postgresql.master.user")
    trigger     = config.getProperty("postgresql.pg_standby.trigger")
    
    # Get dynamic properties. 
    dynamicConfig = getPgWalProperties()
    master_host = dynamicConfig.getProperty("postgresql.master.host")
    master_port = dynamicConfig.getProperty("postgresql.master.port")
    
    # Shut down the standby server, ignoring errors. 
    stop_pg_server(true)
    status_pg_server(false) # Check PostgreSQL is stopped.
    
    # Clean up the archived WAL files times' directory. 
    puts("Cleaning up data about when the WAL files were generated");
    FileUtils.mkdir_p(@pg_wal_when) # Ensure folder is created.
    exec_cmd("rm -r -f #{@pg_wal_when}/*")
    exec_cmd("rm -f #{@wal_restored_when}")
    
    # Clean up the archive log directory. 
    puts("Cleaning up old archive logs");
    exec_cmd("rm -r -f #{pg_archive}/*")
    
    # Ensure remote server knows to ship WAL logs to us. 
    hostname = `hostname`.chomp
    exec_cmd("scp #{@archiving_active_template} #{master_host}:#{@archiving_active_standby}")
    
    # Switch logs on master.
    retry_sleep = 5
    retry_i = 0
    retry_max = @master_max_startup_time / retry_sleep
    begin
      exec_cmd("psql -h#{master_host} -p#{master_port} -U#{master_user} -t -c \"select 'archiving_active written at '||pg_switch_xlog()\";")
    rescue SystemError => e
      puts "Failed to execute pg_switch_xlog() on a master: " + e.message
      if retry_i <= retry_max
        retry_i += 1
        puts "Has master finished recovery? Retry #{retry_i} of #{retry_max} in #{retry_sleep} seconds..."
        sleep retry_sleep
        retry
      else
        raise e
      end
    end
    
    # This has to be in a block so we don't forget to terminate backup. 
    begin
      # Backup and rsync data files. 
      puts "### Starting backup on #{master_host}:#{master_port}"
      exec_cmd("psql -h#{master_host} -p#{master_port} -U#{master_user} -t -c \"select 'Starting online backup at WAL file '|| pg_xlogfile_name(pg_start_backup('base_backup'));\"")
      
      puts "### Rsyncing files from #{master_host}:#{pg_data}..."
      # TENT-159: accepting exit code 24 - partial transfer due to vanished source files.
      exec_cmd2("rsync -azv --delete --exclude=*pg_log* --exclude=*pg_xlog* --exclude=postgresql.conf --exclude=pg_hba.conf --exclude=server.crt --exclude=server.key #{master_host}:#{pg_data}/ #{pg_data}/", 24)
      
    ensure
      puts "### Ending backup on #{master_host}:#{master_port}..."
      exec_cmd("psql -h#{master_host} -p#{master_port} -U#{master_user} -t -c \"select 'Stopping online backup at WAL file ' ||pg_xlogfile_name(pg_stop_backup());\"")
    end
    
    # Clean up the PG data directory. 
    puts("Cleaning up data directory")
    exec_cmd("rm -f #{pg_data}/recovery.*")
    exec_cmd("rm -f #{pg_data}/logfile")
    exec_cmd("rm -f #{pg_data}/postmaster.pid")
    
    # Clean up the pg_xlog directory.  
    puts("Cleaning up pg_xlogs directory")
    exec_cmd("rm -f #{pg_data}/pg_xlog/0*")
    exec_cmd("rm -f #{pg_data}/pg_xlog/archive_status/0*")
    
    # Add recovery.conf file. 
    log("Performing standby online procedure")
    exec_cmd("cp #{@recovery_conf} #{pg_data}")
    
    # Ensure standby won't activate immediately.  
    exec_cmd("rm -f #{trigger}")
    if File.exists?(trigger)
      raise SystemError, "Unable to remove trigger file: #{trigger}"
    end
    
    # Copy archive logs. -- OOPS! This deletes files that have already shipped.
    #puts "### Rsyncing archive logs"
    #exec_cmd("rsync -avz --delete #{master_host}:#{pg_archive}/ #{pg_archive}/ > /dev/null")
    
    log("Provisioning operation succeeded");
  end
  
  ################################
  # UTILITY API CALLS START HERE #  
  ################################
  
  # Load current pg-wal properties. 
  def getPgWalProperties
    if ! @pg_wal_props 
      @pg_wal_props = Properties.new
      @pg_wal_props.load(@pg_wal_properties)
    end
    @pg_wal_props
  end
  
  def get_pg_server_cmd(operation)
    return @config.getProperty("postgresql.boot.script") + " " + operation
  end
  
  # Start PostgreSQL server. 
  def start_pg_server(ignore_failure)
    log("Starting PostgreSQL server")
    start_cmd = get_pg_server_cmd("start")
    exec_root_cmd2(start_cmd, ignore_failure)
  end
  
  # Stop PostgreSQL server. 
  def stop_pg_server(ignore_failure)
    log("Stopping PostgreSQL server")
    stop_cmd = get_pg_server_cmd("stop")
    exec_root_cmd2(stop_cmd, ignore_failure)
  end
  
  def print_batch_mode_suggestion
    log "After correcting the problem you may run './configure -b'"
    log "to repeat the installation quickly in batch mode."
  end
  
  # Checks the status of PostgreSQL server and informs the user
  # if it's not an expected one.
  # Use expect_started=nil in order to just get true or false without
  # informing the user.
  def status_pg_server(expect_started)
    status_cmd = get_pg_server_cmd("status")
    stop_cmd = get_pg_server_cmd("stop")
    start_cmd = get_pg_server_cmd("start")
    if expect_started == nil
      status = "started or stopped"
    elsif expect_started
      status = "started"
    else
      status = "stopped"
    end
    log "Checking PostgreSQL server is " + status
    succeeded = true
    begin
      exec_root_cmd2(status_cmd, false)
    rescue SystemError
      succeeded = false
    end
    if expect_started == nil
      return succeeded
    end
    if expect_started and not succeeded
      log "ERROR: PostgreSQL is stopped when expected to be started or one of"
      log "       the following commands failed. Please ensure both commands"
      log "       can be executed by current account:"
      log "       " + get_root_cmd(start_cmd)
      log "       " + get_root_cmd(status_cmd)
      log ""
      log "       NOTE: the following status command is expected to return"
      log "       a zero return code when PostgreSQL is started:"
      log "       " + get_root_cmd(status_cmd)
      log ""
      print_batch_mode_suggestion
      log ""
      raise SystemError, "PostgreSQL is stopped when expected to be started or status command failed."
    elsif not expect_started and succeeded
      log "ERROR: PostgreSQL is started when expected to be stopped."
      log "       The following command might have failed. Please ensure"
      log "       it can be executed by current account:"
      log "       " + get_root_cmd(stop_cmd)
      log ""
      log "       NOTE: the following status command is expected to return"
      log "       a non zero return code when PostgreSQL is stopped:"
      log "       " + get_root_cmd(status_cmd)
      log ""
      print_batch_mode_suggestion
      log ""
      raise SystemError, "PostgreSQL is started when expected to be stopped."
    end
  end
  
  # Is SR enabled? Returns true or false.
  def is_streaming_replication
    config = getConfig()
    if config.getProperty("postgresql.streaming_replication") == "true"
      return true
    else
      return false
    end
  end
  
  # Gets the current role. 
  def get_role
    # Get current properties and see what role we are playing.
    cprops = getPgWalProperties()
    cprops.getProperty("postgresql.role")
  end
  
  # Return true if we are master. 
  def is_master
    if get_role() == "master"
      true
    else
      false
    end
  end
  
  # Return true if we are a standby. 
  def is_standby
    if get_role() == "slave"
      true
    else
      false
    end
  end
  
  # Read the value of a 1-line status file or return UNKNOWN if file 
  # cannot be read. 
  def read_status_file(file)
    if File.exists?(file)
      contents = "UNKNOWN"
      File.open(file, "r") { | input |
        contents = input.gets()
      }
      if contents == nil
        return "UNKNOWN"
      else
        return contents
      end
    else
      return "UNKNOWN"
    end
  end
end
