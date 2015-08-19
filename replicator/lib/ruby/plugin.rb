# TUNGSTEN SCALE-OUT STACK
# Copyright (C) 2009 Continuent, Inc.
# All rights reserved
#

require 'optparse'
require 'ostruct'

require 'tungsten/properties'
require 'tungsten/exception'

# Superclass for Tungsten plugin implementations.  
class Plugin

  # Initialize configuration arguments. 
  def initialize(cmd, argv)
    # Arguments from ARGV
    @argv = argv

    # Command parameters
    @operation = nil
    @config_file = nil
    @in_params = nil
    @out_params_file = nil
 
    # Data
    @config = nil
    @args = nil

    # Define command name and location. 
    @cmd_basename = File.basename(cmd)
    @cmd_expanded = File.expand_path(cmd + '/..')
    @replicator_bin = File.dirname(@cmd_expanded)
    @replicator_home = File.dirname(@replicator_bin)

    # Control options
    @options = OpenStruct.new
    @options.verbose = false
    @options.skip_restart = false
  end

  # Process the operation. 
  def run
    if ct_parsed_options?
      trace "Start at #{DateTime.now}" 

      # Check arguments and process config.      
      ct_prepare 

      # Perform the operation. 
      begin
        ct_exec
      rescue UserError => userError
        if @out_params_file
          out = File.new(@out_params_file, "w")
          out.puts("errmsg=" + userError.to_s())
        end
        log ("User Error: " + userError.to_s())
        exit 1
      end
    end
  end

  # Parse command line arguments.
  def ct_parsed_options?
    opts=OptionParser.new
    opts.on("-o", "--operation String") {|val| @operation = val }
    opts.on("-c", "--config String")    {|val| @config_file = val }
    opts.on("-i", "--in-params String") {|val| @in_params = val }
    opts.on("-f", "--out-params-file String") {|val| @out_params_file = val }
    opts.on("-s", "--skip-restart")     {@options.skip_restart = true}
    opts.on("-h", "--help")             {|val| output_help; exit 0}
    opts.on("-V", "--verbose")          {@options.verbose = true}

    opts.parse!(@argv) rescue
    begin
      puts "Argument parsing failed"
      return false
    end

    true
  end

  # Print kind help. 
  def output_help
    puts "Usage: #{@cmd_basename} [options]"
    puts "Options:"
    puts "-o, --operation String  Operation to execute"
    puts "-c, --config String     Configuration file location"
    puts "-i, --in-params String  Optional arguments"
    puts "-f, --out-params-file String  Name of file for output parameters"
    puts "-s, --skip-restart      When installing, don't restart PostgreSQL server."
    puts "-h, --help              Display help message"
    puts "-V, --verbose           Print verbose messages"
    puts "Operations:"
    puts "  configure    Read open replicator configuration data"
    puts "  prepare      Get ready to perform open replicator commands"
    puts "  release      Release resources used for open replicator comamnds"
    puts "  provision    Provision from another data source"
    puts "  online       Start replication"
    puts "  offline      Stop replication"
    puts "  setrole      Set role (master, slave, standby, etc.)"
    puts "  status       Return current status of replication"
    puts "  flush        Flush data from master, returning event ID"
    puts "  waitevent    Wait for event ID to be processed on slave"
    puts "  capabilities List replicator capabilities"
  end

  # Check arguments and load configuration information. 
  def ct_prepare
    # Ensure we have an operation. 
    if @operation == nil
      raise UserError, "No operation specified"
    end

    # Load the configuration file. 
    if @config_file == nil
      raise UserError, "No config file specified"
    elsif File.exist?(@config_file)
      @config = Properties.new
      @config.load(@config_file)
    else
      raise UserError, "Config file not found: " + @config_file
    end

    # If there are options, parse them. 
    @args = Properties.new
    if @in_params 
       @args.setPropertiesFromList(@in_params, ";") 
    end
  end

  # Execute the operation with appropriate error checking. 
  def ct_exec
    # Print trace information. 
    trace "Operation: #{@operation}\n"
    trace "Config: #{@config_file}\n"
    trace "Args: #{@args}\n"

    # Dispatch on the command. 
    case @operation
    when "prepare" 
      plugin_prepare
    when "release" 
      plugin_release
    when "configure" 
      plugin_configure
    when "install" 
      plugin_install
    when "uninstall" 
      plugin_uninstall
    when "provision" 
      plugin_provision
    when "online" 
      plugin_online
    when "offline" 
      plugin_offline
    when "offline-deferred" 
      plugin_offline_deferred
    when "halt" 
      plugin_halt
    when "kill" 
      plugin_kill
    when "flush" 
      plugin_flush
    when "waitevent" 
      plugin_waitevent
    when "setrole" 
      plugin_setrole
    when "capabilities" 
      plugin_capabilities
    when "status" 
      plugin_status
    else
      raise UserError, "Unknown operation: #{@operation}"
    end
  end

  # Read Open Replicator 
  # goes online. 
  def plugin_prepare
    raise SystemError, "Not implemented yet"
  end

  # Prepare the plugin for commands from open replicator.  This is issued 
  # when the replicator loads the script plugin. 
  def plugin_prepare
    raise SystemError, "Not implemented yet"
  end

  # Release resources used to process commands from open replicator. 
  def plugin_release
    raise SystemError, "Not implemented yet"
  end

  # Install scripts and other items necessary to ensure that replication
  # can occur properly.  This is called at system configuration time. 
  def plugin_install
    raise SystemError, "Not implemented yet"
  end

  # Remove infrastructure used to support replication.  This cleans up
  # back to a default state. 
  def plugin_uninstall
    raise SystemError, "Not implemented yet"
  end

  # Provision from another database. 
  def plugin_provision
    raise SystemError, "Not implemented yet"
  end

  # Turns the replicator online, which starts replication. 
  def plugin_online
    raise SystemError, "Not implemented yet"
  end

  # Turns the replicator offline, which stops replication. 
  def plugin_offline
    raise SystemError, "Not implemented yet"
  end

  # Halts the replicator gracefully.  
  def plugin_halt
    raise SystemError, "Not implemented yet"
  end

  # Terminates the replicator immediately.  This should not be used!!!
  def plugin_kill
    raise SystemError, "Not implemented yet"
  end

  # Flushes master events into replication stream. 
  def plugin_flush
    raise SystemError, "Not implemented yet"
  end

  # Waits for an event to arrive on the slave. 
  def plugin_waitevent
    raise SystemError, "Not implemented yet"
  end

  # Set the replicator role. 
  def plugin_setrole
    raise SystemError, "Not implemented yet"
  end

  # Return the replicator capabilities. 
  def plugin_capabilities
    raise SystemError, "Not implemented yet"
  end

  # Return status of replication. 
  def plugin_status
    raise SystemError, "Not implemented yet"
  end

  #######################################################################
  # UTILITY API FOR SUBCLASSES
  #######################################################################

  # Return the replicator home directory. 
  def getReplicatorHome
    return @replicator_home
  end

  # Print a normal message to the log. 
  def log(msg)
    puts "[#{DateTime.now}] #{msg}"
  end

  # Print a trace message.  Only appears if verbose login is set. 
  def trace(msg)
    if @options.verbose
      puts msg
    end
  end

  # Return a properties object containing the config file contents. 
  def getConfig
    @config
  end

  # Return a properties object containing the arguments to the 
  # command, if any. 
  def getArgs
    @args
  end

  # Execute a command with logging. Use:
  # a. ignore_fail = true/false (boolean),
  # b. ignore_fail = exit code to be tolerant to (Integer).
  def exec_cmd2(cmd, ignore_fail, silent=false)
    if not silent
      log("Executing command: " + cmd)  
    end
    if silent
      `#{cmd}`
    else
      successful = system(cmd)
    end
    rc = $?.exitstatus
    if not silent
      log("Success: #{successful} RC: #{rc}")
    end
    if rc != 0
      if (ignore_fail.kind_of?(Integer) && rc != ignore_fail) || ! ignore_fail 
        raise SystemError, "Command failed: " + cmd
      end
    end
  end

  # Convenience method for commands that are expected to succeed. 
  def exec_cmd(cmd)
    exec_cmd2(cmd, false)
  end

  # Adds root prefix (eg. "sudo") if one is defined.
  def get_root_cmd(cmd)
    command_prefix = @config.getProperty("postgresql.root.prefix")
    if command_prefix
      cmd = command_prefix + " " + cmd
    end
    return cmd
  end

  # Execute a root command. 
  def exec_root_cmd2(cmd, ignore_fail)
    cmd = get_root_cmd(cmd)
    exec_cmd2(cmd, ignore_fail)
  end

  # Execute a root command. 
  def exec_root_cmd(cmd)
    exec_root_cmd2(cmd, false)
  end

  # Write a tag file. 
  def tag_file_write(file, tag)
    out = File.open(file, "w")
    trace("Writing tag file: name=#{file} tag=#{tag}")
    out.puts("#{DateTime.now}: #{tag}")
    out.close
  end

  # Remove a tag file. 
  def tag_file_remove(file)
    File.delete(file)
  end

  # Return true if tag file exists. 
  def tag_file_exist?(file)
    File.exist?(file)
  end
end
