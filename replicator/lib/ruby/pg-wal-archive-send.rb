#!/usr/bin/env ruby 
#
# TUNGSTEN SCALE-OUT STACK
# Copyright (C) 2010 Continuent, Inc.
# All rights reserved

# == Synopsis 
#   Script to send PostgreSQL WAL files to slave machine.
#
# == Examples
#   TBA
#
# == Usage 
#   Current working directory must be the tungsten-replicator or paths
#   cannot be located. 
#
#   pg-wal-archive-send slave_outbox
#
# == Options
#   (none)
#
# == Author
#   Linas Virbalas, Robert Hodges, based on an example from Simon Riggs
#
# == Copyright
#   Copyright (c) 2010 Continuent, Inc.  All rights reserved.
#   http://www.continuent.com

require 'tungsten/properties'
require 'tungsten/exception'

# Trap control-C interrupt. 
trap("INT") {
  puts("")
  puts("Operation interrupted")
  exit 1
}

# Set file names. 
replicator_home=ENV['REPLICATOR_HOME']
config_file = replicator_home + "/conf/pg-wal/pg-wal.properties"
online = replicator_home + "/conf/pg-wal/online"
wal_archived = replicator_home + "/conf/pg-wal/wal_archived"
slave_outbox = ARGV[1]
slave_outbox_name = File.basename(slave_outbox)
archiving_active_props=replicator_home + "/conf/pg-wal/" + slave_outbox_name
wal_sent=replicator_home + "/conf/pg-wal/wal_sent_#{slave_outbox_name}"
pid_file = replicator_home + "/var/pg-wal-archive-send-#{slave_outbox_name}.pid"
replicator_pid_file = replicator_home + "/var/treplicator.pid"

# Check whether there is no other process for this slave running.
if File.exist?(pid_file)
  puts "ERROR: process seams to be already running, because PID file exists:"
  puts "#{pid_file}"
  exit 2
else
  out = File.open(pid_file, "w")
  out.puts "#{Process.pid}"
  out.close
end

begin

  puts "Entering continuous scan mode for outbox of a slave: #{slave_outbox}"

  loop do
    
    # Quit if Replicator was stopped.
    if ! File.exist?(replicator_pid_file)
      puts "Replicator PID file not found, stopping archive sending"
      exit 5
    end

    # Load current configuration file. 
    if File.exist?(config_file)
      config = Properties.new
      config.load(config_file)
    else
      raise ExecError, "Config file not found: " + config_file
    end

    # Ensure properties are present. 
    role = config.getProperty("postgresql.role");

    if role == nil
      raise UserError, "Config file does not exist or is missing values: " + config_file
    elsif role != "master"
      puts "Role is not master, stopping archive sending"
      exit 3
    elsif ! File.exists?(online)
      puts "Replication is not online, stopping archive sending"
      exit 4
    else 
      # Rsync to standby host.
      outbox = Dir.glob(slave_outbox + "*").sort
      outbox.each_with_index { |standby_file, i|
        # Skip "incomplete/" and any other folders.
        next if File.directory? standby_file
      
        standby_filename = File.basename(standby_file)
        puts("Processing #{standby_file} (#{File.size(standby_file)}B):")
        # The archive_active.{host} file tells us where to send data.
        archive_props = Properties.new
        archive_props.load(archiving_active_props)
        standby_host     = archive_props.getProperty("postgresql.standby.host")
        standby_archive  = archive_props.getProperty("postgresql.standby.archive")
        
        # Update WAL's time on standby, so it could calculate latency.
        wal_archived_when = replicator_home + "/conf/pg-wal/when/#{standby_filename}.when"
        cmd = "rsync -cz #{wal_archived_when} #{standby_host}:#{wal_archived_when}"
        puts "Recording WAL generation time: #{cmd}"
        system(cmd)
        
        # Send the actual WAL file.
        cmd = "rsync -cz #{standby_file} #{standby_host}:#{standby_archive}/#{standby_filename}"
        puts "Executing archive command: #{cmd}"
        system(cmd)
        rc = $?.exitstatus

        # Record name of the archive log so we know it has been sent to the specific slave. 
        out = File.open(wal_sent, "w")
        out.puts "#{standby_filename}"
        out.close

        # Update so standby knows file is received. 
        cmd = "rsync -cz #{wal_sent} #{standby_host}:#{wal_archived}"
        puts "Recording status: #{cmd}"
        system(cmd)
        
        # Check status. 
        if rc != 0
          puts "rsync of WAL failed: #{rc}"
          # Stop archiving to this slave, thus accumulating WAL files, if we can't deliver them.
          puts "ERROR: Stopping archiving for #{standby_host} as the WAL files can't be delivered"
          File.delete(archiving_active_props)
          puts "Fix the underlying problem and reprovision the slave #{standby_host}"
          puts "Exiting..."
          exit rc
        else
          # Delete the sent file.
          puts "Deleting: #{standby_file} (#{File.size(standby_file)}B)"
          File.delete(standby_file)
        end
      }
      
    end
    
    sleep 10
  end

  # No error. 
  exit 0

ensure

  File.delete(pid_file)
  puts "Removed PID file."

end