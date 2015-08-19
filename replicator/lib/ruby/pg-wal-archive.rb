#!/usr/bin/env ruby 
#
# TUNGSTEN SCALE-OUT STACK
# Copyright (C) 2010 Continuent, Inc.
# All rights reserved

# == Synopsis 
#   Script to copy PostgreSQL WAL files.  
#
# == Examples
#   TBA
#
# == Usage 
#   Current working directory must be the tungsten-replicator or paths
#   cannot be located. 
#
#   pg-wal-copy.rb wal_filepath wal_filename
#
# == Options
#   (none)
#
# == Author
#   Robert Hodges, Linas Virbalas, based on an example from Simon Riggs
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
config_file=replicator_home + "/conf/pg-wal/pg-wal.properties"
archiving_active=replicator_home + "/conf/pg-wal/archiving_active"
online=replicator_home + "/conf/pg-wal/online"
wal_archived=replicator_home + "/conf/pg-wal/wal_archived"
replicator_pid_file = replicator_home + "/var/treplicator.pid"

# Quit if Replicator was stopped.
if ! File.exist?(replicator_pid_file)
  puts "Replicator PID file not found, archiving is not enabled @ #{ARGV[2]}"
  # Error code, so PostgreSQL would retry this WAL when master returns ONLINE.
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
role = config.getProperty("postgresql.role")

if role == nil
  raise UserError, "Config file does not exist or is missing values: " + config_file
elsif role != "master"
  puts "Role is not master, archiving is not enabled"
elsif ! File.exists?(online)
  puts "Replication is not online, archiving is not enabled @ #{ARGV[2]}"
  # Error code, so PostgreSQL would retry this WAL when master returns ONLINE.
  exit 4
else 
  # Record name of the archive log so we know it has been archived. 
  out = File.open(wal_archived, "w")  
  out.puts("#{ARGV[2]}")
  out.close
  # Record time when it was archived - used for calculating latency.
  wal_archived_when = replicator_home + "/conf/pg-wal/when/#{ARGV[2]}.when"
  out = File.open(wal_archived_when, "w")
  out.puts("#{Time.now}")
  out.close

  # Copy WAL file to local "outbox" folders of each of the slaves.
  oldest_WAL = nil
  rc_array = []
  Dir.glob(archiving_active + ".*") { |standby_file|
    # The archive_active.{host} file tells us where to send data. 
    archive_props = Properties.new
    archive_props.load(standby_file)
    standby_host     = archive_props.getProperty("postgresql.standby.host")
    standby_archive  = archive_props.getProperty("postgresql.standby.archive")
    
    # Where to store WAL files to be sent to slaves?
    outbox_root      = "#{standby_archive}/outbox/"

    slave_folder = File.basename(standby_file)
    slave_outbox = "#{outbox_root}#{slave_folder}/"
    slave_outbox_incomplete = "#{outbox_root}#{slave_folder}/incomplete/"

    # Ensure that slave has a folder for it.
    Dir.mkdir(outbox_root) unless File.directory?(outbox_root)
    Dir.mkdir(slave_outbox) unless File.directory?(slave_outbox)
    Dir.mkdir(slave_outbox_incomplete) unless File.directory?(slave_outbox_incomplete)

    # Copy WAL file into incomplete outbox folder.
    cmd = "cp #{ARGV[1]} #{slave_outbox_incomplete}"
    puts "Copying WAL file to outbox/incomplete: #{cmd}"
    system(cmd)
    rc = $?.exitstatus
    # Check status. 
    if rc != 0
      puts "Creating a local copy of WAL file failed. Exiting..."
      exit rc
    end
    # Move WAL file from "incomplete" folder into outbox.
    cmd = "mv #{slave_outbox_incomplete}#{ARGV[2]} #{slave_outbox}"
    puts "Moving from incomplete folder to outbox: #{cmd}"
    system(cmd)
    rc = $?.exitstatus
    # Check status. 
    if rc != 0
      puts "Moving local copy of WAL file to outbox failed. Exiting..."
      exit rc
    end
    
    # Determine the oldest WAL file of this outbox.
    outbox = Dir.glob(slave_outbox + "*").sort
    if outbox != nil && outbox.length > 0
      first_WAL = File.basename(outbox.at(0))
      if oldest_WAL == nil || first_WAL < oldest_WAL
        oldest_WAL = first_WAL
      end
      puts "Oldest WAL of all outboxes: " + oldest_WAL
    end

    # Ensure that the WAL rsync deamon is running for this slave.
    pid_file = replicator_home + "/var/pg-wal-archive-send-#{slave_folder}.pid"
    if File.exist?(pid_file) 
      # Check validity of pid by looking for process. 
      pid = `cat #{pid_file}`.chomp
      if `uname -s` =~ /SunOS/
        ps = "/usr/ucb/ps"
      else
        ps = "ps"
      end
      ps_cmd = "#{ps} #{pid} > /dev/null"
      if ! system(ps_cmd) 
        puts "Rsync daemon is defunct; removing PID file: " + pid
        File.delete(pid_file)
      end
    end

    if not File.exist?(pid_file)
      puts "PID file of sender daemon not found: #{pid_file}"
      puts "Starting pg-wal-archive-send for #{slave_folder}..."
      cmd = replicator_home + "/bin/pg/pg-wal-archive-send #{slave_outbox} &"
      puts "Executing: #{cmd}"
      if not system(cmd)
        puts "ERROR: failed to start pg-wal-archive-send"
      end
    end
  }
  
  # Cleanup obsolete "<WAL>.when" files.
  if oldest_WAL != nil
    when_files = Dir.glob(replicator_home + "/conf/pg-wal/when/*").sort
    when_files.each_with_index { |when_file, i|
      when_filename = File.basename(when_file)
      if when_filename < oldest_WAL
        puts "Removing obsolete WAL generation time file: #{when_filename}"
        File.delete(when_file)
      end
    }
  end
end

# No error. 
exit 0
