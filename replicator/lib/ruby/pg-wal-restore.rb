#!/usr/bin/env ruby 
#
# TUNGSTEN SCALE-OUT STACK
# Copyright (C) 2009 Continuent, Inc.
# All rights reserved

# == Synopsis 
#   Script to restore PostgreSQL WAL files on standby.  Called using 
#   command specified in recovery.conf. 
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
#   Robert Hodges, based on an example from Simon Riggs
#
# == Copyright
#   Copyright (c) 2009 Continuent, Inc.  All rights reserved.
#   http://www.continuent.com

require 'fileutils'
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
wal_restored=replicator_home + "/conf/pg-wal/wal_restored"
wal_restored_when=replicator_home + "/conf/pg-wal/wal_restored.when"
replicator_pid_file = replicator_home + "/var/treplicator.pid"
going_online = replicator_home + "/conf/pg-wal/going_online"
online = replicator_home + "/conf/pg-wal/online"

puts "Restore invoked..."
puts "ARGV[1] = #{ARGV[1]}"
puts "ARGV[2] = #{ARGV[2]}"
puts "ARGV[3] = #{ARGV[3]}"

# Don't restore if Replicator is not ONLINE.
if File.exists?(going_online)
  puts "Replicator is going ONLINE"
elsif ! File.exists?(online)
  puts "WARNING: Replicator is not online, but WAL file is received!"
end

# Load current configuration file. 
if File.exist?(config_file)
  config = Properties.new
  config.load(config_file)
else
  raise ExecError, "Config file not found: " + config_file
end

# Ensure properties are present. 
role        = config.getProperty("postgresql.role");
pg_standby  = config.getProperty("postgresql.pg_standby")
pg_standby_trigger = config.getProperty("postgresql.pg_standby.trigger")
pg_archive  = config.getProperty("postgresql.archive")

if role == nil
  raise UserError, "Config file does not exist or is missing values: " + config_file
else
  cmd = "#{pg_standby} -l -d -s 2 -t #{pg_standby_trigger} #{pg_archive} #{ARGV[1]} #{ARGV[2]} #{ARGV[3]} 2>>standby.log"
  puts "cmd = #{cmd}"
  rc = system(cmd)

  # Note that we have restored the file. 
  out = File.open(wal_restored, "w")
  out.puts("#{ARGV[1]}")
  out.close
  
  # Determine time of when this file was archived.
  wal_archived_when = replicator_home + "/conf/pg-wal/when/#{ARGV[1]}.when"
  if File.exist?(wal_archived_when)
    FileUtils.move(wal_archived_when, wal_restored_when)
  else
    puts "WARNING: time file not found, latency calculation might get broken: #{wal_archived_when}"
  end

  # All done. 
  exit rc
end
