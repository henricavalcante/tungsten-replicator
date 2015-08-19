#!/usr/bin/env ruby 
#
# TUNGSTEN SCALE-OUT STACK
# Copyright (C) 2009 Continuent, Inc.
# All rights reserved

# == Synopsis 
#   Open replicator script for PostgreSQL WAL shipping solution. 
#
# == Examples
#   TBA
#
# == Usage 
#   pg-wal-wrapper.rb [options]
#
# == Options
#   -o, --operation     Operation to perform
#   -c, --config        Configuration file
#   -a, --args          Arguments as a set of name-value pairs
#   -h, --help          Displays help message
#   -V, --verbose       Verbose output
#
# == Author
#   Robert Hodges
#
# == Copyright
#   Copyright (c) 2009 Continuent, Inc.  All rights reserved.
#   http://www.continuent.com

require 'pg-wal-plugin.rb'

# Trap control-C interrupt. 
trap("INT") {
  puts("")
  puts("Operation interrupted")
  exit 1
}

# Create and run the WAL manager plugin. 
cmd = ARGV.shift
plugin = PgWalPlugin.new(cmd, ARGV)
plugin.run
