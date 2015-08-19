#!/usr/bin/env ruby 
#
# TUNGSTEN SCALE-OUT STACK
# Copyright (C) 2009 Continuent, Inc.
# All rights reserved
#

# == Synopsis 
#   Automatic configuration script for Continuent Tungsten.  Run this 
#   script after unpacking the Continuent Tungsten release.  You can 
#   rerun configuration using the tungsten.cfg only using the -b option. 
#
# == Author
#   Robert Hodges
#   Jeff Mace
#
# == Copyright
#   Copyright (c) 2009 Continuent, Inc.  All rights reserved.
#   http://www.continuent.com

require 'configurator.rb'

# Trap control-C interrupt. 
trap("INT") {
  puts("")
  puts("Configuration interrupted")
  exit 1
}

# Create and run the configurator. 
Configurator.instance.run()
