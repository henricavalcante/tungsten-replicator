#!/usr/bin/env ruby
#
# This script outputs information about the Tungsten environment
#

require "#{File.dirname(__FILE__)}/../../lib/ruby/tungsten"

class TungstenEnvironment
  include TungstenScript
  
  def main
    pp TI.dataservices()
    pp TI.default_dataservice()
    TI.dataservices().each{
      |dsname|
      pp TI.topology(dsname).to_hash()
      pp TI.status(dsname).to_hash()
    }
  end
  
  def configure
    require_installed_directory?(false)
    
    add_command(:install, {
      :help => "Create an entry"
    })
    
    add_command(:delete, {
      :help => "Delete an entry"
    })
    
    add_command(:list, {
      :default => true,
      :help => "List all entries"
    })
    
    add_option(:test, {
      :on => "--test",
      :help => "A test option",
      :default => false
    })
    
    add_option(:boolean, {
      :on => "--boolean String",
      :parse => method(:parse_boolean_option),
      :help => "This will be parsed to be either true or false",
      :default => false
    })
    
    add_option(:integer, {
      :on => "--integer String",
      :parse => method(:parse_integer_option),
      :help => "This will be parsed as an integer",
      :default => 10
    })
    
    add_option(:latency, {
      :on => "--latency String",
      :help => "The maximum allowed latency",
      :default => 60
    }) {|val|
      if val == "false"
        false
      else
        val.to_i()
      end
    }
  end
  
  def validate
    super()
    
    # Optional logic to test the options
  end
  
  def script_name
    "tungsten_env.sh"
  end
  
  self.new().run()
end