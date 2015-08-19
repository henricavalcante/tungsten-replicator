#!/usr/bin/env ruby

require "./cluster-home/lib/ruby/tungsten"
require 'pp'

class TungstenEnvironment

  include TungstenScript
  include TungstenAPI

  def configure
    #require_installed_directory?(false)
    super()
  end


  def main
      cctrl = TungstenReplicator.new('/opt/continuent/cookbook_test/tungsten/tungsten-replicator/bin')
      cctrl.list()
      puts ''
      json_obj = cctrl.get( 'status', nil)
      # json_obj = cctrl.get( 'properties', nil)
      json_obj = cctrl.get( 'services', nil)
      pp json_obj
      json_obj = cctrl.get( 'thl_headers', nil, nil, '-low 10 -high 13')
      pp json_obj
  end
  
  def script_name
    "tungsten_replicator_API_test.rb"
  end

  self.new().run()
end

