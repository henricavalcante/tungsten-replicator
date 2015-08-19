libdir = File.dirname(__FILE__)
$LOAD_PATH.unshift(libdir) unless $LOAD_PATH.include?(libdir)

unless Object.const_defined?(:JSON)
  begin
    # Attempt to load the json.rb file if available
    require 'json'
  rescue LoadError
    # Look for a json ruby gem
    require 'rubygems'
    begin
      require 'json_pure'
    rescue LoadError
      begin
        require 'json-ruby'
      rescue LoadError
        require 'json'
      end
    end
  end
end
unless Object.const_defined?(:JSON)
  raise "Could not load JSON; did you install one of json_pure, json-ruby, or the C-based json library?"
end

require "date"
require "fileutils"
require 'logger'
require "optparse"
require "pathname"
require 'pp'
require "singleton"
require "tempfile"
require 'timeout'
require "tungsten/common"
require "tungsten/exec"
require "tungsten/install"
require "tungsten/datasource"
require "tungsten/properties"
require "tungsten/script"
require "tungsten/api"
require "tungsten/status"
require "tungsten/util"

# Setup default instances of the TungstenUtil and TungstenInstall classes
# Access these constants anywhere in the code
# TU.info(msg)
TU = TungstenUtil.instance()

begin
  # Intialize a default TungstenInstall object that uses TU.get_base_path()
  # as the default. If --directory is found, that path is used instead.
  
  install_base_path = TU.get_base_path()
  unless TungstenInstall.is_installed?(TU.get_base_path())
    if ENV.has_key?("CONTINUENT_ROOT")
      install_base_path = ENV["CONTINUENT_ROOT"] + "/tungsten"
    end
  end
  
  opts = OptionParser.new
  opts.on("--directory String") {|val| install_base_path = "#{val}/tungsten"}
  TU.run_option_parser(opts)

  TI = TungstenInstall.get(install_base_path)
rescue => e
  TI = nil
end
