DEFAULTS = "__defaults__"
REPL_RMI_PORT = "repl_rmi_port"
MGR_RMI_PORT = "mgr_rmi_port"
HOST_ENABLE_REPLICATOR = "host_enable_replicator"
HOST_ENABLE_MANAGER = "host_enable_manager"
HOST_ENABLE_CONNECTOR = "host_enable_connector"
MGR_API = "mgr_api"
MGR_API_PORT = "mgr_api_port"
MGR_API_ADDRESS = "mgr_api_address"
DEPLOYMENT_HOST = "deployment_host"
DEPLOYMENT_DATASERVICE = "deployment_dataservice"
DEPLOYMENT_SERVICE = "service_name"
HOSTS = "hosts"
DATASERVICES = "dataservices"
DATASOURCES = "datasources"
MANAGERS = "managers"
REPL_SERVICES = "repl_services"
CONNECTORS = "connectors"
CURRENT_RELEASE_DIRECTORY = "tungsten"

class MessageError < StandardError
end

class CommandError < StandardError
  attr_reader :command, :rc, :result, :errors
  
  def initialize(command, rc, result, errors="")
    @command = command
    @rc = rc
    @result = result
    @errors = errors
    
    super(build_message())
  end
  
  def build_message
    if @errors == ""
      errors = "No STDERR"
    else
      errors = "Errors: #{@errors}"
    end
    
    "Failed: #{command}, RC: #{rc}, Result: #{result}, #{errors}"
  end
end

class RemoteCommandError < CommandError
  attr_reader :user, :host
  
  def initialize(user, host, command, rc, result, errors = "")
    @user = user
    @host = host
    super(command, rc, result, errors)
  end
  
  def build_message
    if @errors == ""
      errors = "No STDERR"
    else
      errors = "Errors: #{errors}"
    end
    
    "Failed: #{command}, RC: #{rc}, Result: #{result}, #{errors}"
  end
end

class IgnoreError < StandardError
end

# Disable guessing by the OptionParser
class OptionParser
  def stack
    @stack
  end
  
  class OptionMap < Hash
  end
  
  module Completion
    def complete(key, icase = false, pat = nil)
    end

    def convert(opt = nil, val = nil, *)
      val
    end
  end
end

# Define an additional Logger level
class Logger
  NOTICE = 1.5
end

class String
  if RUBY_VERSION < "1.9"
    def getbyte(index)
      self[index]
    end
  end
end

# Apply sorting when building a JSON string
module JSON
  module Pure
    module Generator
      module GeneratorMethods
        module Hash
          private

          def json_transform(state)
            valid_keys = 0
    
            delim = ','
            delim << state.object_nl
            result = '{'
            result << state.object_nl
            depth = state.depth += 1
            first = true
            indent = !state.object_nl.empty?
            keys().sort{ |a, b| a.to_s() <=> b.to_s() }.each{|key|
              value = self[key]
              json = value.to_json(state)
              if json == ""
                next
              end
              valid_keys = valid_keys+1
      
              result << delim unless first
              result << state.indent * depth if indent
              result << key.to_s.to_json(state)
              result << state.space_before
              result << ':'
              result << state.space
              result << json
              first = false
            }
            depth = state.depth -= 1
            result << state.object_nl
            result << state.indent * depth if indent if indent
            result << '}'
    
            if valid_keys == 0 && depth != 0
              return ""
            end
    
            result
          end
        end

        module Array
          private

          def json_transform(state)
            valid_keys = 0
    
            delim = ','
            delim << state.array_nl
            result = '['
            result << state.array_nl
            depth = state.depth += 1
            first = true
            indent = !state.array_nl.empty?
            each { |value|
              json = value.to_json(state)
              if json == ""
                next
              end
              valid_keys = valid_keys+1
      
              result << delim unless first
              result << state.indent * depth if indent
              result << json
              first = false
            }
            depth = state.depth -= 1
            result << state.array_nl
            result << state.indent * depth if indent
            result << ']'
    
            if valid_keys == 0 && depth != 0
              return ""
            end
    
            result
          end
        end
      end
    end
  end
end