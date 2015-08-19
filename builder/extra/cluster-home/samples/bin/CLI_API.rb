#!/usr/bin/env ruby

#
# Sample command line interface to the Manager API
#

require "./cluster-home/lib/ruby/tungsten"
require 'pp'

class TungstenEnvironment

    include TungstenScript
    include TungstenAPI

    #
    # Initializes the cctrl object
    # and creates a command line option for every API call available
    #
    def configure
        super()
    
        require_installed_directory?(false)
        @cctrl = TungstenDataserviceManager.new(nil)
        apis = @cctrl.to_hash
        # 
        # loops through the API calls and creates an option for each one
        #
        apis.each { |name,api| 
            add_option(name.to_sym, {
                :on => "--#{name}",
                :help => api['help'],
                :default => false
                })
        }
        #
        # Add an option to define to which datasource we want to apply the call
        #
        add_option(:target, {
            :on => "--target String",
            :help => "defines the datasource to which we apply the api call ",
            :default => nil
        })

        #
        # Add an option to show the outline of the returning object
        #
        add_option(:structure, {
            :on => "--structure",
            :help => "Prints an outline of the object structure for each call",
            :default => false
        })
        
        #
        # Add an option to define the data service
        #
        add_option(:service, {
            :on => "--service String",
            :help => "defines the service name for the API call",
            :default => nil
        })

        #
        # Add an option to define the manager API server
        #
        add_option(:api_server, {
            :on => "--api-server String",
            :help => "defines the server to ask for the API",
            :default => 'localhost:8090'
        })

        #
        # We can instead list all calls
        #
        add_option(:list, {
            :on => "--list",
            :help => "Shows the api list",
            :default => false
        })
    end

    #
    # Prints selected fields from a cluster status hash
    #
    def display_cluster (call_name, hash)
        root = hash["outputPayload"]
        unless hash["outputPayload"]
            raise "the response from #{call_name} does not contain 'outputPayload'}"
        end
        %w(replicators dataSources).each { |item|
            unless root[item]
                raise "the response from #{call_name} does not contain 'outputPayload/#{item}'}"
            end
        }
        status_tree = {}
        member_count=0
        root['dataSources'].each { |ds_name, ds|
            status_tree[ds_name] = {}
            unless root['replicators'][ds_name]
                raise "datasource #{ds_name} has no replicator in call to #{call_name}"
            end
            status_tree[ds_name]['state']    = root['dataSources'][ds_name]['state']
            status_tree[ds_name]['role']     = root['dataSources'][ds_name]['role']
            if status_tree[ds_name]['role'] == 'master'
                status_tree[ds_name]['role'] = "*master"
            end
            status_tree[ds_name]['progress'] = root['replicators'][ds_name]['appliedLastSeqno']
            status_tree[ds_name]['latency']  = root['dataSources'][ds_name]['appliedLatency']
        }
        policy = root['policyManagerMode']
        coordinator = root['coordinator']
        service_name = root['name']
        puts service_name
        puts "coordinator: #{coordinator} - policy: #{policy}"
        status_tree.sort.each { |ds_name, ds|
            puts sprintf "\t%-30s (%-8s:%-7s) - progress: %6d [%5.3f]", 
                ds_name,
                ds['role'],
                ds['state'],
                ds['progress'],
                ds['latency']
        }
    end

    #
    # prints the outline of a hash structure
    #
    # If environment variable ALL_FIELDS is set, then it prints all fields,
    # otherwise it only prints the fields at the top leven and the ones that have nested contents
    #
    # IF the environment variable SHOW_BRACES is set, then it also prints braces (or brackets) 
    # around the inner structure
    #

    def show_structure(hash, indent = 0)
        line_prefix = ''
        indent.times { line_prefix += ' ' }
        hash.each { |k,v|
            nested = false
            key_suffix = ''
            closing = ''
            if v.is_a? Hash 
                nested = true
                key_suffix= ' {' if ENV["SHOW_BRACES"]
                closing='}'  if ENV["SHOW_BRACES"]
            end
            if v.is_a? Array
                nested = true
                key_suffix= ' ['
                closing = ']'
            end
            if indent == 0 or nested or ENV["ALL_FIELDS"]
                puts "#{line_prefix} #{k}#{key_suffix}"
            end
            if nested
                show_structure(v, indent+2)
                puts " #{line_prefix}#{closing}" if ENV["SHOW_BRACES"]
            end
        }
    end

    #
    # Main app
    #
    def main
        # sets the defaults
        #pp TI.dataservices()
        #topology = TI.status('chicago')
        #pp topology.datasources()
        #puts '<'
        #pp TI
        #puts '>'

        service = opt(:service)
        service ||= 'chicago'
        api_server = opt(:api_server)
        api_server ||= 'localhost:8090'
        @cctrl.set_server(api_server)

        if opt(:list)
            # displays the list of available API calls
            @cctrl.list()
        else
            # 
            # runs the requested API calls
            #
            apis = @cctrl.to_hash
            target = opt(:target)
            apis.each { |name,api|
                command = name
                if opt(command.to_sym) # if the option corresponding to the API name was selected
                    if target
                        service += "/#{target}"
                    end
                    cluster_hash = @cctrl.call_default(service, command)
                    if opt(:structure)
                        show_structure(cluster_hash)
                        puts ''
                    end
                    if cluster_hash['httpStatus'] != 200
                        puts "*** #{cluster_hash['message']} (#{cluster_hash['httpStatus']})"
                    else
                        if api["type"] == :get 
                            if opt(:status)
                                display_cluster(command, cluster_hash)
                            else
                                pp cluster_hash
                            end
                        else
                            puts "OK: #{cluster_hash['message']} (#{cluster_hash['httpStatus']})"
                        end
                    end
                end
          }     
      end

  end
  
  def script_name
    "CLI_API.rb"
  end

  self.new().run()
end

