class FirewallCommand
  include ConfigureCommand
  include RemoteCommand
  include ClusterCommandModule

  def skip_prompts?
    true
  end
  
  def display_alive_thread?
    false
  end

  def parsed_options?(arguments)
    @start_listeners = false
    @stop_listeners = false
    arguments = super(arguments)

    if display_help?() && !display_preview?()
      return arguments
    end
  
    # Define extra option to load event.  
    opts=OptionParser.new
    
    opts.on("--start-listeners")            { @start_listeners = true}
    opts.on("--stop-listeners")             { @stop_listeners = true}
    
    opts = Configurator.instance.run_option_parser(opts, arguments)

    # Return options. 
    opts
  end

  def output_command_usage
    super()
  end
 
  def run
    if @start_listeners == true
      if RUBY_VERSION < "1.9"
        exit if fork
        Process.setsid
        exit if fork
        Dir.chdir "/" 
        STDIN.reopen "/dev/null"
        STDOUT.reopen "/dev/null", "a" 
        STDERR.reopen "/dev/null", "a" 
      else
        Process.daemon
      end
      
      return FirewallCommand.start_listeners(@config)
    elsif @stop_listeners == true
      return FirewallCommand.stop_listeners(@config)
    else
      first_host=true
      get_deployment_configurations().each{
        |cfg|
        unless first_host == true
          puts ""
        end
        puts "To " + cfg.getProperty(HOST)
        Configurator.instance.write_divider(nil)
        if Configurator.instance.is_enterprise?()
          output_usage_line("From application servers", cfg.getProperty(PORTS_FOR_USERS).sort().join(', '))
          output_usage_line("From connector servers", cfg.getProperty(PORTS_FOR_CONNECTORS).sort().join(', '))
          output_usage_line("From database servers", (cfg.getProperty(PORTS_FOR_MANAGERS)+cfg.getProperty(PORTS_FOR_REPLICATORS)).sort().join(', '))
        else
          output_usage_line("From database servers", (cfg.getProperty(PORTS_FOR_REPLICATORS)).sort().join(', '))
        end
      
        first_host=false
      }

      return is_valid?()
    end
  end
  
  def self.start_listeners(cfg)
    @@threads ||= []
    @@servers ||= []
    listen_ports = (cfg.getPropertyOr(PORTS_FOR_USERS, []) + cfg.getPropertyOr(PORTS_FOR_CONNECTORS, []) + cfg.getPropertyOr(PORTS_FOR_MANAGERS, []) + cfg.getPropertyOr(PORTS_FOR_REPLICATORS, [])).uniq()

    Dir.glob("#{cfg.getProperty(TEMP_DIRECTORY)}/tungsten.listener.#{cfg.getProperty(DEPLOYMENT_HOST)}.*") {
      |fname|
      pid = nil
      
      File.open(fname, "r") {
        |f|
        pid = f.readlines().join().strip()
      }
      
      if pid == nil || pid == Process.pid().to_s()
        next
      end 
      
      begin
        cmd_result("kill -9 #{pid} 2>/dev/null", true)
        FileUtils.rm_f(fname)
      rescue
      end
    }
    
    for port in listen_ports
      @@threads << Thread.new(port){
        |p|
        begin
          server = TCPServer.new(p)
          @@servers << server
        
          listener_filename = "#{cfg.getProperty(TEMP_DIRECTORY)}/tungsten.listener.#{cfg.getProperty(DEPLOYMENT_HOST)}.#{p.to_s}.pid"
          File.open(listener_filename, 'w') do |file|
            file.printf Process.pid().to_s()
          end
        
          loop do
            Thread.start(server.accept) do |client| 
              client.puts "#{cfg.getProperty(HOST)}:#{p}" 
              client.close 
            end 
          end
        rescue
        end
      } 
    end
    
    begin
      Timeout.timeout(120) do
        @@threads.each{ 
          |t|
          t.join()
        }
      end
    rescue Timeout::Error
    end
  end
  
  def self.stop_listeners(cfg)
    @@servers ||= []
    
    @@servers.each{
      |s|
      s.close()
    }
    
    Dir.glob("#{cfg.getProperty(TEMP_DIRECTORY)}/tungsten.listener.#{cfg.getProperty(DEPLOYMENT_HOST)}.*") {
      |fname|
      pid = nil
      
      File.open(fname, "r") {
        |f|
        pid = f.readlines().join().strip()
      }
      
      if pid == nil || pid == Process.pid().to_s()
        FileUtils.rm_f(fname)
        next
      end 
      
      begin
        cmd_result("kill -9 #{pid} 2>/dev/null", true)
        FileUtils.rm_f(fname)
      rescue
      end
    }
    
    @@threads = []
    @@servers = []
  end

  def self.get_command_name
    'firewall'
  end
  
  def self.get_command_description
    "Print firewall information for the listed dataservices"
  end
end