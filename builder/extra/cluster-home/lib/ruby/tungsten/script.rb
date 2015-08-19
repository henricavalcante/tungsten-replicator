module TungstenScript
  NAGIOS_OK=0
  NAGIOS_WARNING=1
  NAGIOS_CRITICAL=2
  
  def run
    begin
      prepare()
      main()
    rescue CommandError => e
      TU.debug(e)
    rescue => e
      TU.exception(e)
    end
    
    if TU.is_valid?()
      cleanup(0)
    else
      cleanup(1)
    end
  end
  
  def initialize
    # A tracking variable that will be set to true when the object is fully
    # initizlied
    @initialized = false
    
    # Does this script required to run against an installed Tungsten directory
    @require_installed_directory = true
    
    # Should unparsed arguments cause an error
    @allow_unparsed_arguments = false
    
    # Definition of each command that this script will support
    @command_definitions = {}
    
    # The command, if any, the script should run
    @command = nil
    
    # Definition of each option that this script is expecting as input
    @option_definitions = {}
    
    # The command-line arguments of all options that have been defined
    # This is used to identify duplicate arguments
    @option_definition_arguments = {}
    
    # The collected option values from the script input
    @options = {}
    
    # Parameters loaded from INI files to be parsed
    @ini_parameters = []
    
    TU.debug("Begin #{$0} #{ARGV.join(' ')}")
    
    begin
      configure()
      @option_definitions.each{
        |option_key,definition|
        if definition.has_key?(:default)
          opt(option_key, definition[:default])
        end
      }
    
      if TU.display_help?()
        display_help()
        cleanup(0)
      end
      
      # Load parameters from the available INI files
      load_ini_files()
      # Parse parameters loaded from the INI files and on command line
      parse_options()
      
      if @options[:autocomplete] == true
        display_autocomplete()
        cleanup(0)
      end
      
      unless TU.is_valid?()
        cleanup(1)
      end
      
      begin
        if script_log_path() != nil
          TU.set_log_path(script_log_path())
        end
      rescue => e
        TU.debug("Unable to set script log path")
        TU.debug(e)
      end
    
      TU.debug("Command: #{@command}")
      TU.debug("Options:")
      @options.each{
        |k,v|
        TU.debug("    #{k} => #{v}")
      }
    
      validate()
    
      unless TU.is_valid?()
        cleanup(1)
      end
      
      if @options[:validate] == true
        cleanup(0)
      end
    rescue => e
      TU.exception(e)
      cleanup(1)
    end
    
    @initialized = true
  end
  
  def prepare
  end
  
  def command
    @command
  end
  
  def configure
    add_option(:validate, {
      :on => "--validate",
      :default => false,
      :help => "Only run the script validation"
    })
    
    add_option(:autocomplete, {
      :on => "--autocomplete",
      :default => false,
      :hidden => true
    })
  end
  
  def opt(option_key, value = nil)
    if value != nil
      @options[option_key] = value
    end
    
    return @options[option_key]
  end
  
  # Set the value for option_key if it has not been set
  def opt_default(option_key, default_value)
    if opt(option_key) == nil
      opt(option_key, default_value)
    end
    
    opt(option_key)
  end
  
  def add_command(command_key, definition)
    begin
      command_key = command_key.to_sym()
      if @command_definitions.has_key?(command_key)
        raise "The #{command_key} command has already been defined"
      end

      if definition[:default] == true
        if @command != nil
          raise "Multiple commands have been specified as the default"
        end
        @command = command_key.to_s()
      end

      @command_definitions[command_key] = definition
    rescue => e
      TU.exception(e)
    end
  end
  
  def add_option(option_key, definition, &parse)
    begin
      option_key = option_key.to_sym()
      if @option_definitions.has_key?(option_key)
        raise "The #{option_key} option has already been defined"
      end

      unless definition[:on].is_a?(Array)
        definition[:on] = [definition[:on]]
      end
      
      # Check if the arguments for this option overlap with any other options
      definition[:on].each{
        |arg|
        
        arg = arg.split(" ").shift()
        if @option_definition_arguments.has_key?(arg)
          raise "The #{arg} argument is already defined for this script"
        end
        @option_definition_arguments[arg] = true
      }

      if parse != nil
        definition[:parse] = parse
      end

      @option_definitions[option_key] = definition
    rescue => e
      TU.exception(e)
    end
  end
  
  def set_option_default(option_key, default = nil)
    unless @option_definitions.has_key?(option_key)
      raise "Unable to set option default for #{:option_key.to_s()} because the option is not defined."
    end
    
    @option_definitions[option_key][:default] = default
  end
  
  def load_ini_files
    # If there is no script name then we cannot load INI files
    if script_name().to_s() == ""
      return
    end
    
    # Calculate the INI section name to use
    section_names = [script_name()]
    matches = script_name().to_s().match("tungsten_(.*)")
    if matches && matches.size() > 0
      script_ini_file = "#{matches[1]}.ini"
      section_names << matches[1]
    else
      script_ini_file = File.basename(script_name(), File.extname(script_name())) + ".ini"
      section_names << File.basename(script_name(), File.extname(script_name()))
    end
    
    load_ini_parameters("/etc/tungsten/scripts.ini", 
      section_names)
    
    if script_ini_file != nil
      load_ini_parameters("/etc/tungsten/#{script_ini_file}", 
        ["__anonymous__"] + section_names)
    end
    
    # Add these arguments to the beginging of the TungstenUtil stack
    # When the script processes command line options it will read these 
    # and then be overwritten by and command line options.
    TU.remaining_arguments = @ini_parameters + TU.remaining_arguments
  end
  
  # Convert the parsed INI contents into the command line argument style
  def load_ini_parameters(file, section_name)
    unless File.exists?(file)
      return
    end
    
    unless section_name.is_a?(Array)
      section_name = [section_name]
    end
    section_name.delete_if{|n| n.to_s() == ""}
    
    if section_name.size() == 0
      return
    end
    
    parameters = TU.parse_ini_file(file, false)
    section_name.each{
      |section|
      if section.to_s() == ""
        next
      end
      
      unless parameters.has_key?(section)
        next
      end
      
      parameters[section].each{
        |line|
        # Single character parameters get a single dash
        if line.length() == 1
          @ini_parameters << "-#{line}"
        else
          @ini_parameters << "--#{line}"
        end
      }
    }
  end
  
  def parse_options
    opts = OptionParser.new()
    
    @option_definitions.each{
      |option_key,definition|
      
      args = definition[:on]
      if definition[:aliases] != nil && definition[:aliases].is_a?(Array)
        definition[:aliases].each{
          |arg_alias|
          args << arg_alias
        }
      end
      
      opts.on(*args) {
        |val|
                
        if definition[:parse] != nil
          begin
            val = definition[:parse].call(val)
            
            unless val == nil
              opt(option_key, val)
            end
          rescue MessageError => me
            TU.error(me.message())
          end
        else  
          opt(option_key, val)
        end
      }
    }
    
    TU.run_option_parser(opts)
    
    if @command_definitions.size() > 0 && TU.remaining_arguments.size() > 0
      if TU.remaining_arguments[0] != nil
        if @command_definitions.has_key?(TU.remaining_arguments[0].to_sym())
          @command = TU.remaining_arguments.shift()
        end
      end
    end
  end
  
  def parse_integer_option(val)
    v = val.to_i()
    unless v.to_s() == val
      raise MessageError.new("Unable to parse '#{val}' as an integer")
    end
    
    return v
  end
  
  def parse_float_option(val)
    val.to_f()
  end
  
  def parse_boolean_option(val)
    if val == "true"
      true
    elsif val == "false"
      false
    else
      raise MessageError.new("Unable to parse value '#{val}' as a boolean")
    end
  end
  
  def parse_boolean_option_blank_is_true(val)
    if val == "true"
      true
    elsif val == "false"
      false
    elsif val.to_s() == ""
      true
    else
      raise MessageError.new("Unable to parse value '#{val}' as a boolean")
    end
  end
  
  def parse_boolean_option_blank_is_false(val)
    if val == "true"
      true
    elsif val == "false"
      false
    elsif val.to_s() == ""
      false
    else
      raise MessageError.new("Unable to parse value '#{val}' as a boolean")
    end
  end
  
  def validate
    if require_installed_directory?()
      if TI == nil
        raise "Unable to run #{$0} without the '--directory' argument pointing to an active Tungsten installation"
      else
        TI.inherit_path()
      end
    end
    
    unless allow_unparsed_arguments?()
      unless TU.remaining_arguments.size == 0
        TU.error("Unable to parse the following arguments: #{TU.remaining_arguments.join(' ')}")
      end
    end
    
    if require_command?() && @command_definitions.size() > 0 && @command == nil
      TU.error("A command was not given for this script. Valid commands are #{@command_definitions.keys().join(', ')} and must be the first argument.")
    end
    
    @option_definitions.each{
      |option_key,definition|
      
      if definition[:required] == true
        if opt(option_key).to_s() == ""
          arg = definition[:on][0].split(" ")[0]
          TU.error("Missing value for the #{arg} option")
        end
      end
    }
  end
  
  def script_name
    nil
  end
  
  def display_help
    if script_name().to_s() != ""
      TU.output("Usage: #{script_name()} [global-options] [script-options]")
      TU.output("")
    end

    unless description() == nil
      description().split("<br>").each{
        |section|
        TU.output(TU.wrapped_lines(section))
      }
      TU.output("")
    end
    
    TU.display_help()
    
    if @command_definitions.size() > 0
      TU.write_header("Script Commands", nil)
      
      commands = @command_definitions.keys().sort { |a, b| a.to_s <=> b.to_s }
      commands.each{
        |command_key|
        definition = @command_definitions[command_key]
        if definition[:default] == true
          default = "default"
        else
          default = ""
        end
        
        TU.output_usage_line(command_key.to_s(), definition[:help], default)
      }
    end
    
    TU.write_header("Script Options", nil)
    
    @option_definitions.each{
      |option_key,definition|
      
      if definition[:hidden] == true
        next
      end
      
      if definition[:help].is_a?(Array)
        help = definition[:help].shift()
        additional_help = definition[:help]
      else
        help = definition[:help]
        additional_help = []
      end
      
      TU.output_usage_line(definition[:on].join(","),
        help, definition[:default], nil, additional_help.join("\n"))
    }
  end
  
  def display_autocomplete
    values = TU.get_autocomplete_arguments()
    if @command_definitions.size() > 0
      @command_definitions.each{
        |command_key,definition|
        values << command_key.to_s()
      }
    end
    
    @option_definitions.each{
      |option_key,definition|
      
      if definition[:hidden] == true
        next
      end
      
      values = values + definition[:on]
    }
    
    values.map!{
      |v|
      parts = v.split(" ")
      if parts.size() == 2
        "#{parts[0]}="
      else
        v
      end
    }
    
    puts values.join(" ")
  end
  
  def require_installed_directory?(v = nil)
    if (v != nil)
      @require_installed_directory = v
    end
    
    @require_installed_directory
  end
  
  def allow_unparsed_arguments?(v = nil)
    if (v != nil)
      @allow_unparsed_arguments = v
    end
    
    @allow_unparsed_arguments
  end
  
  def require_command?
    true
  end
  
  def description(v = nil)
    if v != nil
      @description = v
    end
    
    @description || nil
  end
  
  def script_log_path
    nil
  end
  
  def initialized?
    @initialized
  end
  
  def cleanup(code = 0)
    if code != 0
      log_path = TU.log().path()
      if log_path.to_s() != "" && File.exist?(log_path)
        TU.notice("See #{script_log_path()} for more information")
      end
    end
    
    TU.debug("Finish #{$0} #{ARGV.join(' ')}")
    TU.debug("RC: #{code}")
    
    TU.exit(code)
  end
  
  def nagios_ok(msg)
    puts "OK: #{msg}"
    cleanup(NAGIOS_OK)
  end
  
  def nagios_warning(msg)
    puts "WARNING: #{msg}"
    cleanup(NAGIOS_WARNING)
  end
  
  def nagios_critical(msg)
    puts "CRITICAL: #{msg}"
    cleanup(NAGIOS_CRITICAL)
  end
  
  def sudo_prefix
    TI.sudo_prefix()
  end
end

# Require the script to specify a default replication service. If there is a 
# single replication service, that will be used as the default. If there are
# multiple, the user must specify --service.
module SingleServiceScript
  def configure
    super()
    
    if TI
      if TI.replication_services.size() > 1
        default_service = nil
      else
        default_service = TI.default_dataservice()
      end
      
      add_option(:service, {
        :on => "--service String",
        :help => "Replication service to read information from",
        :default => default_service
      })
    end
  end
  
  def validate
    super()
  
    if @options[:service] == nil
      TU.error("You must specify a dataservice for this command with the --service argument")
    else
      if TI
        unless TI.replication_services().include?(@options[:service])
          TU.error("The #{@options[:service]} service was not found in the replicator at #{TI.hostname()}:#{TI.root()}")
        end
      end
    end
  end
end

# Require all replication services to be OFFLINE before proceeding with the 
# main() method. The user can add --offline to have this done for them, and
# --online to bring them back ONLINE when the script finishes cleanly.
module OfflineServicesScript
  def configure
    super()
    
    add_option(:clear_logs, {
      :on => "--clear-logs",
      :default => false,
      :help => "Delete all THL and relay logs for the service"
    })
    
    add_option(:offline, {
      :on => "--offline String",
      :help => "Put required replication services offline before processing",
      :default => false,
      :parse => method(:parse_boolean_option_blank_is_true)
    })
    
    add_option(:offline_timeout, {
      :on => "--offline-timeout Integer",
      :help => "Put required replication services offline before processing",
      :parse => method(:parse_integer_option),
      :default => 60
    })
    
    add_option(:online, {
      :on => "--online String",
      :help => "Put required replication services online after successful processing",
      :default => false,
      :parse => method(:parse_boolean_option_blank_is_true)
    })
  end
  
  def validate
    super()
    
    # Some scripts may disable the OFFLINE requirement depending on other 
    # arguments. These methods give them hooks to make that decision dynamic.
    if allow_service_state_change?() && require_offline_services?()
      # Check the state of each replication service
      get_offline_services_list().each{
        |ds|
        if TI.trepctl_value(ds, "state") =~ /ONLINE/
          TU.error("The replication service '#{ds}' must be OFFLINE to run this command. You can add the --offline argument to do this automatically.")
        end
      }
    end
    
    unless @options[:offline_timeout] > 0
      TU.error("The --offline-timeout must be a number greater than zero")
    end
  end
  
  def prepare
    super()
    
    if TU.is_valid?()
      begin
        if allow_service_state_change?() == true && @options[:offline] == true
          ds_list = get_offline_services_list()
          
          # Put each replication service OFFLINE in parallel waiting for the
          # command to complete
          TU.notice("Put #{ds_list.join(",")} replication #{TU.pluralize(ds_list, "service", "services")} offline")
          
          threads = []
          begin
            Timeout::timeout(@options[:offline_timeout]) {
              ds_list.each{
                |ds|
                threads << Thread.new{
                  use_manager = false
                  
                  if TI.is_manager?()
                    status = TI.status(ds)
                    
                    # Make sure this is actually a physical dataservice
                    if status.is_physical?()
                      # Does this datasource actually appear in the status
                      # It may not if the host hasn't been provisioned
                      if status.datasources().index(TI.hostname()) != nil
                        use_manager = true
                      end
                    end
                  end
                  
                  begin
                    if use_manager == true
                      get_manager_api.call("#{ds}/#{TI.hostname()}", 'shun')
                    end
                  
                    # The trepctl offline command is required even when using 
                    # the manager because shun doesn't affect the replicator
                    TU.cmd_result("#{TI.trepctl(ds)} offline")
                  rescue => e
                    TU.exception(e)
                    raise("Unable to put replication services offline")
                  end
                }
              }
              threads.each{|t| t.join() }
            }
          rescue Timeout::Error
            raise("The replication #{TU.pluralize(ds_list, "service", "services")} #{TU.pluralize(ds_list, "is", "are")} taking too long to go offline. Check the status for more information or use the --offline-timeout argument.")
          end
        end
      rescue => e
        TU.exception(e)
      end
    end
  end
  
  def cleanup(code = 0)
    if initialized?() == true && TI != nil && code == 0
      begin
        if allow_service_state_change?() == true && @options[:online] == true
          cleanup_services(true, @options[:clear_logs])
        elsif @options[:clear_logs] == true
          cleanup_services(false, @options[:clear_logs])
        end
      rescue => e
        TU.exception(e)
        code = 1
      end
    end
    
    super(code)
  end
  
  def cleanup_services(online = false, clear_logs = false)
    ds_list = get_offline_services_list()

    # Put each replication service ONLINE in parallel waiting for the
    # command to complete
    if online == true
      TU.notice("Put the #{ds_list.join(",")} replication #{TU.pluralize(ds_list, "service", "services")} online")
    end
    
    # Emptying the THL and relay logs makes sure that we are starting with 
    # a fresh directory as if `datasource <hostname> restore` was run.
    if clear_logs == true
      TU.notice("Clear THL and relay logs for the #{ds_list.join(",")} replication #{TU.pluralize(ds_list, "service", "services")}")
    end
    
    threads = []
    begin
      Timeout::timeout(@options[:offline_timeout]) {
        ds_list.each{
          |ds|
          threads << Thread.new{
            if clear_logs == true
              dir = TI.setting(TI.setting_key(REPL_SERVICES, ds, "repl_thl_directory"))
              if File.exists?(dir)
                TU.cmd_result("rm -rf #{dir}/*")
              end
              dir = TI.setting(TI.setting_key(REPL_SERVICES, ds, "repl_relay_directory"))
              if File.exists?(dir)
                TU.cmd_result("rm -rf #{dir}/*")
              end
            end
            
            if online == true
              use_manager = false
              
              if TI.is_manager?()
                status = TI.status(ds)
                
                # Make sure this is actually a physical dataservice
                if status.is_physical?()
                  # Does this datasource actually appear in the status
                  # It may not if the host hasn't been provisioned
                  if status.datasources().index(TI.hostname()) != nil
                    use_manager = true
                  end
                end
              end
              
              begin
                if use_manager == true
                  # Bring the replicator and the datasource ONLINE
                  TU.cmd_result("echo 'datasource #{TI.hostname()} recover' | #{TI.cctrl()}")
                else
                  # Bring just the replicator ONLINE
                  TU.cmd_result("#{TI.trepctl(ds)} online")
                end
              rescue => e
                TU.exception(e)
                raise("The #{ds} replication service did not come online")
              end
              
              # Verify the replicator is in fact ONLINE since the recover 
              # command may have not returned the right error
              unless TI.trepctl_value(ds, "state") == "ONLINE"
                raise("Unable to put the #{ds} replication service online")
              end
            end
          }
        }

        threads.each{|t| t.join() }
      }
    rescue Timeout::Error
      TU.error("The replication #{TU.pluralize(ds_list, "service", "services")} #{TU.pluralize(ds_list, "is", "are")} taking too long to cleanup. Check the replicator status for more information.")
    end
  end
  
  # All replication services must be OFFLINE
  def get_offline_services_list
    TI.replication_services()
  end
  
  def require_offline_services?
    if @options[:offline] == true
      false
    else
      true
    end
  end
  
  def allow_service_state_change?
    if TI == nil
      return false
    end

    if TI.is_replicator?() && TI.is_running?("replicator")
      true
    else
      false
    end
  end
  
  def get_manager_api
    if @api == nil && TI != nil
      @api = TungstenAPI::TungstenDataserviceManager.new(TI.mgr_api_uri())
    end
    
    @api
  end
end

# Only require a single replication service to be OFFLINE
module OfflineSingleServiceScript
  include SingleServiceScript
  include OfflineServicesScript
  
  def get_offline_services_list
    [@options[:service]]
  end
end

# Group all MySQL validation and methods into a single module
module MySQLServiceScript
  include SingleServiceScript
  
  # Allow scripts to turn off MySQL validation of the local server
  def require_local_mysql_service?
    false
  end
  
  def validate
    super()
    
    if @options[:service].to_s() == ""
      return
    end
    
    unless TI.replication_services().include?(@options[:service])
      return
    end
    
    if @options[:mysqlhost] == nil
      @options[:mysqlhost] = TI.setting(TI.setting_key(REPL_SERVICES, @options[:service], "repl_datasource_host"))
    end
    if @options[:mysqlport] == nil
      @options[:mysqlport] = TI.setting(TI.setting_key(REPL_SERVICES, @options[:service], "repl_datasource_port"))
    end
    
    if @options[:my_cnf] == nil
      @options[:my_cnf] = TI.setting(TI.setting_key(REPL_SERVICES, @options[:service], "repl_datasource_mysql_service_conf"))
    end
    if @options[:my_cnf] == nil
      TU.error "Unable to determine location of MySQL my.cnf file"
    else
      unless File.exist?(@options[:my_cnf])
        TU.error "The file #{@options[:my_cnf]} does not exist"
      end
    end
    
    if require_local_mysql_service?()
      if @options[:mysqluser] == nil
        @options[:mysqluser] = get_mysql_option("user")
      end
      if @options[:mysqluser].to_s() == ""
        @options[:mysqluser] = "mysql"
      end
    end
  end
  
  def get_mysql_command
    "mysql --defaults-file=#{@options[:my_cnf]} -h#{@options[:mysqlhost]} --port=#{@options[:mysqlport]}"
  end
  
  def get_mysqldump_command
    "mysqldump --defaults-file=#{@options[:my_cnf]} --host=#{@options[:mysqlhost]} --port=#{@options[:mysqlport]} --opt --single-transaction --all-databases --add-drop-database --master-data=2"
  end
  
  def get_innobackupex_path()
    path = TU.which("innobackupex-1.5.1")
    if path.nil?
      path = TU.which("innobackupex")
    end
    return path
  end

  def get_xtrabackup_command
    # Use the configured my.cnf file, or the additional config file 
    # if we created one
    if @options[:extra_mysql_defaults_file] == nil
      defaults_file = @options[:my_cnf]
    else
      defaults_file = @options[:extra_mysql_defaults_file].path()
    end
    
    "#{get_innobackupex_path()} --defaults-file=#{defaults_file} --host=#{@options[:mysqlhost]} --port=#{@options[:mysqlport]}"
  end
  
  def xtrabackup_supports_argument(arg)
    arg = arg.tr("-", "\\-")
    supports_argument = TU.cmd_result("#{get_xtrabackup_command()} --help /tmp | grep -e\"#{arg}\" | wc -l")
    if supports_argument == "1"
      return true
    else
      return false
    end
  end
  
  def get_mysql_result(command, timeout = 30)
    begin      
      Timeout.timeout(timeout.to_i()) {
        return TU.cmd_result("#{get_mysql_command()} -e \"#{command}\"")
      }
    rescue Timeout::Error
    rescue => e
    end
    
    return nil
  end
  
  def get_mysql_value(command, column = nil)
    response = get_mysql_result(command + "\\\\G")
    if response == nil
      return nil
    end
    
    response.split("\n").each{ | response_line |
      parts = response_line.chomp.split(":")
      if (parts.length != 2)
        next
      end
      parts[0] = parts[0].strip;
      parts[1] = parts[1].strip;
      
      if parts[0] == column || column == nil
        return parts[1]
      end
    }
    
    return nil
  end
  
  # Read the configured value for a mysql variable
  def get_mysql_option(opt)
    begin
      val = TU.cmd_result("my_print_defaults --config-file=#{@options[:my_cnf]} mysqld | grep -e'^--#{opt.gsub(/[\-\_]/, "[-_]")}'")
    rescue CommandError => ce
      return nil
    end

    return val.split("\n")[0].split("=")[1]
  end
  
  # Read the current value for a mysql variable
  def get_mysql_variable(var)
    response = TU.cmd_result("#{get_mysql_command()} -e \"SHOW VARIABLES LIKE '#{var}'\\\\G\"")
    
    response.split("\n").each{ | response_line |
      parts = response_line.chomp.split(":")
      if (parts.length != 2)
        next
      end
      parts[0] = parts[0].strip;
      parts[1] = parts[1].strip;
      
      if parts[0] == "Value"
        return parts[1]
      end
    }
    
    return nil
  end
  
  # Store additional MySQL configuration values in a temporary file
  def set_mysql_defaults_value(value)
    if @options[:extra_mysql_defaults_file] == nil
      @options[:extra_mysql_defaults_file] = Tempfile.new("xtracfg")
      @options[:extra_mysql_defaults_file].puts("!include #{@options[:my_cnf]}")
      @options[:extra_mysql_defaults_file].puts("")
      @options[:extra_mysql_defaults_file].puts("[mysqld]")
    end
    
    @options[:extra_mysql_defaults_file].puts(value)
    @options[:extra_mysql_defaults_file].flush()
  end
  
  def start_mysql_server
    ds = TI.datasource(@options[:service])
    ds.start()
  end
  
  # Make sure that the mysql server is stopped by stopping it and checking
  # the process has disappeared
  def stop_mysql_server
    ds = TI.datasource(@options[:service])
    ds.stop()
  end
end