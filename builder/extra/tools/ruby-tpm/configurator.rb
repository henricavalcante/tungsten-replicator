#!/usr/bin/env ruby
#
# TUNGSTEN SCALE-OUT STACK
# Copyright (C) 2010 Continuent, Inc.
# All rights reserved
#

# System libraries.
require 'system_require'

system_require 'singleton'
system_require 'optparse'
system_require 'ostruct'
system_require 'date'
system_require 'fileutils'
system_require 'socket'
system_require 'logger'
system_require 'tempfile'
system_require 'uri'
system_require 'resolv'
system_require 'ipparse'
system_require 'pp'
system_require 'timeout'
system_require 'cgi'
system_require 'json'
system_require 'iniparse'
system_require 'escape'
system_require 'transformer'
system_require 'validator'
system_require 'properties'
system_require 'stringio'
system_require 'open-uri'
system_require 'open4'
system_require 'configure/parameter_names'
system_require 'configure/configure_messages'
system_require 'configure/configure_prompt_handler'
system_require 'configure/configure_prompt_interface'
system_require 'configure/configure_prompt'
system_require 'configure/group_configure_prompt'
system_require 'configure/configure_validation_handler'
system_require 'configure/validation_check_interface'
system_require 'configure/configure_validation_check'
system_require 'configure/group_validation_check'
system_require 'configure/configure_deployment_handler'
system_require 'configure/configure_command'
system_require 'configure/database_platform'
system_require 'configure/cctrl'
system_require 'configure/topology'
system_require 'configure/watch_files'

begin
  require 'readline'
rescue LoadError
end

class OptionParser
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

class String
  if RUBY_VERSION < "1.9"
    def getbyte(index)
      self[index]
    end
  end
  
  def include_alias?(a)
    self.split(",").map{
      |entry|
      to_identifier(entry)
    }.include?(a)
  end
end

class Logger
  NOTICE = 1.5
end

SYSTEM = "__system_defaults_will_be_overwritten__"
DEFAULTS = "__defaults__"
REMOTE = "__remote_values_will_be_overwritten__"

# Define operating system names.
OS_LINUX = "linux"
OS_MACOSX = "macosx"
OS_SOLARIS = "solaris"
OS_UNKNOWN = "unknown"

OS_DISTRO_REDHAT = "distro_redhat"
OS_DISTRO_DEBIAN = "distro_debian"
OS_DISTRO_UNKNOWN = "distro_unknown"

OS_ARCH_32 = "32-bit"
OS_ARCH_64 = "64-bit"
OS_ARCH_UNKNOWN = "unknown"

REPL_ROLE_M = "master"
REPL_ROLE_S = "slave"
REPL_ROLE_R = "relay"
REPL_ROLE_ARCHIVE =  "archive"
REPL_ROLE_DI = "direct"
REPL_ROLE_S_RELAY = "slave-relay"
REPL_ROLE_S_PRE = "slave-prefetch"
REPL_ROLE_DI_PRE = "direct-prefetch"
REPL_ROLE_LOCAL_PRE = "local-prefetch"

DIRECT_DEPLOYMENT_HOST_ALIAS = "local"

DEFAULT_SERVICE_NAME = "default"
RELEASES_DIRECTORY_NAME = "releases"
LOGS_DIRECTORY_NAME = "service_logs"
METADATA_DIRECTORY_NAME = "metadata"
PREPARE_RELEASE_DIRECTORY = 'install'
CONFIG_DIRECTORY_NAME = "conf"
DIRECTORY_LOCK_FILENAME = ".lock"

CONTINUENT_ROOT_VARIABLE = "CONTINUENT_ROOT"
CONTINUENT_ROOT_ARGUMENT = "--home-directory"
CONTINUENT_ENVIRONMENT_SCRIPT = "share/env.sh"

DELETE_REPLICATION_POSITION = "delete_replication_position"
RESTART_REPLICATORS = "restart_replicators"
RESTART_MANAGERS = "restart_managers"
RESTART_CONNECTORS = "restart_connectors"
RESTART_CONNECTORS_NEEDED = "restart_connectors_needed"
RECONFIGURE_CONNECTORS_ALLOWED = "reconfigure_connectors_allowed"
PROVISION_NEW_SLAVES = "provision_new_slaves"

TPM_COMMAND_NAME = "tpm"
AUTODETECT = 'autodetect'

PRODUCT_VMWARE_CLUSTERING = "VMware Continuent for Clustering"
ABBR_VMWARE_CLUSTERING = "vCC"
PRODUCT_VMWARE_REPLICATION = "VMware Continuent for Replication"
ABBR_VMWARE_REPLICATION = "vCR"
PRODUCT_TUNGSTEN_REPLICATOR = "Tungsten Replicator"
ABBR_TUNGSTEN_REPLICATOR = "TR"

class IgnoreError < StandardError
end

# Manages top-level configuration.
class Configurator
  include Singleton
  
  attr_reader :command
  
  DATASERVICE_CONFIG = "deploy.cfg"
  HOST_CONFIG = "tungsten.cfg"
  TEMP_DEPLOY_HOST_CONFIG = ".tungsten.cfg"
  CURRENT_RELEASE_DIRECTORY = "tungsten"
  SERVICE_CONFIG_PREFIX = "service_"
  DEFAULTS_FILENAME = "tungsten_global_args"
  REPLICATOR_PROFILES = "REPLICATOR_PROFILES"
  PROFILES_VARIABLE = "CONTINUENT_PROFILES"
  
  # Initialize configuration arguments.
  def initialize()
    # This is the configuration object that will be stored
    @config = Properties.new
    
    @command = nil

    # Set command line argument defaults.
    @options = OpenStruct.new
    @options.output_threshold = Logger::NOTICE
    @options.stream_output = false
    @options.fake_tty = false
    @options.default_ssh_options = {
      :timeout => 5,
      :auth_methods => ["publickey", "hostbased"],
      :paranoid => false
    }
    @options.ssh_options = {}
    @options.config = nil
    @options.log_name = nil
    
    @log = nil
    @log_cache = []
    @alive_thread = nil
    
    @mutex = Mutex.new
  end
  
  def cleanup(code = 0)
    stop_alive_thread()
    
    if @log
      begin
        @log.chmod(0660)
      rescue
      end
      
      @log.close
      @log = nil
    end
    
    # Depending on the requested command, we will transfer the log file to
    # the configured directory on each affected server
    if @command && @command.distribute_log?()
      @command.distribute_log(get_log_filename())
    end
    
    exit(code)
  end

  def run
    # Include additional Ruby files from the source tree
    load_include_files()
    
    # The get_default_filenames() files allow users to place configuration
    # defaults in files on the system that will override any programmed
    # default value
    load_default_files()
    
    # Take the arguments provided on the command line and parse out generic
    # options as well as options for the requested command
    parsed_options?()
    
    # Hand of control to the ConfigureCommand object. After completion
    # cleanup the process and exit with a 0 or 1
    begin
      if @command.run() == false
        cleanup(1)
      else
        cleanup()
      end
    rescue IgnoreError
      cleanup()
    rescue => e
      exception(e)
      cleanup(1)
    end
  end
  
  # Each of these directories includes Ruby Classes and Modules that extend
  # the behavior of tpm. Some of them will raise an IgnoreError exception
  # if they should not be enabled.
  def load_include_files
    Dir[File.dirname(__FILE__) + '/configure/modules/*.rb'].sort().each do |file| 
      begin
        require File.dirname(file) + '/' + File.basename(file, File.extname(file))
      rescue IgnoreError
      end
    end
    Dir[File.dirname(__FILE__) + '/configure/commands/*.rb'].sort().each do |file|
      begin
        require File.dirname(file) + '/' + File.basename(file, File.extname(file))
      rescue IgnoreError
      end
    end
    Dir[File.dirname(__FILE__) + '/configure/dbms_types/*.rb'].sort().each do |file| 
      begin
        require File.dirname(file) + '/' + File.basename(file, File.extname(file))
      rescue IgnoreError
      end
    end
    Dir[File.dirname(__FILE__) + '/configure/topologies/*.rb'].sort().each do |file| 
      begin
        require File.dirname(file) + '/' + File.basename(file, File.extname(file))
      rescue IgnoreError
      end
    end
  end
  
  def get_default_filenames
    [
      "/etc/#{DEFAULTS_FILENAME}",
      "#{ENV['HOME']}/#{DEFAULTS_FILENAME}",
      "#{get_base_path()}/#{DEFAULTS_FILENAME}"
    ]
  end
  
  # Parse the get_default_filenames() files to override default values
  # for all configurations
  def load_default_files
    get_default_filenames().each{
      |filename|
      if File.exist?(filename)
        File.open(filename, 'r') do |file|
          file.read.each_line do |line|
            line.strip!

            if (line =~ /^([\-\w\.]+)\s*=\s*(\S.*)/)
              ConfigurePrompt.add_global_default($1, $2)
            elsif (line =~ /^([\-\w\.]+)\s*=/)
              ConfigurePrompt.add_global_default($1, "")
            end
          end
        end
      end
    }
  end
  
  # Parse command line arguments.
  def parsed_options?()
    arguments = ARGV
    if arguments.size() > 0
      log(JSON.pretty_generate(arguments))
    end
    
    begin
      # Eliminate characters that will cause exceptions in the Ruby parsing
      arguments = arguments.map{|arg|
        newarg = ''
        arg.split("").each{|b| 
          unless b.getbyte(0)<32 || b.getbyte(0)>127 then 
            newarg.concat(b) 
          end
        }
        newarg
      }
      
      display_help = false
      advanced = true
      opts=OptionParser.new
      opts.on("-h", "--help")           { display_help = true }
      opts.on("-a", "--advanced")       { advanced = true }
      opts.on("-i", "--info")           { @options.output_threshold = Logger::INFO }
      opts.on("-n", "--notice")         { @options.output_threshold = Logger::NOTICE }
      opts.on("-q", "--quiet")          { @options.output_threshold = Logger::WARN }
      opts.on("-v", "--verbose")        { @options.output_threshold = Logger::DEBUG }
      
      # Backwards compatibility
      opts.on("--master-slave")         {}
      # alias for `tpm configure`
      opts.on("--build-config")         { @command = ConfigureDataServiceCommand.new(@config) }
      # alias for `tpm configure defaults`
      opts.on("--defaults-only")        { @command = ConfigureDataServiceCommand.new(@config)
                                          @command.subcommand(ConfigureDataServiceCommand::CONFIGURE_DEFAULTS)
      }
      # alias for `tpm configure --hosts=<val>`
      opts.on("--build-host String")    { |val|
                                          @command = ConfigureDataServiceCommand.new(@config) 
                                          @command.command_hosts(val)
      }
      # alias for `tpm install`
      opts.on("-b", "--batch")          { @command = InstallCommand.new(@config) }
      # Displays the possible keys that can be used in the deploy.cfg file
      opts.on("--config-file-help")     {
                                          @command = HelpCommand.new(@config)
                                          @command.subcommand(HelpCommand::HELP_CONFIG_FILE)
      }
      # Displays the possible template placeholders
      opts.on("--template-file-help")   {
                                          @command = HelpCommand.new(@config)
                                          @command.subcommand(HelpCommand::HELP_TEMPLATE_FILE)
      }
      # Force logging to be enabled for this command
      opts.on("--log String")           { |val|
        @options.log_name = val
      }
      arguments = run_option_parser(opts, arguments)
    
      # Attempt to identify the TPM command if one has not been chosen
      if arguments.size() > 0
        # Guess the command class based on the first remaining argument
        # This will return nil if the first argument isn't a valid command
        command_class = ConfigureCommand.get_command_class(arguments[0])

        if command_class
          # Remove the first argument since it is the command class
          arguments.shift()
        else
          # Look for a command argument in the remaining values
          opts=OptionParser.new
          # Needed again so that an exception isn't thrown
          opts.on("-p", "--package String") {|klass|
            command_class = Module.const_get(klass)
          }
          opts.on("--command String") {|klass|
            command_class = Module.const_get(klass)
          }
          
          arguments = run_option_parser(opts, arguments)
        end
        
        # Instantiate the command option based on the class that we found
        if @command == nil && command_class
          begin
            unless defined?(command_class)
              raise "Unable to find the #{command_class} command"
            end
            unless command_class.include?(ConfigureCommand)
              raise "Command '#{command_class}' does not include ConfigureCommand"
            end
            
            @command = command_class.new(@config)
          rescue => e
            debug(e)
            error("Unable to instantiate command: #{e.to_s()}")
            return false
          end
        end
      end
      
      if @command == nil
        # Fall back to the help command when nothing else has worked
        @command = HelpCommand.new(@config)
      else
        # Apply the display_help setting to the command object
        @command.display_help?(display_help)
      end  
      @command.advanced?(advanced)
      
      if @command.enable_log?()
        initialize_log()
        debug("Logging started to #{get_log_filename()}")
      else
        disable_log()
      end
      
      debug("The command is set to #{@command.class.to_s}")
      start_alive_thread()
      
      opts=OptionParser.new
      # Define the file to use for storing the staging configuration
      opts.on("--profile String", "-c String", "--config String")  {|val| 
                                        @options.config = val
                                        }
      opts.on("--skip-validation-check String")      {|val|
                                          val.split(",").each{
                                            |v|
                                            ConfigureValidationHandler.mark_skipped_validation_class(v)
                                          }
                                        }
      opts.on("--enable-validation-check String")      {|val|
                                          val.split(",").each{
                                            |v|
                                            ConfigureValidationHandler.mark_enabled_validation_class(v)
                                          }
                                        }
      opts.on("--skip-validation-warnings String")      {|val|
                                          val.split(",").each{
                                            |v|
                                            ConfigureValidationHandler.mark_skipped_validation_warnings(v)
                                          }
                                        }
      opts.on("--enable-validation-warnings String")      {|val|
                                          val.split(",").each{
                                            |v|
                                            ConfigureValidationHandler.mark_enabled_validation_warnings(v)
                                          }
                                        }
      opts.on("--net-ssh-option String")  {|val|
                                          val_parts = val.split("=")
                                          if val_parts.length() !=2
                                            error "Invalid value #{val} given for '--net-ssh-option'.  There should be a key/value pair joined by a single =."
                                          end
                                          
                                          if val_parts[0] == "timeout"
                                            val_parts[1] = val_parts[1].to_i
                                          end

                                          @options.ssh_options[val_parts[0].to_sym] = val_parts[1]
                                        }
    
      # Argument used by the validation and deployment handlers
      opts.on("--stream")               {@options.stream_output = true }
      opts.on("--tty")                  {@options.fake_tty = true }

      remainder = run_option_parser(opts, arguments)

      if arguments_valid?() != true && display_help?() != true
        cleanup(1)
      end
      
      begin
        # Hand off option parsing to the command object. This will run through
        # the command class and any included modules
        remainder = @command.parsed_options?(remainder)
        unless display_help?()
          unless @command.is_valid?() && remainder.empty?()
            error("There was a problem parsing the arguments")
            
            unless remainder.empty?()
              error("Unable to parse the following arguments: #{remainder.join(' ')}")
            end
            
            cleanup(1)
          end

          @command.arguments_valid?()
          unless @command.is_valid?()
            cleanup(1)
          end
        end
      rescue IgnoreError
        cleanup()
      rescue => e
        exception(e)
        unless display_help?()
          cleanup(1)
        end
      end
    
      if display_help?()
        output_help
        cleanup()
      end
    rescue => e
      exception(e)
      cleanup(1)
    end
    
    # Extend the path for this command so utilities in uncommon locations
    # can be found
    h_alias = @config.getNestedProperty([DEPLOYMENT_HOST])
    unless h_alias == nil
      path = @config.getProperty([HOSTS, h_alias, PREFERRED_PATH])
      unless path.to_s() == ""
        debug("Adding #{path} to $PATH")
        ENV['PATH'] = path + ":" + ENV['PATH']
      end
    end
    
    # Some commands (query) need to be able to run while a higher level TPM script is running
    unless @command.allow_multiple_tpm_commands?() || ConfigureValidationHandler.skip_validation_class?("InstallationScriptCheck", @config)
      cmd_result("ps ax 2>/dev/null | grep configure.rb | grep -v firewall | grep -v grep | awk '{print $1}'").split("\n").each{
        |pid|
        if pid != $$.to_s
          error("There is already another Tungsten installation script running")
          cleanup(1)
        end
      }
    end
    
    true
  end

  # True if required arguments were provided
  def arguments_valid?
    if @options.config == nil
      @options.config = @command.get_default_config_file()
    else
      # Evaluate tildes
      @options.config = cmd_result("echo #{@options.config}")
      
      if @options.config.index("/")
        @options.config=File.expand_path(@options.config)
      else
        profiles_dir = @command.get_profiles_dir()
        if profiles_dir.to_s() != ""
          @options.config = File.expand_path("#{profiles_dir}/#{@options.config}")
        end
      end
    end
    
    if File.exist?(@options.config)
      if ! File.writable?(@options.config)
        write "Config file must be writable: #{@options.config}", Logger::ERROR
        return false
      end
      if ! File.readable?(@options.config) && File.exist?(@options.config)
        write "Config file is not readable: #{@options.config}", Logger::ERROR
        return false
      end
    else
      if ! File.writable?(File.dirname(@options.config))
        write "Config file directory must be writable: #{@options.config}", Logger::ERROR
        return false
      end
      if ! File.readable?(File.dirname(@options.config))
        write "Config file directory must be readable: #{@options.config}", Logger::ERROR
        return false
      end
    end
    
    # Load the current configuration values
    if File.exist?(@options.config)
      @config.load(@options.config)
    end
    
    true
  end
  
  def run_option_parser(opts, arguments, allow_invalid_options = true, invalid_option_prefix = nil)
    remaining_arguments = []
    
    while arguments.size() > 0
      begin
        arguments = opts.order!(arguments)
        
        # The next argument does not have a dash so the OptionParser
        # ignores it, we will add it to the stack and continue
        if arguments.size() > 0 && (arguments[0] =~ /-.*/) != 0
          remaining_arguments << arguments.shift()
        end
      rescue OptionParser::InvalidOption => io
        if allow_invalid_options
          # Prepend the invalid option onto the arguments array
          remaining_arguments = remaining_arguments + io.recover([])
        
          # The next argument does not have a dash so the OptionParser
          # ignores it, we will add it to the stack and continue
          if arguments.size() > 0 && (arguments[0] =~ /-.*/) != 0
            remaining_arguments << arguments.shift()
          end
        else
          if invalid_option_prefix != nil
            io.reason = invalid_option_prefix
          end
          
          error(io.to_s)
          
          unless advanced_mode?
            error("Try adding -a to enable advanced command line options")
          end
        end
      rescue => e
        if @command && display_help?()
          output_help
          cleanup()
        end
      
        error("Argument parsing failed: #{e.to_s()}")
        cleanup(1)
      end
    end
    
    remaining_arguments
  end

  def output_help
    output_version
    output_usage
  end

  def output_usage
    @command.output_usage()
  end

  def output_version
    write "#{File.basename(__FILE__)} version #{get_release_version}"
  end
  
  def display_help?()
    return @command.display_help?()
  end
  
  def display_preview?()
    return @command.display_preview?()
  end
  
  def output(content)
    write(content, nil)
  end
  
  # Write a header
  def write_header(content, level=Logger::INFO)
    write("#####################################################################", level, nil, false)
    write("# #{content}", level, nil, false)
    write("#####################################################################", level, nil, false)
  end

  # Write a sub-divider, which is used between sections under a singl header.
  def write_divider(level=Logger::INFO)
    write("-"*[(Configurator.instance.detect_terminal_size[0]-5),0].max(), level, nil, false)
  end
  
  def write(content="", level=Logger::INFO, hostname = nil, add_prefix = true)
    if forced?() && level == Logger::ERROR
      level = Logger::WARN
    end
    
    unless content == "" || level == nil || add_prefix == false
      content = "#{get_log_level_prefix(level, hostname)}#{content}"
    end
    
    log(content)
    
    if enable_log_level?(level)
      if enable_output?()
        stop_alive_thread()
        puts content
        $stdout.flush()
        start_alive_thread()
      end
    elsif @options.stream_output == true
      STDERR.puts content
    end
  end
  
  def force_output(content)
    log(content)
    stop_alive_thread()
    puts(content)
    $stdout.flush()
    start_alive_thread()
  end
  
  def initialize_log
    unless @log
      begin
        @log = File.open(get_log_filename(), "a", 0660)
      rescue => e
        raise e
      end
    end
    
    unless @log_cache == false
      @log_cache.each{
        |s|
        @log.puts DateTime.now.to_s + " " + s
      }
      @log_cache = false
    end
  end
  
  def disable_log
    @log_cache = false
  end
  
  def log(content)
    unless @log
      unless @log_cache == false
        @log_cache << content
      end
      
      return
    end
    
    @log.puts DateTime.now.to_s + " " + content
    @log.flush
  end
  
  def write_from_file(filename, level=Logger::INFO)
    unless enable_log_level?(level)
      return
    end
    
    f = File.open(filename, "r") 
    f.each_line do |line|
      output(line)
    end
    f.close
  end
  
  def info(message, hostname = nil)
    write(message, Logger::INFO, hostname)
  end
  
  def notice(message, hostname = nil)
    write(message, Logger::NOTICE, hostname)
  end
  
  def warning(message, hostname = nil)
    write(message, Logger::WARN, hostname)
  end
  
  def error(message, hostname = nil)
    write(message, Logger::ERROR, hostname)
  end
  
  def exception(e, hostname = nil)
    error(e.to_s(), hostname)
    debug(e.message + "\n" + e.backtrace.join("\n"), hostname)
  end
  
  def debug(message, hostname = nil)
    write(message, Logger::DEBUG, hostname)
  end
  
  def get_log_level(prefix)
    case prefix.strip
    when "ERROR" then Logger::ERROR
    when "WARN" then Logger::WARN
    when "DEBUG" then Logger::DEBUG
    when "NOTE" then Logger::NOTICE
    when "INFO" then Logger::INFO
    else
      nil
    end
  end
  
  def get_log_level_prefix(level=Logger::INFO, hostname = nil)
    case level
    when Logger::ERROR then prefix = "ERROR"
    when Logger::WARN then prefix = "WARN "
    when Logger::DEBUG then prefix = "DEBUG"
    when Logger::NOTICE then prefix = "NOTE "
    else
      prefix = "INFO "
    end
    
    if hostname == nil && !(@config.empty?())
      hostname = @config.getNestedProperty([DEPLOYMENT_HOST])
    end
    
    if hostname == nil
      "#{prefix} >> "
    else
      "#{prefix} >> #{hostname} >> "
    end
  end
  
  def enable_log_level?(level=Logger::INFO)
    if level == nil
      true
    elsif level<@options.output_threshold
      false
    else
      true
    end
  end
  
  def set_log_level(level=Logger::INFO)
    @options.output_threshold = level
  end
  
  # Find out the current user ID.
  def whoami
    if ENV['USER']
      ENV['USER']
    elsif ENV['LOGNAME']
      ENV['LOGNAME']
    else
      `whoami 2>/dev/null`.chomp
    end
  end
  
  def hostname
    `hostname 2>/dev/null`.chomp
  end
  
  def is_localhost?(hostname)
    if hostname == DEFAULTS
      return false
    end
    
    @_is_localhost_cache ||= {}
    unless @_is_localhost_cache.has_key?(hostname)
      @_is_localhost_cache[hostname] = _is_localhost?(hostname)
    end
    
    return @_is_localhost_cache[hostname]
  end
  
  def _is_localhost?(hostname)
    if hostname == hostname()
      return true
    end

    ip_addresses = get_ip_addresses(hostname)
    if ip_addresses == false
      return false
    end

    debug("Search ifconfig for #{ip_addresses.join(', ')}")
    ipparsed = IPParse.new().get_interfaces()
    ipparsed.each{
      |iface, addresses|

      begin
        # Do a string comparison so that we only match the address portion
        addresses.each{
          |type, details|
          if ip_addresses.include?(details[:address])
            return true
          end
        }
      rescue ArgumentError
      end
    }

    false
  end
  
  def get_ip_addresses(hostname)
    begin
      if hostname == DEFAULTS
        return false
      end
      
      ip_addresses = Timeout.timeout(5) {
        Resolv.getaddresses(hostname)
      }
      
      if ip_addresses.length == 0
        begin
          ping_result = cmd_result("ping -c1 #{hostname} 2>/dev/null | grep PING")
          matches = ping_result.match("[0-9]+.[0-9]+.[0-9]+.[0-9]+")
          if matches && matches.size() > 0
            return [matches[0]]
          end
        rescue CommandError
        end
        
        warning "Unable to determine the IP addresses for '#{hostname}'"
        return false
      end
      
      return ip_addresses
    rescue Timeout::Error
      warning "Unable to lookup #{hostname} because of a DNS timeout"
      return false
    rescue
      warning "Unable to determine the IP addresses for '#{hostname}'"
      return false
    end
  end
  
  def is_real_hostname?(hostname)
    begin
      ip_addresses = Timeout.timeout(5) {
        Resolv.getaddresses(hostname)
      }

      if ip_addresses.length == 0
        begin
          ping_result = cmd_result("ping -c1 #{hostname} 2>/dev/null | grep PING")
          matches = ping_result.match("[0-9]+.[0-9]+.[0-9]+.[0-9]+")
          if matches && matches.size() > 0
            ip_addresses = [matches[0]]
          end
        rescue CommandError
        end
      end
    rescue Timeout::Error
    rescue
    end
    
    if ip_addresses.size() == 0
      return false
    else
      return true
    end
  end
  
  def check_addresses_is_pingable(hostname)
      ping_result = cmd_result("ping -c1 #{hostname} 2>/dev/null >/dev/null ; echo $?")
      if ping_result.to_i == 0
        return true
      end
    
    warning "'#{hostname}' is not pingable"
    return false
  end
  
  
  def get_interface_address(interface_name)
    debug("Search ifconfig for interface #{interface_name}")
    details IPParse.new().get_interface_address(interface_name, IPParse::IPV4)
    
    if details == nil
      return nil
    else
      return details[:address]
    end
  end
  
  def get_continuent_root
    if is_locked?()
      return cmd_result("cat #{get_lock_filename()}")
    elsif ENV.has_key?(CONTINUENT_ROOT_VARIABLE)
      return ENV[CONTINUENT_ROOT_VARIABLE]
    else
      continuent_root_argument = nil
      opts = OptionParser.new
      
      opts.on("#{CONTINUENT_ROOT_ARGUMENT} String") {
        |val|
        continuent_root_argument = val
      }
      opts.on("-h") {}
      opts.on("-v") {}
      remainder = run_option_parser(opts, ARGV.clone)
      
      if continuent_root_argument != nil
        ENV[CONTINUENT_ROOT_VARIABLE] = continuent_root_argument
      end
      
      return continuent_root_argument
    end
  end
  
  def get_base_path
    File.expand_path(File.dirname(__FILE__) + "/../../")
  end
  
  def get_log_filename
    if is_locked?
      "#{get_base_path()}/tungsten-configure.log"
    else
      case @options.log_name.to_s().downcase()
      when "pid"
        filename = "tungsten-configure_pid#{Process.pid}.log"
      when "timestamp"
        filename = "tungsten-configure_#{DateTime.now.strftime('%Y%m%d%H%M%S')}.log"
      else
        if @options.log_name == nil
          filename = "tungsten-configure.log"
        else
          if @options.log_name =~ /\//
            return @options.log_name
          else
            filename = @options.log_name
          end
        end
      end
      
      if File.exists?("/tmp") && File.writable?("/tmp")
        "/tmp/#{filename}"
      else
        "#{get_base_path()}/#{filename}"
      end
    end
  end
  
  def get_ruby_prefix
    "tools/ruby"
  end
  
  def get_basename
    File.basename(get_base_path())
  end
  
  def get_unique_basename
    get_basename() + "_pid#{Process.pid}"
  end
  
  def has_tty?
    if @options.fake_tty == true
      return true
    else
      (`tty > /dev/null 2>&1; echo $?`.chomp == "0")
    end
  end
  
  def enable_output?
    (has_tty?() || @options.stream_output == true)
  end
  
  def streaming_output?
    (@options.stream_output == true)
  end
  
  def is_locked?
    File.exists?(get_lock_filename())
  end
  
  def get_lock_filename
    "#{get_base_path()}/#{DIRECTORY_LOCK_FILENAME}"
  end
  
  def get_manifest_file_path
    "#{get_base_path()}/.manifest"
  end
  
  def get_manifest_json_file_path
    "#{get_base_path()}/.manifest.json"
  end
  
  def get_release_details
    version=''
    unless @release_details
      # Read manifest to find build version. 
      begin
        File.open(get_manifest_json_file_path(), 'r') do |file|
          begin
            parsed = JSON.parse(file.readlines().join())
            
            version = "#{parsed['version']['major']}.#{parsed['version']['minor']}.#{parsed['version']['revision']}"
            if parsed['hudson']['buildNumber'].to_s() != ""
              version = version + "-#{parsed['hudson']['buildNumber']}"
            end
            @release_details = {
              "version" => version,
              "product" => parsed['product']
            }
            
            if parsed['product'] == "Tungsten Replicator"
              @release_details[:is_enterprise_package] = false
              
              if parsed["git"]["URL"] =~ /github\.com/
                @release_details[:is_open_source] = true
              else
                @release_details[:is_open_source] = false
              end
            else
              @release_details[:is_enterprise_package] = true
              @release_details[:is_open_source] = false
            end
          rescue JSON::ParserError
            raise "Unable to parse the .manifest.json file"
          end
        end
      rescue Exception => e
        raise "Unable to read .manifest file: #{e.to_s}"
      end
    end
    
    if @release_details
      @release_details
    else
      raise "Unable to determine the current release version"
    end
  end
  
  def get_release_name
    get_basename()
  end
  
  # Parse the manifest to determine what kind of package this is
  def get_release_version
    release_details = get_release_details()
    release_details["version"]
  end
  
  def get_svc_path(svc_name, tungsten_base_path = nil)
    tungsten_base_path ||= get_base_path()
    
    "#{tungsten_base_path}/tungsten-#{svc_name}/bin/#{svc_name}"
  end
  
  def get_cctrl_path(tungsten_base_path = nil, port = nil)
    tungsten_base_path ||= get_base_path()
    if port == nil
      warning("Assuming manager port")
    end
    port ||= 9997
    
    "#{tungsten_base_path}/tungsten-manager/bin/cctrl -port #{port}"
  end
  
  def get_trepctl_path(tungsten_base_path = nil, port = nil)
    tungsten_base_path ||= get_base_path()
    if port == nil
      warning("Assuming replicator port")
    end
    port ||= 10000
    
    "#{tungsten_base_path}/tungsten-replicator/bin/trepctl -port #{port}"
  end
  
  def get_thl_path(tungsten_base_path = nil)
    tungsten_base_path ||= get_base_path()
    
    "#{tungsten_base_path}/tungsten-replicator/bin/thl"
  end
  
  def get_tpm_path(tungsten_base_path = nil)
    tungsten_base_path ||= get_base_path()
    
    "#{tungsten_base_path}/tools/tpm"
  end
  
  def svc_is_running?(cmd)
    begin
      unless File.exists?(cmd)
        # It doesn't exist, it can't be running
        return false
      end
      
      cmd_result("#{cmd} status")
      return true
    rescue CommandError => ce
      return false
    end
    
    return false
  end

  def get_coordinator_line(tungsten_base_path = nil, port = nil)
    return cmd_result("echo 'ls' | " + get_cctrl_path(tungsten_base_path, port) + '| grep "COORDINATOR"')
  end
  
  def get_manager_policy(tungsten_base_path = nil, port = nil)
    return /COORDINATOR\[([^:]+):(\w+)/.match(get_coordinator_line(tungsten_base_path, port))[2]
  end
  
  def get_coordinator(tungsten_base_path = nil, port = nil)
    return /COORDINATOR\[([^:]+):(\w+)/.match(get_coordinator_line(tungsten_base_path, port))[1]
  end
  
  def advanced_mode?
    @command.advanced?()
  end
  
  def forced?
    if @command
      @command.forced?()
    else
      false
    end
  end
  
  def is_enterprise?
    release_details = get_release_details()
    release_details[:is_enterprise_package]
  end
  
  def is_open_source?
    release_details = get_release_details()
    release_details[:is_open_source]
  end
  
  def product_name
    if is_open_source?()
      PRODUCT_TUNGSTEN_REPLICATOR
    elsif is_enterprise?()
      PRODUCT_VMWARE_CLUSTERING
    else
      PRODUCT_VMWARE_REPLICATION
    end
  end
  
  def product_abbreviation
    if is_open_source?()
      ABBR_TUNGSTEN_REPLICATOR
    elsif is_enterprise?()
      ABBR_VMWARE_CLUSTERING
    else
      ABBR_VMWARE_REPLICATION
    end
  end
  
  def version
    release_details = get_release_details()
    release_details["version"]
  end
  
  def get_config
    @config
  end
  
  def get_config_filename
    File.expand_path(@options.config)
  end
  
  def os?
    os = `uname -s`.chomp
    case
      when os == "Linux" then OS_LINUX
      when os == "Darwin" then OS_MACOSX
      when os == "SunOS" then OS_SOLARIS
      else OS_UNKNOWN
    end
  end 
  
  def arch?
    # Architecture is unknown by default.
    arch = `uname -m`.chomp
    case
      when arch == "x86_64" then OS_ARCH_64
      when arch == "i386" then OS_ARCH_32
      when arch == "i686" then OS_ARCH_32
      else OS_ARCH_UNKNOWN
    end
  end

  def distro?
    # If the OS is unknown accept it only if this is a forced configuration.
    case os?()
      when OS_UNKNOWN
        Configurator.instance.debug("Operating system could not be determined")
      when OS_LINUX
        if File.exist?("/etc/redhat-release")
          OS_DISTRO_REDHAT
        elsif File.exist?("/etc/debian_version")
          @options.distro = OS_DISTRO_DEBIAN
        elsif File.exist?("/etc/system-release")
            amazon_check = cmd_result("cat /etc/system-release | grep Amazon | wc -l")
            if amazon_check == '0'
              OS_DISTRO_UNKNOWN
            else
              OS_DISTRO_REDHAT
            end
        else
          OS_DISTRO_UNKNOWN
        end
    end
  end
  
  def can_install_services_on_os?
    # If the OS is unknown accept it only if this is a forced configuration.
    case os?()
      when OS_UNKNOWN
        Configurator.instance.debug("Operating system could not be determined")
      when OS_LINUX
        if File.exist?("/etc/redhat-release")
          true
        elsif File.exist?("/etc/debian_version")
          true
        elsif File.exist?("/etc/system-release")
            amazon_check = cmd_result("cat /etc/system-release | grep Amazon | wc -l")
            if amazon_check == '0'
              false
            else
              true
            end
        else
          false
        end
    end
  end
  
  # Determines if a shell command exists by searching for it in ENV['PATH'].
  def command_exists?(command)
    ENV['PATH'].split(File::PATH_SEPARATOR).any? {|d| File.exists? File.join(d, command) }
  end

  # Returns [width, height] of terminal when detected, nil if not detected.
  # Think of this as a simpler version of Highline's Highline::SystemExtensions.terminal_size()
  def detect_terminal_size
    unless @terminal_size
      if (ENV['COLUMNS'] =~ /^\d+$/) && (ENV['LINES'] =~ /^\d+$/)
        @terminal_size = [ENV['COLUMNS'].to_i, ENV['LINES'].to_i]
      elsif (RUBY_PLATFORM =~ /java/ || (!STDIN.tty? && ENV['TERM'])) && command_exists?('tput')
        @terminal_size = [`tput cols`.to_i, `tput lines`.to_i]
      elsif STDIN.tty? && command_exists?('stty')
        @terminal_size = `stty size`.scan(/\d+/).map { |s| s.to_i }.reverse
      else
        @terminal_size = [80, 30]
      end
    end
    
    return @terminal_size
  rescue => e
    [80, 30]
  end
  
  def get_constant_symbol(value)
    unless @constant_map
      @constant_map = {}
      
      Object.constants.each{
        |symbol|
        next if symbol.to_s == "Config"
        
        @constant_map[Object.const_get(symbol)] = symbol
      }
    end
    
    @constant_map[value]
  end
  
  def self.timer_start(name)
    @@timers ||= {}
    @@timers[name] = Time.now
  end
  
  def self.timer_stop(name)
    @@timers ||= {}
    if @@timers.has_key?(name)
      diff = (Time.now - @@timers[name])*1000
      
      if diff >= 75
        puts(sprintf("TIMER: %10d >> %s", diff, name))
      end
      @@timers.delete(name)
    end
  end
  
  def get_ssh_options
    @options.default_ssh_options.merge(@options.ssh_options)
  end
  
  def get_ssh_command_options
    opts = ["-A", "-oStrictHostKeyChecking=no"]
    @options.ssh_options.each{
      |k,v|
      opts << "-o#{k.to_s()}=#{v}"
    }
    return opts.join(" ")
  end
  
  def get_ssh_user(user = nil)
    ssh_options = get_ssh_options
    if ssh_options.has_key?(:user) && ssh_options[:user].to_s != ""
      ssh_options[:user]
    else
      user
    end
  end
  
  def get_remote_tpm_options
    extra_options = @command.get_remote_tpm_options()
    
    case @options.output_threshold
    when Logger::DEBUG
      extra_options << "--verbose"
    when Logger::WARN
      extra_options << "--quiet"
    when Logger::NOTICE
      extra_options << "--notice"
    when Logger::INFO
      extra_options << "--info"
    end

    ConfigureValidationHandler.get_skipped_validation_classes.each{
      |klass|
      extra_options << "--skip-validation-check=#{klass}"
    }
    ConfigureValidationHandler.get_skipped_validation_warnings.each{
      |klass|
      extra_options << "--skip-validation-warnings=#{klass}"
    }
    ConfigureValidationHandler.get_enabled_validation_classes.each{
      |klass|
      extra_options << "--enable-validation-check=#{klass}"
    }
    ConfigureValidationHandler.get_enabled_validation_warnings.each{
      |klass|
      extra_options << "--enable-validation-warnings=#{klass}"
    }
    if forced?()
      extra_options << "-f"
    end
    
    extra_options
  end
  
  def start_alive_thread
    if @command && @command.display_alive_thread?() == false
      return
    end
    
    self.synchronize() {
      if @alive_thread == nil && enable_log_level?(Logger::NOTICE) && has_tty?() && @options.stream_output == false
        @alive_thread = Thread.new{
          Thread.current[:has_output] = false
          while true
            sleep 2
            putc '.'
            $stdout.flush()
            Thread.current[:has_output] = true
          end
        }
      end
    }
  end
  
  def stop_alive_thread
    self.synchronize() {
      if @alive_thread != nil
        if @alive_thread[:has_output] == true
          puts "\n"
          $stdout.flush()
        end

        begin
          Thread.kill(@alive_thread)
        rescue TypeError
        end

        @alive_thread = nil
      end
    }
  end
  
  def synchronize(&block)
    @mutex.synchronize do
      block.call()
    end
  end
end

def is_port_available?(ip, port)
  begin
    Timeout::timeout(5) do
      begin
        s = TCPSocket.new(ip, port)
        s.close
        return false
      rescue Errno::ECONNREFUSED, Errno::EHOSTUNREACH
        return true
      end
    end
  rescue Timeout::Error
  end

  return true
end

class SSHConnections
  @@init_profile_script = nil
  
  def self.ssh_result(command, host, user, return_object = false)
    if Configurator.instance.display_help? && !Configurator.instance.display_preview?
      raise RemoteCommandNotAllowed.new("Command '#{command}' not allowed because help mode is enabled")
    end

    if host == DEFAULTS
      Configurator.instance.debug("Unable to run '#{command}' because '#{host}' is not valid")
      raise RemoteCommandError.new(user, host, command, nil, '')
    end

    if return_object == false && 
        Configurator.instance.is_localhost?(host) && 
        user == Configurator.instance.whoami()
      return cmd_result(command)
    end

    Configurator.instance.synchronize() {
      unless defined?(Net::SSH)
        begin
          require "openssl"
        rescue LoadError
          Configurator.instance.error("Unable to find the Ruby openssl library")
          Configurator.instance.error("Try installing the openssl package for your version of Ruby (libopenssl-ruby#{RUBY_VERSION[0,3]}).")
          Configurator.instance.cleanup(1)
        end
        system_require 'net/ssh'
        
        self.init_profile_script()
      end
    }

    if return_object
      command = "#{command} --stream"
    end

    ssh_user = Configurator.instance.get_ssh_user(user)
    if user != ssh_user
      Configurator.instance.debug("SSH user changed to #{ssh_user}")
      command = command.tr('"', '\"')
      command = "echo \"#{command}\" | sudo -n -u #{user} -i"
    end

    Configurator.instance.debug("Execute `#{command}` on #{host} as #{user}")
    result = ""
    rc = nil
    exit_signal=nil

    connection_error = "Net::SSH was unable to connect to #{host} as #{ssh_user}.  Check that #{host} is online, #{ssh_user} exists and your SSH private keyfile or ssh-agent settings. Try adding --net-ssh-option=port=<SSH port number> if you are using an SSH port other than 22.  Review http://docs.continuent.com/helpwithsshandtpm for more help on diagnosing SSH problems."
    begin
      Net::SSH.start(host, ssh_user, Configurator.instance.get_ssh_options()) {
        |ssh|
        
        if return_object
          log_level = nil

          buf = ""
          channel = ssh.open_channel {
            |ch|
            ch.exec(". /etc/profile; #{self.init_profile_script()} export LANG=en_US; export LC_ALL=\"en_US.UTF-8\"; #{command}") {
              |ch, success|

              ch.on_data {
                |chan,data|
                (buf ||= '') << data

                while line = buf.slice!(/(.*)\r?\n/)
                  unless line.index('RemoteResult') != nil || result != ""
                    line_log_level = Configurator.instance.get_log_level(line[0,5])
                    if line_log_level != nil
                      log_level = line_log_level
                    end

                    Configurator.instance.output(line)
                  else
                    result += line
                  end
                end
              }

              ch.on_extended_data {
                |chan,type,data| 

                data = data.chomp
                Configurator.instance.log(data) unless data == ""
              }
              channel.on_request("exit-status") do |ch,data|
                rc = data.read_long
              end

              channel.on_request("exit-signal") do |ch, data|
                exit_signal = data.read_long
              end
            }
          }

          channel.wait
          ssh.loop
        else
          stdout_data = ""
          stderr_data = ""

          ssh.open_channel do |channel|
            channel.exec(". /etc/profile; #{self.init_profile_script()} export LANG=en_US; export LC_ALL=\"en_US.UTF-8\"; #{command}") do |ch, success|
              channel.on_data do |ch,data|
                stdout_data+=data
              end

              channel.on_extended_data do |ch,type,data|
                data = data.chomp
                Configurator.instance.log(data) unless data == ""
              end

              channel.on_request("exit-status") do |ch,data|
               rc = data.read_long
              end

              channel.on_request("exit-signal") do |ch, data|
                exit_signal = data.read_long
              end
            end
          end
          ssh.loop
          result = stdout_data.to_s.chomp    
        end
      }
    rescue Errno::ENOENT => ee
      raise MessageError.new("Net::SSH was unable to find a private key to use for SSH authenticaton. Try creating a private keyfile or setting up ssh-agent.")
    rescue OpenSSL::PKey::RSAError
      raise MessageError.new(connection_error)
    rescue Net::SSH::AuthenticationFailed
      raise MessageError.new(connection_error)
    rescue Errno::ECONNREFUSED, Errno::EHOSTUNREACH, Errno::EHOSTDOWN
      raise MessageError.new(connection_error)
    rescue Timeout::Error
      raise MessageError.new(connection_error)
    rescue NotImplementedError => nie
      raise MessageError.new(nie.message + ". Try modifying your ~/.ssh/config file to define values for Cipher and Ciphers that do not include this algorithm.  The supported encryption algorithms are #{Net::SSH::Transport::CipherFactory::SSH_TO_OSSL.keys().delete_if{|e| e == "none"}.join(", ")}.")
    end

    if rc != 0
      raise RemoteCommandError.new(user, host, command, rc, result)
    else
      if return_object
        Configurator.instance.debug("RC: #{rc}, Result length: #{result.length}")
      else
        Configurator.instance.debug("RC: #{rc}, Result: #{result}")
      end
    end

    return result
  end
  
  def self.init_profile_script
    if @@init_profile_script == nil
      init_profile_script_parts = []
      [
        "$HOME/.bash_profile",
        "$HOME/.bash_login",
        "$HOME/.profile"
      ].each{
        |filename|

        if init_profile_script_parts.size() == 0
          init_profile_script_parts << "if"
        else
          init_profile_script_parts << "elif"
        end

        init_profile_script_parts << "[ -f #{filename} ]; then . #{filename};"
      }
      init_profile_script_parts << "fi;"
      @@init_profile_script = init_profile_script_parts.join(" ")
    end
  
    return @@init_profile_script
  end
end

def ssh_result(command, host, user, return_object = false)
  SSHConnections.ssh_result(command, host, user, return_object)
end

def cmd_result(command, ignore_fail = false, hide_result = false)
  errors = ""
  result = ""
  threads = []
  
  Configurator.instance.debug("Execute `#{command}`")
  status = Open4::popen4("export LANG=en_US; #{command}") do |pid, stdin, stdout, stderr|
    stdin.close 
    
    threads << Thread.new{
      while data = stdout.gets()
        if data.to_s() != ""
          result+=data
        end
      end
    }
    threads << Thread.new{
      while edata = stderr.gets()
        if edata.to_s() != ""
          errors+=edata
        end
      end
    }
    
    threads.each{|t| t.join() }
  end
  
  result.strip!()
  errors.strip!()
  
  original_errors = errors
  rc = status.exitstatus
  
  if errors == ""
    errors = "No STDERR"
  else
    errors = "Errors: #{errors}"
  end

  if hide_result == true
    Configurator.instance.debug("RC: #{rc}, Result length: #{result.length}, #{errors}")
  else
    Configurator.instance.debug("RC: #{rc}, Result: #{result}, #{errors}")
  end
  
  if rc != 0 && ! ignore_fail
    raise CommandError.new(command, rc, result, original_errors)
  end

  return result
end

# Find out the full executable path or return nil
# if this is not executable. 
def which(cmd)
  if ! cmd
    nil
  else 
    path = cmd_result("which #{cmd}")
    path.chomp!
    if File.executable?(path)
      path
    else
      nil
    end
  end
end

def scp_result(local_file, remote_file, host, user)
  if Configurator.instance.display_help? && !Configurator.instance.display_preview?
    raise RemoteCommandNotAllowed.new("Copying '#{local_file}' not allowed because help mode is enabled")
  end
  
  if host == DEFAULTS
    debug("Unable to copy '#{local_file}' because '#{host}' is not valid")
    raise RemoteCommandError.new(user, host, "scp #{local_file} #{user}@#{host}:#{remote_file}", nil, '')
  end
  
  unless File.file?(local_file)
    debug("Unable to copy '#{local_file}' because it doesn't exist")
    raise MessageError.new("Unable to copy '#{local_file}' because it doesn't exist")
  end
  
  if Configurator.instance.is_localhost?(host) && 
      user == Configurator.instance.whoami()
    Configurator.instance.debug("Copy #{local_file} to #{remote_file}")
    return FileUtils.cp(local_file, remote_file)
  end
  
  Configurator.instance.synchronize() {
    unless defined?(Net::SCP)
      begin
        require "openssl"
      rescue LoadError
        Configurator.instance.error("Unable to find the Ruby openssl library")
        Configurator.instance.error("Try installing the openssl package for your version of Ruby (libopenssl-ruby#{RUBY_VERSION[0,3]}).")
        Configurator.instance.cleanup(1)
      end
      system_require 'net/scp'
    end
  }
  
  ssh_user = Configurator.instance.get_ssh_user(user)
  if user != ssh_user
    debug("SCP user changed to #{ssh_user}")
  end
  
  connection_error = "Net::SCP was unable to copy #{local_file} to #{host}:#{remote_file} as #{ssh_user}.  Check that #{host} is online, #{ssh_user} exists and your SSH private keyfile or ssh-agent settings. Try adding --net-ssh-option=port=<SSH port number> if you are using an SSH port other than 22.  Review http://docs.continuent.com/helpwithsshandtpm for more help on diagnosing SSH problems."
  Configurator.instance.debug("Copy #{local_file} to #{host}:#{remote_file} as #{ssh_user}")
  begin
    Net::SCP.start(host, ssh_user, Configurator.instance.get_ssh_options) do |scp|
      scp.upload!(local_file, remote_file, Configurator.instance.get_ssh_options)
    end
    
    if user != ssh_user
      ssh_result("sudo -n chown -R #{user} #{remote_file}", host, ssh_user)
    end
    
    return true
  rescue Errno::ENOENT => ee
    raise MessageError.new("Net::SCP was unable to find a private key to use for SSH authenticaton. Try creating a private keyfile or setting up ssh-agent.")
  rescue OpenSSL::PKey::RSAError
    raise MessageError.new(connection_error)
  rescue Net::SSH::AuthenticationFailed
    raise MessageError.new(connection_error)
  rescue Errno::ECONNREFUSED, Errno::EHOSTUNREACH
    raise MessageError.new(connection_error)
  rescue Timeout::Error
    raise MessageError.new(connection_error)
  rescue Exception => e
    raise RemoteCommandError.new(user, host, "scp #{local_file} #{ssh_user}@#{host}:#{remote_file}", nil, '')
  end
end

def remote_file_exists?(remote_file, host, user)
  begin
    exists = ssh_result("if [ -f #{remote_file} ]; then echo 0; else echo 1; fi", host, user)
    if exists == "1"
      return false
    else
      return true
    end
  rescue CommandError
    raise MessageError.new("Unable to check if '#{remote_file}' exists on #{host}")
  end
end

def scp_download(remote_file, local_file, host, user)
  if Configurator.instance.display_help? && !Configurator.instance.display_preview?
    raise RemoteCommandNotAllowed.new("Copying '#{local_file}' not allowed because help mode is enabled")
  end
  
  if host == DEFAULTS
    debug("Unable to download '#{remote_file}' because '#{host}' is not valid")
    raise RemoteCommandError.new(user, host, "scp #{user}@#{host}:#{remote_file} #{local_file}", nil, '')
  end
  
  if Configurator.instance.is_localhost?(host) && 
      user == Configurator.instance.whoami()
    Configurator.instance.debug("Copy #{remote_file} to #{local_file}")
    return FileUtils.cp(remote_file, local_file)
  end
  
  begin
    exists = ssh_result("if [ -f #{remote_file} ]; then echo 0; else echo 1; fi", host, user)
    if exists == "1"
      raise MessageError.new("Unable to download '#{remote_file}' because the file does not exist on #{host}")
    end
  rescue CommandError
    raise MessageError.new("Unable to check if '#{remote_file}' exists on #{host}")
  end
  
  unless defined?(Net::SCP)
    begin
      require "openssl"
    rescue LoadError
      error("Unable to find the Ruby openssl library")
      error("Try installing the openssl package for your version of Ruby (libopenssl-ruby#{RUBY_VERSION[0,3]}).")
      cleanup(1)
    end
    system_require 'net/scp'
  end
  
  ssh_user = Configurator.instance.get_ssh_user(user)
  if user != ssh_user
    debug("SCP user changed to #{ssh_user}")
  end
  
  connection_error = "Net::SCP was unable to copy to #{host}:#{remote_file} #{local_file} as #{ssh_user}.  Check that #{host} is online, #{ssh_user} exists and your SSH private keyfile or ssh-agent settings. Try adding --net-ssh-option=port=<SSH port number> if you are using an SSH port other than 22.  Review http://docs.continuent.com/helpwithsshandtpm for more help on diagnosing SSH problems."
  Configurator.instance.debug("Copy #{host}:#{remote_file} to #{local_file} as #{ssh_user}")
  begin
    Net::SCP.download!(host, ssh_user, remote_file, local_file, Configurator.instance.get_ssh_options)
    
    return true
  rescue Errno::ENOENT => ee
    raise MessageError.new("Net::SCP was unable to find a private key to use for SSH authenticaton. Try creating a private keyfile or setting up ssh-agent.")
  rescue OpenSSL::PKey::RSAError
    raise MessageError.new(connection_error)
  rescue Net::SSH::AuthenticationFailed
    raise MessageError.new(connection_error)
  rescue Errno::ECONNREFUSED, Errno::EHOSTUNREACH
    raise MessageError.new(connection_error)
  rescue Timeout::Error
    raise MessageError.new(connection_error)
  rescue Exception => e
    raise RemoteCommandError.new(user, host, "scp #{ssh_user}@#{host}:#{remote_file} #{local_file}", nil, '')
  end
end

def output_usage_line(argument, msg = "", default = nil, max_line = nil, additional_help = "")
  OutputHandler.add_usage(argument, msg, default, max_line, additional_help)
end

def fill_ports_near_hosts(host_list, port_to_add)
  initial_hosts = nil
  host_list.split(",").each { |host|
    host_addr = host.strip + "[" + port_to_add + "]"
    if initial_hosts
      initial_hosts = initial_hosts + "," + host_addr
    else
      initial_hosts = host_addr
    end
  }
  return initial_hosts
end

def to_identifier(str)
  str.tr('.', '_').tr('-', '_').downcase()
end

def include_dataservice?(ds, check_composite_dataservices = true)
  Configurator.instance.command.include_dataservice?(ds, check_composite_dataservices)
end

# The user has requested to save all current configuration values and exit
class ConfigureSaveConfigAndExit < StandardError
end

# The user has requested to accept the default value for the current and
# all remaining prompts
class ConfigureAcceptAllDefaults < StandardError
end

# The user has requested to return the previous prompt
class ConfigurePreviousPrompt < StandardError
end

class MessageError < StandardError
end

class CommandError < StandardError
  attr_reader :command, :rc, :result, :errors
  
  def initialize(command, rc, result, errors = "")
    @command = command
    @rc = rc
    @result = result
    @errors = errors
    
    super(build_message())
  end
  
  def build_message
    if errors == ""
      errors = "No STDERR"
    else
      errors = "Errors: #{errors}"
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
    if errors == ""
      errors = "No STDERR"
    else
      errors = "Errors: #{errors}"
    end
    
    "Failed: #{command}, RC: #{rc}, Result: #{result}, #{errors}"
  end
end

class RemoteCommandNotAllowed < CommandError
end

class OutputHandler
  @@queue_usage_output = false
  @@pending_usage = {}
  
  def self.queue_usage_output?(v = nil)
    if v != nil
      @@queue_usage_output = v
    end
    
    return @@queue_usage_output
  end
  
  def self.add_usage(argument, msg = "", default = nil, max_line = nil, additional_help = "")
    if self.queue_usage_output?()
      @@pending_usage[argument] = {
        :msg => msg,
        :default => default,
        :max_line => max_line,
        :additional_help => additional_help
      }
    else
      self.output_usage_line(argument, msg, default, max_line, additional_help)
    end
  end
  
  def self.flush_usage()
    @@pending_usage.keys().sort().each{
      |argument|
      h = @@pending_usage[argument]
      self.output_usage_line(argument, h[:msg], h[:default], h[:max_line], h[:additional_help])
    }
    @@pending_usage = {}
    self.queue_usage_output?(false)
  end
  
  def self.output_usage_line(argument, msg = "", default = nil, max_line = nil, additional_help = "")
    if max_line == nil
      max_line = Configurator.instance.detect_terminal_size[0]-5
    end

    if msg.is_a?(String)
      msg = msg.split("\n").join(" ")
    else
      msg = msg.to_s()
    end

    msg = msg.gsub(/^\s+/, "").gsub(/\s+$/, $/)

    if default.to_s() != ""
      if msg != ""
        msg += " "
      end

      msg += "[#{default}]"
    end

    if argument.length > 28 || (argument.length + msg.length > max_line)
      Configurator.instance.output(argument)

      words = msg.split(' ')

      force_add_word = true
      line = format("%-29s", " ")
      while words.length() > 0
        if !force_add_word && line.length() + words[0].length() > max_line
          Configurator.instance.output(line)
          line = format("%-29s", " ")
          force_add_word = true
        else
          line += " " + words.shift()
          force_add_word = false
        end
      end
      Configurator.instance.output(line)
    else
      Configurator.instance.output(format("%-29s", argument) + " " + msg)
    end

    if additional_help.to_s != ""
      additional_help = additional_help.split("\n").map!{
        |line|
        line.strip()
      }.join(' ')
      additional_help.split("<br>").each{
        |line|
        output_usage_line("", line, nil, max_line)
      }
    end
  end
end

module EventHandler
  def listen_event(name, obj = nil, func = nil, &p)
    if name.is_a?(String)
      name = name.to_sym
    end
    
    @_EventListeners ||= {}
    if block_given?
      (@_EventListeners[name] ||= []) << p
    else
      (@_EventListeners[name] ||= []) << obj.method(func)
    end
  end

  def ignore_event(name, obj, func)
    if name.is_a?(String)
      name = name.to_sym
    end
    
    @_EventListeners ||= {}
    return if !@_EventListeners.has_key?(name)
    @_EventListeners[name].delete_if { |o| o == obj.method(func) }
  end

  def trigger_event(name, *args)
    if name.is_a?(String)
      name = name.to_sym
    end
    Configurator.instance.debug("Trigger event #{name}")
    
    @_EventListeners ||= {}
    return if !@_EventListeners.has_key?(name)
    @_EventListeners[name].each { |f|  f.call(*args) }
  end
end

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
            keys().sort().each{|key|
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

class NetworkAdapter
  attr_reader :networks
end

module IniParse
  module Lines
    class Option
      def self.typecast(value)
        value
      end
    end
  end
end