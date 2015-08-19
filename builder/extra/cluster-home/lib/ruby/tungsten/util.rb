class TungstenUtil
  include Singleton
  attr_accessor :remaining_arguments, :extra_arguments
  
  def initialize()
    super()
    
    @logger_threshold = Logger::NOTICE
    @previous_option_arguments = {}
    @ssh_options = {}
    @display_help = false
    @error_mutex = Mutex.new
    @num_errors = 0
    @force = false
    @json_interface = false
    @json_message_cache = []
    
    # Create a temporary file to hold log contents
    @log = Tempfile.new("tlog")
    # Unlink the file so that no other process can read this log
    @log.unlink()
    
    arguments = ARGV.dup
                  
    if arguments.size() > 0
      @extra_arguments = []
      
      # This fixes an error that was coming up reading certain characters wrong
      # The root cause is unknown but this allowed Ruby to parse the 
      # arguments properly
      send_to_extra_arguments = false
      arguments = arguments.map{|arg|
        newarg = ''
        arg.split("").each{|b| 
          unless b.getbyte(0)<32 || b.getbyte(0)>127 then 
            newarg.concat(b) 
          end
        }
        
        if newarg == "--"
          send_to_extra_arguments = true
          nil
        elsif send_to_extra_arguments == true
          @extra_arguments << newarg
          nil
        else
          newarg
        end
      }

      opts=OptionParser.new
      opts.on("-f", "--force")    {@force = true}
      opts.on("-i", "--info")     {@logger_threshold = Logger::INFO}
      opts.on("-n", "--notice")               {@logger_threshold = Logger::NOTICE}
      opts.on("-q", "--quiet")          {@logger_threshold = Logger::WARN}
      opts.on("-v", "--verbose")        {@logger_threshold = Logger::DEBUG}
      opts.on("--json")                 { @json_interface = true }
      opts.on("-h", "--help")           { @display_help = true }
      opts.on("--net-ssh-option String")  {|val|
                                          val_parts = val.split("=")
                                          if val_parts.length() !=2
                                            error "Invalid value #{val} given for '--net-ssh-option'.  There should be a key/value pair joined by a single =."
                                          end
                                        
                                          if val_parts[0] == "timeout"
                                            val_parts[1] = val_parts[1].to_i
                                          end

                                          @ssh_options[val_parts[0].to_sym] = val_parts[1]
                                        }
                           
      log(JSON.pretty_generate(arguments))
      @remaining_arguments = run_option_parser(opts, arguments)
    else
      @remaining_arguments = []
      @extra_arguments = []
    end
  end
  
  def exit(code = 0)
    if @json_interface == true
      puts JSON.pretty_generate({
        "rc" => code,
        "messages" => @json_message_cache
      })
    end
    
    Kernel.exit(code)
  end
    
  def display_help?
    (@display_help == true)
  end
  
  def display_help
    write_header("Global Options", nil)
    output_usage_line("--directory", "Use this installed Tungsten directory as the base for all operations")
    output_usage_line("--quiet, -q")
    output_usage_line("--info, -i")
    output_usage_line("--notice, -n")
    output_usage_line("--verbose, -v")
    output_usage_line("--help, -h", "Display this message")
    output_usage_line("--json", "Provide return code and logging messages as a JSON object after the script finishes")
    output_usage_line("--net-ssh-option=key=value", "Set the Net::SSH option for remote system calls", nil, nil, "Valid options can be found at http://net-ssh.github.com/ssh/v2/api/classes/Net/SSH.html#M000002")
  end
  
  def get_autocomplete_arguments
    [
      '--directory',
      '--quiet', '-q',
      '--info', '-i',
      '--notice', '-n',
      '--verbose', '-v',
      '--help', '-h',
      '--json',
      '--net-ssh-option='
    ]
  end
  
  def get_base_path
    File.expand_path("#{File.dirname(__FILE__)}/../../../..")
  end
  
  def enable_output?
    true
  end
  
  def output(content)
    write(content, nil)
  end
  
  def force_output(content)
    log(content)
    
    puts(content)
    $stdout.flush()
  end
  
  def log(content = nil)
    if content == nil
      @log
    else
      @log.puts DateTime.now.to_s + " " + content
      @log.flush
    end
  end
  
  def set_log_path(path)
    TU.mkdir_if_absent(File.dirname(path))
    old_log = @log
    old_log.rewind()
    
    @log = File.open(path, "w")
    @log.puts(old_log.read())
  end
  
  def reset_errors
    @num_errors = 0
  end
  
  def force?
    @force
  end
  
  def is_valid?
    (@num_errors == 0 || @force == true)
  end
  
  def write(content="", level=Logger::INFO, hostname = nil, add_prefix = true)
    if content.is_a?(Array)
      content.each{
        |c|
        write(c, level, hostname, add_prefix)
      }
      return
    end
    
    # Attempt to determine the level for this message based on it's content
    # If it is forwarded from another Tungsten script it will have a prefix
    # so we know to use stdout or stderr
    if level == nil
      level = parse_log_level(content)
    end
    
    if level == Logger::ERROR
      @error_mutex.synchronize do
        @num_errors = @num_errors + 1
      end
      
      if force?()
        level = Logger::WARN
      end
    end
    
    unless content == "" || level == nil || add_prefix == false
      content = "#{get_log_level_prefix(level, hostname)}#{content}"
    end
    
    log(content)
    
    if enable_log_level?(level)
      if @json_interface == true
        @json_message_cache << content
      else
        if enable_output?()
          if level != nil && level > Logger::NOTICE
            $stdout.puts(content)
            $stdout.flush()
          else
            $stdout.puts(content)
            $stdout.flush()
          end
        end
      end
    end
  end
  
  # Write a header
  def write_header(content, level=Logger::INFO)
    write("#####################################################################", level, nil, false)
    write("# #{content}", level, nil, false)
    write("#####################################################################", level, nil, false)
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
    debug(e, hostname)
  end
  
  def debug(message, hostname = nil)
    if message.is_a?(StandardError)
      message = message.to_s() + ":\n" + message.backtrace.join("\n")
    end
    write(message, Logger::DEBUG, hostname)
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
    
    if hostname == nil
      "#{prefix} >> "
    else
      "#{prefix} >> #{hostname} >> "
    end
  end
  
  def parse_log_level(line)
    prefix = line[0,5]
    
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
  
  # Split a log line into the log level and actual message
  # This is used when forwarding log messages from a remote commmand
  def split_log_content(content)
    level = parse_log_level(content)
    if level != nil
      prefix = get_log_level_prefix(level)
      content = content[prefix.length, content.length]
    end
    
    return [level, content]
  end
  
  def enable_log_level?(level=Logger::INFO)
    if level == nil
      true
    elsif level < @logger_threshold
      false
    else
      true
    end
  end
  
  def get_log_level
    @logger_threshold
  end
  
  def set_log_level(level=Logger::INFO)
    @logger_threshold = level
  end
  
  def whoami
    if ENV['USER']
      ENV['USER']
    elsif ENV['LOGNAME']
      ENV['LOGNAME']
    else
      `whoami 2>/dev/null`.chomp
    end
  end
  
  # Run the OptionParser object against @remaining_arguments or the 
  # {arguments} passed in. If no {arguments} value is given, the function
  # will read from @remaining_arguments and update it with any remaining
  # values
  def run_option_parser(opts, arguments = nil, allow_invalid_options = true, invalid_option_prefix = nil)
    if arguments == nil
      use_remaining_arguments = true
      arguments = @remaining_arguments
    else
      use_remaining_arguments = false
    end
    
    # Collect the list of options and remove the first two that are 
    # created by default
    option_lists = opts.stack()
    option_lists.shift()
    option_lists.shift()
    
    option_lists.each{
      |ol|
      
      ol.short.keys().each{
        |arg|
        if @previous_option_arguments.has_key?(arg)
          error("The -#{arg} argument has already been captured")
        end
        @previous_option_arguments[arg] = true
      }
      ol.long.keys().each{
        |arg|
        if @previous_option_arguments.has_key?(arg)
          error("The --#{arg} argument has already been captured")
        end
        @previous_option_arguments[arg] = true
      }
    }
    
    remainder = []
    while arguments.size() > 0
      begin
        arguments = opts.order!(arguments)
        
        # The next argument does not have a dash so the OptionParser
        # ignores it, we will add it to the stack and continue
        if arguments.size() > 0 && (arguments[0] =~ /-.*/) != 0
          remainder << arguments.shift()
        end
      rescue OptionParser::InvalidOption => io
        if allow_invalid_options
          # Prepend the invalid option onto the arguments array
          remainder = remainder + io.recover([])
        
          # The next argument does not have a dash so the OptionParser
          # ignores it, we will add it to the stack and continue
          if arguments.size() > 0 && (arguments[0] =~ /-.*/) != 0
            remainder << arguments.shift()
          end
        else
          if invalid_option_prefix != nil
            io.reason = invalid_option_prefix
          end
          
          raise io
        end
      rescue => e
        exception(e)
        raise "Argument parsing failed: #{e.to_s()}"
      end
    end
    
    if use_remaining_arguments == true
      @remaining_arguments = remainder
      return nil
    else
      return remainder
    end
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
  
  # Display information about the argument formatting {msg} so all lines
  # appear formatted correctly
  def output_usage_line(argument, msg = "", default = nil, max_line = nil, additional_help = "")
    if max_line == nil
      max_line = detect_terminal_size()[0]-5
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
      output(argument)

      wrapped_lines(msg, 29).each{
        |line|
        output(line)
      }
    else
      output(format("%-29s", argument) + " " + msg)
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
  
  # Break {msg} into lines that have {offset} leading spaces and do
  # not exceed {max_line}
  def wrapped_lines(msg, offset = 0, max_line = nil)
    if max_line == nil
      max_line = detect_terminal_size()[0]-5
    end
    if offset == 0
      default_line = ""
    else
      line_format = "%-#{offset}s"
      default_line = format(line_format, " ")
    end
    
    lines = []
    words = msg.split(' ')

    force_add_word = true
    line = default_line.dup()
    while words.length() > 0
      if !force_add_word && line.length() + words[0].length() > max_line
        lines << line
        line = default_line.dup()
        force_add_word = true
      else
        if line == ""
          line = words.shift()
        else
          line += " " + words.shift()
        end
        force_add_word = false
      end
    end
    lines << line
    
    return lines
  end
  
  def to_identifier(str)
    str.tr('.', '_').tr('-', '_').tr('/', '_').tr('\\', '_').downcase()
  end
  
  # Create a directory if it is absent. 
  def mkdir_if_absent(dirname)
    if dirname == nil
      return
    end
    
    if File.exists?(dirname)
      if File.directory?(dirname)
        debug("Found directory, no need to create: #{dirname}")
      else
        raise "Directory already exists as a file: #{dirname}"
      end
    else
      debug("Creating missing directory: #{dirname}")
      cmd_result("mkdir -p #{dirname}")
    end
  end
  
  def pluralize(array, singular, plural = nil)
    if plural == nil
      singular
    elsif array.size() > 1
      plural
    else
      singular
    end
  end
  
  # Read an INI file and return a hash of the arguments for each section
  def parse_ini_file(file, convert_to_hash = true)
    unless defined?(IniParse)
      require 'tungsten/iniparse'
    end
    
    unless File.exists?(file)
      return
    end
    
    # Read each section and turn the lines into an array of arguments as
    # they would appear in the INI file
    options = {}
    ini = IniParse.open(file)
    ini.each{
      |section|
      key = section.key
      
      # Initialize the array only if it doesn't exist
      # Doing otherwise would overwrite old sections
      unless options.has_key?(key)
        options[key] = []
      end
      
      section.each{
        |line|
        unless line.is_a?(Array)
          values = [line]
        else
          values = line
        end
      
        values.each{
          |value|
          if value.value == nil
            options[key] << "#{value.key}"
          else
            options[key] << "#{value.key}=#{value.value.to_s()}"
          end
        }
      }
    }
    
    # Most users will want a Hash, but this will allow the main
    # TungstenScript class to see the parameters in order
    if convert_to_hash == true
      return convert_ini_array_to_hash(options)
    else
      return options
    end
  end
  
  # Convert the information returned by parse_ini_file into a
  # nested hash of values
  def convert_ini_array_to_hash(options)
    hash = {}
    
    options.each{
      |section,lines|
      unless hash.has_key?(section)
        hash[section] = {}
      end
      
      lines.each{
        |line|
        parts = line.split("=")
        
        # The first part is the argument name
        argument = parts.shift()
        # Any remaining values will be returned as a single string
        # A nil value means there was no value after the '='
        if parts.size() == 0
          v = nil
        else
          v = parts.join("=")
        end
        
        # Return repeat arguments as an array of values
        if hash[section].has_key?(argument)
          unless hash[section][argument].is_a?(Array)
            hash[section][argument] = [hash[section][argument]]
          end
          
          hash[section][argument] << v
        else
          hash[section][argument] = v
        end
      }
    }
    
    return hash
  end
end