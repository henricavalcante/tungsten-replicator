module ConfigureMessages
  attr_reader :errors, :output_properties
  
  def initialize
    reset_errors()
  end
  
  def reset_errors
    @errors = []
    @output_properties = Properties.new
    @output_properties.use_prompt_handler = false
  end
  
  def is_valid?()
    @errors ||= []
    
    @errors.each{
      |error|
      if error.is_fatal?
        return false
      end
    }
    
    true
  end
  
  def include_messages_object(obj)
    @errors = @errors + obj.errors
    obj.output_properties.props.each{
      |key,value|
      @output_properties.setProperty(key, value)
    }
  end
  
  def add_remote_result(result)
    result.errors.each { |err| @errors << err }
    result.properties.props.each{
      |key,value|
      @output_properties.setProperty(key, value)
    }
  end
  
  def get_remote_result
    result = RemoteResult.new()
    result.errors = @errors
    result.replace(@output_properties)
    return result
  end
  
  def output_property(key, value = nil)
    host_key = get_message_host_key()
    if host_key == nil
      @output_properties.setProperty(key, value)
    else
      @output_properties.setProperty("#{host_key}.#{key}", value)
    end
  end
  
  def get_output_property(key)
    host_key = get_message_host_key()
    if host_key == nil
      @output_properties.getProperty(key)
    else
      @output_properties.getProperty("#{host_key}.#{key}")
    end
  end
  
  def info(message)
    Configurator.instance.info(message, get_message_hostname())
  end
  
  def notice(message)
    Configurator.instance.notice(message, get_message_hostname())
  end
  
  def warning(message)
    Configurator.instance.warning(message, get_message_hostname())
  end
  
  def confirm(message, c = nil)
    warning(message)
    
    store_error_object(c || build_confirmation_object(message))
  end
  
  def build_confirmation_object(message)
    get_confirmation_object_class().new(message, get_message_hostname())
  end
  
  def get_confirmation_object_class
    RemoteConfirmation
  end
  
  def error(message, e = nil)
    Configurator.instance.error(message, get_message_hostname())

    store_error_object(e || build_error_object(message))
  end
  
  def exception(e)
    Configurator.instance.error(e.to_s(), get_message_hostname())
    Configurator.instance.debug(e.message + "\n" + e.backtrace.join("\n"), get_message_hostname())
    
    if e.is_a?(get_error_object_class())
      store_error_object(e)
    else
      store_error_object(build_error_object(e.to_s))
    end
  end
  
  def write_header(content, level=Logger::INFO)
    Configurator.instance.write_header(content, level)
  end
  
  def write_divider(level=Logger::INFO)
    Configurator.instance.write_divider(level)
  end
  
  def write_from_file(filename, level=Logger::INFO)
    Configurator.instance.write_from_file(filename, level)
  end
  
  def write(content, level=Logger::INFO)
    Configurator.instance.write(content, level, nil, false)
  end

  def store_error_object(e = nil)
    @errors ||= []
    @errors.push(e)
  end
  
  def build_error_object(message)
    get_error_object_class().new(message, get_message_hostname())
  end
  
  def get_error_object_class
    RemoteError
  end
  
  def debug(message)
    Configurator.instance.debug(message, get_message_hostname())
  end
  
  def trace(message)
  end
  
  def get_message_hostname
    nil
  end
  
  def get_message_host_key
    nil
  end
  
  def force_output(content)
    Configurator.instance.force_output(content)
  end
  
  def output(content)
    Configurator.instance.output(content)
  end
  
  def output_errors
    host_errors = Hash.new()
    generic_errors = []
    
    @errors.each{
      |error|
      
      if (error.is_a?(ValidationError) || error.is_a?(RemoteError)) && error.host.to_s() != ""
        unless host_errors.has_key?(error.host)
          host_errors[error.host] = []
        end
        host_errors[error.host] << error
      else  
        generic_errors << error
      end
    }
    
    unless generic_errors.empty?()
      generic_errors.each{
        |generic_error|
        generic_error.output()
      }
    end
    
    host_errors.each_key{
      |host|
      
      $i = 0

      Configurator.instance.write_header("Errors for #{host}", Logger::ERROR)
      until $i >= host_errors[host].length() do
        $host_error = host_errors[host][$i]
        $i+=1
        $next_host_error = host_errors[host][$i]
        
        Configurator.instance.error($host_error.get_message(), host)
        
        begin
          if $next_host_error == nil || $host_error.check.class != $next_host_error.check.class
            if $host_error.is_a?(ValidationError)
              help = $host_error.get_help()
              unless help == nil || help.empty?()
                Configurator.instance.output(help.join("\n"))
              end
        
              unless help == nil || help.empty?()
                Configurator.instance.write_divider(Logger::ERROR)
              end
            end
          end
        rescue NoMethodError
        end
      end
    }
  end
end

class RemoteError < StandardError
  attr_reader :message, :host

  def initialize(message, host = nil)
    if host == nil
      host = `hostname`.chomp()
    end
    
    @message=message
    @host=host
  end
  
  def output
    Configurator.instance.error(get_message())
  end
  
  def get_message
    @message
  end
  
  def is_fatal?
    true
  end
  
  def clean
  end
end

class RemoteConfirmation < RemoteError
  def initialize(message, host = nil, validator = nil)
    super(message, host)
    
    if validator == nil
      validator = PV_CONFIRMATION
    end
      
    @validator = validator
  end
  
  def is_fatal?
    if Configurator.instance.forced?()
      return false
    end
    
    case get_confirmation_value().to_s().downcase()
    when "y", "yes"
      return false
    else
      return true
    end
  end
  
  def get_confirmation_value
    if @value
      return @value
    end
    
    unless Configurator.instance.has_tty?
      return nil
    end
    
    value = ""
    while value.to_s == ""
      puts "
#{@host} >> #{@message}  
Do you want to continue with the configuration (Y) or quit (Q)?"
      value = STDIN.gets
      
      begin
        value = @validator.validate(value.strip!)
      rescue => e
        Configurator.instance.output(e.message)
        value = nil
      end
    end
    
    @value = value
  end
end

class RemoteResult
  attr_accessor :errors, :properties
  
  def initialize
    @errors = []
    @properties = Properties.new()
    @properties.use_prompt_handler = false
  end
  
  def get(key)
    @properties.getProperty(key)
  end
  
  def set(key, value = nil)
    return @properties.setProperty(key, value)
  end
  
  def replace(new_properties)
    if new_properties
      return (@properties = new_properties.dup())
    end
  end
  
  def clean
    @errors.each{
      |e|
      e.clean()
    }
  end
end