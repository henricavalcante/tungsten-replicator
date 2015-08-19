module ValidationCheckInterface
  include ConfigureMessages
  
  attr_reader :title, :description, :properties, :messages, :fatal_on_error
  def initialize
    @title = nil
    @description = ""
    @properties = []
    @config = nil
    @fatal_on_error = false
    @help = []
    reset_errors()
    
    set_vars()
    
    if @title == nil
      raise "'title' has not been set"
    end
    
    super()
  end
  
  def run
    reset_errors()
    
    unless enabled?()
      return
    end
    
    begin
      info("Start: #{@title}")
      validate()
    rescue MessageError => me
      error(me.to_s())
    rescue Exception => e
      exception(e)
    ensure
      info("Finish: #{@title}")
    end
  end
  
  def set_vars
    raise "The 'set_vars' function should be overwritten"
  end
  
  def validate
    class_name = self.class().name()
    error("The 'validate' function for #{class_name} should be overwritten")
  end
  
  def enabled?
    if ConfigureValidationHandler.skip_validation_class?(self.class.name, @config)
      debug("Skipping validation check '#{self.class.name}'")
      false
    else
      true
    end
  end
  
  def fatal_on_error?
    @fatal_on_error
  end
  
  def set_config(config)
    @config = config
  end
  
  def is_initialized?
    (@title != nil)
  end
  
  def help(message)
    @help << message
  end
  
  def get_help
    if @help.empty?()
      nil
    else
      @help
    end
  end
  
  def get_userid
    @config.getProperty(USERID)
  end
  
  def get_hostname
    @config.getProperty(HOST)
  end
  
  def get_message_hostname
    @config.getProperty(DEPLOYMENT_HOST)
  end
  
  def get_message_host_key
    @config.getProperty([DEPLOYMENT_CONFIGURATION_KEY])
  end
  
  def build_error_object(message)
    get_error_object_class().new(message, get_message_hostname(), self.dup)
  end
  
  def get_error_object_class
    ValidationError
  end
  
  def skip_class_warnings?()
    ConfigureValidationHandler.skip_validation_warnings?(self.class.name, @config)
  end
  
  def skip_class_validation?()
    ConfigureValidationHandler.skip_validation_class?(self.class.name, @config)
  end
  
  def use_firewall_listeners?
    false
  end
  
  def reset_errors
    super()
    @help = []
  end
end