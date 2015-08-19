module ConfigurePromptInterface
  include ConfigureMessages
  attr_accessor :name
  
  # Set the config object that this prompt should modify when saving values
  def set_config(config)
    @config = config
  end
  
  # The config hash key for this prompt
  def get_name
    @name
  end
  
  # The argument name that will be used to set this prompt from 
  # the command line
  def get_command_line_argument()
    @default_command_line_argument || @name.gsub("_", "-")
  end
  
  def get_command_line_aliases
    @command_line_aliases || []
  end
  
  def override_command_line_argument(arg)
    @command_line_aliases ||= []
    @command_line_aliases << get_command_line_argument()
    @default_command_line_argument = arg
  end
  
  def add_command_line_alias(arg)
    @command_line_aliases ||= []
    @command_line_aliases << arg
  end
  
  # The value to set when the command line argument is present
  # If this is not nil, no value will be accepted from the command line
  def get_command_line_argument_value
    nil
  end
  
  # Are all class variables specified
  def is_initialized?
    raise "Undefined function: is_initialized?"
  end
  
  # Interact with the user to get the value for this prompt
  def run
    raise "Undefined function: run"
  end
  
  def save_system_default
    raise "Undefined function: save_system_default"
  end
  
  def prepare_saved_config_value(is_server_config = false)
    raise "Undefined function: prepare_saved_config_value"
  end
  
  # Check that the value currently in the config object is valid
  def validate
    raise "Undefined function: validate"
  end
  
  # Get the list of config hash keys that are allowed for this prompt
  def get_keys
    raise "Undefined function: get_keys"
  end
  
  def required?
    enabled?()
  end
  
  # Does the user need to answer this prompt
  def enabled?
    true
  end
  
  # Is this value allowed in the config file
  # If a prompt is not needed based on other responses, we do not let 
  # them set the value in the config to avoid confusion
  def enabled_for_config?
    enabled?()
  end
  
  # Is this value accepted from the command line
  def enabled_for_command_line?()
    true
  end
  
  # Is the value allowed to be used in template files
  def enabled_for_template_file?()
    true
  end
  
  # Validate the response against the prompt validation rules
  def accept?(raw_value)
    # Sending 'autodetect' should store a blank value in the config
    if raw_value == AUTODETECT
      return nil
    end
    
    if @validator
      @validator.validate raw_value
    else
      raw_value
    end
  end
  
  # Build the description filename based on the config key
  def get_prompt_description_filename()
    "#{get_interface_text_directory()}/prompt_#{get_name()}"
  end
  
  def get_interface_text_directory
    "#{Configurator.instance.get_base_path()}/#{Configurator.instance.get_ruby_prefix()}/configure/interface_text"
  end
  
  # Read the help from the prompt help file
  def get_prompt_description()
    description_filename = get_prompt_description_filename()
    unless File.exists?(description_filename)
      return nil
    end
    
    description = ''
    f = File.open(description_filename, "r") 
    f.each_line do |line|
      description += line.gsub(/\n/," ").scan(/\S.{0,#{70-2}}\S(?=\s|$)|\S+/).join("\n") + "\n"
    end
    f.close
    
    return description
  end
  
  def include_command_line_aliases_in_output_usage?
    true
  end
  
  # Output how to set this value from the command line
  def output_usage
    output_usage_line("--#{get_command_line_argument()}", get_prompt(), get_output_usage_value(), nil, get_prompt_description())
    
    if include_command_line_aliases_in_output_usage?() == true
      get_command_line_aliases().each{
        |a|
        if a == get_command_line_argument()
          next
        end
        output_usage_line("--#{a}", get_prompt() + " (Alias of --#{get_command_line_argument()})", get_output_usage_value(), nil, get_prompt_description())
      }
    end
  end
  
  def get_bash_completion_arguments
    if get_command_line_argument_value() == nil
      equals = "="
    else
      equals = ""
    end
    
    ["--#{get_command_line_argument()}#{equals}"]
  end
  
  def get_output_usage_value
    get_value(true, true)
  end
  
  # Output how to specify this value in a config file
  def output_config_file_usage
    output_usage_line(get_config_file_usage_symbol(), get_prompt(), get_default_value())
  end
  
  # The config hash key to output in output_config_file_usage
  def get_config_file_usage_symbol
    get_name()
  end
  
  # Output how to specify this value in a template file
  def output_template_file_usage
    if enabled_for_command_line?()
      output_usage_line(get_template_file_usage_symbol(), "--#{get_command_line_argument()}")
    else
      output_usage_line(get_template_file_usage_symbol(), get_prompt())
    end
  end
  
  # The template parameter to output in output_template_file_usage
  def get_template_file_usage_symbol
    Configurator.instance.get_constant_symbol(@name)
  end
  
  # Update the config object so that old values are set to their new keys
  def update_deprecated_keys()
  end
  
  def get_userid
    nil
  end
  
  def get_hostname
    nil
  end
  
  def get_error_object_class
    ConfigurePromptError
  end
  
  def build_error_object(message)
    begin
      Timeout.timeout(2) {
        val = get_value()
      }
    rescue
      val = ""
    end
    
    get_error_object_class().new(self.clone(), message, val.to_s)
  end
  
  def skip_class_validation?()
    if ConfigureValidationHandler.skip_validation_class?(self.class.name, @config)
      true
    else
      false
    end
  end
  
  def value_is_different?(old_cfg)
    raise IgnoreError
  end
  
  def get_updated_keys(old_cfg)
    raise IgnoreError
  end
  
  def allow_inplace_upgrade?
    true
  end
  
  def require_replicator_restart?
    true
  end
  
  def require_manager_restart?
    true
  end
  
  def require_connector_restart?
    true
  end
  
  def allow_connector_reconfigure?
    true
  end

  def output_update_components
    unless enabled_for_command_line?()
      return
    end

    messages = ["--" + get_command_line_argument()]
    components = []

    unless allow_inplace_upgrade?()
      messages << "New directory"
    else
      if require_replicator_restart?()
        components << "Replicator"
      end

      if require_manager_restart?()
        components << "Manager"
      end

      if require_connector_restart?()
        components << "Connector"
      end

      if components.size() > 0
        messages << components.join(', ') + " restart"
      end
    end

    output(messages.join(' '))
  end
end

module NewDirectoryUpdate
  def allow_inplace_upgrade?
    false
  end
end

module NoReplicatorRestart
  def require_replicator_restart?
    false
  end
end

module ReplicatorRestart
  def require_replicator_restart?
    true
  end
end

module NoConnectorRestart
  def require_connector_restart?
    false
  end
end

module NoConnectorReconfigure
  def allow_connector_reconfigure?
    false
  end
end

module ConnectorRestart
  def require_connector_restart?
    true
  end
end

module ConnectorReconfigure
  def allow_connector_reconfigure?
    true
  end
end

module NoManagerRestart
  def require_manager_restart?
    false
  end
end

module ManagerRestart
  def require_manager_restart?
    true
  end
end

module NoTemplateValuePrompt
  def enabled_for_template_file?()
    false
  end
end