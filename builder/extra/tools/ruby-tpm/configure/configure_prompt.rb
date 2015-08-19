# The parent class for each configure prompt.  It is responsible for
# collecting a value from the command line, validating it and placing it
# in the config object
class ConfigurePrompt
  include ConfigurePromptInterface
  @@global_defaults = {}
  
  def self.add_global_default(key, value)
    if value == nil
      raise("Unable to make a global replacement using a nil value")
    end
    
    @@global_defaults[key] = value
  end
  
  def self.get_global_default(key)
    if @@global_defaults.has_key?(key)
      return @@global_defaults[key]
    else
      return nil
    end
  end
  
  def self.get_global_defaults
    return @@global_defaults
  end

  def initialize(name, prompt, validator = nil, default = nil)
    @name = name
    @prompt = prompt
    @validator = validator
    @class_default = default
    @default = nil
    @config = nil
  end
  
  # Used to ensure that the basics have been set
  def is_initialized?
    if get_name() == nil || get_prompt() == nil
      false
    else
      true
    end
  end
  
  def get_stored_value()
    @config.getNestedProperty(get_name())
  end
  
  # Get the current value for the prompt, use the default if the config does
  # not have a response for the given config key
  def get_value(allow_default = true, allow_disabled = false)
    value = @config.getProperty(get_name(), allow_disabled)
    if value == nil && allow_default && (enabled_for_config?() || allow_disabled)
      value = get_default_value()
    end
        
    value
  end
  
  def get_template_value
    get_value()
  end
  
  def save_system_default
    begin
      unless Configurator.instance.command.include_host?(get_host_alias())
        trace("Skipping save_system_default of prompt '#{self.class.name}:#{get_name()}' because of --hosts")
        return
      end
    rescue NoMethodError
    end
    
    unless enabled_for_command_dataservice?()
      trace("Skipping save_system_default of prompt '#{self.class.name}:#{get_name()}' because of the dataservice list")
      return
    end
    
    @config.setProperty([SYSTEM] + get_name().split('.'), get_default_value())
  end
  
  def prepare_saved_config_value(is_server_config = false)
  end
  
  # Get the value that should be set if this prompt is disabled
  def get_disabled_value
    nil
  end
  
  def validate
    reset_errors()
    if skip_class_validation?()
      trace("Skipping validation of prompt '#{self.class.name}:#{get_name()}' because of --skip-validation-check")
      return
    end
    
    unless enabled_for_command?()
      return
    end
    
    if enabled_for_config?
      value = get_value()
      if value == nil && required?()
        # The prompt is enabled, the value should not be missing
        error("Value is missing")
      elsif value != nil
        validate_value(value)
      end
    else
      value = get_value(false)
      if value.to_s() == ""
        if get_disabled_value() == nil
          # The prompt is disabled, no value should be given
        elsif required?()
          error("Value is missing")
        end
      else
        if get_disabled_value() == nil
          # The prompt is disabled, the value should be empty
          error("Value should not be given, remove it from the configuration")
        end
      end
    end
    
    is_valid?()
  end
  
  def enabled_for_command?
    begin
      unless Configurator.instance.command.include_host?(get_host_alias())
        # Skipping because of --hosts
        return false
      end
    rescue NoMethodError
    end
    
    unless enabled_for_command_dataservice?()
      # Skipping because of dataservice list
      return false
    end
    
    return true
  end
  
  def enabled_for_command_dataservice?
    begin
      unless Configurator.instance.command.include_dataservice?(get_dataservice_alias())
        return false
      end
    rescue NoMethodError
    end
    
    return true
  end
  
  def validate_value(value)
    begin
      return accept?(value.to_s())
    rescue Exception => e
      # There was an issue in the validation
      error(e.to_s)
    end
  end
  
  def get_keys
    [@name]
  end
  
  # Get the prompt text
  def get_prompt
    @prompt
  end
  
  def get_display_prompt
    get_prompt()
  end
  
  def query_class_default_value
    @class_default
  end
  
  # Get the default value for the prompt
  def get_default_value
    if (v = @config.getNestedProperty([SYSTEM] + get_name().split('.')))
      return v
    end
    
    global_default = ConfigurePrompt.get_global_default(@name)
    if global_default == nil && enabled_for_command_line?()
      global_default = ConfigurePrompt.get_global_default("--#{get_command_line_argument()}")
    end
    if global_default != nil
      begin
        @default = accept?(global_default)
      rescue
        @default = global_default
      end
      
      if @default != nil
        return @default.to_s
      else
        return nil
      end
    end
    
    if enabled_for_load_default_value?()
      begin
        Timeout.timeout(get_timeout_length()) {
          load_default_value()
        }
      rescue Timeout::Error
        @default = @class_default
      rescue IgnoreError
        @default = @class_default
      end
    end
    
    if @default != nil
      unless @default.is_a?(Hash) || @default.is_a?(Array)
        @config.setProperty([SYSTEM] + get_name().split('.'), @default.to_s)
        return @default.to_s
      else
        @config.setProperty([SYSTEM] + get_name().split('.'), @default)
        return @default
      end
    else
      return nil
    end
  end
  
  def get_timeout_length
    10
  end
  
  def enabled_for_load_default_value?
    if Configurator.instance.display_help? && !Configurator.instance.display_preview?
      return false
    end
    
    return enabled?()
  end
  
  def load_default_value
    @default = @class_default
  end
  
  def find_prompt_by_name(name)
    if name != @name
      raise IgnoreError
    end
    
    return self
  end
  
  def find_prompt(attrs)
    if attrs.join('.') != get_name()
      raise IgnoreError
    end
    
    return self
  end
  
  def get_property(attrs, allow_disabled = false)
    if attrs[0] != @name
      raise IgnoreError
    end
    
    if attrs.size > 1
      raise "Unable to get_property:#{attrs.join('.')} for #{self.class.name}"
    end
    
    value = get_value(true, allow_disabled)    
    if @validator.is_a?(FilePropertyValidator) || @validator == PV_FILENAME
      value = format_filename_property(value)
    end
    
    return value
  end
  
  def format_filename_property(value)
    begin
      if value.to_s == ""
        return value
      end
      
      unless value.index('$') || value.index('~')
        return value
      end
      
      @cache ||= {}
      userid = get_userid()
      host = get_hostname()
      
      if userid == nil || host == nil
        raise IgnoreError
      end
      
      cache_key = "#{userid}@#{host}:#{value}"
      unless @cache.has_key?(cache_key)
        @cache[cache_key] = ssh_result("echo #{value}", host, userid)
      end
      
      value = @cache[cache_key]
    rescue
    end
    
    return value
  end
  
  def find_template_value(attrs)
    if attrs[0] != @name
      raise IgnoreError
    end
    
    if attrs.size > 1
      raise "Unable to find_template_value:#{attrs.join('.')} for #{self.class.name}"
    end
    
    get_template_value()
  end
  
  def replace_deprecated_key(deprecated_key)
    if (v = @config.getProperty(deprecated_key)) != nil
      @config.setProperty(get_name(), v)
      @config.setProperty(deprecated_key, nil)
    end
  end
  
  def build_command_line_argument?(member, v)
    enabled_for_command_line?()
  end
  
  def build_command_line_argument(member, v, public_argument = false)
    if build_command_line_argument?(member, v)
      return ["--#{get_command_line_argument()}=#{v}"]
    else
      debug("The argument for #{@name} is not accepted on the command line")
      raise IgnoreError
    end
  end
  
  def value_is_different?(old_cfg)
    old_value = old_cfg.getProperty(get_name())
    if get_value() != old_value
      true
    else
      false
    end
  end
  
  def get_updated_keys(old_cfg)
    unless enabled_for_command?()
      return []
    end
    
    if value_is_different?(old_cfg)
      return [get_name()]
    else
      return []
    end
  end
  
  def get_command_line_argument_value
    if @validator == PV_BOOLEAN
      "true"
    else
      super()
    end
  end
end

module OptionalPromptModule
  def required?
    false
  end
end

module AdvancedPromptModule
  def enabled?
    super() && (Configurator.instance.advanced_mode?())
  end
  
  def enabled_for_command_line?
    if Configurator.instance.display_help?()
      unless Configurator.instance.advanced_mode?()
        return false
      end
    end
    
    super()
  end
  
  def enabled_for_config?
    true
  end
  
  def get_disabled_value
    if enabled_for_config?
      get_value()
    else
      super()
    end
  end
  
  def enabled_for_load_default_value?
    if Configurator.instance.display_help? && !Configurator.instance.display_preview?
      return false
    end
    
    return enabled_for_config?()
  end
end

module ConstantValueModule
  def enabled?
    false
  end
  
  def enabled_for_config?
    true
  end
  
  def get_disabled_value
    get_value()
  end
  
  def enabled_for_command_line?()
    false
  end
  
  def enabled_for_load_default_value?
    if Configurator.instance.display_help? && !Configurator.instance.display_preview?
      return false
    end
    
    return enabled_for_config?()
  end
end

module HiddenValueModule
  def output_usage
  end
  
  def required?
    false
  end
end

module HashPromptDefaultsModule
  def get_hash_prompt_key()
    raise "You must define the #{self.class.name}::get_hash_prompt_key function"
  end
  
  def get_default_value
    if get_member() != DEFAULTS
      value = @config.getNestedProperty(get_hash_prompt_key())
      if value != nil
        return value
      end
    end
    
    return super()
  end
end

module HashPromptModule
  def validate
    reset_errors()
    is_valid?()
  end
  
  def save_system_default
    return
  end
  
  def prepare_prompt(prompt)
    super(prompt)
    
    unless prompt.is_a?(HashPromptMemberModule)
      prompt.extend(HashPromptMemberModule)
    end
    
    prompt
  end
  
  def output_update_components
  end
end

module HashPromptMemberModule
  def enabled?
    true
  end
  
  def enabled_for_config?
    true
  end
  
  def required?
    false
  end
  
  def enabled_for_load_default_value?
    false
  end
end

module FindFilesystemDefaultModule
  def get_search_paths
    raise "You must specify get_search_paths for " + self.class.name
  end
  
  def query_class_default_value
    return get_search_paths()
  end
  
  def load_default_value
    get_search_paths().each{|cnf|
      Timeout.timeout(10){
        begin
          exists = ssh_result("if [ -f #{cnf} ]; then echo 0; else echo 1; fi", @config.getProperty(get_host_key(HOST)), @config.getProperty(get_host_key(USERID)))
          if exists.to_i == 0
            @default = cnf
            return
          end
        rescue CommandError
        end
      }
    }
  end
end

module CommercialPrompt
  def enabled?
    if Configurator.instance.is_enterprise?()
      super()
    else
      super()
    end
  end
  
  def enabled_for_command_line?()
    if Configurator.instance.is_enterprise?()
      super()
    else
      super()
    end
  end
end

module NoSystemDefault
  def save_system_default
  end
end

module NoStoredConfigValue
  def prepare_saved_config_value(is_server_config = false)
    @config.setProperty(get_name(), nil)
    @config.setProperty([SYSTEM] + get_name().split('.'), nil)
  end
end

module NoStoredServerConfigValue
  def prepare_saved_config_value(is_server_config = false)
    if is_server_config == true
      @config.setProperty(get_name(), nil)
      @config.setProperty([SYSTEM] + get_name().split('.'), nil)
    end
  end
end

module PrivateArgumentModule
  def build_command_line_argument(member, v, public_argument = false)
    if public_argument == true
      v = Array.new(v.length).fill("@").join("")
    end
    
    super(member, v, public_argument)
  end
end

module MigrateFromReplicationServices
  def update_deprecated_keys
    @config.getPropertyOr([REPL_SERVICES], {}).each{
      |rs_alias,rs_props|
      if rs_props.has_key?(@name)
        @config.setProperty([HOSTS, rs_props[DEPLOYMENT_HOST], @name], rs_props[@name])
        rs_props.delete(@name)
      end
    }
    @config.getPropertyOr([DATASERVICE_REPLICATION_OPTIONS], {}).each{
      |rs_alias,rs_props|
      if rs_props.has_key?(@name)
        @config.setProperty([DATASERVICE_HOST_OPTIONS, rs_alias, @name], rs_props[@name])
        rs_props.delete(@name)
      end
    }
  end
end

module PortForUsers
  def self.register(group, min_name, max_name = nil)
    @paths ||= []
    @paths << {:gr => group, :min => min_name, :max => max_name}
  end

  def self.paths
    @paths || []
  end
end
module PortForConnectors
  def self.register(group, min_name, max_name = nil)
    @paths ||= []
    @paths << {:gr => group, :min => min_name, :max => max_name}
  end

  def self.paths
    @paths || []
  end
end
module PortForManagers
  def self.register(group, min_name, max_name = nil)
    @paths ||= []
    @paths << {:gr => group, :min => min_name, :max => max_name}
  end

  def self.paths
    @paths || []
  end
end
module PortForReplicators
  def self.register(group, min_name, max_name = nil)
    @paths ||= []
    @paths << {:gr => group, :min => min_name, :max => max_name}
  end

  def self.paths
    @paths || []
  end
end
module DeploymentFiles
  def self.register(local, global, group = HOSTS)
    @prompts ||= []
    @prompts << {:local => local, :global => global, :group => group}
  end

  def self.prompts
    @prompts || []
  end
end