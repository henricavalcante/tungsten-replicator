#
# This class collects GroupConfigurePromptMember classes and repeats them to
# create a hierarchy of prompt information.  The group name is used as the 
# top level value in the config and the alias entered by the user is the 
# second level.  The third level in the config is defined by the prompt 
# object.
#
class GroupConfigurePrompt
  include ConfigurePromptInterface
  attr_accessor :name, :singular, :plural
  
  def initialize(name, prompt, singular, plural, template_prefix)
    @group_prompts = []
    @group_key_index = {}
    @name = name.to_s()
    @prompt = prompt.to_s()
    @config = nil
    @singular = singular.to_s().downcase()
    @plural = plural.to_s().downcase()
    @template_prefix = template_prefix
    
    @prompt_pairs = nil
    @previous_prompts = []
    @last_run_prompt_pair_i = 0
  end
  
  # The config object must be set down on each of the prompts so that they
  # have direct access
  def set_config(config)
    @config = config
    each_prompt{
      |prompt|
      prompt.set_config(config)
    }
  end
  
  def get_display_prompt
    @plural
  end
  
  def is_initialized?
    (get_name() != "" && get_prompts().size() > 0)
  end
  
  def save_system_default
    each_member_prompt(true) {
      |member, prompt|
      prompt.save_system_default()
    }
  end
  
  def prepare_saved_config_value(is_server_config = false)
    each_member_prompt(true) {
      |member, prompt|
      prompt.prepare_saved_config_value(is_server_config)
    }
  end
  
  # Validate each of the prompts across all of the defined members
  def validate
    reset_errors()
    validate_prompts()
    
    is_valid?()
  end
  
  def validate_prompts
    each_member_prompt{
      |member, prompt|
      
      begin
        prompt.validate()
        @errors = @errors + prompt.errors
      rescue => e
        begin
          val = prompt.get_value()
        rescue
          val = ""
        end
        
        Configurator.instance.debug(e.message + "\n" + e.backtrace.join("\n"), get_message_hostname())
        dup_prompt = prompt.dup()
        prepare_prompt(dup_prompt)
        @errors << ConfigurePromptError.new(dup_prompt, e.message, val)
      end
    }
  end
  
  # Collect the full list of keys that are allowed in the config file
  def get_keys
    keys = []
    
    each_prompt {
      |prompt|
      if prompt.allow_group_default()
        curr_member = prompt.get_member()
        prompt.set_member(DEFAULTS)
        keys << prompt.get_name()
        prompt.set_member(curr_member)
      end
    }
    
    each_member_prompt{
      |member, prompt|
      
      if prompt.enabled_for_config?() || (prompt.get_disabled_value() != nil)
        keys << prompt.get_name()
      end
    }
    
    keys
  end

  # Add a single prompt to this group
  def add_prompt(prompt)
    unless prompt.is_a?(ConfigurePrompt)
      raise "Unable to add #{prompt.class().name()}:#{prompt.get_name()} because it does not extend ConfigurePrompt"
    end
    
    prompt = prepare_prompt(prompt)
    @group_prompts << prompt
    @group_key_index[prompt.name] = prompt
  end
  
  def prepare_prompt(prompt)
    unless prompt.is_a?(GroupConfigurePromptMember)
      prompt.extend(GroupConfigurePromptMember)
    end
    
    prompt.set_group(self)
    
    if @config != nil
      prompt.set_config(@config)
    end
    
    prompt
  end
  
  # Add a list of prompts to this group
  # self.add_prompts(prompt1, prompt2, prompt3)
  def add_prompts(*new_prompts)
    new_prompts_count = new_prompts.size
    for i in 0..(new_prompts_count-1)
      add_prompt(new_prompts[i])
    end
  end
  
  # Get the list of prompts in this group
  def get_prompts
    @group_prompts || []
  end
  
  # Get the list of members excluding the defaults entry
  def get_members
    (@config.getPropertyOr(@name, {}).keys() - [DEFAULTS])
  end
  
  # Loop over each member to execute &block
  # This will exclude the defaults entry in the group
  def each_member(&block)
    get_members().each{
      |member|
      
      block.call(member)
    }
  end
  
  # Loop over each prompt to execute &block
  def each_prompt(&block)
    get_prompts().each{
      |prompt|
      
      block.call(prompt)
    }
    
    self
  end
  
  # Loop over each member-prompt combination to execute &block
  # This will exclude the defaults entry in the group
  def each_member_prompt(include_default = false, &block)
    errors = []
    if include_default == true
      members_list = [DEFAULTS] + get_members()
    else
      members_list = get_members()
    end
    
    members_list.each{
      |member|
      each_prompt{
        |prompt|
        begin
          curr_member = prompt.get_member()
          prompt.set_member(member)
          block.call(member, prompt)
          prompt.set_member(curr_member)
        rescue ConfigurePromptError => cpe  
          prompt.set_member(curr_member)
          errors << cpe
        rescue => e  
          prompt.set_member(curr_member)
          begin
            val = prompt.get_value()
          rescue
            val = ""
          end
          
          dup_prompt = prompt.dup()
          prepare_prompt(dup_prompt)
          errors << ConfigurePromptError.new(dup_prompt, e.message, val)
        end
      }
    }
    
    if errors.length > 0
      raise ConfigurePromptErrorSet.new(errors)
    else
      true
    end
  end
  
  def output_config_file_usage
    puts ""
    output_usage_line(@name + ".<alias>", @prompt)
    each_prompt{
      |p|
      p.output_config_file_usage()
    }
  end
  
  def output_template_file_usage
    puts ""
    output_usage_line(@template_prefix)
    each_prompt{
      |p|
      if p.enabled_for_template_file?()
        p.output_template_file_usage()
      end
    }
  end
  
  def output_update_components
    each_prompt{
      |p|
      if p.enabled_for_command_line?()
        p.output_update_components()
      end 
    }
  end
  
  def find_prompt_by_name(name)
    each_prompt{
      |prompt|
      
      begin
        return prompt.find_prompt_by_name(name)
      rescue IgnoreError
        #Do Nothing
      end
    }
    
    raise IgnoreError
  end
  
  def find_prompt(attrs)
    if attrs[0] != @name
      raise IgnoreError
    end
    
    if attrs.size != 3
      raise IgnoreError
    end
    
    prompt = @group_key_index[attrs[2]]
    if prompt != nil
      prompt.set_member(attrs[1])
      return prompt
    end
    
    raise IgnoreError
  end
  
  def get_property(attrs, allow_disabled = false)
    if attrs[0] != @name
      raise IgnoreError
    end
    
    if attrs.size == 1
      return @config.getNestedProperty(attrs)
    end
    
    if attrs.size == 2
      return @config.getNestedProperty(attrs)
    end
    
    prompt = @group_key_index[attrs[2]]
    if prompt != nil
      begin
        curr_member = prompt.get_member()
        prompt.set_member(attrs[1])
        value = prompt.get_property(attrs.slice(2, attrs.length), allow_disabled)
        prompt.set_member(curr_member)
        
        return value
      rescue IgnoreError
        prompt.set_member(curr_member)
        #Do Nothing
      end
    end
    
    raise IgnoreError
  end
  
  def find_template_value(attrs)
    if attrs[0] != @name
      raise IgnoreError
    end
    
    if attrs.size == 1
      return @config.getNestedProperty(attrs)
    end
    
    if attrs.size == 2
      return @config.getNestedProperty(attrs)
    end
    
    prompt = @group_key_index[attrs[2]]
    if prompt != nil
      begin
        curr_member = prompt.get_member()
        prompt.set_member(attrs[1])
        value = prompt.find_template_value(attrs.slice(2, attrs.length))
        prompt.set_member(curr_member)
        
        return value
      rescue IgnoreError
        prompt.set_member(curr_member)
        #Do Nothing
      end
    end
    
    raise IgnoreError
  end
  
  def update_deprecated_keys()
    each_member_prompt{
      |member, prompt|
      
      prompt.update_deprecated_keys()
    }
  end
  
  def get_updated_keys(old_cfg)
    r = []

    each_member_prompt{
      |member, prompt|

      begin
        r = r + prompt.get_updated_keys(old_cfg)
      rescue IgnoreError
      end
    }
    
    r
  end
  
  def enabled_for_command_line?()
    false
  end
end

module GroupConfigurePromptMember
  # Assign the parent group to this prompt
  def set_group(val)
    @parent_group = val
  end
  
  def get_group
    @parent_group
  end
  
  # Assign the current member for this prompt
  def set_member(member_name)
    @member_name = member_name
  end
  
  # Return the current member or the defaults member if none is set
  def get_member
    if @member_name
      @member_name
    else
      DEFAULTS
    end
  end
  
  # Return the name with the full hierarchy included
  def get_name
    "#{@parent_group.name}.#{get_member()}.#{@name}"
  end
  
  # Get an array prepared for the Properties.*Property calls
  def get_member_key(name = nil)
    if name == nil
      name = @name
    end
    
    [@parent_group.name, get_member(), name]
  end
  
  # Get the prompt text with the member prefixed to display
  def get_display_prompt
    "#{get_display_member()}: #{get_prompt()}"
  end
  
  def get_display_member
    "#{@parent_group.singular.capitalize} #{get_member()}"
  end
  
  # Does this prompt support a group-wide default value to be specified
  def allow_group_default
    false
  end
  
  # Build the description filename based on the basic config key
  def get_prompt_description_filename()
    "#{get_interface_text_directory()}/prompt_#{@name}"
  end
  
  def get_config_file_usage_symbol
    "  ." + @name
  end
  
  def get_template_file_usage_symbol
    "  ." + Configurator.instance.get_constant_symbol(@name).to_s()
  end
  
  def get_group_default_value
    if get_member() == DEFAULTS
      nil
    else
      @config.getNestedProperty([@parent_group.name, DEFAULTS, @name])
    end
  end
  
  def get_default_value
    if get_member() != DEFAULTS
      value = get_group_default_value()
      if value != nil
        return value
      end
    end
    
    return super()
  end
  
  def get_stored_value()
    value = super()
    if value == nil
      value = get_group_default_value()
    end
    
    value
  end
  
  def skip_class_validation?()
    ConfigureValidationHandler.skip_validation_class?(self.class.name, @config)
  end
  
  def find_command(cmds, host = nil, userid = nil)
    if host == nil
      host = @config.getProperty(get_member_key(HOST))
    end
    
    if userid == nil
      userid = @config.getProperty(get_member_key(USERID))
    end
    
    cmds.each{|cmd|
      begin
        exists = Timeout.timeout(10){
          begin
            ssh_result("if [ -f #{cmd} ]; then echo 0; else echo 1; fi", host, userid)
          rescue CommandError
          end
        }
        
        if exists.to_i == 0
          return cmd
        end
      rescue Timeout::Error
      end
    }
    
    return nil
  end
end