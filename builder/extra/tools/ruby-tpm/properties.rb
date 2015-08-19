#
# TUNGSTEN SCALE-OUT STACK
# Copyright (C) 2009 Continuent, Inc.
# All rights reserved
#

# Manages store and load of installation properties. 

require 'system_require'
system_require 'date'

# Defines a properties object. 
class Properties
  attr_accessor :props,:use_prompt_handler,:force_json
  
  # Initialize with some base values. 
  def initialize
    @props = {}
    
    # These hashes are used to prevent loops where the prompt handler needs
    # to get additional information from the configuration
    @in_prompt_handler = {}
    @in_template_value_prompt_handler = {}
    
    # The prompt handler provides a structuring for finding values
    # that do no exist in the props Hash. The prompt handler will calculate
    # default values or values based on parent defaults.
    @use_prompt_handler = true
    @prompt_handler = nil
    
    # This is used for translating the configuration from 1.3 deployments
    @force_json = true
  end
  
  def initialize_copy(source)
    super(source)
    @props = Marshal::load(Marshal::dump(@props))
    @in_prompt_handler = {}
    @in_template_value_prompt_handler = {}
    @use_prompt_handler = source.use_prompt_handler
    @prompt_handler = nil
    @force_json = source.force_json
  end
  
  # Read properties from a file. 
  def load(properties_filename)
    file_contents = ""
    
    File.open(properties_filename, 'r') do |file|
      file.read.each_line do |line|
        line.strip!
        unless (line =~ /^#.*/)
          file_contents = file_contents + line
        end
      end
      
      begin
        parsed_contents = JSON.parse(file_contents)
      rescue Exception => e
        if file_contents == ""
          parsed_contents = {}
        elsif file_contents[0,1] == "{"
          raise "There was an error parsing the config file: #{e.message}"
        end
      end
      
      if parsed_contents && parsed_contents.instance_of?(Hash)
        @props = parsed_contents
      elsif @force_json == true
        raise "There was an error parsing the JSON config file: #{properties_filename}.  Try using the migration procedure if you have an old configuration file."
      else
        new_props = {}
        
        file.rewind()
        file.read.each_line do |line|
          line.strip!
          
          if (line =~ /^([\w\.]+)\[?([\w\.]+)?\]?\s*=\s*(\S.*)/)
            key = $1
            value = $3
        
            if $2
              new_props[key] = {} unless new_props[key]
              new_props[key][$2] = value
            else
              new_props[key] = value
            end
          elsif (line =~ /^([\w\.]+)\s*=/)
            key = $1
            value = ""
            new_props[key] = value
          end
        end
        
        @props = new_props
      end
      
      original_props = @props.dup
      
      if original_props != @props
        Configurator.instance.warning("Deprecated keys in the config file were updated")
      end
    end
  end
  
  # Read properties from a file. 
  def load_and_initialize(properties_filename, keys_module)
    load(properties_filename)
    init(keys_module)
  end
  
  def reset
    self.props = {}
  end
  
  def import(properties_obj = {})
    if properties_obj.instance_of?(Hash)
      self.props = properties_obj
    elsif properties_obj.instance_of?(Properties)
      self.props = properties_obj.props
    else
      raise "You must pass in a Hash or Properties object to import"
    end
  end
  
  # Write properties to a file.  We use signal protection to avoid getting
  # interrupted half-way through. 
  def store(properties_filename, use_json = true)
    # Protect I/O with trap for Ctrl-C. 
    interrupted = false
    old_trap = trap("INT") {
      interrupted = true;
    }
    
    # Write. 
    File.open(properties_filename, 'w') do |file|
      file.printf "# Tungsten configuration properties\n"
      file.printf "# Date: %s\n", DateTime.now
      
      if use_json == false
        @props.sort.each do | key, value |
          file.printf "%s=%s\n", key, value
        end
      else
        file.print self.to_s
      end
    end
    
    # Check for interrupt and restore handler. 
    if (interrupted) 
      puts
      puts ("Configuration interrupted") 
      exit 1;
    else
      trap("INT", old_trap);
    end
  end
  
  # Return the size of the properties object. 
  def size()
    @props.size
  end
  
  def to_s
    JSON.pretty_generate(@props)
  end
  
  def output()
    Configurator.instance.output(self.to_s)
  end
  
  def force_output()
    Configurator.instance.force_output(self.to_s)
  end
  
  # Fetch a nested hash value
  def getNestedProperty(attrs)
    if attrs.is_a?(String)
      attrs = attrs.split('.')
    end
    
    attr_count = attrs.size
    current_val = @props
    for i in 0..(attr_count-1)
      attr_name = attrs[i]
      return current_val[attr_name] if i == (attr_count-1)
      return nil if current_val[attr_name].nil?
      current_val = current_val[attr_name]
    end
    
    return nil
  end
  
  def setNestedProperty(new_val, attrs)
    attr_count = attrs.size
    current_val = @props
    for i in 0..(attr_count-1)
      attr_name = attrs[i]
      if i == (attr_count-1)
        return setHashProperty(current_val, attr_name, new_val)
      end
      current_val[attr_name] = {} if current_val[attr_name].nil?
      current_val = current_val[attr_name]
    end
  end
  
  def setHashProperty(hash, key, value)
    if value == nil  || value == []
      return (hash.delete(key))
    else
      if value.is_a?(Hash)
        hash[key] ||= {}
        value.each{|sub_key,sub_value|
          setHashProperty(hash[key], sub_key, sub_value)
        }
      else
        return (hash[key] = value)
      end
    end
  end
  
  # Get a property value. 
  def getProperty(key, allow_disabled = false)
    if key.is_a?(String)
      key_string = key
      key = key.split('.')
    else
      key_string = key.join('.')
    end
    
    value = getNestedProperty(key)
    if value != nil
      return value
    end
    
    if usePromptHandler()
      findProperty = lambda do |keys|
        if @in_prompt_handler[key_string] == true
          return nil
        end

        begin
          @in_prompt_handler[key_string] = true

          value = getPromptHandler().get_property(keys, allow_disabled)

          @in_prompt_handler[key_string] = false
        rescue IgnoreError
          @in_prompt_handler[key_string] = false
        rescue => e
          @in_prompt_handler[key_string] = false
          raise e
        end

        return value
      end
    
    
      value = findProperty.call(key)
    else
      findProperty = lambda do |keys|
        return getNestedProperty(keys)
      end
    end
    
    if value == nil && key.size == 1 && (host = getNestedProperty([DEPLOYMENT_HOST]))
      value = findProperty.call([HOSTS, host, key[0]])
      
      if value == nil && key.size == 1
        dataservice = getProperty(DEPLOYMENT_DATASERVICE)
        
        if dataservice
          value = findProperty.call([DATASERVICES, dataservice, key[0]])
        
          if value == nil
            value = findProperty.call([REPL_SERVICES, dataservice + "_" + host, key[0]])
          end
        
          if value == nil
            value = findProperty.call([MANAGERS, dataservice + "_" + host, key[0]])
          end
        end
      end
      
      if value == nil
        value = findProperty.call([CONNECTORS, host, key[0]])
      end
      
      if value == nil && key.size == 1 && (svc = getNestedProperty([DEPLOYMENT_SERVICE]))
        value = findProperty.call([REPL_SERVICES, svc, key[0]])
      end
    end
    
    value
  end
  
  # Get the config file value for a property. 
  def getTemplateValue(key)
    if key.is_a?(String)
      key_string = key
      key = key.split('.')
    else
      key_string = key.join('.')
    end
    
    if usePromptHandler()
      findProperty = lambda do |keys|
        if @in_template_value_prompt_handler[key_string] == true
          return nil
        end

        begin
          @in_template_value_prompt_handler[key_string] = true

          value = getPromptHandler().find_template_value(keys)

          @in_template_value_prompt_handler[key_string] = false
        rescue IgnoreError
          @in_template_value_prompt_handler[key_string] = false
        rescue => e
          @in_template_value_prompt_handler[key_string] = false
          raise e
        end

        return value
      end
    
      value = findProperty.call(key)
    end
    
    if value == nil && key.size == 1 && (host = getNestedProperty([DEPLOYMENT_HOST]))
      value = findProperty.call([HOSTS, host, key[0]])
      
      if value == nil && key.size == 1
        dataservice = getNestedProperty([DEPLOYMENT_DATASERVICE])
        if dataservice == nil
          dataservice = getProperty(DEPLOYMENT_DATASERVICE)
        end
        
        if dataservice
          value = findProperty.call([DATASERVICES, dataservice, key[0]])
        
          if value == nil
            value = findProperty.call([REPL_SERVICES, dataservice + "_" + host, key[0]])
          end
        
          if value == nil
            value = findProperty.call([MANAGERS, dataservice + "_" + host, key[0]])
          end
        end
      end
      
      if value == nil
        value = findProperty.call([CONNECTORS, host, key[0]])
      end
    end
    
    if value == nil && key.size == 1 && (svc = getNestedProperty([DEPLOYMENT_SERVICE]))
      value = findProperty.call([REPL_SERVICES, svc, key[0]])
    end
    
    value
  end
  
  # Get the property value or return the default if nil
  def getPropertyOr(key, default = "")
    value = getProperty(key)
    if value == nil
      default
    else
      value
    end
  end
  
  def getNestedPropertyOr(key, default = "")
    value = getNestedProperty(key)
    if value == nil
      default
    else
      value
    end
  end
  
  # Set a property value. 
  def setProperty(key, value)
    if key.is_a?(String)
      key = key.split('.')
    end
    
    setNestedProperty(value, key)
  end 
  
  # Set the property to a value only if it is currently unset. 
  def setDefault(key, value)
    if key.is_a?(String)
      key = key.split('.')
    end
    
    if getNestedProperty(key) == nil
      setNestedProperty(value, key)
    end
  end
  
  # Set multiple properties from a delimited string of key value pairs. 
  def setPropertiesFromList(list, delimiter)
    keyValuePairs = list.split(delimiter)
    keyValuePairs.each do |pair|
      if pair =~ /^(.*)=(.*)$/
        key = $1
        value = $2
        setProperty(key, value)
      end
    end
  end
  
  # Get the underlying hash table. 
  def hash()
    @props
  end
  
  def usePromptHandler
    return @use_prompt_handler
  end
  
  def getPromptHandler
    unless usePromptHandler()
      return nil
    end
    
    unless @prompt_handler
      @prompt_handler = ConfigurePromptHandler.new(self)
    end
    
    return @prompt_handler
  end
  
  def empty?
    (@props.size() == 0)
  end
  
  def has_key?(key)
    (getNestedProperty(key) != nil)
  end
  
  def override(key, value = {})
    if value == nil || value == ""
      value = {}
    end
    
    setProperty(key, getNestedPropertyOr(key, {}).merge(value))
  end
  
  def include(key, value = {})
    if value == nil || value == ""
      value = {}
    end
    
    setProperty(key, value.merge(getNestedPropertyOr(key, {})))
  end
  
  def append(key, value = [])
    if value == nil || value == ""
      value = []
    end
    
    if ! value.kind_of?(Array)
       value=Array(value) 
    end
    currentvalue=getNestedPropertyOr(key, [])
    if ! currentvalue.kind_of?(Array)
        currentvalue=Array(currentvalue)
    end
    setProperty(key, (currentvalue+value).uniq())
  end
end
