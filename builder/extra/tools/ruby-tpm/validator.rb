#
# TUNGSTEN SCALE-OUT STACK
# Copyright (C) 2009 Continuent, Inc.
# All rights reserved
#

# Manages store and load of installation properties. 

require 'system_require'
system_require 'date'

# Super class for property validators. 
class PropertyValidator
  def initialize(regex, message, ignore_blank_values = false) 
    @regex = Regexp.new(regex)
    @message = message
    @ignore_blank_values = ignore_blank_values
  end
  def validate(value)
    if value.to_s() == ""
      return value
    end
    if value =~ @regex
      return value
    end
    
    raise PropertyValidatorException, @message, caller
  end
end

# File property validator.  Type must be "file" or "directory". 
class FilePropertyValidator 
  def initialize(ftype, writable, message, create)
    if ftype != "file" && ftype != "directory" 
      raise RuntimeError, "Invalid type for file checking: " + type
    end

    @ftype = ftype
    @writable = writable
    @message = message
    @create = create
  end
  def validate(value)
    # Ensure the file or directory exists. 
    ok = true
    if @ftype == "file" && ! File.file?(value)
      ok = false
    elsif @ftype == "directory" && ! File.exists?(value)
      if @create
        if confirm "Directory #{value} does not exist, ok to create it?", "yes"
          retvalue = `mkdir -p #{value}`.strip
          unless $? == 0
            ok = false
            puts "There was an error creating the directory: #{retvalue}"
          else
            if File.exists?(value)
              return value
            else
              puts "Unable to create directory!"
            end
          end
        end
      end
      ok = false
    elsif @ftype == "directory" && ! File.directory?(value)
      ok = false
    elsif ! File.readable?(value)
      ok = false
    elsif @writable && ! File.writable?(value)
      ok = false
    else
      return value
    end

    # Desired directory/file does not exist and/or could not be created. 
    raise PropertyValidatorException, @message, caller
  end

  def confirm(prompt, default)
    while true
      printf "%s [%s]: ", prompt, default
      value = STDIN.gets
      value.strip!
      if value == ""
        value = default
      end
      if value =~ /yes|YES|y|Y/
        return true
      elsif value =~ /no|NO|n|N/
        return false
      end
    end
  end
end

# Integer range validator.  Note that range value are assumed to be 
# strings. 
class IntegerRangeValidator < PropertyValidator
  def initialize(low, high, message) 
    super('^[0-9]+$', message)
    if low > high
      raise RuntimeError, "Invalid integer range: low=" + low + " high=" + high
    end
    @low = low.to_i
    @high = high.to_i
  end
  def validate(value)
    super(value)
    v_int = value.to_i
    if v_int >= @low && v_int <= @high
      return value
    else
      raise PropertyValidatorException, @message, caller
    end
  end
end

class ConfirmationValueValidator < PropertyValidator
  def initialize
    super('true|false|y|yes|n|no|q|quit', "Value must be Y or Q")
  end
  
  def validate(value)
    value = super(value.to_s().downcase())
  end
end

class BooleanValidator < PropertyValidator
  def initialize
    super('true|false|y|yes|n|no', "Value must be true or false")
  end
  
  def validate(value)
    if value =~ /^true|y|yes$/i
      value = "true"
    elsif value =~ /^false|n|no$/i
      value = "false"
    else
      raise PropertyValidatorException, @message, caller
    end
    
    return value
  end
end

class DataserverValidator < PropertyValidator
  def initialize
    @message = "Value must be a valid dataserver uri"
  end
  
  def validate(value)
    begin
      ConfigureDatabasePlatform.parse(value)
    rescue => e
      raise PropertyValidatorException, @message, caller
    end
    
    return value
  end
end

# Define standard validators. 
PV_INTEGER = PropertyValidator.new('^[0-9]+$', "Value must be an integer")
PV_ANY_INTEGER = PropertyValidator.new('^[-]?[0-9]+$', "Value must be an integer")
PV_BOOLEAN = BooleanValidator.new()
PV_CONFIRMATION = ConfirmationValueValidator.new()
PV_IDENTIFIER = PropertyValidator.new('^[A-Za-z0-9_]+$', 
  "Value must consist only of letters, digits, and underscore (_)")
PV_MULTIIDENTIFIER = PropertyValidator.new('^[A-Za-z0-9_,]+$', 
  "Value must consist only of letters, digits, and underscore (_)")
PV_MULTIHOSTPORT = PropertyValidator.new('^[A-Za-z0-9_:,.\-]+$', 
  "Value must consist only of valid hostname and hostname:port values separated by commas (,)")
PV_FILENAME = PropertyValidator.new('^(\/[A-Za-z0-9\-_\.\/]+|~[A-Za-z0-9\-_\.\/]*)$',
  "Value must be a valid filename")
PV_FILENAME_OR_EMPTY = PropertyValidator.new('^/[A-Za-z0-9_\.\/]+',
  "Value must be a valid filename", true)
PV_SCRIPTNAME = PropertyValidator.new('^[A-Za-z0-9_\.]+$',
  "Value must be a valid script filename")
PV_HOSTNAME = PropertyValidator.new('^[A-Za-z0-9_.\-]+$', 
  "Value must consist only of letters, digits, hyphen, underscore (_) and periods")
PV_LOGTYPE = PropertyValidator.new("^dbms|disk$", 
  "Value must be a supported replicator log type:  dbms (store in db) or disk")
PV_DBMSROLE = PropertyValidator.new("^master|slave$", 
  "Value must be master or slave")
PV_GC_MEMBERSHIP_PROTOCOL = PropertyValidator.new("^multicast|gossip|ping$", 
  "Value must be multicast, gossip or ping")
PV_URI = PropertyValidator.new('^(http|https|ftp|file)\://([a-zA-Z0-9\.\-]+(\:[a-zA-Z0-9\.&amp;%\$\-]+)*@)*((25[0-5]|2[0-4][0-9]|[0-1]{1}[0-9]{2}|[1-9]{1}[0-9]{1}|[1-9])\.(25[0-5]|2[0-4][0-9]|[0-1]{1}[0-9]{2}|[1-9]{1}[0-9]{1}|[1-9]|0)\.(25[0-5]|2[0-4][0-9]|[0-1]{1}[0-9]{2}|[1-9]{1}[0-9]{1}|[1-9]|0)\.(25[0-5]|2[0-4][0-9]|[0-1]{1}[0-9]{2}|[1-9]{1}[0-9]{1}|[0-9])|localhost|remote|([a-zA-Z0-9\-]+\.)*[a-zA-Z0-9\-]+\.(com|edu|gov|int|mil|net|org|biz|arpa|info|name|pro|aero|coop|museum|[a-zA-Z]{2}))(\:[0-9]+)*(/($|[a-zA-Z0-9\.\,\?\'\\\+&amp;%\$#\=~_\-]+))*$',
  "Value must be a URI")
PV_ANY = PropertyValidator.new('.*', "Value may be any string")
PV_JAVA_MEM_SIZE = IntegerRangeValidator.new(128, 2048, 
  "Java heap size must be between 128 and 2048")
PV_REPL_BUFFER_SIZE = IntegerRangeValidator.new(1, 100, 
  "Replication transaction buffer size must be between 1 and 100")
PV_READABLE_DIR = FilePropertyValidator.new("directory", false, 
  "Value must be a readable directory", false)
PV_WRITABLE_DIR = FilePropertyValidator.new("directory", true, 
  "Value must be a writable directory", false)
PV_WRITABLE_OUTPUT_DIR = FilePropertyValidator.new("directory", true, 
  "Value must be a writable directory", true)
PV_EXECUTABLE_FILE = FilePropertyValidator.new("file", false, 
  "Value must be an executable file", false)
PV_READABLE_FILE = FilePropertyValidator.new("file", false,
  "Value must be a readable file", false)
PV_WRITABLE_FILE = FilePropertyValidator.new("file", true,
  "Value must be a writable file", false)

# Exception to identify a property validation error. 
class PropertyValidatorException < RuntimeError
end
