class HelpCommand
  include ConfigureCommand
  include ClusterCommandModule
  
  HELP_COMMANDS = "commands"
  HELP_CONFIG_FILE = "config-file"
  HELP_TEMPLATE_FILE = "template-file"
  HELP_UPDATE = "update"
  
  def initialize(config)
    super(config)
  end
  
  def subcommand_allowed?(v)
    super(v) || (ConfigureCommand.get_command_class(v) != nil)
  end
  
  def allowed_subcommands
    [HELP_COMMANDS, HELP_CONFIG_FILE, HELP_TEMPLATE_FILE]
  end
  
  def allow_command_hosts?
    false
  end
  
  def allow_command_dataservices?
    false
  end
  
  def allow_multiple_tpm_commands?
    true
  end
  
  def display_help?(v = nil)
    true
  end
  
  def output_usage
    if @subcommand == HELP_CONFIG_FILE
      write_header('Config File Options', nil)
      prompt_handler = ConfigurePromptHandler.new(@config)
      prompt_handler.output_config_file_usage()
    elsif @subcommand == HELP_TEMPLATE_FILE
      write_header('Template File Options', nil)
      prompt_handler = ConfigurePromptHandler.new(@config)
      prompt_handler.output_template_file_usage()
    elsif @subcommand == HELP_UPDATE
      write_header('Update Actions', nil)
      prompt_handler = ConfigurePromptHandler.new(@config)
      prompt_handler.output_update_components()
    elsif @subcommand == HELP_COMMANDS
      super()
    else
      command_class = ConfigureCommand.get_command_class(@subcommand)
      if command_class
        command_class.new(@config).output_usage()
      else
        super()
      end
    end
  end
  
  def output_command_usage()
    Configurator.instance.write_divider(Logger::ERROR)
    Configurator.instance.output("Commands:")
    
    commands = {}
    ConfigureCommand.subclasses().each{
      |klass|
      begin
        if klass.display_command() == false
          next
        end
      rescue NoMethodError
      end
      
      begin
        cmd = klass.get_command_name()
      rescue NoMethodError
        next
      end
      
      begin
        desc = klass.get_command_description()
      rescue NoMethodError
        desc = ""
      end
      
      commands[cmd] = desc
    }
    
    commands.keys().sort().each{
      |cmd|
      output_usage_line(cmd, commands[cmd])
    }
  end
  
  def self.get_command_name
    'help'
  end
  
  def self.get_command_description
    "Display a list of all commands available in #{TPM_COMMAND_NAME}"
  end
end