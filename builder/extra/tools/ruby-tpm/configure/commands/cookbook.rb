class CookbookCommand
  include ConfigureCommand
  
  def skip_prompts?
    true
  end
  
  def display_alive_thread?
    false
  end
  
  def allowed_subcommands
    templates = []
    
    Dir[Configurator.instance.get_base_path() + '/cookbook/*.tmpl'].sort().each do |file| 
      templates << File.basename(file, File.extname(file))
    end
    
    return templates
  end
  
  def parsed_options?(arguments)
    arguments = super(arguments)
    
    if display_help?() && !display_preview?()
      return true
    end
    
    @cookbook_arguments = arguments
    
    []
  end
   
  def run
    exec("cd #{Configurator.instance.get_base_path()}; ./cookbook/.test/tungsten-cookbook #{subcommand()} #{@cookbook_arguments.join(' ')}")
  end
  
  def output_usage()
    puts cmd_result("cd #{Configurator.instance.get_base_path()}; ./cookbook/.test/tungsten-cookbook -h")
  end
  
  def allow_command_dataservices?
    false
  end
  
  def self.display_command
    false
  end
  
  def self.get_command_name
    'cookbook'
  end
  
  def self.get_command_description
    "Include cookbook/USER_VALUES.sh and run the associated cookbook script"
  end
end