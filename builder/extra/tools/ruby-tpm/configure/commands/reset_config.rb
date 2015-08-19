class ResetConfigCommand
  include ConfigureCommand
  include DisabledForExternalConfiguration
  
  def run
    @config.props = {}
  
    save_config_file()
  end
  
  def self.display_command
    !(Configurator.instance.is_locked?())
  end
  
  def self.get_command_name
    'reset-config'
  end
  
  def self.get_command_description
    "Clear the current global configuration object.  This will not work on an installation directory."
  end
  
  def self.display_command
    false
  end
end