class ConfigurationDiffCommand
  include ConfigureCommand
  include ClusterCommandModule
  
  def run
    WatchFiles.show_differences(Configurator.instance.get_base_path())
  end
  
  def self.display_command
    if Configurator.instance.is_locked?()
      true
    else
      false
    end
  end
  
  def self.get_command_name
    'diff'
  end
  
  def self.get_command_description
    "Output the manual changes that have been made to configuration files."
  end
end