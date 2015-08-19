class ConfigureDataServiceCommand
  include ConfigureCommand
  include ResetConfigPackageModule
  include ClusterCommandModule
  include DisabledForExternalConfiguration
  
  CONFIGURE_DEFAULTS = "defaults"
  
  def defaults_only?
    (subcommand() == CONFIGURE_DEFAULTS)
  end
  
  def delete_dataservice?(val = nil)
    if val != nil
      @delete_dataservice = val
    end
    
    if @delete_dataservice == nil
      false
    else
      @delete_dataservice
    end
  end
  
  def load_prompts
    if delete_dataservice?()
      dataservices = command_dataservices()
      if dataservices.empty?()
        error("You must specify the data service name to delete")
      else
        dataservices.each{
          |ds_alias|
          @config.setProperty([DATASERVICES, ds_alias], nil)
        }
        
        clean_cluster_configuration()

        if is_valid?()
          notice("Data service(s) #{dataservices.join(',')} deleted from #{Configurator.instance.get_config_filename()}")
          save_config_file()
        end
      end
    elsif defaults_only?()
      load_cluster_defaults()
    else
      load_cluster_options()
    end
    
    is_valid?()
  end
  
  def skip_validation?(v = nil)
    true
  end
  
  def skip_deployment?(v = nil)
    true
  end
  
  def parsed_options?(arguments)
    arguments = super(arguments)
    
    if display_help?() && !display_preview?()
      return arguments
    end
    
    opts=OptionParser.new
    opts.on("--delete") { delete_dataservice?(true) }
    
    Configurator.instance.run_option_parser(opts, arguments)
  end
  
  def output_subcommand_usage
    output_usage_line("defaults", "Modify the default values used for each data service or host")
  end
  
  def output_command_usage()
    super()
    output_usage_line("--delete", "Delete the named data service from the configuration")
    
    OutputHandler.queue_usage_output?(true)
    display_cluster_options()
    OutputHandler.flush_usage()
  end
  
  def get_bash_completion_arguments
    super() + ["--delete"] + get_cluster_bash_completion_arguments()
  end
  
  def allowed_subcommands
    [CONFIGURE_DEFAULTS]
  end
  
  def allow_undefined_dataservice?
    true
  end
  
  def allow_check_current_version?
    true
  end
  
  def self.get_command_name
    "configure"
  end
  
  def self.get_command_description
    "Update the data service settings in the global configuration"
  end
end