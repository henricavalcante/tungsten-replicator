class CopyConfigCommand
  include ConfigureCommand
  include RemoteCommand
  include ClusterCommandModule
  include DisabledForExternalConfiguration
  
  def run
    save_config_file()
  end
  
  def parsed_options?(arguments)
    if Configurator.instance.is_locked?()
      raise("Unable to fetch configurations to an installation directory")
    end
    
    return super(arguments)
  end
  
  def require_remote_config?
    hosts = []
    user = nil
    directory = nil
    
    @config.getPropertyOr([DATASERVICES], {}).each_key{
      |ds_alias|
      unless include_dataservice?(@config.getProperty([DATASERVICES, ds_alias, DATASERVICENAME]))
        next
      end
      
      hosts = hosts + @config.getPropertyOr([DATASERVICES, ds_alias, DATASERVICE_MEMBERS], "").split(",")
      hosts = hosts + @config.getPropertyOr([DATASERVICES, ds_alias, DATASERVICE_CONNECTORS], "").split(",")
    }
    
    hosts.uniq!()
    hosts.delete_if{
      |host|
      (include_host?(host) != true)
    }
    hosts.each{
      |host|
      h_alias = to_identifier(host)
      host_user = @config.getProperty([HOSTS, h_alias, USERID])
      if user == nil
        user = host_user
      elsif user != host_user
        error("The user for #{host} differs from others in the dataservice.  Specify the You must provide --user, --hosts and --directory arguments.")
      end
      
      host_directory = @config.getProperty([HOSTS, h_alias, HOME_DIRECTORY])
      if directory == nil
        directory = host_directory
      elsif directory != host_directory
        error("The home directory for #{host} differs from others in the dataservice.  Specify the You must provide --user, --hosts and --directory arguments.")
      end
    }
    
    unless is_valid?()
      return true
    end
    
    load_remote_config(hosts, user, directory)
    
    if is_valid?()
      return false
    else
      return true
    end
  end
  
  def self.display_command
    !(Configurator.instance.is_locked?())
  end
  
  def self.get_command_name
    'fetch'
  end
  
  def self.get_command_description
    "Fetch the configuration from each host to the local configuration file."
  end
end