class QueryCommand
  include ConfigureCommand
  include ClusterCommandModule

  QUERY_VERSION = "version"
  QUERY_MANIFEST = "manifest"
  QUERY_CONFIG = "config"
  QUERY_TOPOLOGY = "topology"
  QUERY_DATASERVICES = "dataservices"
  QUERY_STAGING = "staging"
  QUERY_EXTERNAL_CONFIGURATION = "external-configuration"
  QUERY_DEFAULT = "default"
  QUERY_VALUES = "values"
  QUERY_MODIFIED_FILES = "modified-files"
  QUERY_USERMAP = "usermap"
  QUERY_DEPLOYMENTS = "deployments"
  
  def allowed_subcommands
    [QUERY_VERSION, QUERY_MANIFEST, QUERY_CONFIG, QUERY_TOPOLOGY, QUERY_DATASERVICES, QUERY_STAGING, QUERY_DEFAULT, QUERY_VALUES, QUERY_MODIFIED_FILES, QUERY_USERMAP, QUERY_DEPLOYMENTS, QUERY_EXTERNAL_CONFIGURATION]
  end
  
  def allow_multiple_tpm_commands?
    true
  end
  
  def run
    case subcommand()
    when QUERY_VERSION
      force_output(Configurator.instance.get_release_version())
    when QUERY_MANIFEST
      manifest = Properties.new()
      manifest.load(Configurator.instance.get_manifest_json_file_path())
      manifest.force_output()
    when QUERY_CONFIG
      @config.force_output()
    when QUERY_TOPOLOGY
      force_output(get_topology())
    when QUERY_DATASERVICES
      output_dataservices()
    when QUERY_STAGING
      output_staging()
    when QUERY_EXTERNAL_CONFIGURATION
      output_external_configuration()
    when QUERY_DEFAULT
      output_defaults()
    when QUERY_VALUES
      output_values()
    when QUERY_MODIFIED_FILES
      output_modified_files()
    when QUERY_USERMAP
      output_usermap_summary()
    when QUERY_DEPLOYMENTS
      output_deployments()
    else
      output_usage()
    end
    
    return is_valid?()
  end
  
  def parsed_options?(arguments)
    @default_arguments = nil
    
    remainder = super(arguments)
    
    if subcommand() == QUERY_DEFAULT
      @default_arguments = remainder
      remainder = []
    elsif subcommand() == QUERY_VALUES
      @values_arguments = remainder
      remainder = []
    end
    
    return remainder
  end
  
  def output_subcommand_usage()
    output_usage_line(QUERY_VERSION, "The version number for this directory")
    output_usage_line(QUERY_MANIFEST, "The software manifest that describes when this directory was built and what SVN revisions were used")
    output_usage_line(QUERY_CONFIG, "The full configuration object used for this directory")
    output_usage_line(QUERY_DATASERVICES, "The list of data services defined in the configuration")
    output_usage_line(QUERY_TOPOLOGY, "The roles of each member of the data service this host belongs to")
  end
  
  def get_topology
    c = Configurator.instance
    unless c.is_enterprise?()
      raise "This command is not supported in the Tungsten Replicator package"
    end
    unless c.svc_is_running?(c.get_svc_path("manager", c.get_base_path()))
      raise "Tungsten Manager is not running on this machine"
    end
    
    build_topologies(@config)

    begin
      cctrl_output = Timeout.timeout(120) {
        cmd_result("echo \"ls\" | #{c.get_cctrl_path(Configurator.instance.get_base_path(), @config.getProperty(MGR_RMI_PORT))} | tr -d \"|+\" | egrep -e \"(master|slave|relay|standby):\" | awk -F: '{print $1}' | tr \"\(\" \" \"")
      }
    rescue Timeout::Error
      raise "Unable to connect to the manager to get the topology"
    rescue CommandError => ce
      raise "There was an issue getting the topology from CCTRL"
    end

    topology = {}
    cctrl_output.split("\n").each{
      |line|
      parts = line.split(" ")
      unless parts.length == 2
        raise "Unable to parse CCTRL output line '#{line}'"
      end
      topology[parts[0]] = parts[1]
    }

    return JSON.pretty_generate(topology)
  end
  
  def output_dataservices
    @config.getPropertyOr(DATASERVICES, {}).keys().delete_if{|k| (k == DEFAULTS)}.each{
      |ds_alias|
      
      if @config.getProperty([DATASERVICES, ds_alias, DATASERVICE_IS_COMPOSITE]) == "true"
        type = "COMPOSITE"
      else
        type = "PHYSICAL"
      end
      
      force_output("#{format("%-30s", ds_alias)}: #{type}")
    }
  end
  
  def output_defaults
    build_topologies(@config)
    @default_arguments.map!{
      |a|
      
      if a[0..1] == "--"
        a[2..-1]
      else
        a
      end
    }
    
    displayed_classes = {}
    default_matches = {}
    
    printDefaults = lambda do |p|
      if displayed_classes[p.class.name] == true
        return
      end
      
      arguments = [p.get_command_line_argument()] + p.get_command_line_aliases()
      matches = arguments & @default_arguments
      
      if matches.size() > 0
        matches.each{
          |m|
          default_matches["--" + m] = p.query_class_default_value()
        }
        
        displayed_classes[p.class.name] = true
      end
    end
    
    ph = @config.getPromptHandler()
    ph.each_prompt{
      |p|
      if p.is_a?(ConfigurePrompt)
        printDefaults.call(p)
      else
        p.each_prompt{
          |gp|
          if gp.is_a?(ConfigurePrompt)
            printDefaults.call(gp)
          end
        }
      end
    }
    
    puts JSON.pretty_generate(default_matches)
  end
  
  def output_values
    @config.setProperty([SYSTEM], nil)
    build_topologies(@config)
    values_matches = {}
    @values_arguments.each{
      |a|
      values_matches[a] = @config.getProperty(a)
    }
    
    puts JSON.pretty_generate(values_matches)
  end
  
  def allow_command_line_cluster_options?
    false
  end
  
  def output_staging
    unless Configurator.instance.is_locked?()
      error("Unable to show staging information because this is not the installed directory. If this is the staging directory, try running tpm from an installed Tungsten directory.")
      return
    end
    
    staging_host = @config.getProperty(STAGING_HOST)
    staging_user = @config.getProperty(STAGING_USER)
    staging_directory = @config.getProperty(STAGING_DIRECTORY)
    
    unless staging_host.to_s() == ""
      force_output("#{staging_user}@#{staging_host}:#{staging_directory}")
    end
  end
  
  def output_external_configuration
    external_type = @config.getNestedProperty([DEPLOYMENT_EXTERNAL_CONFIGURATION_TYPE])
    external_source = @config.getNestedProperty([DEPLOYMENT_EXTERNAL_CONFIGURATION_SOURCE])
    if external_type == "ini"
      force_output(external_source)
    end
  end
  
  def output_modified_files
    WatchFiles.show_differences(Configurator.instance.get_base_path())
  end
  
  def output_usermap_summary
    unless Configurator.instance.is_enterprise?()
      raise "This command is not supported in the Tungsten Replicator package"
    end
    unless Configurator.instance.is_locked?()
      error("Unable to show usermap summary because this is not the installed directory. If this is the staging directory, try running tpm from an installed Tungsten directory.")
      return
    end
    
    user_map = "#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-connector/conf/user.map"
    if File.exists?(user_map)
      output "# user.map Summary"

      output ""
      output "# Configured users"
      cmd_result("sed 's/^[[:space:]]*//g' #{user_map} | grep -v \"^#\" | grep -v \"^@\" | sed -e /^$/d", true).each_line{
        |line|
        line_parts = line.split(/[ \t]/)
        if line_parts.length >= 2
          line_parts[1] = Array.new(line_parts[1].length).fill('*').join('')
        end
        output line_parts.join(' ')
      }

      output ""
      output "# Script entries"
      output cmd_result("sed 's/^[[:space:]]*//g' #{user_map} | grep -v \"^#\"  | grep \"@script\"", true)
      
      output ""
      output "# DirectRead users"
      output cmd_result("sed 's/^[[:space:]]*//g' #{user_map} | grep -v \"^#\"  | grep \"@direct\"", true)

      output ""
      output "# Host-based routing entries"
      output cmd_result("sed 's/^[[:space:]]*//g' #{user_map} | grep -v \"^#\"  | grep \"@hostoption\"", true)
    else
      error("No file available at tungsten-connector/conf/user.map")
      return
    end
  end
  
  def output_deployments
    get_deployment_configurations().each{
      |cfg|
      cfg.setProperty(SYSTEM, nil)
      cfg.setProperty(REMOTE, nil)
      
      build_topologies(cfg)
      Configurator.instance.output(cfg.to_s())
    }
  end
  
  def allow_command_hosts?
    false
  end
  
  def allow_command_dataservices?
    false
  end
  
  def display_alive_thread?
    false
  end
  
  def self.get_command_name
    "query"
  end
  
  def self.get_command_description
    "Get information about the configuration, topology and version of this directory"
  end
end