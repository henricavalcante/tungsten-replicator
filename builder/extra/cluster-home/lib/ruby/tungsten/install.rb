class TungstenInstall
  def initialize(base_path)
    unless self.class.is_installed?(base_path)
      raise "Unable to use #{base_path} because it is not an installed Tungsten directory"
    end
    
    @root = TU.cmd_result("cat #{base_path}/.lock")
    TU.debug("Initialize #{self.class.name} from #{@root}")
    @settings = {}
    @topology = nil
    
    begin
      @has_tpm = (TU.cmd_result("#{tpm()} query staging") != "")
      if @has_tpm == false
        @has_tpm = (TU.cmd_result("#{tpm()} query dataservices") != "")
      end
    rescue
      @has_tpm = false
    end
    
    # Preload settings about this installation
    if use_tpm?()
      # Pull the values from tpm
      settings([
        "user",
        "host_name",
        HOST_ENABLE_REPLICATOR,
        HOST_ENABLE_MANAGER,
        HOST_ENABLE_CONNECTOR,
        REPL_RMI_PORT,
        MGR_RMI_PORT,
        MGR_API,
        MGR_API_PORT,
        MGR_API_ADDRESS,
        "preferred_path"
        ])
    else
      # Read the values from files
      setting(HOST_ENABLE_REPLICATOR, "true")
      setting(HOST_ENABLE_MANAGER, "false")
      setting(HOST_ENABLE_CONNECTOR, "false")
      setting(REPL_RMI_PORT, TU.cmd_result("grep rmi_port #{@root}/#{CURRENT_RELEASE_DIRECTORY}/tungsten-replicator/conf/services.properties | grep -v '^#' | awk -F= '{print $2}' | tr -d ' '"))
      setting("host_name", TU.cmd_result("egrep '^replicator.host=' #{@root}/#{CURRENT_RELEASE_DIRECTORY}/tungsten-replicator/conf/services.properties | awk -F= '{print $2}'"))
    end
  end
  
  def self.is_installed?(base_path)
    if File.exists?("#{base_path}/.manifest") && File.exists?("#{base_path}/.lock")
      return true
    else
      return false
    end
  end
  
  # Path to the install directory for Continuent Tungsten. This will include 
  # all previous releases and a symlink to the current running version.
  def root
    @root
  end
  
  # Path to the current running version of Continuent Tungsten.
  def base_path
    "#{@root}/#{CURRENT_RELEASE_DIRECTORY}"
  end
  
  def hostname
    setting("host_name")
  end
  
  def user
    # Access the array directly to avoid an infinite loop
    @settings["user"]
  end
  
  def dataservices
    ds_list = TU.cmd_result("egrep \"^service.name\" #{@root}/#{CURRENT_RELEASE_DIRECTORY}/tungsten-replicator/conf/static-* | awk -F \"=\" '{print $2}'").split("\n")
    
    if use_tpm?() && is_manager?()
      ds_list = ds_list + TU.cmd_result("#{tpm()} query dataservices | awk -F \" \" '{print $1}'").split("\n")
    end
    
    ds_list.uniq()
  end
  
  def default_dataservice
    if is_manager?()
      setting("dataservice_name")
    elsif is_replicator?()
      begin
        local_services = TU.cmd_result("egrep -l \"^replicator.service.type=local\" #{@root}/#{CURRENT_RELEASE_DIRECTORY}/tungsten-replicator/conf/static*").split("\n")
      rescue
        local_services = []
      end
      
      if local_services.size() == 0
        dataservices()[0]
      else
        TU.cmd_result("egrep \"^service.name\" #{local_services[0]} | awk -F \"=\" '{print $2}'")
      end
    else
      dataservices()[0]
    end
  end
  
  def replication_services
    begin
      TU.cmd_result("egrep \"^service.name\" #{@root}/#{CURRENT_RELEASE_DIRECTORY}/tungsten-replicator/conf/static-* | awk -F \"=\" '{print $2}'").split("\n")
    rescue
      []
    end
  end
  
  def tpm
    "#{tungsten_sudo_prefix()}#{@root}/#{CURRENT_RELEASE_DIRECTORY}/tools/tpm"
  end
  
  def setting(key, v = nil)
    if v == nil
      return settings([key])[key]
    else
      @settings[key] = v
      
      return v
    end
  end
  
  def settings(keys)
    remaining_keys = keys - @settings.keys()
    if remaining_keys.size() > 0
      if use_tpm?()    
        begin
          JSON.parse(TU.cmd_result("#{tpm()} query values #{remaining_keys.join(' ')}")).each{
            |k, v|
            @settings[k] = v
          }
        rescue => e
          TU.exception(e)
          raise "Unable to load tpm values #{keys.join(' ')}"
        end
      else
        TU.debug("Unable to autodetect settings because tpm was not used to install this directory")
      end
    end
    
    return_values = {}
    keys.each{
      |k|
      return_values[k] = @settings[k]
    }
    return_values
  end

  def setting_key(first, second, third = nil)
    if first == CONNECTORS
      "#{first}.#{TU.to_identifier(hostname())}.#{second}"
    elsif first == DATASERVICES
      if third == nil
        raise "Unable to create setting key for #{first}.#{second}"
      end
      
      service_aliases = setting(setting_key(HOSTS, "deployment_dataservice_aliases"))
      unless service_aliases.has_key?(second)
        raise "Unable to find a configuration alias for the #{second} service"
      end
      
      "#{first}.#{TU.to_identifier(service_aliases[second])}.#{third}"
    elsif first == HOSTS
      "#{first}.#{TU.to_identifier(hostname())}.#{second}"
    elsif first == MANAGERS
      if third == nil
        raise "Unable to create setting key for #{first}.#{second}"
      end
      
      service_aliases = setting(setting_key(HOSTS, "deployment_dataservice_aliases"))
      unless service_aliases.has_key?(second)
        raise "Unable to find a configuration alias for the #{second} service"
      end
      
      "#{first}.#{TU.to_identifier(service_aliases[second])}_#{TU.to_identifier(hostname())}.#{third}"
    elsif first == REPL_SERVICES
      if third == nil
        raise "Unable to create setting key for #{first}.#{second}"
      end
      
      service_aliases = setting(setting_key(HOSTS, "deployment_dataservice_aliases"))
      unless service_aliases.has_key?(second)
        raise "Unable to find a configuration alias for the #{second} service"
      end
      
      "#{first}.#{TU.to_identifier(service_aliases[second])}_#{TU.to_identifier(hostname())}.#{third}"
    else
      "#{first}.#{TU.to_identifier(hostname())}.#{second}"
    end
  end
  
  def cctrl
    "#{tungsten_sudo_prefix()}#{@root}/#{CURRENT_RELEASE_DIRECTORY}/tungsten-manager/bin/cctrl -expert -port #{setting(MGR_RMI_PORT)}"
  end
  
  def mgr_api_uri
    if setting(MGR_API_ADDRESS) == "0.0.0.0"
      "#{hostname()}:#{setting(MGR_API_PORT)}"
    else
      "#{setting(MGR_API_ADDRESS)}:#{setting(MGR_API_PORT)}"
    end
  end
  
  def status(dataservice = nil)
    if dataservice == nil
      dataservice = default_dataservice()
    end
    
    unless dataservices().include?(dataservice)
      raise "Unable to provide a status for #{dataservice} because it is not defined on this host"
    end
    
    return TungstenStatus.new(self, dataservice)
  end
  
  def topology(dataservice = nil)
    if dataservice == nil
      dataservice = default_dataservice()
    end
    
    unless dataservices().include?(dataservice)
      raise "Unable to provide a topology for #{dataservice} because it is not defined on this host"
    end
    
    return TungstenTopology.new(self, dataservice)
  end
  
  def trepctl(service)
    "#{tungsten_sudo_prefix()}#{@root}/#{CURRENT_RELEASE_DIRECTORY}/tungsten-replicator/bin/trepctl -port #{setting(REPL_RMI_PORT)} -service #{service}"
  end
  
  def trepctl_value(service, key)
    TU.cmd_result("#{trepctl(service)} status | grep #{key} | awk -F: '{ st = index($0,\":\");print substr($0,st+1)}' | tr -d ' '")
  end

  def trepctl_name_value(service, name_value, stage, key)
    begin
      output=JSON.parse(TU.cmd_result("#{trepctl(service)} status -name #{name_value} -json"))
    rescue Exception => e
      output=nil
    end
    stage_values=output.find{|x| x['stage']=stage}
    stage_values[key]
  end

  def trepctl_name_all(service, name_value)
    begin
      output=JSON.parse(TU.cmd_result("#{trepctl(service)} status -name #{name_value} -json"))
    rescue Exception => e
      output=nil
    end
    output
  end

  def trepctl_property(service, key)
    properties = JSON.parse(TU.cmd_result("#{trepctl(service)} properties -filter #{key}"))
    if properties.has_key?(key)
      return properties[key]
    else
      raise "Unable to find a value for #{key} in the output of `trepctl -service #{service} properties`."
    end
  end
  
  def thl(service = nil)
    if service == nil
      "#{tungsten_sudo_prefix()}#{@root}/#{CURRENT_RELEASE_DIRECTORY}/tungsten-replicator/bin/thl"
    else
      "#{tungsten_sudo_prefix()}#{@root}/#{CURRENT_RELEASE_DIRECTORY}/tungsten-replicator/bin/thl -service #{service}"
    end
  end
  
  def service_path(component)
    "#{@root}/#{CURRENT_RELEASE_DIRECTORY}/tungsten-#{component}/bin/#{component}"
  end
  
  def is_running?(component)
    begin
      TU.cmd_result("#{service_path(component)} status")
      return true
    rescue CommandError
      return false
    end
  end
  
  def is_replicator?
    (setting(HOST_ENABLE_REPLICATOR) == "true")
  end
  
  def is_manager?
    (setting(HOST_ENABLE_MANAGER) == "true")
  end
  
  def is_connector?
    (setting(HOST_ENABLE_CONNECTOR) == "true")
  end
  
  def is_commercial?
    File.exists?("#{@root}/#{CURRENT_RELEASE_DIRECTORY}/tungsten-manager")
  end
  
  def use_tpm?
    @has_tpm
  end
  
  def ensure_cctrl(cmd, max_tries = 5)
    i=0
    while (i<max_tries)
      begin
        return TU.cmd_result("echo #{cmd}| #{cctrl}")
      rescue CommandError
        TU.debug(e)
      end
      i+=1
    end
    
    raise "Unable to execute '#{cmd}' in cctrl"
  end
  
  def inherit_path
    if setting("preferred_path") != ""
      ENV['PATH'] = setting("preferred_path").to_s() + ":" + ENV['PATH']
    end
  end
  
  # Build a sudo prefix to run a command as the tungsten system user
  def tungsten_sudo_prefix
    if self.user() == nil || ENV['USER'] == self.user()
      return ""
    else
      return "sudo -u #{self.user()} -n -i "
    end
  end
  
  def sudo_prefix
    if ENV['USER'] == "root" || setting("root_command_prefix") != "true"
      return ""
    else
      return "sudo -n "
    end
  end
  
  def datasource_type(service, use_direct_extractor = false)
    if use_direct_extractor == true
      key = "repl_direct_datasource_type"
    else
      key = "repl_datasource_type"
    end
    
    setting(setting_key(REPL_SERVICES, service, key))
  end
  
  def datasource(service, use_direct_extractor = false)
    type = datasource_type(service, use_direct_extractor)
    
    case type
    when "mysql"
      require "#{File.dirname(__FILE__)}/datasources/mysql.rb"
      return TungstenScriptMySQLDatasource.new(self, service, use_direct_extractor)
    else
      raise "Unable to create a TungstenDatasource for #{service} because it uses #{type}"
    end
  end
  
  def can_sql?(service, use_direct_extractor = false)
    case datasource_type(service, use_direct_extractor)
    when "mysql"
      return true
    when "redshift"
      return true
    else
      return false
    end
  end
  
  def sql_results(service, sql, use_direct_extractor = false)
    unless can_sql?(service, use_direct_extractor)
      raise "Unable to run SQL command for the #{service} dataservice. The datasource type does not support SQL commands."
    end
    
    cfg = nil
    command = nil
    begin
      # Disable logging of command results so the password doesn't end up in a log file
      TU.log_cmd_results?(false)
      if use_direct_extractor == true
        url= setting(setting_key(REPL_SERVICES, service, "repl_direct_datasource_jdbcqueryurl"))
        user = setting(setting_key(REPL_SERVICES, service, "repl_direct_datasource_user"))
        password = setting(setting_key(REPL_SERVICES, service, "repl_direct_datasource_password"))
      else
        url = setting(setting_key(REPL_SERVICES, service, "repl_datasource_jdbcqueryurl"))
        user = setting(setting_key(REPL_SERVICES, service, "repl_datasource_user"))
        password = setting(setting_key(REPL_SERVICES, service, "repl_datasource_password"))
      end
      TU.log_cmd_results?(true)
      
      cfg = Tempfile.new("cnf")
      cfg.puts("url=#{url}")
      cfg.puts("user=#{user}")
      cfg.puts("password=#{password}")
      cfg.close()
      
      command = Tempfile.new("query")
      if sql.is_a?(Array)
        command.puts(sql.join("\n"))
      else
        command.puts(sql)
      end
      command.close()
      
      output = TU.cmd_result("#{self.base_path()}/tungsten-replicator/bin/query -conf #{cfg.path} -file #{command.path}")
    rescue CommandError => ce
      TU.debug(ce)
      raise "There was an error processing the query: #{ce.result}"
    ensure
      if cfg != nil
        cfg.close()
        cfg.unlink()
      end
      
      if command != nil
        command.close()
        command.unlink()
      end
    end
    
    results = JSON.parse(output)
    results.each {
      |result|
      if result["rc"] == 0
        result["error"] = nil
      else
        obj = CommandError.new(result["statement"], result["rc"], result["results"], result["error"])
        result["error"] = obj
      end
    }
    results
  end
  
  def sql_result(service, sql, use_direct_extractor = false)
    results = sql_results(service, sql, use_direct_extractor)
    
    if results.size() == 0
      raise "No results were returned by #{sql}"
    end
    
    if results[0]["error"] == nil
      return results[0]["results"][0]
    else
      raise results[0]["error"]
    end
  end
  
  def check_sql_results(results)
    results.each{
      |r|
      if r["error"] != nil
        raise r["error"]
      end
    }
  end
  
  def self.get(path)
    @@instances ||= {}
    unless @@instances.has_key?(path)
      @@instances[path] = TungstenInstall.new(path)
    end
    return @@instances[path]
  end
  
  class TungstenTopology
    attr_reader :datasources, :master, :connectors, :dataservices, :type

    def initialize(install, dataservice)
      @install = install
      @name = dataservice
      
      unless @install.use_tpm?()
        raise "Unable to parse the topology for #{@name} from #{@install.hostname()}:#{@install.root()} because tpm was not used for installation"
      end
      
      values = @install.settings([
        "dataservices.#{@name}.dataservice_hosts",
        "dataservices.#{@name}.dataservice_master_host",
        "dataservices.#{@name}.dataservice_connectors",
        "dataservices.#{@name}.dataservice_composite_datasources",
        "dataservices.#{@name}.dataservice_topology"
      ])
      
      @type = values["dataservices.#{@name}.dataservice_topology"]
      @members = values["dataservices.#{@name}.dataservice_hosts"].to_s().split(",")
      @master = values["dataservices.#{@name}.dataservice_master_host"]
      @connectors = values["dataservices.#{@name}.dataservice_connectors"].to_s().split(",")
      @dataservices = values["dataservices.#{@name}.dataservice_composite_datasources"].to_s().split(",")
    end
    
    def is_composite?
      (@dataservices.size() > 0)
    end
    
    def to_hash
      {
        :hostname => @install.hostname(),
        :root => @install.root(),
        :is_composite => is_composite?(),
        :type => @type,
        :members => @members,
        :master => @master,
        :connectors => @connectors,
        :dataservices => @dataservices
      }
    end
  end
end