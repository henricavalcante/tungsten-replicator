class MySQLTerminalCommand
  include ConfigureCommand
  include ClusterCommandModule

  def skip_prompts?
    true
  end
  
  def display_alive_thread?
    false
  end
  
  def get_bash_completion_arguments
    super() + ["--extractor"]
  end
  
  def output_command_usage
    super()
    
    output_usage_line("--extractor", "Create a connection to the direct extraction datasource (if available).")
  end
  
  def parsed_options?(arguments)
    arguments = super(arguments)
    
    if Configurator.instance.display_help? && !Configurator.instance.display_preview?()
      return true
    end
    
    @extractor = false
    
    # Define extra option to load event.  
    opts=OptionParser.new
    opts.on("--extractor") { @extractor = true }
    remainder = Configurator.instance.run_option_parser(opts, arguments)
    
    @terminal_args = remainder
    
    []
  end
 
  def run
    unless Configurator.instance.is_locked?()
      error("Unable to run this command because this is not the installed directory. If this is the staging directory, try running tpm from an installed Tungsten directory.")
      return
    end
    
    build_topologies(@config)
    unless @config.getProperty(HOST_ENABLE_REPLICATOR) == "true"
      error("Unable to run this command because the current host is not a database server")
      return false
    end
    
    if command_dataservices().size() > 0
      @service = command_dataservices()[0]
    else
      @service = nil
    end
    
    rs_alias = nil
    @config.getPropertyOr(REPL_SERVICES, {}).each_key{
      |rs|
      if rs == DEFAULTS
        next
      end
      if @service != nil
        if @config.getProperty([REPL_SERVICES, rs, DEPLOYMENT_SERVICE]) == @service
          rs_alias = rs
          break
        end
      else
        rs_alias = rs
        break
      end
    }
    
    # Override the --extractor flag if this is not a direct replicator
    unless @config.getProperty([REPL_SERVICES, rs_alias, REPL_ROLE]) == REPL_ROLE_DI
      @extractor = false
    end
    
    ds = ConfigureDatabasePlatform.build([REPL_SERVICES, rs_alias], @config, @extractor)
    
    case ds.class().to_s()
    when "MySQLDatabasePlatform"
      open_mysql(rs_alias, ds)
    when "RedshiftDatabasePlatform"
      open_redshift(rs_alias, ds)
    else
      error("Unable to open connection to non MySQL server")
      return false
    end
  end
    
  def open_mysql(rs_alias, ds)
    if @extractor == true
      conf = @config.getProperty([REPL_SERVICES, rs_alias, EXTRACTOR_REPL_MYSQL_SERVICE_CONF])
    else
      conf = @config.getProperty([REPL_SERVICES, rs_alias, REPL_MYSQL_SERVICE_CONF])
    end
    
    if ENV.has_key?("OLDPWD")
      pre = "cd #{ENV["OLDPWD"]};"
    else
      pre = ""
    end
    
    exec("#{pre}mysql --defaults-file=#{conf} --host=#{ds.host} --port=#{ds.port} #{@terminal_args.join(' ')}")
  end
  
  def open_redshift(rs_alias, ds)
    dbname = @config.getProperty([REPL_SERVICES, rs_alias, REPL_REDSHIFT_DBNAME])
  
    conf = Tempfile.new("redpass")
    conf.puts("#{ds.host}:#{ds.port}:#{dbname}:#{ds.username}:#{ds.password}")
    conf.close()
    ENV['PGPASSFILE'] = conf.path()
    
    if ENV.has_key?("OLDPWD")
      pre = "cd #{ENV["OLDPWD"]};"
    else
      pre = ""
    end
    
    exec("#{pre}psql --host=#{ds.host} --port=#{ds.port} --dbname=#{dbname} --username=#{ds.username} --no-password; rm #{conf.path()}")
  end

  def self.get_command_name
    'mysql'
  end
  
  def self.get_command_aliases
    ['db']
  end
  
  def self.get_command_description
    "Open a terminal to the DBMS"
  end
end