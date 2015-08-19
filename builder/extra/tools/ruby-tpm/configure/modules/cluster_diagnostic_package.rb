module ClusterDiagnosticPackage
  LOG_SIZE = 10*1024*1024

  def get_diagnostic_file
    @zip_file
  end

  def parsed_options?(arguments)
    remainder = super(arguments)

    @zip_file = nil

    return remainder
  end

  def call_mysql(config,h_alias,ds,sql)
    ret=nil
    begin
      ret=ssh_result("mysql --defaults-file=#{config.getProperty([REPL_SERVICES, h_alias, REPL_MYSQL_SERVICE_CONF])}  --host=#{ds.host} --port=#{ds.port} --skip-column-names -e\"#{sql}\"", config.getProperty(HOST), config.getProperty(USERID),false)
    rescue
      ret=nil
    end
    ret
  end

  def write_file(file,contents)
    out = File.open(file, "w")
    out.puts(contents)
    out.close
  end

  def run_command(config,command)

    ret=nil

    command_a=command.split(" ")
    begin
      path = ssh_result("which #{command_a[0]} 2>/dev/null", config.getProperty(HOST), config.getProperty(USERID))
      if path != ""
        ret=ssh_result(command, config.getProperty(HOST), config.getProperty(USERID))
      end
    rescue
      ret=nil
    end
    ret
  end

  def get_log(config,source,dest)


    if remote_file_exists?(source, config.getProperty(HOST), config.getProperty(USERID))
      begin
        scp_download(source, "#{dest}.tmp", config.getProperty(HOST), config.getProperty(USERID))
        copy_log("#{dest}.tmp", dest, LOG_SIZE)
        FileUtils.rm("#{dest}.tmp")
      rescue
      end
    end
  end

  def commit
    super()

    begin
      diag_dir = "#{ENV['OLDPWD']}/tungsten-diag-#{Time.now.localtime.strftime("%Y-%m-%d-%H-%M-%S")}"
      Timeout.timeout(5) {
        while File.exists?(diag_dir)
          diag_dir = "#{ENV['OLDPWD']}/tungsten-diag-#{Time.now.localtime.strftime("%Y-%m-%d-%H-%M-%S")}"
        end
      }
    rescue Timeout::Error
      error("Unable to use the #{diag_dir} directory because it already exists")
    end
    FileUtils.mkdir_p(diag_dir)

    get_deployment_configurations().each{
      |config|
      build_topologies(config)
      c_key = config.getProperty(DEPLOYMENT_CONFIGURATION_KEY)
      h_alias = config.getProperty(DEPLOYMENT_HOST)

      FileUtils.mkdir_p("#{diag_dir}/#{h_alias}")
      FileUtils.mkdir_p("#{diag_dir}/#{h_alias}/os_info")
      FileUtils.mkdir_p("#{diag_dir}/#{h_alias}/conf")

      write_file("#{diag_dir}/#{h_alias}/manifest.json",@promotion_settings.getProperty([c_key, "manifest"]))
      write_file("#{diag_dir}/#{h_alias}/tpm.txt",@promotion_settings.getProperty([c_key, "tpm_reverse"]))
      write_file("#{diag_dir}/#{h_alias}/tpm_diff.txt",@promotion_settings.getProperty([c_key, "tpm_diff"]))

      get_log(config,"/etc/hosts", "#{diag_dir}/#{h_alias}/os_info/etc_hosts.txt")
      get_log(config, "/etc/system-release", "#{diag_dir}/#{h_alias}/os_info/system-release.txt")

      #Run a lsb_release -a  if it's available in the path
      write_file("#{diag_dir}/#{h_alias}/os_info/lsb_release.txt",run_command(config,"lsb_release -a"))

      if @promotion_settings.getProperty([c_key, REPLICATOR_ENABLED]) == "true"


        write_file("#{diag_dir}/#{h_alias}/trepctl.json", @promotion_settings.getProperty([c_key, "replicator_json_status"]))

        out = File.open("#{diag_dir}/#{h_alias}/trepctl.txt", "w")
        config.getPropertyOr([REPL_SERVICES], {}).keys().sort().each{
          |rs_alias|
          if rs_alias == DEFAULTS
            next
          end
          out.puts(@promotion_settings.getProperty([c_key, "replicator_status_#{rs_alias}"]))
        }
        out.close

        out = File.open("#{diag_dir}/#{h_alias}/thl.txt", "w")
        config.getPropertyOr([REPL_SERVICES], {}).keys().sort().each{
          |rs_alias|
          if rs_alias == DEFAULTS
            next
          end
          out.puts(@promotion_settings.getProperty([c_key, "thl_info_#{rs_alias}"]))
        }
        out.close

        out = File.open("#{diag_dir}/#{h_alias}/thl_index.txt", "w")
        config.getPropertyOr([REPL_SERVICES], {}).keys().sort().each{
            |rs_alias|
          if rs_alias == DEFAULTS
            next
          end
          out.puts(@promotion_settings.getProperty([c_key, "thl_index_#{rs_alias}"]))
        }
        out.close

        get_log(config,"#{config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-replicator/log/trepsvc.log","#{diag_dir}/#{h_alias}/trepsvc.log")
        get_log(config,"#{config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-replicator/log/xtrabackup.log", "#{diag_dir}/#{h_alias}/xtrabackup.log")
        get_log(config,"#{config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-replicator/log/mysqldump.log", "#{diag_dir}/#{h_alias}/mysqldump.log")
        get_log(config,"#{config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-replicator/log/script.log","#{diag_dir}/#{h_alias}/script.log")

        #Get Replicator Static/Dynamic properties from each host
        config.getPropertyOr([REPL_SERVICES], {}).keys().sort().each{
            |rs_alias|
          if rs_alias == DEFAULTS
            next
          end
          command="cat #{config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-replicator/conf/static-#{config.getProperty([REPL_SERVICES, rs_alias, DEPLOYMENT_SERVICE])}.properties|grep -v password"
          fileName="#{diag_dir}/#{h_alias}/conf/static-#{config.getProperty([REPL_SERVICES, rs_alias, DEPLOYMENT_SERVICE])}.properties"
          write_file(fileName,run_command(config,command))

          command="cat #{config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-replicator/conf/dynamic-#{config.getProperty([REPL_SERVICES, rs_alias, DEPLOYMENT_SERVICE])}.properties|grep -v password"
          fileName="#{diag_dir}/#{h_alias}/conf/dynamic-#{config.getProperty([REPL_SERVICES, rs_alias, DEPLOYMENT_SERVICE])}.properties"
          write_file(fileName,run_command(config,command))
        }

        if @promotion_settings.getProperty([c_key, MANAGER_ENABLED]) == "true"
          get_log(config,"#{config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-manager/log/tmsvc.log", "#{diag_dir}/#{h_alias}/tmsvc.log")
          write_file("#{diag_dir}/#{h_alias}/cctrl.txt",@promotion_settings.getProperty([c_key, "cctrl_status"]))
          write_file("#{diag_dir}/#{h_alias}/cctrl_simple.txt",@promotion_settings.getProperty([c_key, "cctrl_status_simple"]))
          write_file("#{diag_dir}/#{h_alias}/cctrl_ping.txt",@promotion_settings.getProperty([c_key, "cctrl_ping"]))
          write_file("#{diag_dir}/#{h_alias}/cctrl_validate.txt",@promotion_settings.getProperty([c_key, "cctrl_validate"]))
          get_log(config,"/home/#{config.getProperty(USERID)}/.cctrl_history","#{diag_dir}/#{h_alias}/cctrl_history.txt")
        end

        ds=ConfigureDatabasePlatform.build([REPL_SERVICES, h_alias], config)

        if ds.getVendor == "mysql"
          FileUtils.mkdir_p("#{diag_dir}/#{h_alias}/mysql")
          write_file("#{diag_dir}/#{h_alias}/mysql/innodb_status.txt",call_mysql(config,h_alias,ds,'show engine innodb status'))
          write_file("#{diag_dir}/#{h_alias}/mysql/global_variables.txt",call_mysql(config,h_alias,ds,'show global variables'))
          write_file("#{diag_dir}/#{h_alias}/mysql/status.txt",call_mysql(config,h_alias,ds,'show status'))

          #This will probably fail unless the tungsten user has access to the logfile
          mysql_error_log = call_mysql(config,h_alias,ds,"select variable_value from information_schema.global_variables where variable_name='log_error'")
          get_log(config, mysql_error_log,"#{diag_dir}/#{h_alias}/mysql/mysql_error.log")
          # If it does fail we'll try another way
          unless File.exist?("#{diag_dir}/#{h_alias}/mysql/mysql_error.log")
            mysql_error_log_output=ssh_result("sudo -n cat #{mysql_error_log}|tail -n 1000", config.getProperty(HOST), config.getProperty(USERID))
            write_file("#{diag_dir}/#{h_alias}/mysql/mysql_error.log", mysql_error_log_output)
          end
        end
      end

      if @promotion_settings.getProperty([c_key, CONNECTOR_ENABLED]) == "true"
        get_log(config,"#{config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-connector/log/connector.log", "#{diag_dir}/#{h_alias}/connector.log" )
      end

      begin
        df_output=ssh_result("df -hP| grep -v Filesystem", config.getProperty(HOST), config.getProperty(USERID)).split("\n")
        out = File.open("#{diag_dir}/#{h_alias}/os_info/df.txt", "w")
        df_output.each {|partition|
          out.puts(partition)
          partition_a=partition.split(" ")
          if partition_a[4] == '100%'
           error ("Partition #{partition_a[0]} on #{config.getProperty(HOST)} is full - Check and free disk space if required")
          end
        }
        out.close
      rescue CommandError => ce
        exception(ce)
      rescue MessageError => me
        exception(me)
      end

      write_file("#{diag_dir}/#{h_alias}/os_info/ifconfig.txt",run_command(config,"ifconfig") )
      write_file("#{diag_dir}/#{h_alias}/os_info/netstat.txt",run_command(config,"netstat -nap 2>&1") )
      write_file("#{diag_dir}/#{h_alias}/os_info/netstat_sudo.txt",run_command(config,"sudo netstat -nap 2>&1") )
      write_file("#{diag_dir}/#{h_alias}/os_info/free.txt",run_command(config,"free -m") )
      write_file("#{diag_dir}/#{h_alias}/os_info/java_info.txt",run_command(config,"java -version 2>&1") )
      write_file("#{diag_dir}/#{h_alias}/os_info/ruby_info.txt",run_command(config,"ruby -v") )
      write_file("#{diag_dir}/#{h_alias}/os_info/uptime.txt",run_command(config,"uptime") )
      write_file("#{diag_dir}/#{h_alias}/tpm_validate.txt",run_command(config,"#{config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tools/tpm validate-update --tty") )

    }

    require 'zip/zip'
    require 'find'

    @zip_file = "#{diag_dir}.zip"
    Zip::ZipFile.open(@zip_file, Zip::ZipFile::CREATE) do |zipfile|
      Find.find(diag_dir) do |path|
        entry = path.gsub(File.dirname(diag_dir) + "/", "")
        zipfile.add(entry, path)
      end
      zipfile.close
    end
    FileUtils.rmtree(diag_dir)
  end

  # Copy specified log's last n bytes.
  def copy_log(src_path, dest_path, bytes)
    if File.exist?(src_path)
      fout = File.open(dest_path, 'w')

      File.open(src_path, "r") do |f|
        if bytes < File.size(src_path)
          f.seek(-bytes, IO::SEEK_END)
        end
        while (line = f.gets)
          fout.puts line
        end
      end

      fout.close
    end
  end
end

class ClusterDiagnosticCheck < ConfigureValidationCheck
  include CommitValidationCheck

  def set_vars
    @title = "Collect diagnostic information"
  end

  def validate
    c = Configurator.instance
    current_release_directory = @config.getProperty(CURRENT_RELEASE_DIRECTORY)
    cctrl_cmd = c.get_cctrl_path(current_release_directory, @config.getProperty(MGR_RMI_PORT))
    trepctl_cmd = c.get_trepctl_path(current_release_directory, @config.getProperty(REPL_RMI_PORT))
    thl_cmd = c.get_thl_path(current_release_directory)
    tpm_cmd = c.get_tpm_path(current_release_directory)

    begin
      output_property("manifest", cmd_result("cat #{current_release_directory}/.manifest.json"))
      output_property("tpm_reverse", cmd_result("#{tpm_cmd} reverse --public"))
      output_property("tpm_diff", cmd_result("#{tpm_cmd} query modified-files"))

      ["manager", "replicator", "connector"].each {
        |svc|
        svc_path = c.get_svc_path(svc, c.get_base_path())

        if c.svc_is_running?(svc_path)
          cmd_result("#{svc_path} dump", true)
        end
      }

      if c.svc_is_running?(c.get_svc_path("manager", c.get_base_path()))
        cmd_result("echo 'physical;*/*/manager/ServiceManager/diag' | #{cctrl_cmd} -expert", true)
        cmd_result("echo 'physical;*/*/router/RouterManager/diag' | #{cctrl_cmd} -expert", true)
        output_property("cctrl_status", cmd_result("echo 'ls -l' | #{cctrl_cmd} -expert", true))
        output_property("cctrl_status_simple", cmd_result("echo 'ls ' | #{cctrl_cmd} -expert", true))
        output_property("cctrl_ping", cmd_result("echo 'ping ' | #{cctrl_cmd} -expert", true))
        output_property("cctrl_validate", cmd_result("echo 'cluster validate ' | #{cctrl_cmd} -expert", true))
      end

      if c.svc_is_running?(c.get_svc_path("replicator", c.get_base_path()))
        output_property("replicator_json_status", cmd_result("#{trepctl_cmd} services -full -json", true))

        @config.getPropertyOr([REPL_SERVICES], {}).keys().sort().each{
          |rs_alias|
          if rs_alias == DEFAULTS
            next
          end
          output_property("replicator_status_#{rs_alias}", cmd_result("#{trepctl_cmd} -service #{@config.getProperty([REPL_SERVICES, rs_alias, DEPLOYMENT_SERVICE])} status", true))
          output_property("thl_info_#{rs_alias}", cmd_result("#{thl_cmd} -service #{@config.getProperty([REPL_SERVICES, rs_alias, DEPLOYMENT_SERVICE])} info", true))
          output_property("thl_index_#{rs_alias}", cmd_result("#{thl_cmd} -service #{@config.getProperty([REPL_SERVICES, rs_alias, DEPLOYMENT_SERVICE])} index", true))
        }
      end
    rescue CommandError => ce
      exception(ce)
      error(ce.message)
    end
  end

  def enabled?
    super() && (@config.getProperty(HOST_ENABLE_REPLICATOR) == "true")
  end
end

class OldServicesRunningCheck < ConfigureValidationCheck
  include CommitValidationCheck

  def set_vars
    @title = "Check for Tungsten services running outside of the current install directory"
  end

  def validate
    current_release_directory = @config.getProperty(CURRENT_RELEASE_DIRECTORY)
    if File.exists?(current_release_directory)
      current_release_target_dir = File.readlink(current_release_directory)
    else
      return
    end

    current_pid_files = cmd_result("find #{@config.getProperty(HOME_DIRECTORY)}/#{RELEASES_DIRECTORY_NAME} -name *.pid").split("\n")
    allowed_pid_files = []
    if @config.getProperty(HOST_ENABLE_REPLICATOR) == "true"
      allowed_pid_files << "#{current_release_target_dir}/tungsten-replicator/var/treplicator.pid"
    end
    if @config.getProperty(HOST_ENABLE_MANAGER) == "true"
      allowed_pid_files << "#{current_release_target_dir}/tungsten-manager/var/tmanager.pid"
    end
    if @config.getProperty(HOST_ENABLE_CONNECTOR) == "true"
      allowed_pid_files << "#{current_release_target_dir}/tungsten-connector/var/tconnector.pid"
    end

    extra_pid_files = current_pid_files - allowed_pid_files
    extra_pid_files.each{
      |p|
      match = p.match(/([\/a-zA-Z0-9\-\._]*)\/tungsten-([a-zA-Z]*)\//)
      if match
        error("There is an extra #{match[2]} running in #{match[1]}")
        help("shell> #{match[1]}/tungsten-#{match[2]}/bin/#{match[2]} stop; #{current_release_directory}/tungsten-#{match[2]}/bin/#{match[2]} start")
      end
    }
  end
end
