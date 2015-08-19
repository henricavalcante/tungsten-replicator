module ClusterHostCheck
  include GroupValidationCheckMember
  
  def self.included(subclass)
    @subclasses ||= []
    @subclasses << subclass
  end

  def self.subclasses
    @subclasses || []
  end
  
  def get_member_key(key)
    [key]
  end
  
  def get_dataservice_key(key)
    [DATASERVICES, @config.getProperty(DEPLOYMENT_DATASERVICE), key]
  end
end

module ConnectorOnlyCheck
  def enabled?
    super() && @config.getProperty(HOST_ENABLE_CONNECTOR) == "true"
  end
end

class OpensslLibraryCheck < ConfigureValidationCheck
  include ClusterHostCheck
  include LocalValidationCheck
  
  def set_vars
    @title = "OpenSSL Library Check"
    @description = "Look for the Ruby OpenSSL library needed to connecto to remote hosts"
    @fatal_on_error = true
  end
  
  def validate
    begin
      require "openssl"
    rescue LoadError
      error "Unable to find the Ruby openssl library"
      help "Try installing the openssl package for your version of Ruby (libopenssl-ruby#{RUBY_VERSION[0,3]})."
    end
  end
  
  def enabled?
    super() && !(Configurator.instance.is_localhost?(@config.getProperty(HOST)))
  end
end

class SSHLoginCheck < ConfigureValidationCheck
  include ClusterHostCheck
  include LocalValidationCheck
  
  def set_vars
    @title = "SSH login"
    @description = "Ensure that the configuration host can login to each member of the dataservice via SSH"
    @properties << USERID
    @fatal_on_error = true
  end
  
  def validate
    # whoami will output the current user and we can confirm that the login succeeded
    begin
      login_result = ssh_result("whoami", @config.getProperty(HOST), @config.getProperty(USERID))
    rescue => e
      exception(e)
      login_result = ""
    end
    
    
    if login_result != @config.getProperty(USERID)
      if login_result=~ /#{@config.getProperty(USERID)}/
          error "SSH to #{@config.getProperty(HOST)} as #{@config.getProperty(USERID)} is returning more than one line."
          help "Ensure that the .bashrc and .bash_profile files are not printing messages on #{@config.getProperty(HOST)} with out using a test like this. if [ \"$PS1\" ]; then echo \"Your message here\"; fi"
          help "If you are using the 'Banner' argument in /etc/ssh/sshd_config, try putting the contents into /etc/motd"
      else
        error "Unable to SSH to #{@config.getProperty(HOST)} as #{@config.getProperty(USERID)}."
        
        if Configurator.instance.is_localhost?(@config.getProperty(HOST))
          help "You are not running the command as #{@config.getProperty(USERID)}.  There are three ways to fix the error:"
          help "  1. Install Tungsten as the '#{@config.getProperty(USERID)}' user"
          help "  2. Run Tungsten as the '#{Configurator.instance.whoami()}' user by modifying the configuration with --user=#{Configurator.instance.whoami()}"
          help "  3. Setup SSH access from the '#{Configurator.instance.whoami()}' user to '#{@config.getProperty(USERID)}' on the current host"
        else
          help "Ensure that the host is running and that you can login as #{@config.getProperty(USERID)} via SSH using key authentication"
        end
      end
    else
      debug "SSH login successful"
    end
  end
  
  def enabled?
    if Configurator.instance.is_localhost?(@config.getProperty(HOST))
      if Configurator.instance.whoami() == @config.getProperty(USERID)
        return false
      end
    end
    
    return super()
  end
end

class WriteableTempDirectoryCheck < ConfigureValidationCheck
  include ClusterHostCheck
  include LocalValidationCheck
  
  def set_vars
    @title = "Writeable temp directory"
    @properties << TEMP_DIRECTORY
  end
  
  def validate
    validation_temp_directory = "#{@config.getProperty(TEMP_DIRECTORY)}/#{@config.getProperty(CONFIG_TARGET_BASENAME)}/#{Configurator.instance.get_basename()}/"
    debug "Checking #{validation_temp_directory}"
    
    user = @config.getProperty(USERID)
    begin
      ssh_result("mkdir -p #{validation_temp_directory}", @config.getProperty(HOST), user)
    rescue RemoteCommandError => rce
      error("Unable to create the temporary directory '#{validation_temp_directory}':#{rce.result}")
      
      # Do not process the other parts of this check
      return
    end
    
    # The -D flag will tell us if it is a directory
    is_directory = ssh_result("if [ -d #{validation_temp_directory} ]; then echo 0; else echo 1; fi", @config.getProperty(HOST), user)
    unless is_directory == "0"
      error "#{validation_temp_directory} is not a directory"
    else
      debug "#{validation_temp_directory} is a directory"
    end
    
    # The -w flag will tell us if it is writeable
    writeable = ssh_result("if [ -w #{validation_temp_directory} ]; then echo 0; else echo 1; fi", @config.getProperty(HOST), user)   
    unless writeable == "0"
      error "#{validation_temp_directory} is not writeable by #{user}"
    else
      debug "#{validation_temp_directory} is writeable by #{user}"
    end
  
    # Check to see if the tmp directory is executable
    noexec = ssh_result("echo  'echo 0' > #{validation_temp_directory}/check.sh;chmod +x #{validation_temp_directory}/check.sh; #{validation_temp_directory}/check.sh; rm -f #{validation_temp_directory}/check.sh", @config.getProperty(HOST), user)   
    unless noexec == "0"
      error "Unable to execute scripts on #{validation_temp_directory} is noexec set?"
    else
      debug "#{validation_temp_directory} is executable by #{user}"
    end
  end
  
  def enabled?
    super() && !(Configurator.instance.is_localhost?(@config.getProperty(HOST)))
  end
end

class WhichAvailableCheck < ConfigureValidationCheck
  include ClusterHostCheck
  include LocalValidationCheck
  
  def set_vars
    @title = "which in path"
    @fatal_on_error = true
  end
  
  def validate
    begin
      which_path = ssh_result("which which 2>/dev/null", @config.getProperty(HOST), @config.getProperty(USERID))
      if which_path == ""
        error("Unable to find which in the path")
      end
    rescue CommandError
      error("Unable to find which in the path")
    end
  end
end

class RsyncAvailableCheck < ConfigureValidationCheck
  include ClusterHostCheck
  include LocalValidationCheck
  
  def set_vars
    @title = "rsync in path"
  end
  
  def validate
    rsync_path = ssh_result("which rsync 2>/dev/null", @config.getProperty(HOST), @config.getProperty(USERID))
    if rsync_path == ""
      error("Unable to find rsync in the path")
    end
  end
  
  def enabled?
    super() && !(Configurator.instance.is_localhost?(@config.getProperty(HOST)))
  end
end

class InstallationScriptCheck < ConfigureValidationCheck
  include ClusterHostCheck
  include LocalValidationCheck
  
  def set_vars
    @title = "Installation script already running"
  end
  
  def validate
    ssh_result("ps ax 2>/dev/null | grep configure.rb | grep -v firewall | grep -v grep | awk '{print $1}'", @config.getProperty(HOST), @config.getProperty(USERID)).split("\n").each{
      |pid|
      error("There is already another Tungsten installation script running")
    }
  end
  
  def enabled?
    super() && !(Configurator.instance.is_localhost?(@config.getProperty(HOST)))
  end
end

class ConfigurationStorageDirectoryCheck < ConfigureValidationCheck
  include ClusterHostCheck
  include LocalValidationCheck
  
  def set_vars
    @title = "Check that $REPLICATOR_PROFILES is defined if $CONTINUENT_PROFILES is"
  end
  
  def validate
    if Configurator.instance.is_enterprise?()
      return
    end
    
    if ENV.has_key?("CONTINUENT_PROFILES")
      unless ENV.has_key?("REPLICATOR_PROFILES")
        warning("You have defined $CONTINUENT_PROFILES but not $REPLICATOR_PROFILES. This may cause problems if you are working with Continuent Tungsten and Tungsten Replicator on the same staging server. This will not cause any issues if you are only using Tungsten Replicator.")
      else
        if ENV["CONTINUENT_PROFILES"] == ENV["REPLICATOR_PROFILES"]
          error("You have set $CONTINUENT_PROFILES and $REPLICATOR_PROFILES to #{ENV["CONTINUENT_PROFILES"]}. The values for these environment variables must be different.")
        end
      end
    end
  end
end

class MatchingHomeDirectoryCheck < ConfigureValidationCheck
  include ClusterHostCheck
  
  def set_vars
    @title = "Home directory matches current directory"
    @properties << HOME_DIRECTORY
    @fatal_on_error = true
  end
  
  def validate
    debug "Checking #{@config.getProperty(HOME_DIRECTORY)} matches the current running directory"
    
    unless File.exists?("#{@config.getProperty(HOME_DIRECTORY)}/tungsten")
      error("You are running from a configured directory but the new configuration does not update the current directory.")
    else
      unless File.readlink("#{@config.getProperty(HOME_DIRECTORY)}/tungsten") == Configurator.instance.get_base_path()
        error("You are running from a configured directory but the new configuration does not update the current directory.")
      end
    end
  end
  
  def enabled?
    super() && (@config.getProperty(HOME_DIRECTORY) != nil) && Configurator.instance.is_locked?()
  end
end

class WriteableHomeDirectoryCheck < ConfigureValidationCheck
  include ClusterHostCheck
  
  def set_vars
    @title = "Writeable home directory"
    @properties << HOME_DIRECTORY
  end
  
  def validate
    debug "Checking #{@config.getProperty(HOME_DIRECTORY)} can be created"
    
    if @config.getProperty(HOME_DIRECTORY) =~ /^#{Configurator.instance.get_base_path()}[\/]?.*/ &&
        !(@config.getProperty(HOME_DIRECTORY) =~ /^#{Configurator.instance.get_base_path()}[\/]?$/)
      error("Unable to create the home directory as a sub-directory of the current package")
    end
    
    if @config.getProperty(HOME_DIRECTORY) == @config.getProperty(CURRENT_RELEASE_DIRECTORY)
      dir = File.dirname(@config.getProperty(HOME_DIRECTORY))
    else
      dir = @config.getProperty(HOME_DIRECTORY)
    end
    
    begin
      ssh_result("mkdir -p #{dir}", @config.getProperty(HOST), @config.getProperty(USERID))
    
      # The -d flag will tell us if it is a directory
      is_directory = ssh_result("if [ -d #{dir} ]; then echo 0; else echo 1; fi", @config.getProperty(HOST), @config.getProperty(USERID))
    
      unless is_directory == "0"
        error "#{dir} is not a directory"
      else
        debug "#{dir} is a directory"
      end
    
      # The -w flag will tell us if it is writeable
      writeable = ssh_result("if [ -w #{dir} ]; then echo 0; else echo 1; fi", @config.getProperty(HOST), @config.getProperty(USERID))
    
      unless writeable == "0"
        error "#{dir} is not writeable"
      else
        debug "#{dir} is writeable"
      end
    rescue CommandError
      error("There was an error creating #{dir} on #{@config.getProperty(HOST)}.")
      help("Make sure that the directory exists and is writeable by #{@config.getProperty(USERID)}")
    end
  end
  
  def enabled?
    @config.getProperty(HOME_DIRECTORY) != nil
  end
end

class RubyVersionCheck < ConfigureValidationCheck
  include ClusterHostCheck
  include LocalValidationCheck
  
  def set_vars
    @title = "Ruby version"
  end
  
  def validate
    
    ruby_version = cmd_result("ruby -v | cut -f 2 -d ' '")
    
    if ruby_version =~ /^1\.8\.[5-9]/
      debug "Ruby version (#{ruby_version}) OK"
    elsif ruby_version =~ /^1\.8/
      error "Ruby version must be at least 1.8.5"
    elsif ruby_version =~ /^1\.9\.2/
      warning "Ruby version may not work; try Ruby 1.8.5-1.8.7 or 1.9.3"
    elsif ruby_version =~ /^1\.9/
      debug "Ruby version (#{ruby_version}) OK"
    elsif ruby_version =~ /^2\.0/
      debug "Ruby version (#{ruby_version}) OK"
    else
      error "Unrecognizable Ruby version: #{ruby_version}"
    end
  end
  
  def enabled?
    super() && (!Configurator.instance.is_localhost?(@config.getProperty(HOST)))
  end
end

class OSCheck < ConfigureValidationCheck
  include ClusterHostCheck
  
  def set_vars
    @title = "Operating system"
  end
  
  def validate
    # Check operating system.
    debug "Checking operating system type"
    uname = cmd_result("uname -a")
    uname_s = cmd_result("uname -s")
    os = case
    when uname_s == "Linux" then OS_LINUX
    when uname_s == "Darwin" then OS_MACOSX
    when uname_s == "SunOS" then OS_SOLARIS
    else OS_UNKNOWN
    end
    if os == OS_UNKNOWN
      raise "Could not determine OS!  Tungsten currently supports Linux, Solaris or OS X"
    elsif os == OS_MACOSX
      warning "Mac OS X is only provisionally supported"
    end

    # Architecture is unknown by default.
    debug "Checking processor architecture" 
    uname_m = cmd_result("uname -m")
    arch = case
    when uname_m == "x86_64" then OS_ARCH_64
    when uname_m == "i386" then OS_ARCH_32
    when uname_m == "i686" then OS_ARCH_32
    else
      OS_ARCH_UNKNOWN
    end
    if arch == OS_ARCH_UNKNOWN
      raise "Could not determine OS architecture.  The `uname -m` response does not match \"x86_64\", \"i386\" or \"i686\""
    elsif arch == OS_ARCH_32
      warning "32-bit architecture not recommended for DBMS nodes"
    end

    # Report on Linux distribution.
    if os == OS_LINUX
      debug "Checking Linux distribution" 
      if File.exist?("/etc/redhat-release")
        system = cmd_result("cat /etc/redhat-release")
      elsif File.exist?("/etc/debian_version")
        system = cmd_result("cat /etc/debian_version")
      elsif File.exist?("/etc/system-release")
            system = cmd_result("cat /etc/system-release")
            amazon_check = cmd_result("cat /etc/system-release | grep Amazon | wc -l")
            if amazon_check == '0'
               raise "Could not determine Linux distribution.  Tungsten has been tested on RedHat and Debian systems."
                debug "Found \"/etc/system-release\" but it does not appear to be an Amazon distribution."
            end
      else
        debug "Tungsten checks for the presence of \"/etc/redhat-release\" or \"/etc/debian_version\" to determine the distribution." 
        raise "Could not determine Linux distribution.  Tungsten has been tested on RedHat and Debian systems."
      end
    end

    debug "Supported operating system found: #{system}"
  end
end

class JavaVersionCheck < ConfigureValidationCheck
  include ClusterHostCheck
  
  def set_vars
    @title = "Java version"
  end
  
  def validate
    # Look for Java.
    java_out = cmd_result("java -version 2>&1")
    if $? == 0
      if java_out =~ /Java|JDK/
        debug "Supported Java found"
        
        java_version = java_out.scan(/(?:java|openjdk) version \"1.(?:6|7|8)./)
        unless java_version.length == 1
          error "Java 1.6 or greater is required to run Tungsten"
        end
      else
        error "Unknown Java version"
      end
    else
      error "Java binary not found in path"
    end
  end
end

class OpenFilesLimitCheck < ConfigureValidationCheck
  include ClusterHostCheck
  
  def set_vars
    @title = "Allowed open files check"
  end
  
  def validate
    begin
      limit = Process.getrlimit(Process::RLIMIT_NOFILE)[0]
    
      if limit.to_i < 65535
        warning("The open file limit is set to #{limit}, we suggest a value of 65535. Add '*       -    nofile  65535' to your /etc/security/limits.conf and restart your session")
      else
        info("The open files limit is set to #{limit}")
      end
    rescue
      error("There was an error while checking the open files limit")
    end
  end
end

class ProcessLimitCheck < ConfigureValidationCheck
  include ClusterHostCheck
  
  def set_vars
    @title = "Allowed number of processes check" 
  end
  
  def validate
    begin
      limit = Process.getrlimit(Process::RLIMIT_NPROC)[0]
    
      if limit.to_i < 8096
        warning("The process limit is set to #{limit}, we suggest a value of at least 8096. Add '#{@config.getProperty(USERID)}       -    nproc  8096' to your /etc/security/limits.conf and restart Tungsten processes.")
      else
        info("The processes limit is set to #{limit}")
      end
    rescue
      error("There was an error while checking the processes limit")
    end
  end

  def enabled?
    super() && ((@config.getProperty(HOST_ENABLE_MANAGER) == "true") || 
      (@config.getProperty(HOST_ENABLE_CONNECTOR) == "true"))
  end
end

class SudoCheck < ConfigureValidationCheck
  include ClusterHostCheck
  
  def set_vars
    @title = "Sudo"
  end
  
  def validate
    sudo_ls_output = cmd_result("sudo -n which which", true)
    if $? != 0
      error("Unable to run 'sudo which'")
      add_help()
    else
      # Get the allowed sudo settings and commands
      sudo_output = cmd_result("script -q -c \"sudo -l\"")
      if sudo_output =~ /requiretty/
        unless sudo_output =~ /!requiretty/
          error "Sudo has the requiretty option enabled"
        end
      end
      
      if is_valid?()
        debug "Sudo access is setup correctly"
      else
        add_help()
      end
    end
  end
  
  def add_help
    help("Update the /etc/sudoers file or disable sudo by adding --enable-sudo-access=false")
    help("Review https://docs.continuent.com/ for more details on the /etc/sudoers file")
  end
  
  def enabled?
    (super() && @config.getProperty(ROOT_PREFIX) == "true")
  end
end

class HostnameCheck < ConfigureValidationCheck
  include ClusterHostCheck
  include ClusteringHostCheck
  
  def set_vars
    @title = "Hostname"
    @description = "Ensure hostname is legal host name, not localhost"
  end

  # Check the host name. 
  def validate
    unless Configurator.instance.hostname().casecmp(@config.getProperty(HOST)) == 0
      error "Hostname must be #{@config.getProperty(HOST)}, or change the configuration to use #{Configurator.instance.hostname()}"
    else
      debug "Hostname is OK"
    end
  end
end

class InstallServicesCheck < ConfigureValidationCheck
  include ClusterHostCheck
  
  def set_vars
    @title = "Install services check"
  end
  
  def validate
    if File.exist?("/etc/redhat-release")
      info("OS supports service installation")
    elsif File.exist?("/etc/debian_version")
      info("OS supports service installation")
    else
      error("OS is unable to support service installation")
    end
  end
  
  def enabled?
    (@config.getProperty(SVC_INSTALL) == "true")
  end
end

class ConfiguredDirectoryCheck < ConfigureValidationCheck
  include ClusterHostCheck
  
  def set_vars
    @title = "Directory is already configured"
    self.extend(NotTungstenUpdateCheck)
  end
  
  def validate
    if File.exists?(@config.getProperty(DIRECTORY_LOCK_FILE))
      error("This directory has already been configured.  Try using `tools/tpm update` to make changes.")
      help("If you are installing from a previously used directory, download a fresh copy and try again.")
    end
  end
end


class HostReplicatorServiceRunningCheck < ConfigureValidationCheck
  include ClusterHostCheck
  include ReplicatorEnabledCheck
  
  def set_vars
    @title = "Replicator is running check"
    self.extend(NotTungstenUpdateCheck)
  end
  
  def validate
    if Configurator.instance.svc_is_running?(@config.getProperty(SVC_PATH_REPLICATOR))
      error("The replicator in #{@config.getProperty(HOME_DIRECTORY)} is still running.  You must stop it before installation can continue.")
    else
      info("The replicator in #{@config.getProperty(HOME_DIRECTORY)} is stopped.")
    end
  end
end

class TransferredLogStorageCheck < ConfigureValidationCheck
  include ClusterHostCheck
  include ReplicatorEnabledCheck
  
  def set_vars
    @title = "Transferred log storage check"
  end
  
  def validate
    if File.exists?(@config.getProperty(REPL_RELAY_LOG_DIR)) && !File.directory?(@config.getProperty(REPL_RELAY_LOG_DIR))
      error("Transferred log directory #{@config.getProperty(REPL_RELAY_LOG_DIR)} already exists as a file")
    end
  end
  
  def enabled?
    super() && @config.getProperty(REPL_RELAY_LOG_DIR).to_s != ""
  end
end

class InstallingOverExistingInstallation < ConfigureValidationCheck
  include ClusterHostCheck
  
  def set_vars
    @title = "Check that there is not already an active installation"
    self.extend(NotTungstenUpdateCheck)
  end
  
  def validate
    current_release_dir = @config.getProperty(CURRENT_RELEASE_DIRECTORY)
    if File.exist?(current_release_dir)
      current_release_target_dir = File.readlink(current_release_dir)
      if current_release_target_dir && File.exist?(current_release_target_dir)
        error("There is already an installation at #{current_release_dir}")
        help("Use the tools/tpm update to modify an existing installation")
      end
    end
  end
end

class StartingStoppedServices < ConfigureValidationCheck
  include ClusterHostCheck
  
  def set_vars
    @title = "Check if we are being asked to start services which are not currently running"
    self.extend(TungstenUpdateCheck)
  end
  
  def validate
    are_stopped = false

    current_release_directory = @config.getProperty(CURRENT_RELEASE_DIRECTORY)
    if @config.getProperty(HOST_ENABLE_REPLICATOR)
      unless Configurator.instance.svc_is_running?("#{current_release_directory}/tungsten-manager/bin/manager")
        are_stopped = true
      end
      
      unless Configurator.instance.svc_is_running?("#{current_release_directory}/tungsten-replicator/bin/replicator")
        are_stopped = true
      end
    end
    
    if @config.getProperty(HOST_ENABLE_CONNECTOR)
      unless Configurator.instance.svc_is_running?("#{current_release_directory}/tungsten-connector/bin/connector")
        are_stopped = true
      end
    end
    
    if are_stopped
      error("The Tungsten are currently stopped though you have configured them to be started automatically.")
      help("Start the Tungsten services or run the command again with '--start=false")
    end
  end
  
  def enabled?
    # Disabling this check until it accounts for services being added to a configured machine
    return false
    
    current_release_directory = @config.getProperty(CURRENT_RELEASE_DIRECTORY)
    
    super() && (@config.getProperty(SVC_START) == "true" && File.exist?(current_release_directory))
  end
end

class TargetDirectoryDoesNotExist < ConfigureValidationCheck
  include ClusterHostCheck
  
  def set_vars
    @title = "Check that the target directory does not exist"
    self.extend(NotTungstenUpdateCheck)
  end
  
  def validate
    target = @config.getProperty(TARGET_DIRECTORY)
    if File.exist?(target)
      error("There is already a directory at #{target}")
      help("Use the `tools/tpm update` script to modify an existing installation")
    end
  end
  
  def enabled?
    super() && @config.getProperty(TARGET_DIRECTORY) != Configurator.instance.get_base_path()
  end
end

class UpgradeSameProductCheck < ConfigureValidationCheck
  include ClusterHostCheck
  
  def set_vars
    @title = "Check that the target directory is running the same product type that we are installing"
    self.extend(TungstenUpdateCheck)
  end
  
  def validate
    current_dir = @config.getProperty(CURRENT_RELEASE_DIRECTORY)
    if File.exist?(current_dir)
      if File.exist?(current_dir + "/tungsten-manager") == true
        current_product_name = "Continuent Tungsten"
      else
        current_product_name = "Tungsten Replicator"
      end
      
      if Configurator.instance.product_name() != current_product_name
        error("The directory #{@config.getProperty(HOME_DIRECTORY)} is already running #{current_product_name}. That is not compatible for upgrade with this #{Configurator.instance.product_name()} release.")
        help("Check the configuration using `tpm reverse` and use `tpm fetch --reset --hosts=#{Configurator.instance.hostname()},autodetect --user=#{@config.getProperty(USERID)} --directory=/path/to/directory` to reload the correct configuration before proceeding.")
      end
    end
  end
end

class CurrentReleaseDirectoryIsSymlink < ConfigureValidationCheck
  include ClusterHostCheck
  
  def set_vars
    @title = "No hidden services check"
  end
  
  def validate
    current_release_directory = @config.getProperty(CURRENT_RELEASE_DIRECTORY)
    if File.exists?(current_release_directory) && !File.symlink?(current_release_directory)
      if File.directory?(current_release_directory)
        entry_type = "directory"
      else
        entry_type = "file"
      end
      
      error("The #{entry_type} at #{current_release_directory} is not a symlink")
      help("Remove the #{entry_type} and restart installation")
    end
  end
end

class CommitDirectoryCheck < ConfigureValidationCheck
  include ClusterHostCheck
  include CommitValidationCheck
  
  def set_vars
    @title = "Correct commit directory check"
  end
  
  def validate
    unless Configurator.instance.get_base_path() == @config.getProperty(TARGET_DIRECTORY)
      if Configurator.instance.get_base_path() == @config.getProperty(PREPARE_DIRECTORY)
        error("Unable to commit this directory because it is in the wrong location")
        help("Move this directory to #{@config.getProperty(TARGET_DIRECTORY)} and rerun the command")
      end
    end
  end
end

class CommittingActiveDirectoryCheck < ConfigureValidationCheck
  include ClusterHostCheck
  include CommitValidationCheck
  
  def set_vars
    @title = "Check if we are promoting the active directory"
  end
  
  def validate
    require 'pathname'
    p = Pathname.new(@config.getProperty(CURRENT_RELEASE_DIRECTORY))
    if p.exist?()
      output_property(ACTIVE_DIRECTORY_PATH, p.realpath().to_s())
    end
  end
end

class CurrentTopologyCheck < ConfigureValidationCheck
  include ClusterHostCheck
  include CommitValidationCheck
  
  def set_vars
    @title = "Commit topology check"
  end
  
  def validate
    current_release_directory = @config.getProperty(CURRENT_RELEASE_DIRECTORY)
    configured_role = nil
    dynamic_properties = "#{current_release_directory}/tungsten-replicator/conf/#{File.basename(@config.getProperty(REPL_SVC_DYNAMIC_CONFIG))}"
    if File.exists?(dynamic_properties)
      begin
        configured_role = cmd_result("grep replicator.role #{dynamic_properties} | awk -F= '{print $2}'")
      rescue CommandError
      end
    end
    
    if configured_role == nil
      info("Unable to read the replicator dynamic.properties file, using the default configuration")
      configured_role = @config.getProperty(REPL_ROLE)
    end
    
    begin
      cctrl = CCTRL.new(Configurator.instance.get_cctrl_path(@config.getProperty(CURRENT_RELEASE_DIRECTORY), @config.getProperty(MGR_RMI_PORT)))
      parsed_topology = cctrl.to_a()
    rescue CommandError => ce
      exception(ce)
      error(ce.message)
      return
    end

    unless parsed_topology != nil && 
        parsed_topology.has_key?(CCTRL::DATASOURCES) && 
        parsed_topology[CCTRL::DATASOURCES].has_key?(@config.getProperty(HOST))
      return
    end
    
    if parsed_topology[CCTRL::DATASOURCES][@config.getProperty(HOST)]["role"] != configured_role
      warning("The host is configured to be '#{configured_role}' but it is currently operating as a '#{parsed_topology[CCTRL::DATASOURCES][@config.getProperty(HOST)]["role"]}'")
    end
    
    output_property(MANAGER_COORDINATOR, parsed_topology[CCTRL::COORDINATOR]["host"])
    output_property(MANAGER_COORDINATOR_IS_LOCALHOST, Configurator.instance.is_localhost?(parsed_topology[CCTRL::COORDINATOR]["host"]))
    output_property(MANAGER_POLICY, parsed_topology[CCTRL::COORDINATOR]["mode"])
  end
  
  def enabled?
    super() && File.exists?("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tools/tpm") &&
    @config.getProperty(HOST_ENABLE_REPLICATOR) == "true" &&
    Configurator.instance.svc_is_running?("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-manager/bin/manager")
  end
end

# This class runs after the post-commit validation checks. It will set a 
# flag that only one of the hosts executes commands like setting cluster 
# policy. It will use the cluster coordinator by default but that host
# may not be part of the command so the first config will assume the role
# for the live of the command. It will not directly affect the cluster 
# coordinator.
class CurrentCommandCoordinatorCheck < ConfigureValidationCheck
  include ClusterHostCheck
  include PostValidationCommitCheck
  
  def set_vars
    @title = "Command coordinator check"
  end
  
  def validate
    coordinator_configuration_key = nil
    props = Configurator.instance.command.get_validation_handler().output_properties.props
    
    props.each_key{
      |k|
      if props[k][MANAGER_COORDINATOR_IS_LOCALHOST] == true
        props[k][IS_COMMAND_COORDINATOR] = true
        # return so no other keys are set
        return
      end
    }
    
    Configurator.instance.command.get_deployment_configurations.each{
      |cfg|
      cfg_key = cfg.getProperty([DEPLOYMENT_CONFIGURATION_KEY])
      if props.has_key?(cfg_key)
        props[cfg_key][IS_COMMAND_COORDINATOR] = true
        # return so no other keys are set
        return
      end
    }
  end
  
  def enabled?
    super() && Configurator.instance.is_enterprise?()
  end
end

class ActiveDirectoryIsRunningCheck < ConfigureValidationCheck
  include ClusterHostCheck
  include CommitValidationCheck
  
  def set_vars
    @title = "Collect values prior to promotion"
  end
  
  def validate
    current_release_directory = @config.getProperty(CURRENT_RELEASE_DIRECTORY)
    if File.exists?(current_release_directory)
      output_property(MANAGER_IS_RUNNING, Configurator.instance.svc_is_running?("#{current_release_directory}/tungsten-manager/bin/manager").to_s())
      output_property(REPLICATOR_IS_RUNNING, Configurator.instance.svc_is_running?("#{current_release_directory}/tungsten-replicator/bin/replicator").to_s())
      output_property(CONNECTOR_IS_RUNNING, Configurator.instance.svc_is_running?("#{current_release_directory}/tungsten-connector/bin/connector").to_s())
      
      if cmd_result("cat #{current_release_directory}/cluster-home/bin/startall | grep tungsten-manager | wc -l") == "1"
        output_property(MANAGER_ENABLED, true.to_s())
      else
        output_property(MANAGER_ENABLED, false.to_s())
      end
      if cmd_result("cat #{current_release_directory}/cluster-home/bin/startall | grep tungsten-replicator | wc -l") == "1"
        output_property(REPLICATOR_ENABLED, true.to_s())
      else
        output_property(REPLICATOR_ENABLED, false.to_s())
      end
      if cmd_result("cat #{current_release_directory}/cluster-home/bin/startall | grep tungsten-connector | wc -l") == "1"
        output_property(CONNECTOR_ENABLED, true.to_s())
      else
        output_property(CONNECTOR_ENABLED, false.to_s())
      end
    else
      output_property(MANAGER_IS_RUNNING, false.to_s())
      output_property(REPLICATOR_IS_RUNNING, false.to_s())
      output_property(CONNECTOR_IS_RUNNING, false.to_s())
      
      output_property(MANAGER_ENABLED, false.to_s())
      output_property(REPLICATOR_ENABLED, false.to_s())
      output_property(CONNECTOR_ENABLED, false.to_s())
    end
  end
end

class RestartComponentsCheck < ConfigureValidationCheck
  include ClusterHostCheck
  include CommitValidationCheck
  
  def set_vars
    @title = "Determine which components to restart"
  end
  
  def validate
    current_release_directory = @config.getProperty(CURRENT_RELEASE_DIRECTORY)
    if File.exists?(current_release_directory)
      realpath = File.readlink(current_release_directory)
      if realpath == @config.getProperty(TARGET_DIRECTORY)
        @original_config = Properties.new()
        @original_config.load(current_release_directory + "/." + Configurator::HOST_CONFIG + '.orig')
        Configurator.instance.command.build_topologies(@original_config)
        
        updated_keys = @config.getPromptHandler().get_updated_keys(@original_config)
        unless is_valid?()
          return is_valid?()
        end
        
        output_property(RESTART_REPLICATORS, false)
        output_property(RESTART_MANAGERS, false)
        output_property(RESTART_CONNECTORS, false)
        output_property(RECONFIGURE_CONNECTORS_ALLOWED, true)
        
        # Check the .changedfiles file to see what components need to be restarted
        changedfiles = @config.getProperty(PREPARE_DIRECTORY) + "/.changedfiles"
        if File.exists?(changedfiles)
          {
            "^tungsten-replicator" => RESTART_REPLICATORS,
            "^tungsten-manager" => RESTART_MANAGERS,
            "^cluster-home" => RESTART_MANAGERS,
            "^tungsten-connector" => RESTART_CONNECTORS,
            "router" => RESTART_CONNECTORS
          }.each{
            |pattern,component|
            begin
              lines = cmd_result("egrep \"#{pattern}\" #{changedfiles} | wc -l").to_i()
              if lines > 0
                unless get_output_property(component) == true
                  Configurator.instance.debug("Found #{lines} files that match #{pattern}. Setting #{component} to true.")
                  output_property(component, true)
                end
              end
            rescue CommandError
              Configurator.instance.debug("Unable to compare #{changedfiles} with '#{pattern}'")
            end
          }
        end
        
        # Check the changed values to see what components need to be restarted
        updated_keys.each{
          |k|
          p = @config.getPromptHandler().find_prompt(k.split('.'))
        
          # Once something states that the component must be restarted
          # nothing should turn that off.  A nil state means we will set it
          # to the default
          unless get_output_property(RESTART_REPLICATORS) == true
            output_property(RESTART_REPLICATORS, p.require_replicator_restart?())
          end
          unless get_output_property(RESTART_MANAGERS) == true
            output_property(RESTART_MANAGERS, p.require_manager_restart?())
          end
          unless get_output_property(RESTART_CONNECTORS) == true
            output_property(RESTART_CONNECTORS, p.require_connector_restart?())
          end
          unless get_output_property(RECONFIGURE_CONNECTORS_ALLOWED) == false
            output_property(RECONFIGURE_CONNECTORS_ALLOWED, p.allow_connector_reconfigure?())
          end
        }
        
        if get_output_property(RESTART_CONNECTORS) == true
          output_property(RESTART_CONNECTORS_NEEDED, true)
        end
      else
        output_property(RESTART_REPLICATORS, true)
        output_property(RESTART_MANAGERS, true)
        output_property(RESTART_CONNECTORS, true)
        output_property(RESTART_CONNECTORS_NEEDED, true)
        output_property(RECONFIGURE_CONNECTORS_ALLOWED, false)
      end
    else
      # No effect since there isn't an existing directory
      output_property(RESTART_REPLICATORS, nil)
      output_property(RESTART_MANAGERS, nil)
      output_property(RESTART_CONNECTORS, nil)
      output_property(RESTART_CONNECTORS_NEEDED, nil)
      output_property(RECONFIGURE_CONNECTORS_ALLOWED, nil)
    end
  end
end

class CurrentVersionCheck < ConfigureValidationCheck
  include ClusterHostCheck
  include CommitValidationCheck
  
  def set_vars
    @title = "Collect current version"
  end
  
  def validate
    begin
      if File.exists?("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tools/tpm")
        unless File.exists?("#{@config.getProperty(HOME_DIRECTORY)}/configs/tungsten.cfg")
          current_version = cmd_result("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tools/tpm query version")
          output_property(ACTIVE_VERSION, current_version)
        end
      end
    rescue CommandError => ce
      exception(ce)
      error(ce.message)
    end
  end
  
  def enabled?
    return File.exists?(@config.getProperty(CURRENT_RELEASE_DIRECTORY))
  end
end

class InstallServicesCheck < ConfigureValidationCheck
  include ClusterHostCheck
  
  def set_vars
    @title = "Install services check"
  end
  
  def validate
    if @config.getProperty(USERID) != "root" && @config.getProperty(ROOT_PREFIX) != "true"
      error("The installer is unable to install the services if the user is not root or --root-command-prefix is not set to true")
    end
  end
  
  def enabled?
    (@config.getProperty(SVC_INSTALL) == "true")
  end
end

class ProfileScriptCheck < ConfigureValidationCheck
  include ClusterHostCheck
  
  def set_vars
    @title = "Profile script check"
  end
  
  def validate
    if @config.getProperty(PROFILE_SCRIPT) != ""
      full_path = File.expand_path(@config.getProperty(PROFILE_SCRIPT), @config.getProperty(HOME_DIRECTORY))
      unless File.exist?(full_path)
        error("Unable to find the profile script at #{full_path}")
        help("Specify the path to an existing profile script for this user which is executed during login")
      else
        unless File.writable?(full_path)
          error("The profile script at #{full_path} is not writable")
        end
      end
    end
  end
  
  def enabled?
    (@config.getProperty(PROFILE_SCRIPT) != "")
  end
end

class PortAvailabilityCheck < ConfigureValidationCheck
  include ClusterHostCheck
  include NotUnlessEnabledCheck
  
  def set_vars
    @title = "Port availability check"
  end
  
  def validate
    # All of the ports we should be able to listen on
    all_ports = @config.getPropertyOr(PORTS_FOR_USERS, [])
    all_ports = all_ports + @config.getPropertyOr(PORTS_FOR_MANAGERS, [])
    all_ports = all_ports + @config.getPropertyOr(PORTS_FOR_REPLICATORS, [])
    all_ports = all_ports + @config.getPropertyOr(PORTS_FOR_CONNECTORS, [])
    all_ports.collect!{|port| port.to_i}
    
    # The process ids for each of the Tungsten components
    tungsten_pids = {Process.pid().to_i() => true, Process.ppid().to_i() => true}
    cmd_result("find #{@config.getProperty(HOME_DIRECTORY)} -name *.pid -print0 | xargs -0 cat", true).split("\n").each{
      |ppid|
      begin
        pid = cmd_result("ps -opid= --ppid=#{ppid}")
        tungsten_pids[pid.to_i()] = true
      rescue CommandError
      end
    }
    
    # Listening ports as reported by netstat
    system_listening_ports = {}
    cmd_result("netstat -nlp 2>/dev/null | egrep '^tcp' | awk '{print $4,$7}'").scan(/[0-9\.:]+:([0-9]+) (([0-9]+)\/(.*)|-)/).each{
      |match|
      system_listening_ports[match[0].to_i()] = {:pid => match[2].to_i(), :program => match[3]}
    }
    
    # Ports that tpm is listening on
    tpm_listening_ports = {}
    Dir.glob("#{@config.getProperty(TEMP_DIRECTORY)}/tungsten.listener.#{@config.getProperty(DEPLOYMENT_HOST)}.*.pid") {
      |fname|
      File.open(fname, 'r') do |file|
        port = fname.split(".")[3]
        pid = file.readlines().join().strip()
        tpm_listening_ports[port.to_i()] = pid.to_i()
      end
    }
    
    # Get the list of ports that are expected to be bound
    ignore_ports = {}
    @config.getPropertyOr(REPL_SERVICES, []).each_key{
      |rs_alias|
      if rs_alias == DEFAULTS
        next
      end
      
      ignore_ports[@config.getProperty([REPL_SERVICES, rs_alias, REPL_DBPORT]).to_i()] = true
    }
    
    all_ports.sort().uniq().each{
      |port|
      if tpm_listening_ports.has_key?(port)
        next
      end
      
      if ignore_ports.has_key?(port)
        next
      end
      
      if system_listening_ports.has_key?(port)
        if system_listening_ports[port][:pid] == 0
          error("There is another process listening on port #{port}")
        else
          unless tungsten_pids.has_key?(system_listening_ports[port][:pid])
            error("The '#{system_listening_ports[port][:program]}' process is listening on port #{port}")
          end
        end
      else
        error("TPM is unable to test port #{port}")
      end
    }
  end
  
  def use_firewall_listeners?
    true
  end
end

class FirewallCheck < ConfigureValidationCheck
  include ClusterHostCheck
  include NotUnlessEnabledCheck
  
  def set_vars
    @title = "Firewall check"
  end
  
  def validate
    @config.getPropertyOr([REMOTE, HOSTS], {}).each_key{
      |h_alias|
      host = @config.getProperty([REMOTE, HOSTS, h_alias, HOST])
      if host == nil
        next
      end
      
      all_ports = @config.getPropertyOr([REMOTE, HOSTS, h_alias, PORTS_FOR_USERS], [])
      
      if @config.getProperty(HOST_ENABLE_MANAGER) == "true"
        all_ports = all_ports + @config.getPropertyOr([REMOTE, HOSTS, h_alias, PORTS_FOR_MANAGERS], [])
      end

      if @config.getProperty(HOST_ENABLE_REPLICATOR) == "true"
        all_ports = all_ports + @config.getPropertyOr([REMOTE, HOSTS, h_alias, PORTS_FOR_REPLICATORS], [])
      end

      if @config.getProperty(HOST_ENABLE_CONNECTOR) == "true"
        all_ports = all_ports + @config.getPropertyOr([REMOTE, HOSTS, h_alias, PORTS_FOR_CONNECTORS], [])
      end
      
      all_ports.collect!{|port| port.to_i}

      all_ports.sort().uniq().each{
        |port|
        begin
          begin
            r = TCPSocket.new(host, port)
          rescue
            raise "Unable to get connection to #{host}:#{port}"
          end
        rescue => e
          error(e.message())
        end
      }
    }
  end
  
  def use_firewall_listeners?
    true
  end
end

class HostsFileCheck < ConfigureValidationCheck
  include ClusterHostCheck
  
  def set_vars
    @title = "/etc/hosts file check"
  end
  
  def validate
    begin
      @config.getPropertyOr(DATASERVICES, {}).each_key{
        |ds_alias|
        if ds_alias == DEFAULTS
          next
        end
        if @config.getProperty([DATASERVICES, ds_alias, DATASERVICE_IS_COMPOSITE]) == "true"
          next
        end
        
        return_addresses = {}
        return_addresses[ds_alias] = {}
        members = @config.getPropertyOr([DATASERVICES, ds_alias, DATASERVICE_MEMBERS], "").split(",")

        members.each{
          |h_alias|
          h_name = @config.getProperty([HOSTS, h_alias, HOST])
          # grep (case-insensitive) (whole word) hostname | grep (does not begin with #)
          matching_lines = cmd_result("grep -iw '#{h_name}' /etc/hosts | grep -v \"^#\"",true).split("\n")

          case matching_lines.length()
          when 0
            return_addresses[ds_alias][h_alias] = nil
          when 1
            line = matching_lines[0]
            
            # Look for 127 at the beginning of the line
            if line =~ /^127/
              error("An IP address in /etc/hosts for #{h_name} begins with 127")
              help("Eliminate any loopback addresses associated with #{h_name} from /etc/hosts")
            elsif line =~ /^::1/
              error("An IP address in /etc/hosts for #{h_name} begins with ::1")
              help("Eliminate any loopback addresses associated with #{h_name} from /etc/hosts")
            else
              line_parts = line.split(/\s/)
              if members.include?(@config.getProperty(HOST))
                return_addresses[ds_alias][h_alias] = line_parts[0]
              end
            end
          else
            error("There are multiple entries for #{h_name} in the /etc/hosts file")
            help("Clean up the file to have one entry for #{h_name}")
          end
        }
        output_property("HostsFileCheck", return_addresses)
      }
    rescue CommandError
      error("Unable to check /etc/hosts for entries matching #{@config.getProperty(HOST)}")
    end
  end
end

class ModifiedConfigurationFilesCheck < ConfigureValidationCheck
  include ClusterHostCheck
  
  def set_vars
    @title = "Check for modified configuration files"
  end
  
  def validate
    basedir = @config.getProperty(CURRENT_RELEASE_DIRECTORY)
    if File.exist?(basedir)
      modified_files = WatchFiles.get_modified_files(basedir)
      if modified_files == nil
        debug("No #{basedir}/.watchfiles to use for comparision")
      else
        modified_files.each{
          |fname|
          error("Changes have been made to #{fname}")
        }
      end
    end
    
    unless is_valid?()
      help("Run 'tpm query modified-files' on the affected servers to see all changes. Revert those changes and use the --property flag to make sure all changes are permanent. You may also run 'tpm update' with the '-f' flag to bypass the errors.")
    end
  end
end

class GlobalHostAddressesCheck < ConfigureValidationCheck
  include PostValidationCheck
  
  def set_vars
    @title = "Matching IP addresses check"
  end
  
  def validate
    host_addresses = {}
    
    Configurator.instance.command.get_validation_handler().output_properties.props.each {
      |ch_alias, props|
      if props.has_key?("HostsFileCheck")
        props["HostsFileCheck"].each{
          |ds_alias, addresses|
          addresses.each{
            |host, address|
            
            if host_addresses.has_key?(host)
              if host_addresses[host] != address
                error("The IP address for #{host} is not consistent accross all servers")
                host_addresses[host] = false
              end
            elsif host_addresses[host] == false
              # Do Nothing
            else
              host_addresses[host] = address
            end
          }
        }
      end
    }
  end
end

class GlobalMatchingPingMethodCheck < ConfigureValidationCheck
  include PostValidationCheck
  
  def set_vars
    @title = "Matching manager ping method check"
  end
  
  def validate
    ping_methods = []
    
    Configurator.instance.command.get_deployment_configurations().each {
      |cfg|
      if cfg.getProperty(HOST_ENABLE_MANAGER) == "true"
        ping_methods << cfg.getProperty(MGR_PING_METHOD)
      end
    }
    
    ping_methods.uniq!()
    
    if ping_methods.size() > 1
      error("There are multiple ping methods (#{ping_methods.join(',')}) being used. Specify a ping method with the --mgr-ping-method option.")
    end
  end
end

class ConflictingReplicationServiceTHLPortsCheck < ConfigureValidationCheck
  include ClusterHostCheck
  include ReplicatorEnabledCheck
  
  def set_vars
    @title = "Conflicting THL listening port check"
  end
  
  def validate
    thl_ports = {}
    
    @config.getNestedPropertyOr([REPL_SERVICES], {}).each_key{
      |rs_alias|
      if rs_alias == DEFAULTS
        next
      end
      
      if @config.getProperty([REPL_SERVICES, rs_alias, REPL_SVC_SERVICE_TYPE]) == "remote"
        next
      end
      
      thl_port = @config.getProperty([REPL_SERVICES, rs_alias, REPL_SVC_THL_PORT])
      
      if thl_ports.has_key?(thl_port)
        error("The #{@config.getProperty([REPL_SERVICES, rs_alias, DEPLOYMENT_DATASERVICE])} has a conflicting THL port with #{thl_ports[thl_port]}")
      else
        thl_ports[thl_port] = @config.getProperty([REPL_SERVICES, rs_alias, DEPLOYMENT_DATASERVICE])
      end
    }
  end
end

class JavaUserTimezoneCheck < ConfigureValidationCheck
  include ClusterHostCheck
  include ReplicatorEnabledCheck
 
  def set_vars
    @title = "Java user timezone check"
  end
 
  def validate
    user_timezone = @config.getProperty(REPL_JAVA_USER_TIMEZONE)
    unless user_timezone == ""
      warning("The --java-user-timezone option is deprecated! Replicators use GMT by default. Please check product documentation for instructions on changing the Java time zone safely.")
    end
  end
end

class MissingReplicationServiceConfigurationCheck < ConfigureValidationCheck
  include ClusterHostCheck
  include ReplicatorEnabledCheck
  
  def set_vars
    @title = "Looking for existing replication services that are not in the configuration"
    self.extend(TungstenUpdateCheck)
  end
  
  def validate
    existing_static_files = {}
    Dir.glob("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-replicator/conf/static-*") {
      |fname|
      existing_static_files[File.basename(fname)] = true
    }
    
    @config.getNestedPropertyOr([REPL_SERVICES], {}).each_key{
      |rs_alias|
      static_filename = File.basename(@config.getProperty([REPL_SERVICES, rs_alias, REPL_SVC_CONFIG_FILE]))
      
      if existing_static_files.has_key?(static_filename)
        existing_static_files.delete(static_filename)
      end
    }
    
    existing_static_files.each_key{
      |filename|
      
      error("The configuration file #{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-replicator/conf/#{filename} exists and represents a replication service that does not appear in this configuration")
    }
    
    unless is_valid?()
      help("Run the `tpm delete-service` command to remove this service from the configuration")
    end
  end
end

class EncryptionKeystoreCheck < ConfigureValidationCheck
  include ClusterHostCheck
  
  def set_vars
    @title = "Test that all files for RMI encryption are created properly"
  end
  
  def validate
    ssl_enabled = false
    if @config.getProperty(ENABLE_RMI_SSL) == "true"
      ssl_enabled = true
    end
    @config.getPropertyOr([REPL_SERVICES], {}).keys().each{
      |rs_alias|
      if rs_alias == DEFAULTS
        next
      end
      if @config.getProperty([REPL_SERVICES, rs_alias, REPL_ENABLE_THL_SSL]) == "true"
        ssl_enabled = true
      end
    }
    
    if @config.getProperty(HOST_ENABLE_CONNECTOR) == "true" && (
        @config.getProperty(ENABLE_CONNECTOR_SERVER_SSL) == "true" ||
        @config.getProperty(ENABLE_CONNECTOR_CLIENT_SSL) == "true"
      )
      connector_ssl_enabled = true
    else
      connector_ssl_enabled = false
    end
    
    if ssl_enabled == false && connector_ssl_enabled == false
      return
    end
    
    begin
      keytool_path = which("keytool")
    rescue CommandError
      keytool_path = nil
    end
    
    if ssl_enabled == true
      keystore_path = @config.getProperty(JAVA_KEYSTORE_PATH)
      if keystore_path.to_s() == ""
        if File.exist?(@config.getTemplateValue(JAVA_KEYSTORE_PATH))
          keystore_path = @config.getTemplateValue(JAVA_KEYSTORE_PATH)
        else
          error("Unable to find #{@config.getTemplateValue(JAVA_KEYSTORE_PATH)} for use with SSL encryption. You must create the file or provide a path to it with --java-keystore-path.")
        end
      end
    
      if keystore_path.to_s() != "" && keytool_path != nil
        begin
          cmd_result("keytool -list -keystore #{keystore_path} -storepass #{@config.getProperty(JAVA_KEYSTORE_PASSWORD)}")
        rescue CommandError => ce
          error("There was an issue validating the SSL keystore: #{ce.result}. Check the values of --java-keystore-path and --java-keystore-password. If you did not provide --java-keystore-path, check the file at #{@config.getTemplateValue(JAVA_KEYSTORE_PATH)}")
        end
      end
    
      truststore_path = @config.getProperty(JAVA_TRUSTSTORE_PATH)
      if truststore_path.to_s() == ""
        if File.exist?(@config.getTemplateValue(JAVA_TRUSTSTORE_PATH))
          truststore_path = @config.getTemplateValue(JAVA_TRUSTSTORE_PATH)
        else
          error("Unable to find #{@config.getTemplateValue(JAVA_TRUSTSTORE_PATH)} for use with SSL encryption. You must create the file or provide a path to it with --java-truststore-path.")
        end
      end
    
      if truststore_path.to_s() != "" && keytool_path != nil
        begin
          cmd_result("keytool -list -keystore #{truststore_path} -storepass #{@config.getProperty(JAVA_TRUSTSTORE_PASSWORD)}")
        rescue CommandError => ce
          error("There was an issue validating the SSL truststore: #{ce.result}. Check the values of --java-truststore-path and --java-truststore-password. If you did not provide --java-truststore-path, check the file at #{@config.getTemplateValue(JAVA_TRUSTSTORE_PATH)}")
        end
      end
    end
    
    if connector_ssl_enabled == true
      keystore_path = @config.getProperty(JAVA_CONNECTOR_KEYSTORE_PATH)
      if keystore_path.to_s() == ""
        if File.exist?(@config.getTemplateValue(JAVA_CONNECTOR_KEYSTORE_PATH))
          keystore_path = @config.getTemplateValue(JAVA_CONNECTOR_KEYSTORE_PATH)
        else
          error("Unable to find #{@config.getTemplateValue(JAVA_CONNECTOR_KEYSTORE_PATH)} for use with Connector SSL encryption. You must create the file or provide a path to it with --java-connector-keystore-path.")
        end
      end
    
      if keystore_path.to_s() != "" && keytool_path != nil
        begin
          cmd_result("keytool -list -keystore #{keystore_path} -storepass #{@config.getProperty(JAVA_CONNECTOR_KEYSTORE_PASSWORD)}")
        rescue CommandError => ce
          error("There was an issue validating the SSL keystore: #{ce.result}. Check the values of --java-connector-keystore-path and --java-connector-keystore-password. If you did not provide --java-connector-keystore-path, check the file at #{@config.getTemplateValue(JAVA_CONNECTOR_KEYSTORE_PATH)}")
        end
      end
    
      truststore_path = @config.getProperty(JAVA_CONNECTOR_TRUSTSTORE_PATH)
      if truststore_path.to_s() == ""
        if File.exist?(@config.getTemplateValue(JAVA_CONNECTOR_TRUSTSTORE_PATH))
          truststore_path = @config.getTemplateValue(JAVA_CONNECTOR_TRUSTSTORE_PATH)
        else
          error("Unable to find #{@config.getTemplateValue(JAVA_CONNECTOR_TRUSTSTORE_PATH)} for use with SSL encryption. You must create the file or provide a path to it with --java-connector-truststore-path.")
        end
      end
    
      if truststore_path.to_s() != "" && keytool_path != nil
        begin
          cmd_result("keytool -list -keystore #{truststore_path} -storepass #{@config.getProperty(JAVA_CONNECTOR_TRUSTSTORE_PASSWORD)}")
        rescue CommandError => ce
          error("There was an issue validating the SSL truststore: #{ce.result}. Check the values of --java-truststore-path and --java-connector-truststore-password. If you did not provide --java-connector-truststore-path, check the file at #{@config.getTemplateValue(JAVA_CONNECTOR_TRUSTSTORE_PATH)}")
        end
      end
    end
  end
end

class NtpdRunningCheck < ConfigureValidationCheck
  include ClusterHostCheck

  def set_vars
    @title = "ntpd running check"
  end

  def validate
    if cmd_result("pgrep ntpd", true).to_s == ""
       warning("ntpd is not running. It is important that configured hosts have time synchronised.")
    end
  end

  def enabled?
    super()
  end
end
