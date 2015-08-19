class ManagerChecks < GroupValidationCheck
  include ClusterHostCheck
  
  def initialize
    super(MANAGERS, "manager", "managers")
    
    ManagerCheck.submodules().each{
      |klass|
      
      self.add_check(klass.new())
    }
  end
  
  def set_vars
    @title = "Manager checks"
  end
end

module ManagerCheck
  include GroupValidationCheckMember
  include ManagerEnabledCheck
  
  def get_host_key(key)
    [HOSTS, @config.getProperty(get_member_key(DEPLOYMENT_HOST)), key]
  end
  
  def get_dataservice
    if self.is_a?(HashPromptMemberModule)
      get_member()
    else
      @config.getPropertyOr(get_member_key(DEPLOYMENT_DATASERVICE), nil)
    end
  end
  
  def get_dataservice_key(key)
    return [DATASERVICES, get_dataservice, key]
  end
  
  def self.included(subclass)
    @submodules ||= []
    @submodules << subclass
  end

  def self.submodules
    @submodules || []
  end
end

class VIPEnabledHostAllowsRootCommands < ConfigureValidationCheck
  include ManagerCheck
  
  def set_vars
    @title = "VIP-enabled host allows root commands"
  end
  
  def validate
    unless @config.getProperty(get_host_key(ROOT_PREFIX)) == "true"
      error("You have VIP enabled but the software is not configured to use sudo")
      help("Try setting --enable-sudo-access to true")
    end
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(MGR_VIP_ENABLED)) == "true"
  end
end

class VIPEnabledHostArpPath < ConfigureValidationCheck
  include ManagerCheck
  
  def set_vars
    @title = "Check VIP-enabled arp path"
  end
  
  def validate
    arp_path = @config.getProperty(get_member_key(MGR_VIP_ARP_PATH))
    unless arp_path && File.exists?(arp_path)
      error("The arp command '#{arp_path}' does not exist")
    end
  end

  def enabled?
    super() && @config.getProperty(get_member_key(MGR_VIP_ENABLED)) == "true"
  end
end

class VIPEnabledHostIfconfigPath < ConfigureValidationCheck
  include ManagerCheck
  
  def set_vars
    @title = "Check VIP-enabled ifconfig path"
  end

  def validate
    ifconfig_path = @config.getProperty(get_member_key(MGR_VIP_IFCONFIG_PATH))
    unless ifconfig_path && File.exists?(ifconfig_path)
      error("The ifconfig command '#{ifconfig_path}' does not exist")
    end
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(MGR_VIP_ENABLED)) == "true"
  end
end

class PingSyntaxCheck < ConfigureValidationCheck
  include ManagerCheck
  
  def set_vars
    @title = "Check the ping syntax is supported"
  end
  
  def validate
    plat = IO.popen('uname'){ |f| f.gets.strip }
    case plat
    when 'Darwin'
      cmd_array = ["ping", "-c", "1", "-W", "1000", "localhost"]
    else
      cmd_array = ["ping", "-c", "1", "-w", "1000", "localhost"]
    end
    
    begin
      Timeout::timeout(5) do
        cmd = Escape.shell_command(cmd_array).to_s
        cmd_result(cmd)
      end
    rescue Timeout::Error
      error("It is taking longer than 5 seconds to ping localhost")
    rescue CommandError
      error("Unable to run the ping utility with '#{cmd}'")
    end
  end
end

class ManagerListenerAddressCheck < ConfigureValidationCheck
  include ManagerCheck
  
  def set_vars
    @title = "Manager listener address check"
  end
  
  def validate
    addr = @config.getProperty(get_member_key(MGR_LISTEN_ADDRESS))
    if addr.to_s() == ""
      error("Unable to determine the manager listening IP address for interface #{@config.getProperty(get_member_key(MGR_LISTEN_INTERFACE))}")
    end
  end
end

class ManagerWitnessNeededCheck < ConfigureValidationCheck
  include ManagerCheck
  
  def set_vars
    @title = "Manager Witness is needed check"
  end
  
  def validate
    witnesses = @config.getProperty(DATASERVICE_WITNESSES).to_s()
    members_count = @config.getProperty(DATASERVICE_REPLICATION_MEMBERS).to_s().split(",").size()

    if members_count == 1
      # Single member
      if witnesses != ""
        warning("A witness should not be configured for single member dataservices.")
      end
    elsif (members_count % 2) == 0
      # Is even
      if witnesses == ""
        error("This dataservice is configured with an even number of replication members and no witnesses. Update the configuration with an active witness for the highest stability. Visit http://docs.continuent.com/ct/host-types for more information.")
      end
    else
      # Is odd
      if witnesses != ""
        warning("This dataservice is configured with an odd number of replication members and a witness. Update the configuration without a witness for the highest stability. Visit http://docs.continuent.com/ct/host-types for more information.")
      end
    end
    
    if @config.getProperty(ENABLE_ACTIVE_WITNESSES) == "false" && witnesses != ""
      warning("This dataservice is using a passive witness. Continuent Tungsten has support for active witnesses that improve stability over passive witnesses. Visit http://docs.continuent.com/ct/host-types for more information.")
    end
  end
  
  def enabled?
    super() && (@config.getProperty(MGR_VALIDATE_WITNESS) == "true")
  end
end

class ManagerWitnessAvailableCheck < ConfigureValidationCheck
  include ManagerCheck
  
  def set_vars
    @title = "Manager Witness is available check"
  end
  
  def validate
    mgr_address = @config.getProperty(get_member_key(MGR_LISTEN_ADDRESS))
    mgr_netmask = nil
    IPParse.new().get_interfaces().each{
      |iface, addresses|
      if addresses.has_key?(IPParse::IPV4) != true
        next
      end
      
      if addresses[IPParse::IPV4][:address] != mgr_address
        next
      end
      
      mgr_netmask = addresses[IPParse::IPV4][:netmask]
    }
    
    if mgr_netmask == nil
      error("Unable to identify the netmask for the Manager IP address")
      return
    end
    
    mgr_address_octets = mgr_address.split(".")
    mgr_netmask_octets = mgr_netmask.split(".")
    
    @config.getProperty(DATASERVICE_WITNESSES).to_s().split(",").each{
      |witness|
      witness_ips = Configurator.instance.get_ip_addresses(witness)
      if witness_ips == false
        error("Unable to find an IP address for the passive witness #{witness}. Continuent Tungsten has support for active witnesses that improve stability over passive witnesses. Visit http://docs.continuent.com/ct/host-types for more information.")
        next
      end
      
      debug("Check if witness #{witness} is pingable")
      if Configurator.instance.check_addresses_is_pingable(witness) == false
        error("The passive witness address '#{witness}' is not returning pings. Continuent Tungsten has support for active witnesses that improve stability over passive witnesses. Visit http://docs.continuent.com/ct/host-types for more information.")
        help("Specify a valid hostname or ip address for the passive witness host ")
      end
      
      witness_octets = witness_ips[0].split(".")
      same_network = true
      
      4.times{
        |i|
        
        a = (mgr_address_octets[i].to_i() & mgr_netmask_octets[i].to_i())
        b = (witness_octets[i].to_i() & mgr_netmask_octets[i].to_i())
        if a != b
          same_network = false
        end
      }
      
      if same_network != true
        error("The passive witness address '#{witness}' is not in the same subnet as the manager. Continuent Tungsten has support for active witnesses that improve stability over passive witnesses. Visit http://docs.continuent.com/ct/host-types for more information.")
      end
    }
  end
  
  def enabled?
    super() && (@config.getProperty(DATASERVICE_WITNESSES).to_s() != "") &&
      (@config.getProperty(ENABLE_ACTIVE_WITNESSES) == "false") &&
      (@config.getProperty(MGR_VALIDATE_WITNESS) == "true")
  end
end

class ManagerActiveWitnessConversionCheck < ConfigureValidationCheck
  include ManagerCheck
  
  def set_vars
    @title = "Active witness is not a current replicator check"
  end
  
  def validate
    current_release_directory = @config.getProperty(CURRENT_RELEASE_DIRECTORY)
    if File.exists?(current_release_directory)
      # Check if replicator is in cluster-home/bin/startall
      is_replicator = cmd_result("cat #{current_release_directory}/cluster-home/bin/startall | grep tungsten-replicator | wc -l")
      if is_replicator == "1"
        error("The active witness \"#{@config.getProperty(HOST)}\" is already running as a replicator. If you proceed it will no longer be available as a datasource. Specify an active witness that is not already running a replicator.")
      end
    end
  end
  
  def enabled?
    super() && (@config.getProperty(MGR_IS_WITNESS).to_s() == "true")
  end
end

class ManagerHeapThresholdCheck < ConfigureValidationCheck
  include ManagerCheck
  
  def set_vars
    @title = "Manager Java Heap threshold check"
  end
  
  def validate
    mem = @config.getProperty(get_member_key(MGR_JAVA_MEM_SIZE))
    threshold = @config.getProperty(get_member_key(MGR_HEAP_THRESHOLD))
    
    if threshold.to_i() <= 0
      error("The value for --mgr-heap-threshold must be greater than zero")
    elsif threshold.to_i() >= mem.to_i()
      error("The value for --mgr-heap-threshold must be less than --mgr-java-mem-size")
    end
  end
end

class ManagerPingMethodCheck < ConfigureValidationCheck
  include ManagerCheck
  
  def set_vars
    @title = "Manager ping method check"
  end
  
  def validate
    method = @config.getProperty(get_member_key(MGR_PING_METHOD))
    # Generate a list of nodes that the manager may attempt to ping
    hosts = @config.getProperty(get_dataservice_key(DATASERVICE_MEMBERS)).split(",")
    if @config.getProperty(get_dataservice_key(ENABLE_ACTIVE_WITNESSES)) == "true"
      hosts = hosts + @config.getProperty(get_dataservice_key(DATASERVICE_WITNESSES)).split(",")
    end
    hosts = hosts.uniq()
    
    case method
    when "ping"
      hosts.each{
        |h|
        begin
          Timeout.timeout(2) {
            ping_result = cmd_result("ping -c1 #{h} 2>/dev/null >/dev/null ; echo $?", true)
            if ping_result.to_i != 0
              error("Unable to contact #{h} via ICMP Ping")
            end
          }
        rescue Timeout::Error
          error("Unable to contact #{h} via ICMP Ping")
        rescue
          error("Unable to contact #{h} via ICMP Ping")
        end
      }
    when "echo"
      tping = "#{Configurator.instance.get_base_path()}/cluster-home/bin/tping"
      hosts.each{
        |h|
        begin
          Timeout.timeout(2) {
            echo_result = cmd_result("#{tping} #{h} 7 2000 2>/dev/null >/dev/null ; echo $?", true)
            if echo_result.to_i != 0
              error("Unable to contact #{h} via TCP Echo")
            end
          }
        rescue Timeout::Error
          error("Unable to contact #{h} via TCP Echo")
        rescue
          error("Unable to contact #{h} via TCP Echo")
        end
      }
    else
      error("The '#{method}' ping method is not supported")
    end
  end
end