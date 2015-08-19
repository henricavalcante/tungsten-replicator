# VMware Continuent Tungsten Replicator
# Copyright (C) 2015 VMware, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Initial developer(s): Jeff Mace

class IPParse
  IPV4 = :ipv4
  IPV6 = :ipv6
  
  def initialize
    @klass = get_platform_klass().new()
    @parsed = nil
  end
  
  # Return a nested array of the parsed interface information
  # { 
  #   "eth0" => {
  #     :ipv4 => { :address => "1.2.3.4", :netmask => "255.255.255.0" },
  #     :ipv6 => { :address => "fe80::a00:27ff:fe1f:84a5", :netmask => "64" }
  #   }
  # }
  def get_interfaces
    parse()
    return @parsed
  end
  
  def get_interface_address(interface, type = IPV4)
    parse()
    
    unless @parsed.has_key?(interface)
      return nil
    end
    
    unless @parsed[interface].has_key?(type)
      return nil
    end
    
    return @parsed[interface][type]
  end
  
  private
  
  def get_platform
    RUBY_PLATFORM
  end
  
  # Have the platform class parse the IP configuration information 
  # it just collected. If the information has already been parsed
  # the steps can be skipped.
  def parse
    if @parsed != nil
      return
    end
    
    @parsed = @klass.parse(get_raw_ip_configuration())
  end
  
  # Have the platform class to collect IP configuration information
  def get_raw_ip_configuration
    @klass.get_raw_ip_configuration()
  end
  
  # Iterate through all of the platform classes until one declares 
  # it is able to support the current system.
  def get_platform_klass(platform = nil)
    if platform == nil
      platform = get_platform()
    end
    
    platform = platform.downcase()
    
    IPParsePlatform.subclasses().each{
      |klass|
      begin
        supports_platform = klass.supports_platform?(platform)
        if supports_platform == true
          return klass
        end
      rescue NoMethodError
        # Eat this error because the platform didn't define the required method
      end
    }
    
    throw Exception.new("Unable to support the #{platform} platform")
  end
end

class IPParsePlatform
  def initialize
    @interfaces = {}
  end
  
  def add_ipv4(name, address, netmask)
    unless @interfaces.has_key?(name)
      @interfaces[name] = {}
    end
    
    @interfaces[name][IPParse::IPV4] = {
      :address => address,
      :netmask => netmask
    }
  end
  
  def add_ipv6(name, address, netmask)
    unless @interfaces.has_key?(name)
      @interfaces[name] = {}
    end
    
    @interfaces[name][IPParse::IPV6] = {
      :address => address,
      :netmask => netmask
    }
  end
  
  def get_raw_ip_configuration
    throw Exception.new("Undefined method #{self.class.name}:get_raw_ip_configuration")
  end
  
  def self.inherited(subclass)
    @subclasses ||= []
    @subclasses << subclass
  end
  
  def self.subclasses
    @subclasses
  end
end

# Load all platform classes so they can be registered
Dir.glob("#{File.dirname(__FILE__)}/ipparse/platforms/*.rb").each{|platform|
  require platform
}