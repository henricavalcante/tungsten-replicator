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

class LinuxIPParsePlatform < IPParsePlatform
  def self.supports_platform?(platform)
    if platform.downcase() =~ /linux/
      true
    else
      false
    end
  end
  
  def get_raw_ip_configuration
    path = `which ifconfig 2>/dev/null`.chomp()
    if path == ""
      path = "/sbin/ifconfig"
    end
    
    results = `export LANG=en_US; #{path} -a`
    if results == false
      raise "Unable to collect IP configuration from ifconfig"
    else
      return results
    end
  end
  
  def parse(raw)
    name_regex = Regexp.compile(/^([a-zA-Z0-9]+)/)
    encapsulation_regex = Regexp.compile(/Link encap:(\S+)/)
    flags_regex = Regexp.compile(/flags=([0-9]+)\<([A-Z,]+)\>/)
    inet4_regex1 = Regexp.compile(/inet addr:([0-9\.]+)[ ]+(Bcast:[0-9\.]+ )?[ ]+Mask:([0-9\.]+)/)
    inet4_regex2 = Regexp.compile(/inet ([0-9\.]+)[ ]+netmask ([0-9\.]+)[ ]+(broadcast [0-9\.]+ )?/)
    inet6_regex1 = Regexp.compile(/inet6 addr:[ ]*([a-f0-9:]+)\/([0-9]+)/)
    inet6_regex2 = Regexp.compile(/inet6 ([a-f0-9:]+)[ ]*prefixlen ([0-9]+)/)

    raw.split("\n\n").each{
      |ifconfig|
      include_interface = false
      
      begin
        encapsulation = encapsulation_regex.match(ifconfig)[1]
        
        if encapsulation.downcase() == "ethernet"
          include_interface = true
        end
      rescue
        # Catch the exception and move on
      end
      
      begin
        flags = flags_regex.match(ifconfig)[2]
        flags = flags.split(",")

        if flags.include?("LOOPBACK") == false
          include_interface = true
        end
      rescue
        # Catch the exception and move on
      end
      
      if include_interface == false
        next
      end
      
      name = name_regex.match(ifconfig)[1]
      if name == nil
        raise "Unable to parse IP configuration because a valid name does not exist"
      end
      
      m1 = inet4_regex1.match(ifconfig)
      m2 = inet4_regex2.match(ifconfig)
      if m1
        add_ipv4(name, m1[1], m1[3])
      elsif m2
        add_ipv4(name, m2[1], m2[2])
      end
      
      m1 = inet6_regex1.match(ifconfig)
      m2 = inet6_regex2.match(ifconfig)
      if m1
        add_ipv6(name, m1[1], m1[2])
      elsif m2
        add_ipv6(name, m2[1], m2[2])
      end
    }
    
    return @interfaces
  end
end