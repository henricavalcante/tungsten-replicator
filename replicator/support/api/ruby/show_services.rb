# Make sure the script can find the json libraries
# You can remove this line and ignore the included json library if you 
# have already installed the json gem
$LOAD_PATH.unshift(File.dirname(__FILE__) + "/lib") 

require 'json'
require 'net/http'
require 'uri'
require 'pp'

url = URI.parse('http://localhost:19999/jolokia/exec/com.continuent.tungsten.replicator.management:type=ReplicationServiceManager/services')
req = Net::HTTP::Get.new(url.path)

# Update this line if you have set a custom username or password in the
# wrapper.conf file when enabling Jolokia
req.basic_auth('tungsten', 'secret')

# Initiate a connection to the host and make the request
res = Net::HTTP.new(url.host, url.port).start {
  |http|
  http.request(req)
}

# Check the state of the response and output the requested value
case res
when Net::HTTPSuccess, Net::HTTPRedirection
  parsed = JSON.parse(res.body)
  
  # There are several values of metadata included, we are only interested
  # in the actual response value
  pp parsed['value']
else
  res.error!
end