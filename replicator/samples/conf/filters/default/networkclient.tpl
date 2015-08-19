# Filter sending values to a custom TCP server for filtering. 
# Supports ROW events only.
replicator.filter.networkclient=com.continuent.tungsten.replicator.filter.NetworkClientFilter
replicator.filter.networkclient.definitionsFile=${replicator.home.dir}/samples/extensions/java/networkclient.json
replicator.filter.networkclient.serverPort=3112
# Timeout for network operations in seconds.
replicator.filter.networkclient.timeout=10