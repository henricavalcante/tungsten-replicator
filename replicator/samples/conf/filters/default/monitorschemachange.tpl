# Monitor schema change and optionally force a commit when such changes occur.
# This filter requires that events are previously processed using the 
# schemachange filter. 
replicator.filter.monitorschemachange=com.continuent.tungsten.replicator.filter.JavaScriptFilter                                  
replicator.filter.monitorschemachange.script=${replicator.home.dir}/support/filters-javascript/monitorschemachange.js

# If true force commit on schema change. 
replicator.filter.monitorschemachange.commit=true

# If true, generate a notification file into the notification directory each
# time there is a schema change. 
replicator.filter.monitorschemachange.notify=false
replicator.filter.monitorschemachange.notifyDir=${replicator.home.dir}/log/schemachanges/${service.name}
