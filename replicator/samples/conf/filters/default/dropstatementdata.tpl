# Remove statement events and drop the event if the result is empty
replicator.filter.dropstatementdata=com.continuent.tungsten.replicator.filter.JavaScriptFilter                                  
replicator.filter.dropstatementdata.script=${replicator.home.dir}/support/filters-javascript/dropstatementdata.js