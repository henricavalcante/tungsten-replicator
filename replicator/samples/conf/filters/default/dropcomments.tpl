# Remove comments from a SQL string and drop the event if the result is empty
replicator.filter.dropcomments=com.continuent.tungsten.replicator.filter.JavaScriptFilter                                  
replicator.filter.dropcomments.script=${replicator.home.dir}/support/filters-javascript/dropcomments.js