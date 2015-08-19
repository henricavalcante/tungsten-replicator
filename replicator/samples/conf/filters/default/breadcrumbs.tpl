# Breadcrumbs filter.  This annotates transactions with breadcrumbs values 
# to enable restart when switching masters between different MySQL logs.  You
# must include a server ID. 
replicator.filter.breadcrumbs=com.continuent.tungsten.replicator.filter.JavaScriptFilter
replicator.filter.breadcrumbs.script=${replicator.home.dir}/support/filters-javascript/breadcrumbs.js
replicator.filter.breadcrumbs.server_id=33
