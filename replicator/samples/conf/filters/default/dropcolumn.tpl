# Drops columns specified in definitions JSON file. For exact format see an example:
# support/filters-javascript/dropcolumn.json
replicator.filter.dropcolumn=com.continuent.tungsten.replicator.filter.JavaScriptFilter
replicator.filter.dropcolumn.script=${replicator.home.dir}/support/filters-javascript/dropcolumn.js
replicator.filter.dropcolumn.definitionsFile=~/dropcolumn.json