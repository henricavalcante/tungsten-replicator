# JavaScript call out filter. Calls script prepare(), filter(event) and
# release() functions. Define multiple filters with different names in case you
# need to call more than one script.
replicator.filter.javascript=com.continuent.tungsten.replicator.filter.JavaScriptFilter
replicator.filter.javascript.script=${replicator.home.dir}/support/filters-javascript/filter.js
replicator.filter.javascript.sample_custom_property=Sample