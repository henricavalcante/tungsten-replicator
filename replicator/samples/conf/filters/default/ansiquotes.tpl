# Enables ANSI_QUOTES mode for incoming events. Use case: Slony -> MySQL.
replicator.filter.ansiquotes=com.continuent.tungsten.replicator.filter.JavaScriptFilter
replicator.filter.ansiquotes.script=${replicator.home.dir}/support/filters-javascript/ansiquotes.js
