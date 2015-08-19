# Filter to be used on slaves For every
# CREATE|DROP|ALTER|RENAME TABLE event adds a "SET foreign_key_checks=0"
# statement.
replicator.filter.foreignkeychecks=com.continuent.tungsten.replicator.filter.JavaScriptFilter 
replicator.filter.foreignkeychecks.script=${replicator.home.dir}/support/filters-javascript/foreignkeychecks.js