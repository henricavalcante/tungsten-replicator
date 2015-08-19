# A filter (to be extended by the end user!) that transforms MySQL DDL dialect to PostgreSQL.
replicator.filter.pgddl=com.continuent.tungsten.replicator.filter.JavaScriptFilter
replicator.filter.pgddl.script=${replicator.home.dir}/support/filters-javascript/pgddl.js