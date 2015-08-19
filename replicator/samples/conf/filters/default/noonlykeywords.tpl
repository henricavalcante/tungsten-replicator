# Translates DELETE FROM ONLY -> DELETE FROM and UPDATE ONLY -> UPDATE. Use case: Slony -> MySQL.
replicator.filter.noonlykeywords=com.continuent.tungsten.replicator.filter.JavaScriptFilter
replicator.filter.noonlykeywords.script=${replicator.home.dir}/support/filters-javascript/noonlykeywords.js