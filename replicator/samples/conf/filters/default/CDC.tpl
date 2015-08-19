# Filter to transform a specific database name and table into a new value.
# Eg. of usage: transform heartbeat table from Oracle schema to MySQL schema
# under Oracle to MySQL replication.

replicator.filter.CDC=com.continuent.tungsten.replicator.filter.TungstenTableCDCTransformFilter
replicator.filter.CDC.from=@{REPL_SVC_SCHEMA}.HEARTBEAT
replicator.filter.CDC.to=tungsten_@{DSNAME}.heartbeat