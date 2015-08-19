# Transforms database, table and column names into upper or lower case. In case
# of statement replication generally it transforms everything except quoted
# string values.
replicator.filter.casetransform=com.continuent.tungsten.replicator.filter.CaseMappingFilter
replicator.filter.casetransform.to_upper_case=true