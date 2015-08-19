# Filter to forward or ignore particular schemas and/or databases.  Entries
# are comma-separated lists of the form schema[.table] where the table is
# optional.  List entries may use * and ? as wild cards.  When both 
# filter lists are empty updates on all tables are allowed. 
replicator.filter.replicate=com.continuent.tungsten.replicator.filter.ReplicateFilter
replicator.filter.replicate.ignore=
replicator.filter.replicate.do=
