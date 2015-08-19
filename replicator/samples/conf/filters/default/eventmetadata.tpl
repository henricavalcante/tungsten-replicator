# Filter to parse SQL statements, assign shard ID, and assign originating
# service name.  Runs implicitly in the MySQL extractor but also used for
# multi-master and parallel replication.  
replicator.filter.eventmetadata=com.continuent.tungsten.replicator.event.EventMetadataFilter
