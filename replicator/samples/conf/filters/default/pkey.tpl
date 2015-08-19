# Primary key filter.  This filter is required for MySQL row replication to 
# reduce the number of columns used in comparisons for updates and deletes. 
replicator.filter.pkey=com.continuent.tungsten.replicator.filter.PrimaryKeyFilter

# Use the default data source. 
replicator.filter.pkey.dataSource=global

# Set to true in order to add primary keys to INSERT operations.  This is
# required for batch loading. 
replicator.filter.pkey.addPkeyToInserts=@{ENABLE_HETEROGENOUS_MASTER}

# Set to true in order to add full column metadata to DELETEs.  This is
# likewise required for batch loading. 
replicator.filter.pkey.addColumnsToDeletes=@{ENABLE_HETEROGENOUS_MASTER}
