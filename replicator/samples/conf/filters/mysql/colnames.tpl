# Column name filter.  Adds column name metadata to row updates.  This is 
# required for MySQL row replication if you have logic that requires column
# names.
replicator.filter.colnames=com.continuent.tungsten.replicator.filter.ColumnNameFilter

# Use the default data source. 
replicator.filter.colnames.dataSource=global

# Heterogeneous topologies need signed/unsigned information.
replicator.filter.colnames.addSignedFlag=true

# If true, ignore tables that have no metadata and generate automatic column
# names. This can happen when reading an old DBMS log in which case current 
# metadata is not accessible if tables have sinc been deleted. If false, 
# the filter will stop with an exception. 
replicator.filter.colnames.ignoreMissingTables=true
