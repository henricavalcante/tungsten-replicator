# Filter which for each transaction adds a change data capture (CDC) row to the
# change table.
replicator.filter.customcdc=com.continuent.tungsten.replicator.filter.CDCMetadataFilter

# Where to put CDC columns:
# true - CDC columns are expected to be at front of the original ones;
# false - at the end, after the original ones.
replicator.filter.customcdc.cdcColumnsAtFront=false

# Change table should be named with a suffix or be in a separate schema. 
replicator.filter.customcdc.schemaNameSuffix=
replicator.filter.customcdc.tableNameSuffix=

# All change tables might be contained in a single schema.
replicator.filter.customcdc.toSingleSchema=

# Where to start counter for fresh CDC rows.
replicator.filter.customcdc.sequenceBeginning=1