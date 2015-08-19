# Filter to identify schema changes that may require downstream regen of 
# DBMS schema on replicas, especially in heterogeneous topologies.  In 
# response to CREATE/DROP DATABASE or CREATE/DROP/ALTER tabel it sets the
# tag "schema_change" on the overall event and annotates affected statements 
# with the affected schema, table, and operation type. 
replicator.filter.schemachange=com.continuent.tungsten.replicator.filter.SchemaChangeFilter
