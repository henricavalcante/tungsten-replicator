# Filter which drops INSERTs when some column value is matched. 
# Supports ROW events only.
replicator.filter.droponvalue=com.continuent.tungsten.replicator.filter.DropOnValueFilter
replicator.filter.droponvalue.definitionsFile=${replicator.home.dir}/support/filters-config/droponvalue.json

