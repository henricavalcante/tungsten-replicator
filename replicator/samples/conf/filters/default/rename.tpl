# Filter for renaming schemas, tables and columns. 
# This will only work on ROW events
replicator.filter.rename=com.continuent.tungsten.replicator.filter.RenameFilter
replicator.filter.rename.definitionsFile=${replicator.home.dir}/support/filters-config/rename.csv
