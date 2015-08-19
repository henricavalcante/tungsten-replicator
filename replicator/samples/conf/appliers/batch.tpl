# Batch applier basic configuration information. 
replicator.applier.dbms=com.continuent.tungsten.replicator.applier.batch.SimpleBatchApplier
@{#(APPLIER.REPL_SVC_DATASOURCE_APPLIER_INIT_SCRIPT)}replicator.applier.dbms.initScript=@{APPLIER.REPL_SVC_DATASOURCE_APPLIER_INIT_SCRIPT}

# Data source to which to apply. 
replicator.applier.dbms.dataSource=global

# Location of the load script. 
replicator.applier.dbms.loadScript=${replicator.home.dir}/appliers/batch/@{SERVICE.BATCH_LOAD_TEMPLATE}.js

# Number of threads to use for loading.  The default is 1.  Values greater 
# than 1 are recommended only for idempotent data sources like Hadoop whose 
# load scripts automatically clean up partially loaded data.  
replicator.applier.dbms.parallelization=1

# Timezone and character set.  
replicator.applier.dbms.timezone=GMT+0:00
#replicator.applier.dbms.charset=UTF-8

# Location for writing CSV files. 
replicator.applier.dbms.stageDirectory=/tmp/staging/${service.name}

# Prefixes for stage table and schema names.  These are added to the base
# table and schema respectively. 
replicator.applier.dbms.stageTablePrefix=stage_xxx_
replicator.applier.dbms.stageSchemaPrefix=

# Staging header columns to apply.  Changing these can cause incompatibilities
# with generated SQL for Hive tables. 
replicator.applier.dbms.stageColumnNames=opcode,seqno,row_id,commit_timestamp

# Prefix for Tungsten staging column names.  This is added to header 
# column names and prevents naming clashes with user tables. 
replicator.applier.dbms.stageColumnPrefix=tungsten_

# Properties to enable 'partition by' support, which splits CSV files for a 
# single table based on a key generated from one of the tungsten header files.
# To partition files by commit hour including date, set the partitionBy
# property to tungsten_commit_timestamp.  Note that the full name including 
# prefix must be used here. 
replicator.applier.dbms.partitionBy=
replicator.applier.dbms.partitionByClass=com.continuent.tungsten.replicator.applier.batch.DateTimeValuePartitioner
replicator.applier.dbms.partitionByFormat='commit_hour='yyyy-MM-dd-HH

# Clear files after each transaction.  
replicator.applier.dbms.cleanUpFiles=true

# If true, use update opcode (U) instead of splitting updates into insert
# followed by delete.  This setting is not recommended for standard batch
# loading as it may not work for row changes that do not have keys. 
replicator.applier.dbms.useUpdateOpcode=false

# If true, use 'UI' and 'UD' opcodes for update operations, as opposed to
# plain 'I' and 'D'. Useful to be able to distinguish updates from regular
# inserts and deletes.
replicator.applier.dbms.distinguishUpdates=false
