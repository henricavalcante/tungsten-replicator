replicator.extractor.dbms=com.continuent.tungsten.replicator.extractor.oracle.OracleCDCReaderExtractor
replicator.extractor.dbms.dataSource=oracle_extractor
replicator.extractor.dbms.transaction_frag_size=10

# Max. delay in querying CDC window. Used to lessen redo log being generated.
replicator.extractor.dbms.minSleepTime=5
replicator.extractor.dbms.sleepAddition=1
replicator.extractor.dbms.maxSleepTime=15

# Reconnection mechanism for Oracle extractor thread. This cleans up resources
# that could be left opened (even by CDC itself). Timeout is in seconds.
replicator.extractor.dbms.reconnectTimeout=1200