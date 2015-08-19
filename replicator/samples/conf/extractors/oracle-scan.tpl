replicator.extractor.oracle.service=@{EXTRACTOR.REPL_ORACLE_SERVICE}
replicator.extractor.oracle.scan=@{EXTRACTOR.REPL_ORACLE_SCAN}
replicator.extractor.dbms=com.continuent.tungsten.replicator.extractor.oracle.OracleCDCReaderExtractor
replicator.extractor.dbms.url=jdbc:oracle:thin:@//${replicator.extractor.oracle.scan}:${replicator.global.extract.db.port}/${replicator.extractor.oracle.service}
replicator.extractor.dbms.user=${replicator.global.extract.db.user}
replicator.extractor.dbms.password=${replicator.global.extract.db.password}
replicator.extractor.dbms.transaction_frag_size=10
replicator.extractor.dbms.serviceName=${service.name}

# Max. delay in querying CDC window. Used to lessen redo log being generated.
replicator.extractor.dbms.minSleepTime=5
replicator.extractor.dbms.sleepAddition=1
replicator.extractor.dbms.maxSleepTime=15

# Reconnection mechanism for Oracle extractor thread. This cleans up resources
# that could be left opened (even by CDC itself). Timeout is in seconds.
replicator.extractor.dbms.reconnectTimeout=1200