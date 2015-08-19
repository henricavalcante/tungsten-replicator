# MySQL binlog extractor properties.
replicator.extractor.dbms=com.continuent.tungsten.replicator.extractor.mysql.MySQLExtractor

# Data source from which we are extracting.  This must correspond to a MySQL
# DBMS. 
replicator.extractor.dbms.dataSource=extractor

# Location of MySQL binlog and pattern for binlog file names. 
replicator.extractor.dbms.binlog_dir=@{EXTRACTOR.REPL_MASTER_LOGDIR}
replicator.extractor.dbms.binlog_file_pattern=@{EXTRACTOR.REPL_MASTER_LOGPATTERN}

# Parse statements for binary data.
replicator.extractor.dbms.parseStatements=true

# Use bytes to transfer strings.  This should generally be set to true 
# for MySQL-to-MySQL replication or embedded character set or binary data
# will be corrupted in statements.  It should be set to false for heterogeneous
# replication. 
replicator.extractor.dbms.usingBytesForString=@{REPL_MYSQL_USE_BYTES_FOR_STRING}

# Fragment size in bytes for transaction splitting.  0 means no splitting
# will occur.  1M bytes is a good size for most installations.
replicator.extractor.dbms.transaction_frag_size=1000000

# When using relay logs we download from the master into binlog_dir.  This
# is used for off-board replication.
replicator.extractor.dbms.useRelayLogs=@{EXTRACTOR.REPL_DISABLE_RELAY_LOGS}

# When you turn on relay logs, you must define a location for them.  This
# overrides the normal log directory.
replicator.extractor.dbms.relayLogDir=@{SERVICE.REPL_RELAY_LOG_DIR}

# The relay log wait timeout is the number of seconds to wait for the relay
# log position to catch up to the current extract position that Tungsten
# replication wants.  0 is infinite.
replicator.extractor.dbms.relayLogWaitTimeout=0

# The relay log read timeout is the number of seconds to wait for partial
# reads to complete.  Increasing this value can help if you get 
# EOFExceptions from reading relay logs. 
replicator.extractor.dbms.relayLogReadTimeout=30

# The relay log retention is the number of relay logs to keep before deleting
# them automatically.
replicator.extractor.dbms.relayLogRetention=10

# The serverId is the ID used when logging into MySQL to download binlog 
# data.  MySQL requires all clients to use a unique value.  If multiple
# replicators or MySQL slaves read from the same master, you must ensure
# values are different or MySQL will kill the earlier session with the 
# same server ID. 
replicator.extractor.dbms.serverId=@{REPL_MYSQL_SERVER_ID}
