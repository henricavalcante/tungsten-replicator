# PostgreSQL Dump Agent--Backup using pg_dump utility; restore with pg_restore.
replicator.backup.agent.pg_dump=com.continuent.tungsten.replicator.backup.postgresql.PostgreSqlDumpAgent
replicator.backup.agent.pg_dump.host=${replicator.global.db.host}
replicator.backup.agent.pg_dump.port=${replicator.global.db.port}
replicator.backup.agent.pg_dump.user=${replicator.global.db.user}
replicator.backup.agent.pg_dump.password=${replicator.global.db.password}
replicator.backup.agent.pg_dump.dumpDirName=@{REPL_BACKUP_DUMP_DIR}
replicator.backup.agent.pg_dump.pgdumpOptions=-Fc
replicator.backup.agent.pg_dump.pgrestoreOptions=-Fc
replicator.backup.agent.pg_dump.ignoreDatabaseList=postgres template0 template1
replicator.backup.agent.pg_dump.databaseToConnect=template1
replicator.backup.agent.pg_dump.hotBackupEnabled=true