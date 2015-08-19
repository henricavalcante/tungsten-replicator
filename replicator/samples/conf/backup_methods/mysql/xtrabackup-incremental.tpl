replicator.backup.agent.xtrabackup-incremental=com.continuent.tungsten.replicator.backup.generic.ScriptDumpAgent
replicator.backup.agent.xtrabackup-incremental.script=${replicator.home.dir}/bin/xtrabackup.sh
replicator.backup.agent.xtrabackup-incremental.commandPrefix=@{REPL_BACKUP_COMMAND_PREFIX}
replicator.backup.agent.xtrabackup-incremental.hotBackupEnabled=true
replicator.backup.agent.xtrabackup-incremental.logFilename=${replicator.home.dir}/log/xtrabackup.log
replicator.backup.agent.xtrabackup-incremental.options=incremental=true&directory=@{REPL_MYSQL_XTRABACKUP_DIR}&tungsten_backups=@{SERVICE.REPL_BACKUP_STORAGE_DIR}&mysqllogdir=@{APPLIER.REPL_MASTER_LOGDIR}&mysqllogpattern=@{APPLIER.REPL_MASTER_LOGPATTERN}&my_cnf=@{APPLIER.REPL_MYSQL_SERVICE_CONF}&restore_to_datadir=@{SERVICE.REPL_MYSQL_XTRABACKUP_RESTORE_TO_DATADIR}&service=@{SERVICE.DEPLOYMENT_SERVICE}