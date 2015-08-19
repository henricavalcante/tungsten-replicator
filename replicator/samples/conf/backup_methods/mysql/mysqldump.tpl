# MySqlDump Agent--backup using mysql dump utility; restore with mysql.
replicator.backup.agent.mysqldump=com.continuent.tungsten.replicator.backup.generic.ScriptDumpAgent
replicator.backup.agent.mysqldump.script=${replicator.home.dir}/bin/mysqldump.sh
replicator.backup.agent.mysqldump.hotBackupEnabled=true
replicator.backup.agent.mysqldump.logFilename=${replicator.home.dir}/log/mysqldump.log
replicator.backup.agent.mysqldump.options=tungsten_backups=@{SERVICE.REPL_BACKUP_STORAGE_DIR}&my_cnf=@{APPLIER.REPL_MYSQL_SERVICE_CONF}&gz=true&service=@{SERVICE.DEPLOYMENT_SERVICE}