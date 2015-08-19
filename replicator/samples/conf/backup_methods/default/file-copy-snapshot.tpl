replicator.backup.agent.file-copy-snapshot=com.continuent.tungsten.replicator.backup.generic.ScriptDumpAgent
replicator.backup.agent.file-copy-snapshot.script=${replicator.home.dir}/bin/file_copy_snapshot.sh
replicator.backup.agent.file-copy-snapshot.commandPrefix=@{REPL_BACKUP_COMMAND_PREFIX}
replicator.backup.agent.file-copy-snapshot.hotBackupEnabled=true
replicator.backup.agent.file-copy-snapshot.logFilename=${replicator.home.dir}/log/file-copy-snapshot.log
replicator.backup.agent.file-copy-snapshot.options=service=@{SERVICE.DEPLOYMENT_SERVICE}