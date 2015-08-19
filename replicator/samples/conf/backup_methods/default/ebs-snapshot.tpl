replicator.backup.agent.ebs-snapshot=com.continuent.tungsten.replicator.backup.generic.ScriptDumpAgent
replicator.backup.agent.ebs-snapshot.script=${replicator.home.dir}/bin/ebs_snapshot.sh
replicator.backup.agent.ebs-snapshot.commandPrefix=@{REPL_BACKUP_COMMAND_PREFIX}
replicator.backup.agent.ebs-snapshot.hotBackupEnabled=true
replicator.backup.agent.ebs-snapshot.logFilename=${replicator.home.dir}/log/ebs-snapshot.log
replicator.backup.agent.ebs-snapshot.options=service=@{SERVICE.DEPLOYMENT_SERVICE}