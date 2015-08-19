# Script Agent--Executes a script to backup or restore. 
replicator.backup.agent.script=com.continuent.tungsten.replicator.backup.generic.ScriptDumpAgent
replicator.backup.agent.script.script=@{REPL_BACKUP_SCRIPT}
replicator.backup.agent.script.commandPrefix=@{REPL_BACKUP_COMMAND_PREFIX}
replicator.backup.agent.script.hotBackupEnabled=@{REPL_BACKUP_ONLINE}
replicator.backup.agent.script.logFilename=${replicator.home.dir}/log/script.log
replicator.backup.agent.script.options=