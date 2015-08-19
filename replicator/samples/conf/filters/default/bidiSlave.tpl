# Remote slave filter.  This filter sanitizes events on remote slaves by
# dropping events produced on the same service ID.  To use this you *must*
# set the localServiceName parameter, which must be the same as the
# service.name parameter of the local service that reads the binlog.
replicator.filter.bidiSlave=com.continuent.tungsten.replicator.filter.BidiRemoteSlaveFilter
replicator.filter.bidiSlave.localServiceName=${local.service.name}

# If true allow statements that may be unsafe for bi-directional replication. 
replicator.filter.bidiSlave.allowBidiUnsafe=@{REPL_SVC_ALLOW_BIDI_UNSAFE}

# If true allow updates from all other remote services, not just this one. 
replicator.filter.bidiSlave.allowAnyRemoteService=@{REPL_SVC_ALLOW_ANY_SERVICE}