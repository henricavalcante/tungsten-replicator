####################################
# SAMPLE.POSTGRESQL-WAL.PROPERTIES #
####################################
#
# This file contains properties for WAL-based PostgreQL replication. 

# Enable streaming replication (available from PostgreSQL 9)?
postgresql.streaming_replication=@{REPL_PG_STREAMING}

# Port of current PostgreSQL.
postgresql.port=@{APPLIER.REPL_DBPORT}

# PostgreSQL home directory. 
postgresql.data=@{APPLIER.REPL_PG_HOME}

# PostgreSQL configuration file. 
postgresql.conf=@{APPLIER.REPL_PG_CONF}

# Standard archive log directory. 
postgresql.archive=@{APPLIER.REPL_PG_ARCHIVE}

# Master host name. 
postgresql.master.host=@{REPL_MASTERHOST}
postgresql.master.port=@{APPLIER.REPL_DBPORT}

# Master database administrator user name and password. 
postgresql.master.user=@{APPLIER.REPL_DBLOGIN}
postgresql.master.password=@{APPLIER.REPL_DBPASSWORD}

# Database role.  Acceptable values are 'master' and 'standby'. 
postgresql.role=@{REPL_PG_WAL_ROLE}

# PostgreSQL boot command.  The postgres user must be able to execute 
# this command without a password.  The script must accept standard 
# stop/start/restart options. 
postgresql.boot.start=@{APPLIER.REPL_DBSERVICE_START}
postgresql.boot.stop=@{APPLIER.REPL_DBSERVICE_STOP}
postgresql.boot.restart=@{APPLIER.REPL_DBSERVICE_RESTART}
postgresql.boot.status=@{APPLIER.REPL_DBSERVICE_STATUS}

# Archive timeout.  Maximum time before sending an unfilled WAL buffer to 
# standby.  This is your maximum data loss.  
postgresql.archive_timeout=@{REPL_PG_ARCHIVE_TIMEOUT}

# Location of pg_standby executable. 
postgresql.pg_standby=@{REPL_PG_STANDBY}

# Location of archive cleanup command (for SR).
postgresql.pg_archivecleanup=@{REPL_PG_ARCHIVECLEANUP}

# Location of pg_standby trigger file. 
postgresql.pg_standby.trigger=@{REPL_PG_STANDBYTRIGGER}

# Command prefix used for root commands.  'sudo' is most common for 
# non-priveged accounts. 
postgresql.root.prefix=@{ROOT_PREFIX}