# Prefetch applier.  This applier depends on a PrefetchStore to handle
# flow control on transactions that require prefetching. 
replicator.applier.dbms=com.continuent.tungsten.replicator.prefetch.PrefetchApplier

# URL, login, and password of Tungsten slave for which we are prefetching. 
# The URL must specify the replicator catalog schema name. 
replicator.applier.dbms.url=@{APPLIER.REPL_DBBASICJDBCURL}
replicator.applier.dbms.user=${replicator.global.db.user}
replicator.applier.dbms.password=${replicator.global.db.password}

# Slow query cache parameters.  Slow queries include those with large
# numbers of rows or poor selectivity, where selectivity is the
# fraction of the rows selected.  Any query that exceeds these is
# not repeated for the number of seconds in the cache duration property.  
replicator.applier.dbms.slowQueryCacheSize=10000
replicator.applier.dbms.slowQueryRows=1000
replicator.applier.dbms.slowQuerySelectivity=.05
replicator.applier.dbms.slowQueryCacheDuration=60

# Maximum number of rows to return when transforming a statement to a query. 
# Higher values can cause the replicator to run out of memory.  Lower values
# reduce prefetching on operations that affect large numbers of rows.  This 
# value is a compromise.  0 removes limits. 
replicator.applier.dbms.prefetchRowLimit=25000

# Maximum number of SQL errors to tolerate before logging a replicator error. 
replicator.applier.dbms.maxErrors=1000
