# MongoDB applier.  You must specify a connection string for the server.
# This currently supports only a single server.
replicator.applier.dbms=com.continuent.tungsten.replicator.applier.MongoApplier
replicator.applier.dbms.connectString=${replicator.global.db.host}:${replicator.global.db.port}

# Auto-indexing will add indexes to MongoDB collections corresponding to keys
# in row updates.  This boosts performance considerably. 
replicator.applier.dbms.autoIndex=true
