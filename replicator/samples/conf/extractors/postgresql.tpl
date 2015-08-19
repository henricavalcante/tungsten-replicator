# PostgreSQL Slony log extractor properties. 
replicator.extractor.dbms=com.continuent.tungsten.replicator.extractor.postgresql.PostgreSQLSlonyExtractor 
replicator.extractor.dbms.host=${replicator.global.extract.db.host}
replicator.extractor.dbms.database=@{REPL_POSTGRESQL_DBNAME}
replicator.extractor.dbms.slonySchema=_@{SERVICE.DEPLOYMENT_SERVICE}
replicator.extractor.dbms.port=${replicator.global.extract.db.port}
replicator.extractor.dbms.user=${replicator.global.extract.db.user}
replicator.extractor.dbms.password=${replicator.global.extract.db.password}