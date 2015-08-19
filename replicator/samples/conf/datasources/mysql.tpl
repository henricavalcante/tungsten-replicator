# MySQL datasource. 
replicator.datasource.global=com.continuent.tungsten.replicator.datasource.SqlDataSource

# Service name of the replicator.
replicator.datasource.global.serviceName=${service.name}

# Whether to create catalog tables. 
replicator.datasource.global.createCatalog=true

# Connection information for MySQL. 
replicator.datasource.global.connectionSpec=com.continuent.tungsten.replicator.datasource.SqlConnectionSpecMySQL
replicator.datasource.global.connectionSpec.host=@{APPLIER.REPL_DBHOST}
replicator.datasource.global.connectionSpec.port=@{APPLIER.REPL_DBPORT}
replicator.datasource.global.connectionSpec.user=@{APPLIER.REPL_DBLOGIN}
replicator.datasource.global.connectionSpec.password=@{APPLIER.REPL_DBPASSWORD}
replicator.datasource.global.connectionSpec.schema=${replicator.schema}
replicator.datasource.global.connectionSpec.sslEnabled=${replicator.global.db.sslEnabled}
@{#(APPLIER.REPL_SVC_DATASOURCE_THL_INIT_SCRIPT)}replicator.datasource.global.connectionSpec.initScript=@{APPLIER.REPL_SVC_DATASOURCE_THL_INIT_SCRIPT}

# CSV specification type.  This is the conventions for writing CSV files,
# which tend to be slightly different for each data source.  If set to 
# custom, use the custom CSV settings.  Other supported settings are 
# default, hive, etc.
replicator.datasource.global.csvType=mysql

# CSV type settings.  These are used if the csv type is custom.
replicator.datasource.global.csv=com.continuent.tungsten.common.csv.CsvSpecification
replicator.datasource.global.csv.fieldSeparator=,
replicator.datasource.global.csv.RecordSeparator=\\n
replicator.datasource.global.csv.nullValue=\\N
replicator.datasource.global.csv.useQuotes=true

# CSV data formatter.  This is the class responsible for translating 
# from Java objects to CSV strings.  The data format can vary independently
# from the CSV type based where data are extracted from or the types of
# tools that will process data. 
replicator.datasource.global.csvFormatter=com.continuent.tungsten.replicator.csv.DefaultCsvDataFormat

# Commands to use for managing the datasource process
# These are commented out because the datasource class doesn't support them
# at the time.
#replicator.datasource.global.service.start=@{APPLIER.REPL_DBSERVICE_START}
#replicator.datasource.global.service.stop=@{APPLIER.REPL_DBSERVICE_STOP}
#replicator.datasource.global.service.restart=@{APPLIER.REPL_DBSERVICE_RESTART}
#replicator.datasource.global.service.status=@{APPLIER.REPL_DBSERVICE_STATUS}
