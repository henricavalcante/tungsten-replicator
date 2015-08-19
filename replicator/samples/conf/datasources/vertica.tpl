# Vertica datasource. 
replicator.datasource.global=com.continuent.tungsten.replicator.datasource.VerticaDataSource

# Service name of the replicator. 
replicator.datasource.global.serviceName=${service.name}

# Connection information for Vertica. 
replicator.datasource.global.connectionSpec=com.continuent.tungsten.replicator.datasource.SqlConnectionSpecVertica
replicator.datasource.global.connectionSpec.host=@{APPLIER.REPL_DBHOST}
replicator.datasource.global.connectionSpec.port=@{APPLIER.REPL_DBPORT}
replicator.datasource.global.connectionSpec.user=@{APPLIER.REPL_DBLOGIN}
replicator.datasource.global.connectionSpec.password=@{APPLIER.REPL_DBPASSWORD}
replicator.datasource.global.connectionSpec.databaseName=@{REPL_VERTICA_DBNAME}
replicator.datasource.global.connectionSpec.schema=${replicator.schema}

# CSV specification type.  This is the conventions for writing CSV files,
# which tend to be slightly different for each data source.  If set to 
# custom, use the custom CSV settings.  Other supported settings are 
# default, hive, etc.
replicator.datasource.global.csvType=vertica

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
