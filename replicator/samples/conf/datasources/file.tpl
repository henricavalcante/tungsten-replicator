# File datasource. 
replicator.datasource.global=com.continuent.tungsten.replicator.datasource.FileDataSource
replicator.datasource.global.serviceName=${service.name}

# Storage location for replication catalog data. 
replicator.datasource.global.directory=@{REPL_METADATA_DIRECTORY}

# CSV specification type.  This is the conventions for writing CSV files,
# which tend to be slightly different for each data source.  If set to 
# custom, use the custom CSV settings.  Other supported settings are 
# default, hive, etc. 
replicator.datasource.global.csvType=hive

# CSV type settings.  These are used if the csv type is custom. 
replicator.datasource.global.csv=com.continuent.tungsten.common.csv.CsvSpecification
replicator.datasource.global.csv.fieldSeparator=,
replicator.datasource.global.csv.RecordSeparator=\\n
replicator.datasource.global.csv.nullValue=\\N
replicator.datasource.global.csv.useQuotes=true
replicator.datasource.global.csv.useHeaders=false

# CSV data formatter.  This is the class responsible for translating
# from Java objects to CSV strings.  The data format can vary independently
# from the CSV type based where data are extracted from or the types of 
# tools that will process data. 
replicator.datasource.global.csvFormatter=com.continuent.tungsten.replicator.csv.DefaultCsvDataFormat
