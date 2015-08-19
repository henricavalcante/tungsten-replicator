# HDFS data source. 
replicator.datasource.global=com.continuent.tungsten.replicator.datasource.HdfsDataSource
replicator.datasource.global.serviceName=${service.name}
# Storage location for replication catalog data. 
replicator.datasource.global.directory=/user/tungsten/metadata

# HDFS-specific information. 
replicator.datasource.global.hdfsUri=hdfs://@{APPLIER.REPL_DBHOST}:@{APPLIER.REPL_DBPORT}/user/tungsten/metadata
replicator.datasource.global.hdfsConfigProperties=${replicator.home.dir}/conf/hdfs-config.properties

# CSV specification type.  This is the conventions for writing CSV files,
# which tend to be slightly different for each data source.  If set to 
# custom, use the custom CSV settings.  Other supported settings are 
# default, hive, etc.
replicator.datasource.global.csvType=hive

# CSV type settings.  These are used if the csv type is custom.  The 
# The file and record separator values are congenial for Hive external 
# tables but it is simpler to use the hive csvType. 
replicator.datasource.global.csv=com.continuent.tungsten.common.csv.CsvSpecification
replicator.datasource.global.csv.fieldSeparator=\\u0001
replicator.datasource.global.csv.RecordSeparator=\\n
replicator.datasource.global.csv.useQuotes=false

# CSV data formatter.  This is the class responsible for translating 
# from Java objects to CSV strings.  The data format can vary independently
# from the CSV type based where data are extracted from or the types of
# tools that will process data. 
replicator.datasource.global.csvFormatter=com.continuent.tungsten.replicator.csv.DefaultCsvDataFormat
