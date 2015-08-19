/**
 * VMware Continuent Tungsten Replicator
 * Copyright (C) 2015 VMware, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *      
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Load script for Redshift through S3. Uses AWS credentials from
 * share/s3-config-{service}.json configuration file.
 *
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 */

// AWS details.
var awsS3Path;
var awsAccessKey;
var awsSecretKey;
var serviceName;

var filter;

/** Reads AWS configuration file into a string. */
function readAWSConfigFile()
{
  var awsConfigFileName = "s3-config-" + serviceName + ".json";
  var awsConfigFile = "../../../../share/" + awsConfigFileName;
  var f = new java.io.File(awsConfigFile);
  if (!f.isFile())
  {
    message = "AWS S3 configuration file (share/" + awsConfigFileName
    ") does not exist, "
        + "create one by using a sample (tungsten/cluster-home/samples/conf/s3-config.json)";
    throw new com.continuent.tungsten.replicator.ReplicatorException(message);
  }

  logger.info("redshift.js using AWS S3 configuration: " + awsConfigFile);
  var file = new java.io.BufferedReader(new java.io.FileReader(f));
  var sb = new java.lang.StringBuffer();
  while ((line = file.readLine()) != null)
  {
    sb.append(line);
    sb.append(java.lang.System.getProperty("line.separator"));
  }

  return sb.toString();
}

/** Parses and validates "cleanUpS3Files" parameter. */
function initCleanUpS3Files()
{
  if (cleanUpS3Files == null)
  {
    logger
        .info("Will remove S3 files by default, though \"cleanUpS3Files\" is undefined");
    cleanUpS3Files = true;
  }
  else if (cleanUpS3Files == "true")
    cleanUpS3Files = true;
  else if (cleanUpS3Files == "false")
    cleanUpS3Files = false;
  else
  {
    throw new com.continuent.tungsten.replicator.ReplicatorException(
        "Parameter value for \"cleanUpS3Files\" must be a quoted string \"true\" or \"false\", currently: "
            + cleanUpS3Files);
  }
}

/** Parses and validates "gzipFiles" parameter. */
function initGzipS3Files()
{
  if (gzipS3Files == null)
    gzipS3Files = false;
  else if (gzipS3Files == "true")
  {
    logger
        .info("Will compress files uploaded to S3 with gzip");
    gzipS3Files = true;
  }
  else if (gzipS3Files == "false")
    gzipS3Files = false;
  else
  {
    throw new com.continuent.tungsten.replicator.ReplicatorException(
        "Parameter value for \"gzipS3Files\" must be a quoted string \"true\" or \"false\", currently: "
            + gzipS3Files);
  }
}

/** Called once when applier goes online. */
function prepare()
{
  serviceName = runtime.getContext().getServiceName();
  
  // Read AWS details from configuration file.
  var json = readAWSConfigFile();
  awsConfig = eval("(" + json + ")");

  awsS3Path = awsConfig["awsS3Path"];
  awsAccessKey = awsConfig["awsAccessKey"];
  awsSecretKey = awsConfig["awsSecretKey"];
  gzipS3Files = awsConfig["gzipS3Files"];
  cleanUpS3Files = awsConfig["cleanUpS3Files"];
  storeCDCIn = awsConfig["storeCDCIn"];
  
  filterConfFile=awsConfig["replicateCDC"];
  if(filterConfFile != null && filterConfFile.length > 0)
  {
    logger.info("Using CDC filtering configuration file prefix : " + filterConfFile)
    filter.setSchemaTableFilterFilePrefix(filterConfFile);
  }
  
  logger.info("AWS S3 CSV staging path: " + awsS3Path);

  initGzipS3Files();
  
  initCleanUpS3Files();
  logger.info("Remove CSV files after upload: " + cleanUpS3Files);
  
  if (storeCDCIn != null && storeCDCIn.length > 0)
  {
    logger.info("Save Change Data Capture to: " + storeCDCIn);
  }
}

/** Called at start of batch transaction. */
function begin()
{
  // Start the transaction.
  sql.begin();
}

/** Called for each table in the transaction.  Load rows to staging table. */
function apply(csvinfo)
{
  // Fill in variables required to create SQL to merge data for current table.
  csv_file = csvinfo.file.getAbsolutePath();
  csv_filename = csvinfo.file.getName();
  csv_extension = "";
  gzip_option = "";
  schema = csvinfo.schema;
  table = csvinfo.table;
  seqno = csvinfo.startSeqno;
  key = csvinfo.key;
  stage_table_fqn = csvinfo.getStageTableFQN(sql);
  base_table_fqn = csvinfo.getBaseTableFQN(sql);
  base_columns = csvinfo.getBaseColumnList(sql);
  pkey_columns = csvinfo.getPKColumnList(sql);
  where_clause = csvinfo.getPKColumnJoinList(sql, stage_table_fqn, base_table_fqn);
  
  // Compress file if needed.
  if (gzipS3Files)
  {
    csv_extension = ".gz";
    gzip_option = "GZIP";
    gzipCmd = runtime.sprintf("gzip -c %s > %s%s", csv_file, csv_file,
        csv_extension);
    runtime.exec(gzipCmd);
    if (logger.isDebugEnabled())
    {
      logger.debug(gzipCmd);
    }
  }

  // Upload CSV to S3.
  s3PutCmd = runtime.sprintf("s3cmd put %s%s %s/%s/", csv_file, csv_extension,
      awsS3Path, serviceName);
  runtime.exec(s3PutCmd);
  if (logger.isDebugEnabled())
  {
    logger.debug(s3PutCmd);
  }

  // Clear the staging table.
  clear_sql = runtime.sprintf("DELETE FROM %s", stage_table_fqn);
  if (logger.isDebugEnabled())
    logger.debug("CLEAR: " + clear_sql);
  sql.execute(clear_sql);

  // Create and execute copy command.
  copy_sql = runtime.sprintf(
          "COPY %s FROM '%s/%s/%s%s' CSV NULL AS 'null' %s CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'",
          stage_table_fqn, awsS3Path, serviceName, csv_filename, csv_extension,
          gzip_option, awsAccessKey, awsSecretKey);
  if (logger.isDebugEnabled())
    logger.debug("COPY: "
        + copy_sql.substring(0, copy_sql.indexOf("CREDENTIALS") + 12) + "...");
  sql.execute(copy_sql);

  // Check loaded row count.
  expected_copy_rows = runtime.exec("cat " + csv_file + " |wc -l");
  rows = sql.retrieveRowCount(stage_table_fqn);
  if (rows != expected_copy_rows)
  {
    message = "Row count in staging table does not match: sql=" + copy_sql
        + " expected_copy_rows=" + expected_copy_rows + " rows=" + rows;
    logger.error(message);
    throw new com.continuent.tungsten.replicator.ReplicatorException(message);
  }
  else
  {
    if (logger.isDebugEnabled())
    {
      logger.debug("COUNT: " + rows);
    }
  }

  // Change Data Capture.
  if (storeCDCIn != null && storeCDCIn.length > 0)
  {
    if(filter == null || !filter.filterEvent(schema,table))
    {
      cdc_table_fqn = storeCDCIn.replace("{table}", table).replace("{schema}",
          schema);
      insert_select_sql = runtime.sprintf("INSERT INTO %s SELECT * FROM %s",
          cdc_table_fqn, stage_table_fqn);
      if (logger.isDebugEnabled())
        logger.debug(insert_select_sql);
      sql.execute(insert_select_sql);
    }
  }

  // Remove deleted rows from base table.
  delete_sql = runtime.sprintf(
    "DELETE FROM %s WHERE EXISTS (SELECT * FROM %s WHERE %s AND %s.tungsten_opcode IN ('D', 'UD'))",
    base_table_fqn, 
    stage_table_fqn, 
    where_clause, 
    stage_table_fqn
  );
  if (logger.isDebugEnabled())
    logger.debug("DELETE: " + delete_sql);
  sql.execute(delete_sql);

  // Insert non-deleted INSERT rows, i.e. rows not followed by another INSERT
  // or a DELETE.
  insert_sql = runtime.sprintf(
    "INSERT INTO %s (%s) SELECT %s FROM %s WHERE tungsten_opcode IN ('I', 'UI') AND tungsten_row_id IN (SELECT MAX(tungsten_row_id) FROM %s GROUP BY %s)", 
    base_table_fqn, 
    base_columns, 
    base_columns, 
    stage_table_fqn, 
    stage_table_fqn, 
    pkey_columns
  );
  if (logger.isDebugEnabled())
    logger.debug("INSERT: " + insert_sql);
  sql.execute(insert_sql);

  // Clean-up CSV file from S3 if desired.
  if (cleanUpS3Files)
  {
    s3DelCmd = runtime.sprintf("s3cmd del %s/%s/%s%s", awsS3Path, serviceName,
        csv_filename, csv_extension);
    if (logger.isDebugEnabled())
    {
      logger.debug(s3DelCmd);
    }
    runtime.exec(s3DelCmd);
  }
}

/** Called at commit time for a batch. */
function commit()
{
  // Commit the transaction.
  sql.commit();
}

/** Called when the applier goes offline. */
function release()
{
  // Does nothing.
}
