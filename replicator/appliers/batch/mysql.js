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
 * Load script for MySQL.  
 */

// Called once when applier goes online. 
function prepare()
{
  logger.info("Preparing load script for MySQL");
}

// Called at start of batch transaction. 
function begin()
{
  // Does nothing. 
}

// Called once for each table that must be loaded. 
function apply(csvinfo)
{
  // Collect useful data. 
  sqlParams = csvinfo.getSqlParameters();
  csv_file = sqlParams.get("%%CSV_FILE%%");

  // Load CSV to staging table.  This script *must* run on the server.  Tungsten
  // uses drizzle JDBC which does not handle LOAD DATA LOCAL INFILE properly.
  load_data_template = 
    "LOAD DATA INFILE '%%CSV_FILE%%' INTO TABLE %%STAGE_TABLE_FQN%% " 
    + "CHARACTER SET utf8 FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'"
  load_data = runtime.parameterize(load_data_template, sqlParams);
  logger.info(load_data);
  rows = sql.execute(load_data);

  // Delete rows.  This query applies all deletes that match, need it or not.
  // The inner join syntax used avoids an expensive scan of the base table 
  // by putting it second in the join order. 
  delete_sql_template = "DELETE %%BASE_TABLE%% "
    + " FROM %%STAGE_TABLE_FQN%% s "
    + " INNER JOIN %%BASE_TABLE%% "
    + " ON s.%%PKEY%% = %%BASE_TABLE%%.%%PKEY%% AND s.tungsten_opcode = 'D'"
  delete_sql = runtime.parameterize(delete_sql_template, sqlParams);
  logger.info(delete_sql);
  rows = sql.execute(delete_sql);

  // Insert rows.  This query loads each inserted row provided that the
  // insert is (a) the last insert processed and (b) is not followed by a
  // delete.  The subquery could probably be optimized to a join. 
  replace_template = "REPLACE INTO %%BASE_TABLE%%(%%BASE_COLUMNS%%) "
    + "SELECT %%BASE_COLUMNS%% FROM %%STAGE_TABLE_FQN%% AS stage_a "
    + "WHERE tungsten_opcode='I' AND tungsten_row_id IN "
    + "(SELECT MAX(tungsten_row_id) FROM %%STAGE_TABLE_FQN%% GROUP BY %%PKEY%%)"
  replace = runtime.parameterize(replace_template, sqlParams);
  logger.info(replace);
  rows = sql.execute(replace);
}

// Called at commit time for a batch. 
function commit()
{
  // Does nothing. 
}

// Called when the applier goes offline. 
function release()
{
  // Does nothing. 
}
