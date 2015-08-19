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
 * Sample load script for file data sources.  It stores data in a single 
 * file per table, with changes appended to each file in succeeding batches. 
 */

// Called once when applier goes online. 
function prepare()
{
  logger.info("Setting up directory for loading data.");
  file_base = "/home/tungsten/staging";
  runtime.exec("mkdir -p " + base);
}

// Called at start of batch transaction. 
function begin()
{
  // Does nothing. 
}

// Appends CSV data from a single table to a file. 
function apply(csvinfo)
{
  // Collect useful data. 
  csv_file = csvinfo.file.getAbsolutePath();
  schema = csvinfo.schema;
  table = csvinfo.table;
  seqno = csvinfo.startSeqno;
  file_dir = file_base + '/' + schema + "/" + table;
  file_file = file_dir + '/' + table + '-' + seqno + ".csv";
  logger.info("Writing file: " + csv_file + " to: " + file_file);

  // Ensure the directory exists for the table. 
  runtime.exec('mkdir -p ' + file_dir);

  // Append data to file. 
  runtime.exec('echo "' + csv_file + '" >> ' + file_file);
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
