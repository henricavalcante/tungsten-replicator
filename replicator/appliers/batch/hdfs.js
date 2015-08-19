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
 * Load script for Hadoop data that uses direct connection to HDFS.  Note 
 * that this script requires Hadoop JAR files to be in the replicator 
 * path and is not tested for all distributions.  To avoid problems with 
 * JAR dependencies use the hadoop.js script. 
 *
 * This script handles data loading from multiple replication services by
 * ensuring that each script includes the replication service name in the
 * HDFS directory.  The target directory format is the following:
 *
 *   /user/tungsten/staging/<service name>
 */

// Called once when applier goes online. 
function prepare()
{
  // Ensure target directory exists.  This must contain the service name.
  logger.info("Ensuring data directory is created");
  service_name = runtime.getContext().getServiceName();
  hadoop_base = '/user/tungsten/staging/' + service_name;
  hdfs.mkdir(hadoop_base, true);
}

// Called at start of batch transaction. 
function begin()
{
  // Does nothing. 
}

// Loads CSV file to HDFS.  If the key is present that becomes a sub-directory
// so that data are distributed. 
function apply(csvinfo)
{
  // Assemble the parts of the file. 
  csv_file = csvinfo.file.getAbsolutePath();
  schema = csvinfo.schema;
  table = csvinfo.table;
  seqno = csvinfo.startSeqno;
  key = csvinfo.key;
  if (key == "") {
    hadoop_dir = hadoop_base + '/' + schema + "/" + table;
  }
  else {
    hadoop_dir = hadoop_base + '/' + schema + "/" + table + "/" + key
  }
  hadoop_file = hadoop_dir + '/' + table + '-' + seqno + ".csv";
  logger.info("Writing file: " + csv_file + " to: " + hadoop_file);

  // Ensure the directory exists. 
  hdfs.mkdir(hadoop_dir, true);

  // Copy the file into HDFS. 
  hdfs.put(csv_file, hadoop_file);
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
