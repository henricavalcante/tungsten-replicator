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
 * Load script for HDFS using hadoop utility.  Tables load to corresponding
 * directories in HDFS.  
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
  logger.info("Executing hadoop connect script to create data directory");
  service_name = runtime.getContext().getServiceName();
  hadoop_base = '/user/tungsten/staging/' + service_name;
  runtime.exec('hadoop fs -mkdir -p ' + hadoop_base);
}

// Called at start of batch transaction. 
function begin()
{
  // Does nothing. 
}

// Appends data from a single table into a file within an HDFS directory. 
// If the key is present that becomes a sub-directory so that data are 
// distributed. 
function apply(csvinfo)
{
  // Collect useful data. 
  csv_file = csvinfo.file.getAbsolutePath();
  plain_csv_file = decodeURIComponent(csv_file);
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
  logger.info("Writing file: " + plain_csv_file + " to: " + hadoop_file);

  // Ensure the directory exists for the file. 
  runtime.exec('hadoop fs -mkdir -p ' + hadoop_dir);

  // Remove any previous file with this name.  That takes care of restart
  // by wiping out earlier loads of the same data. 
  runtime.exec('hadoop fs -rm -f ' + hadoop_file);

  // Load file to HDFS. 
  runtime.exec('hadoop fs -put ' + plain_csv_file + ' ' + hadoop_file);
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
