/*
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
 * This simple applier will copy the temporary CSV file to a stable directory within the replicator tree
 * The purpose is to enable a generic file applier.
 * this file should be in ./tungsten-replicator/appliers/batch/
 *
 * and the installation should be invoked as
 *
./tools/tpm configure $SERVICE_NAME \
    --slaves=$slave_host \
    --master=$master_host \
    --role=slave \
    --batch-enabled=true \
    --batch-load-language=js \
    --batch-load-template=donothing \
    --datasource-type=file \
    --install-directory=$CONTINUENT_ROOT \
    --enable-heterogenous-service=true \
    --repl-svc-applier-filters=monitorschemachange \
    --property=replicator.filter.monitorschemachange.notify=true \
    --java-file-encoding=UTF8 \
    --java-user-timezone=GMT \
    --start=true

 *
 * Author : MC Brown 
 * Contribution and implementation: Giuseppe Maxia
*/

function prepare() {}
function begin() {}
function apply(csvinfo) {

  csv_file = csvinfo.file.getAbsolutePath();
  schema = csvinfo.schema;
  table = csvinfo.table;
  seqno = csvinfo.startSeqno;
  
  service_name = runtime.getContext().getServiceName();

  replicator_properties = runtime.getContext().getReplicatorProperties()
  thl_dir = replicator_properties.getString("replicator.store.thl.log_dir");
  home_dir = thl_dir + '/../../tungsten/tungsten-replicator' ;

  // The above commands can be replaced by the more cryptic, albeit simple, following command
  //
  // home_dir =  '../../../../tungsten/tungsten-replicator' ;

  // 
  // For debugging , we can send some environment info to the replicator log
  //
  //logger.info('### Found  thl_dir = <' + thl_dir +"> ### "); 
  //logger.info('### Found  home_dir = <' + home_dir +"> ### "); 
  //logger.info('### Found  = <' + replicator_properties.toJSON(true) +"> ###"); 

  // The csv_dir is where we save all csv files, split by schema
  csv_dir = home_dir + '/data/' + service_name ;

  // The destination file is inside its own schema
  // Each file name contains the current seqno
  dest_file = csv_dir + '/' + schema + '/' + table + '-' + seqno + ".csv";

  // The containing directory is created
  runtime.exec('mkdir -p ' + csv_dir + '/' + schema );
  
  // Finally, the temporary CSV is copied to its final destination 
  runtime.exec('cp ' + csv_file + ' ' + dest_file);

}
function commit() {}
function release() {}

