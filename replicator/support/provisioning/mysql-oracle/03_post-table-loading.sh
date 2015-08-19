#!/bin/bash

#
# This script stops replication from the sandbox and starts replicating live
# from the main server
#

. ./env.sh
## NOTICE: we are overwriting some values from env.sh
MYSQL_PORT=3306
MY_CNF=/etc/my.cnf
BINLOG_DIR=/var/lib/mysql

$REPLICATOR stop
$SANDBOX_PATH/stop

$TUNGSTEN_DEPLOY/tungsten/tools/configure-service -U \
    -a \
    --verbose \
    --datasource-user=tungsten \
    --datasource-password=ac3cr3t \
    --datasource-port=$MYSQL_PORT \
    --datasource-mysql-conf=$MY_CNF \
    --datasource-log-directory=$BINLOG_DIR \
    --svc-extractor-filters=replicate,colnames \
    --skip-validation-check=MySQLNoMySQLReplicationCheck \
    --mysql-enable-settostring=true \
    --mysql-enable-enumtostring=true \
    --property=replicator.filter.replicate.do="$SCHEMAS_TO_REPLICATE_FILTER" \
    --property=replicator.extractor.dbms.useRelayLogs=false \
    --property=replicator.store.thl.log_file_retention=25d \
    --auto-enable=false \
    --svc-start $SERVICE_NAME

$TUNGSTEN_DEPLOY/tungsten/tungsten-replicator/bin/trepctl services


$TREPCTL online -from-event mysql-bin.000001:107
$TREPCTL services


