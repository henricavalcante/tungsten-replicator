#!/bin/bash
set -x
. ./env.sh

if [ ! -f $HOME/downloads/$TUNGSTEN_PACKAGE.tar.gz ]
then
    echo "package $TUNGSTEN_PACKAGE not found in $HOME/downloads"
    exit 1
fi

if [ -x $REPLICATOR ]
then
    echo "Uninstalling replicator."
    echo "Since there are many THL files, this may take quite a long time."
    echo "Please be patient."
    $REPLICATOR stop
    rm -rf $TUNGSTEN_DEPLOY/*
    rm -rf $THL_DIR/*
fi

cd $HOME/install
tar -xzf $HOME/downloads/$TUNGSTEN_PACKAGE.tar.gz
cd $TUNGSTEN_PACKAGE

$SANDBOX_PATH/clear
$SANDBOX_PATH/start
$SANDBOX_PATH/use -e 'set global binlog_format=row'

./tools/tungsten-installer \
--master-slave \
--master-host=$MASTER_HOST \
--datasource-user=$DB_USER \
--datasource-password=$DB_PASSWORD \
--datasource-port=$MYSQL_PORT \
--service-name=$SERVICE_NAME \
--home-directory=$TUNGSTEN_DEPLOY \
--cluster-hosts=$MASTER_HOST \
--datasource-mysql-conf=$MY_CNF \
--datasource-log-directory=$BINLOG_DIR \
--rmi-port=$RMI_PORT \
--thl-port=$THL_PORT -a \
--disable-relay-logs \
--thl-directory=$THL_DIR \
--mysql-enable-enumtostring=true \
--mysql-enable-settostring=true \
--mysql-use-bytes-for-string=false \
--java-mem-size=2048 \
--java-file-encoding=$CHARACTER_SET \
--skip-validation-check=MySQLNoMySQLReplicationCheck \
--svc-extractor-filters=replicate,colnames \
--property=replicator.store.thl.log_file_retention=15d \
--property=replicator.filter.replicate.do="$SCHEMAS_TO_REPLICATE_FILTER" \
--start-and-report
