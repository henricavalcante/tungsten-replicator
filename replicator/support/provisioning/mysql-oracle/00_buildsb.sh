#!/bin/bash
. ./env.sh


if [ ! -d $HOME/opt/mysql/$MYSQL_VERSION ]
then
    echo "MySQL repository not found in $HOME/opt/mysql/$MYSQL_VERSION"
    exit 1
fi

MSB=$(which make_sandbox)
if [ "$?" != "0" ]
then
    echo "make_sandbox not found - Please install MySQL Sandbox"
    exit 1
fi


SANDBOX_OPTIONS="--no_confirm  --no_show -c server-id=10 -c log-bin=mysql-bin -c log-slave-updates "
export SANDBOX_OPTIONS="$SANDBOX_OPTIONS -c max_allowed_packet=48M --remote_access=%"
export SANDBOX_OPTIONS="$SANDBOX_OPTIONS -c innodb-flush-method=O_DIRECT -c innodb-log-file-size=50M -c innodb_buffer_pool_size=5G "
export SANDBOX_OPTIONS="$SANDBOX_OPTIONS -c innodb_buffer_pool_size=1G -c max_allowed_packet=48M -c max-connections=350 "
export SANDBOX_OPTIONS="$SANDBOX_OPTIONS -c innodb-additional-mem-pool-size=50M -c innodb-log-buffer-size=50M -c sync_binlog=0 -c innodb-thread-concurrency=0"

if [ -d $PROVISIONING_SANDBOX ]
then
    echo "removing the sandbox ..."
    du -sh $PROVISIONING_SANDBOX
    sbtool -o delete -s $PROVISIONING_SANDBOX
fi

make_sandbox $MYSQL_VERSION -- --sandbox_port=$MYSQL_PORT \
   --sandbox_directory=$SB_DIR  $SANDBOX_OPTIONS

$PROVISIONING_SANDBOX/use -u root -e "grant all on *.* to $DB_USER identified by '$DB_PASSWORD' with grant option"
$PROVISIONING_SANDBOX/use -u root -e 'set global binlog_format=ROW'
