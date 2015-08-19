#!/bin/bash

#
# This scripts loads data into the sandbox , to start provisioning the slave.
# It gets the schema for each database we need to replicate.
# Then it gets the data and saves it to a compressed file.
# Once all the data is collected, it is loaded to the sandbox
# 
# The task is done when all the data from the last database is loaded.
# To check that the task is over, look for a table named 'task_finished' in the 'test' database.
# That is the sign that the load has completed

. ./env.sh
set -x
MYSQL="mysql -u$DB_USER -P3306 -h$MASTER_HOST -p$DB_PASSWORD"
MYSQLADMIN="mysqladmin -u$DB_USER -P3306 -h$MASTER_HOST -p$SB_PASSWORD"
MYSQLDUMP="mysqldump -u$DB_USER -P3306 -h$MASTER_HOST -p$DB_PASSWORD"
SANDBOX="mysql -h 127.0.0.1 -P $MYSQL_PORT -u msandbox -pmsandbox"
SCHEMAS=$SCHEMAS_TO_REPLICATE
MYSQL_LOG_POSITIONS=$HOME/install/mysql_starting_point.txt
SQL_STORAGE=/mnt/data/tungsten/dump

$MYSQL -e 'stop slave io_thread'

date  > $MYSQL_LOG_POSITIONS
echo "show slave status"           >> $MYSQL_LOG_POSITIONS
$MYSQL -e 'show slave status\G'    >> $MYSQL_LOG_POSITIONS

echo "show master status"          >> $MYSQL_LOG_POSITIONS
$MYSQL -e 'show master status\G'   >> $MYSQL_LOG_POSITIONS

echo "making sure the slave has caught up"
MASTER_FILE=$($MYSQL -e 'show slave status\G' |grep -w Master_Log_File | awk '{print $2}')
MASTER_POS=$($MYSQL -e 'show slave status\G' |grep -w Read_Master_Log_Pos | awk '{print $2}')
$MYSQL -e "SELECT MASTER_POS_WAIT('$MASTER_FILE', $MASTER_POS )"

$MYSQL -e 'stop slave'

echo "show slave status"           >> $MYSQL_LOG_POSITIONS
$MYSQL -e 'show slave status\G'    >> $MYSQL_LOG_POSITIONS

echo "show master status"          >> $MYSQL_LOG_POSITIONS
$MYSQL -e 'show master status\G'   >> $MYSQL_LOG_POSITIONS


for SCHEMA in $SCHEMAS
do
    echo "dumping schema $SCHEMA"
    $SANDBOX -e "drop schema if exists $SCHEMA"
    $SANDBOX -e "create schema $SCHEMA"

    $MYSQLDUMP --no-data --skip-triggers $SCHEMA | perl -pe 's/(myisam|innodb)/Blackhole/i' > ${SCHEMA}_schema.sql
    $SANDBOX $SCHEMA < ${SCHEMA}_schema.sql
    if [ "$?" != "0" ] ; then exit; fi
    $MYSQLDUMP --single-transaction --skip-triggers --no-create-info $SCHEMA |gzip -c > $SQL_STORAGE/${SCHEMA}_dump.sql.gz
    ls -lh $SQL_STORAGE
    if [ "$?" != "0" ] ; then exit; fi
done

$MYSQL -e "drop schema if exists tungsten_$SERVICE_NAME"
$MYSQL -e 'reset master'

echo "show master status after reset"  >> $MYSQL_LOG_POSITIONS
$MYSQL -e 'show master status\G'       >> $MYSQL_LOG_POSITIONS
$MYSQL -e 'set global binlog_format=row'

$MYSQL -e 'start slave'

for SCHEMA in $SCHEMAS
do
    echo "loading schema $SCHEMA"
    gunzip -c $SQL_STORAGE/${SCHEMA}_dump.sql.gz | $SANDBOX $SCHEMA
    if [ "$?" != "0" ] ; then exit; fi
done

$SANDBOX -e 'set sql_log_bin=0;create table test.task_finished(i int)'

