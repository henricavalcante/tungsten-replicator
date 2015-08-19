#!/bin/bash
# 
# Reset local replicator. 
#

# Set variables. 
cd `dirname $0`
. variables.sh

dropCreateDatabase() {
  local db=$1
  echo "# Recreating database: $db"
  sql="drop database if exists $db; create database $db;"
  echo "$sql" | mysql -u$TUNGSTEN_LOGIN -p$TUNGSTEN_PASSWORD
}

# Stop replicator. 
echo "### Stopping replicator"
$trep_stop

# Clear logs. 
echo "### Clearing log files"
rm $REPLICATOR_HOME/log/*

# Remove dynamic properties. 
echo "### Clearing dynamic properties"
rm $REPLICATOR_HOME/conf/dynamic-*.properties

# Clear catalogs and sample DB.  We deduce catalogs from their
# corresponding property files. 
echo "### Cleaning tungsten catalog and sample databases"
for svcprops in $TUNGSTEN_HOME/conf/static-*.properties
do
  svc=`basename "$svcprops" | sed -e 's/static\-//' -e 's/\.properties//'` 
  dropCreateDatabase tungsten_${svc}
done
dropCreateDatabase sample

echo "Done!"
