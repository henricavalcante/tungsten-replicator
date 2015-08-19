#!/bin/bash
# Script to refresh MySQL table by selecting into temp file and reloading. 
# The table will then replicate fully out to slaves, thereby making them
# fully consistent with the master copy. 
#
# To refresh a table, issue a command like the following: 
#
#   [sudo] ./reload-table.sh -utungsten -psecret -t mkt_test.test_foo 
# 
# You must run as root or you will not be able to clean up the output file
# used to dump and reload data. 

# Prints usage. 
usage() {
  echo "Usage: $0 -u user -p password -h host -P port -t tablename"
}

# Parse options. 
USER=`whoami`
PASSWORD=
TABLE=
HOST=`hostname`
PORT=3306

while getopts 'h:p:P:t:u:' OPTION
do
  case $OPTION in
    h) HOST=$OPTARG;;
    p) PASSWORD=$OPTARG;;
    P) PORT=$OPTARG;;
    t) TABLE=$OPTARG;;
    u) USER=$OPTARG;;
    *) usage; exit 2;;
  esac
done

if [ -z $TABLE ]; then
  usage
  exit 1
fi

echo "Reloading table..."
echo "HOST=$HOST";
echo "PORT=$PORT";
echo "USER=$USER";
echo "PASSWORD=$PASSWORD";
echo "TABLE=$TABLE";

# Remove dump file if still present. 
dumpfile=/tmp/${TABLE}.dmp
if [ -e $dumpfile ]; then
  rm $dumpfile
fi

# Execute MySQL commands for SELECT ... INTO and LOAD DATA.  
set -x
mysql -v --skip-column-names -u$USER -p$PASSWORD -h$HOST -P$PORT << EOF
BEGIN;
SELECT * FROM $TABLE INTO OUTFILE '$dumpfile' FOR UPDATE;
DELETE FROM $TABLE; 
LOAD DATA INFILE '$dumpfile' REPLACE
  INTO TABLE $TABLE FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';
COMMIT;
EOF

# Remove dumpfile. 
rm $dumpfile

echo "Done!"
