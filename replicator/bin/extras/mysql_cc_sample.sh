#!/bin/sh
#
# This is a sample consistency check script for use with MySQL Tungsten Cluster.
#
# The user has to set CHECK_... and MYSQL_... variables
#
# The following example assumes the table to check was created by
# "CREATE TABLE `cc` (`id` int(11) NOT NULL auto_increment, `value` int(11) NOT NULL, PRIMARY KEY  (`id`)) ENGINE=InnoDB;"
# and the rows with id between 0 and 100 must be checked.
# Note that CHECK_COLUMNS and CHECK_WHERE serve as parts of SQL statement
# and should follow SQL syntax.
#
CHECK_DB="test"
CHECK_TBL="cc"
CHECK_ID="1"
CHECK_COLUMNS="id, value"
CHECK_WHERE="WHERE id > 0 AND id < 100"

MYSQL_USER="root"
MYSQL_PSWD="rootpass"

#=============================================================================

TUNGSTEN_DB="tungsten"
TUNGSTEN_TBL="$TUNGSTEN_DB.consistency"

WHERE="WHERE db = '$CHECK_DB' AND tbl = '$CHECK_TBL' AND id = $CHECK_ID"

REPLACE="SET @crc:='', @cnt:=0; "
REPLACE="$REPLACE DELETE FROM $TUNGSTEN_TBL $WHERE; "
REPLACE="$REPLACE INSERT INTO $TUNGSTEN_TBL (id, db, tbl, this_cnt, this_crc) SELECT $CHECK_ID, '$CHECK_DB', '$CHECK_TBL', COUNT(*) AS cnt, RIGHT(MAX(@crc := CONCAT(LPAD(@cnt := @cnt + 1, 16, '0'), MD5(CONCAT(@crc, MD5(CONCAT_WS($CHECK_COLUMNS)))))), 32) AS crc FROM $CHECK_DB.$CHECK_TBL $CHECK_WHERE LOCK IN SHARE MODE; "

SELECT="SET @crc:='', @cnt:=0; SELECT this_cnt, this_crc INTO @cnt, @crc FROM $TUNGSTEN_TBL $WHERE; "

REPLACE_QUOTED=$(echo $REPLACE | sed s/\'/\'\'/g) # escapes quotes for the following UPDATE

UPDATE="UPDATE $TUNGSTEN_TBL SET master_cnt = @cnt, master_crc = @crc, command = '$REPLACE_QUOTED' $WHERE; "

CHECK_TRX="START TRANSACTION; "
CHECK_TRX="$CHECK_TRX $REPLACE"
CHECK_TRX="$CHECK_TRX $SELECT"
CHECK_TRX="$CHECK_TRX $UPDATE"
CHECK_TRX="$CHECK_TRX COMMIT;"

mysql -u$MYSQL_USER -p$MYSQL_PSWD -B -e "$CHECK_TRX" $CHECK_DB

#=============================================================================
