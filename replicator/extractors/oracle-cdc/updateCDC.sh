#!/bin/bash

CNF_FILE="setupCDC.conf"

[ ! -z "${1}" ] && CNF_FILE=${1} && echo "Using configuration ${CNF_FILE}"

[ ! -f "${CNF_FILE}" ] && echo "ERROR: Configuration file '${CNF_FILE}' was not found" && exit 1
. ${CNF_FILE}

# Issue 1014 - normalizing CDC type names for setupCDC and tpm
if [ "$cdc_type" = "CDCASYNC" ]
then
   cdc_type="HOTLOG_SOURCE"
elif [ "$cdc_type" = "CDCSYNC" ]
then
   cdc_type="SYNC_SOURCE"
fi

DEFAULT_CHANGE_SET="TUNGSTEN_CHANGE_SET"
CHANGE_SET=${DEFAULT_CHANGE_SET}
[ ! -z "${service}" ] && CHANGE_SET="TUNGSTEN_CS_${service}"

if [ -n "${sys_user}" ]
then
   if [ -n "${sys_pass}" ]
   then
      syspass=$sys_pass
   else
      read -s -p "Enter password for $sys_user, if any :" syspass;
   fi
else
   syspass=
fi
SYSDBA="$sys_user/$syspass AS SYSDBA"

oracle_version="`sqlplus -S ${SYSDBA} @get_oracle_version`"

[ -z "${2}" ] && echo "ERROR: Missing paramater for table name" && exit 1

TABLE_NAME=${2}
echo "Updating configuration for table ${TABLE_NAME} in Change Set '${CHANGE_SET}'"

if [ $specific_tables -eq 1 ]
then
   if [ -n "${specific_path}" ]
   then
      specificpath="${specific_path}"
   else
      specificpath="`pwd`"
   fi

   if [ ! -r "$specificpath/tungsten.tables" ]
   then
      echo "File tungsten.tables cannot be read in $specificpath"
      exit 1
   fi
else
   specificpath="`pwd`"
fi

echo "Setup tungsten_load (SYSDBA)"
sqlplus -S -L ${SYSDBA} @create_tungsten_load.sql $specificpath $pub_user $specific_tables
RC=$?
[ ${RC} -ne 0 ] && echo "ERROR: [$RC] sqlplus statement failed" && exit 1
echo "Done."

[ -z "$pub_tablespace" ] && pub_tablespace=1
[ $pub_tablespace -eq 0 ] && echo "WARNING: using default system tablespace (pub_tablespace=0), which is not recommended for production-like deployments"

# 1) disable change set if needed (if async change set)
# 2) if change table already exists-> drop change table ?
# 3) prepare table instanciation
# 4) create change table
# 5) enable change set if needed (if async change set)

# 1) Disable change set if needed
if [ "$cdc_type" = "HOTLOG_SOURCE" ]
then
  sqlplus -S -L $pub_user/$pub_password @enable_disable_change_set.sql ${CHANGE_SET} false
fi

# 2) if change table already exists-> drop change table ?
if [ $oracle_version -ge 11 ]
then
  sqlplus -S -L $pub_user/$pub_password @drop_change_table.sql  $pub_user ${CHANGE_SET} ${TABLE_NAME}
else
  sqlplus -S -L $pub_user/$pub_password @drop_change_table-10.sql  $pub_user ${CHANGE_SET} ${TABLE_NAME}
fi

# 3) prepare table instanciation
sqlplus -S -L ${SYSDBA} @prepare_tables.sql $source_user $tungsten_user $cdc_type ${TABLE_NAME}

# 4) create change table
sqlplus -S -L $pub_user/$pub_password @add_change_table.sql $source_user $cdc_type $tungsten_user $pub_user ${CHANGE_SET} ${TABLE_NAME} $pub_tablespace

# 5) enable change set if needed (if async change set)
if [ "$cdc_type" = "HOTLOG_SOURCE" ]
then
  sqlplus -S -L $pub_user/$pub_password @enable_disable_change_set.sql ${CHANGE_SET} true
fi