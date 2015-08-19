--For DEBUG purpose, comment the following 3 lines and uncomment the 4th one
--set feedback off
--set echo off
--set term off
set serveroutput on

SHUTDOWN IMMEDIATE;
STARTUP MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;

EXIT
