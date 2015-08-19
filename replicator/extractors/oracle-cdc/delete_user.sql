set termout off
set feedback off
set echo off
whenever sqlerror exit SQL.SQLCODE;
drop user &1 cascade;
exit;
