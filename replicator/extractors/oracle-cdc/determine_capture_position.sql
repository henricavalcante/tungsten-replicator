set termout on
set feedback off
set echo off
set head off
set verify off
set serveroutput on

DECLARE
v_change_set_name varchar2(30) := UPPER('&1');
position varchar2(500);
BEGIN
select 'Capture started at position '||first_scn into position from all_capture, change_sets where all_capture.CAPTURE_NAME = change_sets.CAPTURE_NAME and change_sets.SET_NAME= v_change_set_name;
DBMS_OUTPUT.PUT_LINE(position);
END;
/
exit
