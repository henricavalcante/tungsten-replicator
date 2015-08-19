set feedback off
set echo off
set term on
set serveroutput on

DECLARE
 i_version number;
v_version varchar(50);
v_table_name varchar2(30);

BEGIN
SELECT version into v_version from v$instance;
i_version := TO_NUMBER(SUBSTR(v_version, 1, INSTR(v_version, '.') -1));
DBMS_OUTPUT.PUT_LINE(i_version);
END;
/
EXIT