--For DEBUG purpose, comment the following 3 lines and uncomment the 4th one
set feedback off
set echo off
set term off
--set serveroutput on

DECLARE
v_user_pub varchar2(30) := '&1';
v_version varchar2(17);
i_version number;

BEGIN

SELECT version into v_version from v$instance;
i_version := TO_NUMBER(SUBSTR(v_version, 1, INSTR(v_version, '.') -1));

IF i_version > 10 THEN
   EXECUTE IMMEDIATE 'create synonym TUNGSTEN_SOURCE_TABLES for '|| v_user_pub ||'.TUNGSTEN_SOURCE_TABLES';
   EXECUTE IMMEDIATE 'create synonym TUNGSTEN_PUBLISHED_COLUMNS for '|| v_user_pub ||'.TUNGSTEN_PUBLISHED_COLUMNS';
END IF;

END;
/
EXIT
