--For DEBUG purpose, comment the following 3 lines and uncomment the 4th one
set feedback off
set echo off
set term off
--set serveroutput on

DECLARE

v_user varchar2(30);
v_change_set_name varchar2(30) := UPPER('&2');
v_table_name varchar2(30) := UPPER('&3');
v_change_table_name varchar2(30);
v_existing_change_table varchar2(30) := '';


BEGIN
  v_change_table_name := 'CT_' || SUBSTR(v_table_name, 1, 27);
  BEGIN
    SELECT TRIM(USERNAME) into v_user from ALL_USERS where USERNAME=UPPER('&1');
  EXCEPTION WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('Exception while checking if user &1 exists.');
    RETURN;
  END;

  IF v_user IS NULL THEN
    -- User not found
    -- Table cannot be dropped
    DBMS_OUTPUT.PUT_LINE('User not found (&1). Nothing to do.'); 
    RETURN;
  END IF;

  SELECT CHANGE_TABLE_NAME INTO v_existing_change_table 
  FROM ALL_CHANGE_TABLES 
  WHERE CHANGE_TABLE_NAME = v_change_table_name
    AND CHANGE_TABLE_SCHEMA=UPPER('&1');
  
  IF v_existing_change_table IS NOT NULL THEN
    DBMS_CDC_PUBLISH.DROP_CHANGE_TABLE(v_user,v_change_table_name,'Y');
    DBMS_OUTPUT.PUT_LINE('Change table was removed : '||v_change_table_name);
  END IF;

END;
/

EXIT