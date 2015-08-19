--For DEBUG purpose, comment the following 3 lines and uncomment the 4th one
set feedback off
set echo off
set term off
--set serveroutput on

DECLARE

v_user varchar2(30);
v_change_set_name varchar2(30) := UPPER('&2');
v_existing_change_set varchar2(30) := '';

v_table_name varchar2(30);
CURSOR C1 IS SELECT CHANGE_TABLE_NAME FROM ALL_CHANGE_TABLES where CHANGE_SET_NAME=v_change_set_name;

BEGIN

  BEGIN
    SELECT TRIM(USERNAME) into v_user from ALL_USERS where USERNAME=UPPER('&1');
  EXCEPTION WHEN OTHERS THEN
    RETURN;
  END;

  IF v_user IS NULL THEN
     -- User not found
     RETURN;
  END IF;

  SELECT SET_NAME into v_existing_change_set from ALL_CHANGE_SETS where SET_NAME=v_change_set_name;

  IF v_existing_change_set IS NOT NULL THEN
    OPEN C1;
    LOOP
      FETCH C1 INTO v_table_name;
      EXIT WHEN C1%NOTFOUND;

      DBMS_CDC_PUBLISH.DROP_CHANGE_TABLE(v_user,v_table_name,'Y');
    END LOOP;
    CLOSE C1;

    DBMS_OUTPUT.PUT_LINE('Removing existing change set '||v_change_set_name);
    DBMS_CDC_PUBLISH.DROP_change_set(v_change_set_name);
  END IF;

END;
/

EXIT