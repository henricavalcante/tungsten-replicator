--For DEBUG purpose, comment the following 3 lines and uncomment the 4th one
set feedback off
set echo off
set term off
--set serveroutput on

DECLARE

v_change_set_name varchar2(30) := UPPER('&1');
b_enabled boolean := &2;

BEGIN

  IF b_enabled THEN
    DBMS_OUTPUT.PUT_LINE('Enabling change set '||v_change_set_name);
    DBMS_CDC_PUBLISH.ALTER_CHANGE_SET(change_set_name=>'',enable_capture=>'Y');
    DBMS_OUTPUT.PUT_LINE('Change set '|| v_change_set_name ||' is now enabled);
  ELSE
    DBMS_OUTPUT.PUT_LINE('Disabling change set '||v_change_set_name);
    DBMS_CDC_PUBLISH.ALTER_CHANGE_SET(change_set_name=>'',enable_capture=>'N');
    DBMS_OUTPUT.PUT_LINE('Change set '|| v_change_set_name ||' is now disabled);  
  END IF;
  
END;
/

EXIT