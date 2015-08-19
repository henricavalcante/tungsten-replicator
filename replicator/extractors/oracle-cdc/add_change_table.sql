--For DEBUG purpose, comment the following 3 lines and uncomment the 4th one
--set feedback off
--set echo off
--set term off
set serveroutput on
set linesize 150
set verify off

DECLARE
DEBUG_LVL number := 1; -- 1 : INFO (default) , 2 : DEBUG (more verbose)

v_user varchar2(30) := '&1';
/* 
Change CDC type as desired :
- SYNC_SOURCE : synchronous capture
- HOTLOG_SOURCE : asynchronous capture (HOTLOG)
*/
v_cdc_type varchar(30) := '&2';
v_tungsten_user varchar2(30) := '&3';
v_pub_user varchar2(100):= '&4';
v_change_set_name varchar2(30) := UPPER('&5');
i_pub_tablespace number := '&7';

b_sync boolean := (v_cdc_type = 'SYNC_SOURCE');

v_version varchar2(17);
i_version number;
v_table_name varchar2(40) := '';
v_column_name varchar2(100);
v_column_type varchar(50);
column_type_len integer; 
column_prec integer;
column_scale integer;
v_column_list varchar(32737);

err_found boolean := false;
warn_found boolean := false;

BEGIN

  SELECT version into v_version from v$instance;
  DBMS_OUTPUT.PUT_LINE ('Oracle version : ' || v_version);
  i_version := TO_NUMBER(SUBSTR(v_version, 1, INSTR(v_version, '.') -1));

  BEGIN
    SELECT table_name into v_table_name FROM ALL_TABLES where owner=v_user and table_name = '&6';
  EXCEPTION
     WHEN NO_DATA_FOUND THEN
       DBMS_OUTPUT.PUT_LINE ('Unable to find source table ' || v_user || '.' || '&6');
    RETURN;
  END;

  IF v_table_name IS NULL THEN
    DBMS_OUTPUT.PUT_LINE ('Unable to find source table ' || v_user || '.' || v_table_name);
    RETURN;
  END IF;
 
  DECLARE
    CURSOR CUR IS select distinct col.column_name, data_type, decode(char_used, 'C', char_length, data_length), data_precision, data_scale from all_tab_columns col where owner=v_user and col.table_name = v_table_name;
  BEGIN
    OPEN CUR;
    LOOP
      FETCH CUR INTO v_column_name, v_column_type, column_type_len, column_prec, column_scale;
      EXIT WHEN CUR%NOTFOUND;

      IF DEBUG_LVL > 1 THEN      
        DBMS_OUTPUT.PUT_LINE ('Found :' || v_column_type || ' / ' || column_type_len);
      END IF;
      
      IF LENGTH(v_column_list) > 0 THEN
        v_column_list := v_column_list || ', ';
      END IF;
      
      IF v_column_type = 'NUMBER' AND column_prec IS NOT NULL THEN
        v_column_list := v_column_list || v_column_name || ' ' ||v_column_type || '('||column_prec||','||column_scale || ')';
      ELSIF instr(v_column_type, 'INTERVAL')>=1 THEN
        v_column_list := v_column_list || v_column_name || ' ' ||v_column_type;
      ELSIF i_version > 10 OR instr(v_column_type, 'NCLOB') < 1 THEN
        v_column_list := v_column_list ||  v_column_name || ' ' ||v_column_type;
        IF v_column_type != 'DATE'
          AND v_column_type != 'NUMBER'
          AND instr(v_column_type, 'NCLOB') < 1
          AND instr(v_column_type, 'BLOB') < 1
          AND instr(v_column_type, 'TIMESTAMP') < 1 then
            v_column_list := v_column_list || '('||column_type_len||')';
        END IF;
      ELSE
        /* NCLOB not supported by Oracle 10G */
        DBMS_OUTPUT.PUT_LINE ('WARNING : NCLOB unsupported datatype for column ' || v_table_name || '.' || v_column_name || ' : skipping.' );
        warn_found := true;
      END IF;
    END LOOP;
    CLOSE CUR;
  END;

  /* Create the change table */
  IF LENGTH(v_column_list) > 0 THEN
    @@create_change_table.sql
  END IF;

  IF err_found or warn_found THEN
     DBMS_OUTPUT.PUT_LINE('**********************************************************************************');
     IF warn_found THEN
        DBMS_OUTPUT.PUT_LINE('* WARNING : SOME COLUMNS CANNOT BE REPLICATED.   PLEASE CHECK OUTPUT FOR ERRORS. *');
     END IF;
     IF err_found THEN
        DBMS_OUTPUT.PUT_LINE('* WARNING : SOME CHANGE TABLES WERE NOT CREATED. PLEASE CHECK OUTPUT FOR ERRORS. *');
     END IF; 
     DBMS_OUTPUT.PUT_LINE('**********************************************************************************');
  END IF;

END;
/
EXIT
