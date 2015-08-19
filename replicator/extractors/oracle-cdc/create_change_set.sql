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
i_pub_tablespace number := '&6';

b_sync boolean := (v_cdc_type = 'SYNC_SOURCE');

v_version varchar2(17);
i_version number;
v_table_name varchar2(40);
v_column_name varchar2(100);
v_column_type varchar(50);
column_type_len integer; 
column_prec integer;
column_scale integer;
v_column_list varchar(32737);
v_column_names varchar2(32737);


err_found boolean := false;
warn_found boolean := false;

tableCount NUMBER;

CURSOR C2 IS select distinct col.column_name, data_type, decode(char_used, 'C', char_length, data_length), data_precision, data_scale from all_tab_columns col where owner=v_user and col.table_name = v_table_name;

CURSOR C3 IS select distinct col.column_name, data_type, decode(char_used, 'C', char_length, data_length), data_precision, data_scale from all_tab_columns col where owner=v_user and col.table_name = v_table_name 
and col.column_name in (select trim(regexp_substr(v_column_names,'[^,]+', 1, level)) from dual connect by regexp_substr(v_column_names, '[^,]+', 1, level) is not null);
BEGIN

SELECT count(*) into tableCount from SYS.tungsten_load;

SELECT version into v_version from v$instance;
DBMS_OUTPUT.PUT_LINE ('Oracle version : ' || v_version);
i_version := TO_NUMBER(SUBSTR(v_version, 1, INSTR(v_version, '.') -1));

IF b_sync THEN
DBMS_OUTPUT.PUT_LINE ('Setting Up Synchronous Data Capture ' || v_change_set_name  || ' for Oracle ' || TO_CHAR(i_version)); 
DBMS_CDC_PUBLISH.CREATE_CHANGE_SET(change_set_name => v_change_set_name, description => 'Change set used by Tungsten Replicator', change_source_name =>v_cdc_type);
ELSE
DBMS_OUTPUT.PUT_LINE ('Setting Up Asynchronous Data Capture ' || v_change_set_name); 
DBMS_CDC_PUBLISH.CREATE_CHANGE_SET(change_set_name => v_change_set_name, description => 'Change set used by Tungsten Replicator', change_source_name => v_cdc_type, stop_on_ddl => 'n');
END IF;

IF tableCount > 0 THEN
   DECLARE
      CURSOR C1 IS SELECT table_name FROM ALL_TABLES where table_name in (SELECT tableName FROM SYS.tungsten_load) and owner=v_user and table_name not like 'AQ$%' and table_name not like 'CDC$%';
   BEGIN
      OPEN C1;
      LOOP
         FETCH C1 INTO v_table_name;
         EXIT WHEN C1%NOTFOUND;

         IF DEBUG_LVL > 1 THEN 
            DBMS_OUTPUT.PUT_LINE ('Processing table ' || v_user || '.' || v_table_name);
         END IF;
         v_column_list := '';
         
         select columns into v_column_names from SYS.tungsten_load where tableName = v_table_name;
         
         IF (v_column_names IS NULL) THEN
            OPEN C2;
            LOOP
               FETCH C2 into v_column_name, v_column_type, column_type_len, column_prec, column_scale;
               EXIT WHEN C2%NOTFOUND;

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
                  v_column_list := v_column_list || v_column_name || ' ' ||v_column_type;
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
            CLOSE C2;
         ELSE
            OPEN C3;
            LOOP
               FETCH C3 into v_column_name, v_column_type, column_type_len, column_prec, column_scale;
               EXIT WHEN C3%NOTFOUND;

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
                  v_column_list := v_column_list || v_column_name || ' ' ||v_column_type;
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
            CLOSE C3;
         END IF;
         
         /* Create the change table */
         IF LENGTH(v_column_list) > 0 THEN
           @@create_change_table.sql
         END IF;
      END LOOP;
      CLOSE C1;
   END;
ELSE
   DECLARE
      CURSOR C1 IS SELECT table_name FROM ALL_TABLES where owner=v_user and table_name not like 'AQ$%'and table_name not like 'CDC$%';
   BEGIN
      OPEN C1;
      LOOP
         FETCH C1 INTO v_table_name;
         EXIT WHEN C1%NOTFOUND;

         IF DEBUG_LVL > 1 THEN 
            DBMS_OUTPUT.PUT_LINE ('Processing table ' || v_user || '.' || v_table_name);
         END IF;
         v_column_list := '';

         OPEN C2;
         LOOP
            FETCH C2 INTO v_column_name, v_column_type, column_type_len, column_prec, column_scale;
            EXIT WHEN C2%NOTFOUND;

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
               v_column_list := v_column_list || v_column_name || ' ' ||v_column_type;
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
         CLOSE C2;

         /* Create the change table */
         IF LENGTH(v_column_list) > 0 THEN
           @@create_change_table.sql
         END IF;
      END LOOP;
      CLOSE C1;
   END;
END IF;

IF not b_sync THEN
   DBMS_OUTPUT.PUT_LINE ('Enabling change set : ' || v_change_set_name);
   DBMS_CDC_PUBLISH.ALTER_CHANGE_SET(change_set_name => v_change_set_name,enable_capture => 'y');
/* 
ELSE => in case of Synchronous change set, it is enabled by default (and cannot be disabled)
*/ 
END IF;

IF i_version > 10 THEN
  DECLARE
  CURSOR C1 IS select view_name from all_views where view_name like 'TUNGSTEN%' AND OWNER=v_pub_user;
  v_view_name varchar2(30);
  BEGIN

    OPEN C1;
    LOOP
      FETCH C1 INTO v_view_name;
      EXIT WHEN C1%NOTFOUND;
      DBMS_OUTPUT.PUT_LINE ('Dropping view ' || v_view_name);
      EXECUTE IMMEDIATE 'DROP VIEW ' || v_view_name;
    END LOOP;
    CLOSE C1;
  END;

   EXECUTE IMMEDIATE 'create view TUNGSTEN_SOURCE_TABLES as SELECT DISTINCT s.source_schema_name, s.source_table_name FROM sys.cdc_change_tables$ s, all_tables t WHERE s.change_table_schema=t.owner AND s.change_table_name=t.table_name';
   EXECUTE IMMEDIATE 'create view TUNGSTEN_PUBLISHED_COLUMNS as SELECT s.change_set_name, s.obj# as pub_id, s.source_schema_name, s.source_table_name, c.column_name, c.data_type, c.data_length, c.data_precision, c.data_scale, c.nullable FROM sys.cdc_change_tables$ s, all_tables t, all_tab_columns c WHERE s.change_table_schema=t.owner AND s.change_table_name    =t.table_name AND c.owner=s.change_table_schema AND c.table_name=s.change_table_name AND c.column_name NOT IN (''OPERATION$'',''CSCN$'',''DDLDESC$'',''DDLPDOBJN$'', ''DDLOPER$'',''RSID$'',''SOURCE_COLMAP$'',''TARGET_COLMAP$'', ''COMMIT_TIMESTAMP$'',''TIMESTAMP$'',''USERNAME$'',''ROW_ID$'', ''XIDUSN$'',''XIDSLT$'',''XIDSEQ$'',''SYS_NC_OID$'')';

   EXECUTE IMMEDIATE 'GRANT SELECT ON TUNGSTEN_SOURCE_TABLES TO ' || v_tungsten_user;
   EXECUTE IMMEDIATE 'GRANT SELECT ON TUNGSTEN_PUBLISHED_COLUMNS TO ' || v_tungsten_user;
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

IF tableCount > 0 THEN
   DECLARE
      CURSOR C1 IS SELECT tableName FROM SYS.tungsten_load WHERE tableName NOT IN (SELECT table_name FROM ALL_TABLES WHERE owner=v_user and table_name not like 'AQ$%' and table_name not like 'CDC$%');
   BEGIN
      OPEN C1;
      LOOP
         FETCH C1 INTO v_table_name;
         EXIT WHEN C1%NOTFOUND;

         DBMS_OUTPUT.PUT_LINE ('ERROR: TABLE NOT FOUND: ' || v_table_name);
      END LOOP;
      CLOSE C1;
   END;
END IF;

END;
/
EXIT
