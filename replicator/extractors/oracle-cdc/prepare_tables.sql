--For DEBUG purpose, comment the following 3 lines and uncomment the 4th one
set feedback off
set echo off
set term off
--set serveroutput on

DECLARE
v_version varchar2(17);
i_version number;
tableCount NUMBER;

v_user varchar2(30) := '&1';
v_tungsten_user varchar2(30) := '&2';

v_cdc_type varchar(30) := '&3';
v_sync boolean := (v_cdc_type = 'SYNC_SOURCE');

v_table_name varchar2(30) := '&4';

BEGIN

SELECT count(*) into tableCount from tungsten_load;

SELECT version into v_version from v$instance;
DBMS_OUTPUT.PUT_LINE ('Oracle version : ' || v_version || '/'|| TO_CHAR(INSTR( v_version, '.')));
i_version := TO_NUMBER(SUBSTR(v_version, 1, INSTR(v_version, '.') -1));

IF v_table_name IS NOT NULL THEN
  -- A table name was provided. Take care only of this table.
  DBMS_OUTPUT.PUT_LINE ('Processing table ' || v_user || '.' || v_table_name);
  
  -- Test if the table exists ?
  
  -- And prepare table for instantiation
  IF not v_sync THEN
    DBMS_OUTPUT.PUT_LINE ('Adding supplemental log data');
    BEGIN
      EXECUTE IMMEDIATE  'ALTER TABLE ' || v_user || '.' || v_table_name || ' DROP SUPPLEMENTAL LOG DATA (ALL) COLUMNS';
    EXCEPTION WHEN OTHERS THEN
      NULL;
    END;
    EXECUTE IMMEDIATE  'ALTER TABLE ' || v_user || '.' || v_table_name || ' ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS';

    DBMS_OUTPUT.PUT_LINE ('Preparing table instanciation');
    DBMS_CAPTURE_ADM.PREPARE_TABLE_INSTANTIATION(TABLE_NAME => v_user || '.' || v_table_name );
  END IF;
  EXECUTE IMMEDIATE 'GRANT SELECT ON "'|| v_user || '"."' || v_table_name ||'" TO '||v_tungsten_user;

ELSIF tableCount > 0 THEN
   DECLARE
      CURSOR C IS SELECT table_name FROM ALL_TABLES where owner=v_user AND table_name in (SELECT tableName FROM SYS.tungsten_load);
   BEGIN
      OPEN C;
      LOOP
         FETCH C INTO v_table_name;
         EXIT WHEN C%NOTFOUND;
   
         DBMS_OUTPUT.PUT_LINE ('Processing table ' || v_user || '.' || v_table_name);


         IF not v_sync THEN
            DBMS_OUTPUT.PUT_LINE ('Adding supplemental log data');
            BEGIN
               EXECUTE IMMEDIATE  'ALTER TABLE ' || v_user || '.' || v_table_name || ' DROP SUPPLEMENTAL LOG DATA (ALL) COLUMNS';
            EXCEPTION WHEN OTHERS THEN
               NULL;
            END;
            EXECUTE IMMEDIATE  'ALTER TABLE ' || v_user || '.' || v_table_name || ' ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS';

            DBMS_OUTPUT.PUT_LINE ('Preparing table instanciation');
            DBMS_CAPTURE_ADM.PREPARE_TABLE_INSTANTIATION(TABLE_NAME => v_user || '.' || v_table_name );
         END IF;
         
   
         EXECUTE IMMEDIATE 'GRANT SELECT ON "'|| v_user || '"."' || v_table_name ||'" TO '||v_tungsten_user;

      END LOOP;
      CLOSE C;
   END;
ELSE
   DECLARE
      CURSOR C IS SELECT table_name FROM ALL_TABLES where owner=v_user;
   BEGIN
      OPEN C;
      LOOP
         FETCH C INTO v_table_name;
         EXIT WHEN C%NOTFOUND;

         DBMS_OUTPUT.PUT_LINE ('Processing table ' || v_user || '.' || v_table_name);

         IF not v_sync THEN
            DBMS_OUTPUT.PUT_LINE ('Adding supplemental log data');
            BEGIN
               EXECUTE IMMEDIATE  'ALTER TABLE ' || v_user || '.' || v_table_name || ' DROP SUPPLEMENTAL LOG DATA (ALL) COLUMNS';
            EXCEPTION WHEN OTHERS THEN
               NULL;
            END;
            EXECUTE IMMEDIATE  'ALTER TABLE ' || v_user || '.' || v_table_name || ' ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS';

            DBMS_OUTPUT.PUT_LINE ('Preparing table instanciation');
            DBMS_CAPTURE_ADM.PREPARE_TABLE_INSTANTIATION(TABLE_NAME => v_user || '.' || v_table_name );
         END IF;
         EXECUTE IMMEDIATE 'GRANT SELECT ON "'|| v_user || '"."' || v_table_name ||'" TO '||v_tungsten_user;
      END LOOP;
      CLOSE C;
   END;
END IF;

END;
/
EXIT
