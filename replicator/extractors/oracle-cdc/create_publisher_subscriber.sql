--set feedback off
--set echo off
--set term off
set verify off
set serveroutput on

DECLARE
--For DEBUG purpose, set debug to true
debug boolean := false;

v_version varchar2(17);
i_version number;
tableCount NUMBER;

v_user varchar2(30) := '&1';
v_pub_user varchar2(30) := '&2';
v_password varchar2(30) := '&3'; 
v_tmp_user varchar2(30) :='';

v_tungsten_user varchar2(30) := '&4';
v_tungsten_pwd varchar2(30) := '&5';

v_cdc_type varchar(30) := '&6';
v_sync boolean := (v_cdc_type = 'SYNC_SOURCE');

i_pub_tablespace number := '&7';
v_tablespace varchar2(50) := ' DEFAULT TABLESPACE '||v_pub_user;

v_table_name varchar2(30);

BEGIN

SELECT count(*) into tableCount from tungsten_load;

SELECT version into v_version from v$instance;
IF debug THEN DBMS_OUTPUT.PUT_LINE ('Oracle version : ' || v_version || '/'|| TO_CHAR(INSTR( v_version, '.'))); END IF;
i_version := TO_NUMBER(SUBSTR(v_version, 1, INSTR(v_version, '.') -1));

BEGIN
SELECT TRIM(USERNAME) into v_tmp_user from ALL_USERS where USERNAME=v_pub_user;
EXCEPTION WHEN OTHERS THEN
   DBMS_OUTPUT.PUT_LINE ('Unable to find user ' || v_pub_user);
END;

IF i_pub_tablespace = 0 THEN
   v_tablespace := '';
END IF;

IF v_tmp_user IS NULL THEN
   DBMS_OUTPUT.PUT_LINE ('Creating user ' || v_pub_user);
   -- User not found : create it
   EXECUTE IMMEDIATE 'CREATE USER '||v_pub_user||' IDENTIFIED BY '||v_password||' '||v_tablespace||' QUOTA UNLIMITED ON SYSTEM QUOTA UNLIMITED ON SYSAUX';
   EXECUTE IMMEDIATE 'GRANT CREATE SESSION TO ' || v_pub_user;
   EXECUTE IMMEDIATE 'GRANT CREATE TABLE TO ' || v_pub_user;
   EXECUTE IMMEDIATE 'GRANT CREATE TABLESPACE TO ' || v_pub_user;
   EXECUTE IMMEDIATE 'GRANT UNLIMITED TABLESPACE TO ' || v_pub_user;
   EXECUTE IMMEDIATE 'GRANT SELECT_CATALOG_ROLE TO ' || v_pub_user;
   EXECUTE IMMEDIATE 'GRANT EXECUTE_CATALOG_ROLE TO ' || v_pub_user;
   EXECUTE IMMEDIATE 'GRANT CREATE SEQUENCE TO ' || v_pub_user;
   EXECUTE IMMEDIATE 'GRANT DBA TO ' || v_pub_user;
   EXECUTE IMMEDIATE 'GRANT EXECUTE on DBMS_CDC_PUBLISH TO ' || v_pub_user;
   DBMS_STREAMS_AUTH.GRANT_ADMIN_PRIVILEGE(GRANTEE => v_pub_user);

   IF i_version > 10 THEN
      EXECUTE IMMEDIATE 'GRANT INSERT,UPDATE,DELETE ON SYS.CDC_CHANGE_TABLES$ TO ' || v_pub_user;
      EXECUTE IMMEDIATE 'GRANT SELECT ON SYS.CDC_CHANGE_TABLES$ TO ' || v_pub_user || ' WITH GRANT OPTION';
   END IF;
   
   EXECUTE IMMEDIATE 'GRANT SELECT ON tungsten_load TO ' || v_pub_user;
   EXECUTE IMMEDIATE 'GRANT READ ON DIRECTORY tungsten_dir TO ' || v_pub_user;

END IF;

v_tmp_user := NULL;
BEGIN
SELECT USERNAME into v_tmp_user from ALL_USERS where USERNAME = UPPER(v_tungsten_user);
EXCEPTION WHEN OTHERS THEN
   DBMS_OUTPUT.PUT_LINE ('Unable to find user ' || v_tungsten_user);
END;

IF v_tmp_user IS NULL THEN
   -- User not found : create it
   DBMS_OUTPUT.PUT_LINE ('Creating user ' || v_tungsten_user);
   EXECUTE IMMEDIATE 'CREATE USER '||v_tungsten_user||' IDENTIFIED BY '||v_tungsten_pwd||' DEFAULT TABLESPACE USERS';
   EXECUTE IMMEDIATE 'GRANT CONNECT,RESOURCE,CREATE SYNONYM TO ' || v_tungsten_user;
   EXECUTE IMMEDIATE 'GRANT SELECT ON sys.v_$instance TO ' || v_tungsten_user;
END IF;

IF not v_sync THEN
   /* STEP 2 : Alter the source database */
   BEGIN
      EXECUTE IMMEDIATE 'ALTER DATABASE FORCE LOGGING';
      EXECUTE IMMEDIATE 'ALTER DATABASE ADD SUPPLEMENTAL LOG DATA';
   EXCEPTION WHEN OTHERS THEN
      IF debug THEN DBMS_OUTPUT.PUT_LINE ('Unable to add supplemental log data : ' || SQLERRM ); END IF;
   END;
   DBMS_CAPTURE_ADM.BUILD();
END IF;

IF tableCount > 0 THEN
   DECLARE
      CURSOR C IS SELECT table_name FROM ALL_TABLES where owner=v_user AND table_name in (SELECT tableName FROM SYS.tungsten_load);
   BEGIN
      OPEN C;
      LOOP
         FETCH C INTO v_table_name;
         EXIT WHEN C%NOTFOUND;
   
         IF debug THEN DBMS_OUTPUT.PUT_LINE ('Processing table ' || v_user || '.' || v_table_name); END IF;

         IF not v_sync THEN
            IF debug THEN DBMS_OUTPUT.PUT_LINE ('Adding supplemental log data'); END IF;
            BEGIN
               EXECUTE IMMEDIATE  'ALTER TABLE "' || v_user || '"."' || v_table_name || '" DROP SUPPLEMENTAL LOG DATA (ALL) COLUMNS';
            EXCEPTION WHEN OTHERS THEN
               NULL;
            END;
            EXECUTE IMMEDIATE  'ALTER TABLE "' || v_user || '"."' || v_table_name || '" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS';

            IF debug THEN DBMS_OUTPUT.PUT_LINE ('Preparing table instantiation'); END IF;
            DBMS_CAPTURE_ADM.PREPARE_TABLE_INSTANTIATION(TABLE_NAME => '"' || v_user || '"."' || v_table_name || '"'  );
         END IF;
         
         EXECUTE IMMEDIATE 'GRANT SELECT,FLASHBACK ON "'|| v_user || '"."' || v_table_name ||'" TO '||v_tungsten_user;
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

         IF debug THEN DBMS_OUTPUT.PUT_LINE ('Processing table ' || v_user || '.' || v_table_name); END IF;

         IF not v_sync THEN
            IF debug THEN DBMS_OUTPUT.PUT_LINE ('Adding supplemental log data'); END IF;
            BEGIN
               EXECUTE IMMEDIATE  'ALTER TABLE "' || v_user || '"."' || v_table_name || '" DROP SUPPLEMENTAL LOG DATA (ALL) COLUMNS';
            EXCEPTION WHEN OTHERS THEN
               NULL;
            END;
            EXECUTE IMMEDIATE  'ALTER TABLE "' || v_user || '"."' || v_table_name || '" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS';

            IF debug THEN DBMS_OUTPUT.PUT_LINE ('Preparing table instantiation'); END IF;
            DBMS_CAPTURE_ADM.PREPARE_TABLE_INSTANTIATION(TABLE_NAME => '"' || v_user || '"."' || v_table_name || '"' );
         END IF;
         EXECUTE IMMEDIATE 'GRANT SELECT,FLASHBACK ON "'|| v_user || '"."' || v_table_name ||'" TO '||v_tungsten_user;
      END LOOP;
      CLOSE C;
   END;
END IF;

-- GRANT SELECT on v$_database to tungsten user in order to read current SCN, if needed
EXECUTE IMMEDIATE 'GRANT SELECT ON v_$database TO  '||v_tungsten_user;


END;
/
EXIT
