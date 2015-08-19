--For DEBUG purpose, comment the following 3 lines and uncomment the 4th one
set feedback off
set echo off
set term off
--set serveroutput on

DECLARE

v_path varchar2(300) := '&1';
v_pub_user varchar2(30) := '&2';
specific_file boolean := ('&3' = '1');

BEGIN

BEGIN
   -- Dropping table if it already exists
   EXECUTE IMMEDIATE 'DROP TABLE tungsten_load';
EXCEPTION WHEN OTHERS THEN
   -- Table did not exist - no matter
   NULL;
END;

-- Create directory either way, so we can grant permissions in other scripts
EXECUTE IMMEDIATE 'CREATE OR REPLACE DIRECTORY tungsten_dir AS ''' || v_path ||'''';

IF specific_file THEN 
   BEGIN
      EXECUTE IMMEDIATE 'CREATE TABLE tungsten_load(tableName VARCHAR2(30), columns VARCHAR2(4000)) ORGANIZATION EXTERNAL (TYPE ORACLE_LOADER DEFAULT DIRECTORY tungsten_dir
         ACCESS PARAMETERS (RECORDS DELIMITED BY NEWLINE
         NOLOGFILE NOBADFILE NODISCARDFILE LOAD WHEN ((1:1) != "#") FIELDS TERMINATED BY ''	'' LRTRIM MISSING FIELD VALUES ARE NULL REJECT ROWS WITH ALL NULL FIELDS) LOCATION (''tungsten.tables'')) REJECT LIMIT UNLIMITED';
   EXCEPTION WHEN OTHERS THEN
      -- This should not happen
      DBMS_OUTPUT.PUT_LINE ('Table tungsten_load already exists. Skipping');
   END;

ELSE
   BEGIN 
      EXECUTE IMMEDIATE 'CREATE TABLE tungsten_load(tableName VARCHAR2(30), columns VARCHAR2(4000))';
   EXCEPTION WHEN OTHERS THEN
      -- This should not happen
      DBMS_OUTPUT.PUT_LINE ('Table tungsten_load already exists. Skipping');
   END;
END IF;

END;
/

EXIT
