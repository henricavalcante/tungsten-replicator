DECLARE

DEBUG_LVL number := 1; -- 1 : INFO (default) , 2 : DEBUG (more verbose)
/* 
This script is intended to be called by another script (nested scripts).
The calling script should have all the following variables defined.
v_user varchar2(30);
v_pub_user varchar2(100);
v_tungsten_user varchar2(30);
b_sync boolean;
v_table_name varchar2(40);
v_column_list varchar(10000);
i_pub_tablespace number;
*/
v_ct_name varchar2(100);
v_quoted_ct_name varchar2(100);
v_quoted_table varchar2(100);
v_quoted_user varchar2(100);
v_quoted_cs_name varchar2(100);
v_quoted_schema varchar2(100);

v_create_ct_statement varchar2(32737);

v_tablespace varchar2(30) := 'TABLESPACE ' || v_pub_user;

BEGIN
  IF DEBUG_LVL > 1 THEN      
    DBMS_OUTPUT.PUT_LINE ('Processing ' || v_user || '.' || v_table_name || '(' || v_column_list || ')');
  ELSE
    DBMS_OUTPUT.PUT ('Processing ' || v_user || '.' || v_table_name);
  END IF;
      
  v_column_list := '''' || v_column_list || '''';
  v_quoted_table := '''' || v_table_name  || '''';
  v_ct_name := 'CT_' || SUBSTR(v_table_name, 1, 27);
  v_quoted_ct_name := '''' || v_ct_name  || '''';
  v_quoted_schema := '''' || v_user || '''';
  v_quoted_cs_name := '''' || v_change_set_name || ''''; 
  v_quoted_user := '''' || v_pub_user  || '''';
  
  IF i_pub_tablespace = 0 THEN
   v_tablespace := '';
  END IF;

  IF b_sync THEN
    IF i_version > 10 THEN
      v_create_ct_statement :='BEGIN DBMS_CDC_PUBLISH.CREATE_CHANGE_TABLE(owner=>'||v_quoted_user||', change_table_name=> '||v_quoted_ct_name||', change_set_name=>'|| v_quoted_cs_name ||', source_schema=>'|| v_quoted_schema ||', source_table=>'|| v_quoted_table ||', column_type_list => '|| v_column_list||', capture_values => ''both'',  rs_id => ''y'', row_id => ''n'', user_id => ''n'', timestamp => ''n'', object_id => ''n'', source_colmap => ''y'', target_colmap => ''y'', DDL_MARKERS=> ''n'', options_string => '''||v_tablespace||''');END;';
    ELSE
      v_create_ct_statement :='BEGIN DBMS_CDC_PUBLISH.CREATE_CHANGE_TABLE(owner=>'||v_quoted_user||', change_table_name=> '||v_quoted_ct_name||', change_set_name=>'|| v_quoted_cs_name ||', source_schema=>'|| v_quoted_schema ||', source_table=>'|| v_quoted_table ||', column_type_list => '|| v_column_list||', capture_values => ''both'',  rs_id => ''y'', row_id => ''n'', user_id => ''n'', timestamp => ''n'', object_id => ''n'', source_colmap => ''y'', target_colmap => ''y'', options_string => '''||v_tablespace||''');END;';
    END IF;
  ELSE
    v_create_ct_statement :='BEGIN DBMS_CDC_PUBLISH.CREATE_CHANGE_TABLE(owner=>'||v_quoted_user||', change_table_name=> '||v_quoted_ct_name||', change_set_name=>'|| v_quoted_cs_name ||', source_schema=>'|| v_quoted_schema ||', source_table=>'|| v_quoted_table ||', column_type_list => '|| v_column_list||', capture_values => ''both'',  rs_id => ''y'', row_id => ''n'', user_id => ''n'', timestamp => ''n'', object_id => ''n'', source_colmap => ''n'', target_colmap => ''y'', options_string => '''||v_tablespace||''');END;';
  END IF;
            
  DECLARE
    createdSCN number;
  BEGIN
    IF DEBUG_LVL > 1 THEN      
      DBMS_OUTPUT.PUT_LINE (v_create_ct_statement);
    END IF;

    EXECUTE IMMEDIATE v_create_ct_statement;
    IF DEBUG_LVL > 1 THEN
      DBMS_OUTPUT.PUT_LINE ('Running GRANT SELECT ON '|| v_ct_name || ' TO ' || v_tungsten_user);
    END IF;
    
    EXECUTE IMMEDIATE 'GRANT SELECT ON '|| v_ct_name || ' TO ' || v_tungsten_user;
               
    IF not b_sync THEN
      DBMS_OUTPUT.PUT_LINE(' -> ' ||  v_quoted_ct_name || ' : OK' );
    ELSE
      SELECT CREATED_SCN INTO createdSCN FROM change_tables WHERE CHANGE_SET_NAME=v_change_set_name and CHANGE_TABLE_NAME=v_ct_name;
      DBMS_OUTPUT.PUT_LINE(' -> ' ||  v_quoted_ct_name || ' : created at SCN ' || createdSCN);
    END IF;
  EXCEPTION WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE(' -> ' || v_quoted_ct_name || ' : ERROR (' ||SUBSTR(SQLERRM, 1, 100) || ')');
    err_found := TRUE;
  END;
END;
