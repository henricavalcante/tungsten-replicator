DECLARE
v_user varchar2(30) := 'GRANITT';
CURSOR C1 IS SELECT table_name FROM ALL_TABLES where owner= v_user;
CURSOR C2 IS select 'alter table '||a.owner||'.'||a.table_name||' disable constraint '||a.constraint_name
from all_constraints a, all_constraints b
where a.constraint_type = 'R'
and a.r_constraint_name = b.constraint_name
and a.r_owner  = b.owner and b.owner = v_user;

v_table_name varchar2(30);
v_statement varchar2(4000);
BEGIN

OPEN C2;
LOOP
FETCH C2 INTO v_statement;
EXIT WHEN C2%NOTFOUND;
DBMS_OUTPUT.PUT_LINE ('Executing ' || v_statement);
EXECUTE IMMEDIATE v_statement;
END LOOP;
CLOSE C2;

OPEN C1;
LOOP

FETCH C1 INTO v_table_name;
EXIT WHEN C1%NOTFOUND;

DBMS_OUTPUT.PUT_LINE ('Processing table ' || v_user || '.' || v_table_name);
EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || v_user || '.' || v_table_name;

END LOOP;
CLOSE C1;
END;
/
