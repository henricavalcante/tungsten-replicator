/* This script is provided in order to drop all subscription views on Oracle 10G */
/* This does not seem to be required on 11G as dropping the subscription probably also drop subscriber views */ 
DECLARE
CURSOR C1 IS select view_name from all_views where view_name like 'VW_TUNGSTEN%';

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
/
