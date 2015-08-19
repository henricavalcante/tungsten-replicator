-- TRIGGERS.SQL
-- 
-- Sample script showing how to create stored procedures that fire only
-- on the master.  

-- Create sample tables.  
drop table if exists sample_table;
create table sample_table (
  id int auto_increment primary key, 
  data varchar(25)
) engine=InnoDB
;

drop table if exists sample_log;
create table sample_log (
  id int auto_increment primary key, 
  update_id int, 
  update_event varchar(25), 
  update_time timestamp default now()
) engine=InnoDB
;

-- Create log trigger that will fire on master after an insert but
-- not on the slave.  TREPSLAVE is set only on a slave. 
drop trigger if exists sample_table_trigger
;
delimiter |
create trigger sample_table_trigger after insert on sample_table
  for each row begin
    if @TREPSLAVE is null then
      insert into sample_log(update_id, update_event) values(NEW.id, 'insert');
    end if;
  end;
|
delimiter ;
