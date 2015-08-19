

drop function if exists tungsten_master_pos_wait;
delimiter //

create function tungsten_master_pos_wait 
(
	log_name varchar(255),
	log_pos int,
	timeout int
) returns varchar(80)
begin

  declare source_id varchar(255);
  declare log_file_id int;
  declare tungsten_log_file_id int;
  declare tungsten_log_pos int;
  declare time_waited int;
  declare time_to_wait int;
  declare nada int;
  declare dbg_value varchar(255);

  return "This function does not currently work! Use the stored procedure.";

  set time_waited = 0;
  set time_to_wait = timeout;
  set @ret = null;
  set @iterations = 0;
  if timeout is null then
    set timeout = 0;
  end if;
  
  /* select log_name, log_pos, timeout; */
 
  OuterLoop: loop
   
  if log_name is null then
    set @ret = null;
    leave OuterLoop;
  end if;

  if log_pos is null || log_pos < 0 then
    set @ret = null;
    leave OuterLoop;
  end if;

  PollingLoop: loop
  while true do

    set transaction isolation level read committed;
    
    select t.source_id from trep_commit_seqno t into source_id;

    if source_id = @@hostname then
      set @ret = null;
      leave PollingLoop;
    end if;
   
    select cast(substring_index(log_name, '.', -1) as UNSIGNED) into log_file_id;

    select cast(substring_index(t.eventid, ':', 1) as UNSIGNED) 
       from trep_commit_seqno t into tungsten_log_file_id;

    select cast(substring_index(substring_index(t.eventid, ':', -1), ';', 1) as UNSIGNED) 
       from trep_commit_seqno t into tungsten_log_pos; 
    
    set @ret = tungsten_log_pos;

    if tungsten_log_file_id > log_file_id then
     set @ret = 0;
     leave PollingLoop;
    end if;
 
    if tungsten_log_file_id = log_file_id then
       if tungsten_log_pos >= log_pos then
         set @ret = time_waited;
         leave PollingLoop;
       end if;
    end if;
    
    if timeout <= 0 then
      set @ret = -1;
      leave PollingLoop;
    end if;

    set time_waited = time_waited + 1;
    select sleep(1) into nada;
    if time_waited >= timeout then
      /* set @ret = time_waited;*/
      set @ret = tungsten_log_pos;
      leave PollingLoop;
    end if;

 end while;
 end loop PollingLoop;
 leave OuterLoop;
 end loop OuterLoop;
 return @ret;
end;
//
delimiter ;

    

	


