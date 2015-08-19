

drop procedure if exists tungsten_master_pos_wait;
delimiter //

create procedure tungsten_master_pos_wait 
(
	in log_name varchar(255),
	in log_pos integer,
	in timeout integer
)
begin

  declare source_id varchar(255);
  declare log_file_id integer;
  declare tungsten_log_file_id integer;
  declare tungsten_log_pos integer;
  declare time_waited integer;
  declare time_to_wait integer;
  declare nada integer;

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
    if @tungsten_debug then
      select "Exiting because of invalid log_name";
    end if;
    set @ret = null;
    leave OuterLoop;
  end if;

  if log_pos is null || log_pos < 0 then
    if @tungsten_debug then
      select "Exiting because of invalid log_pos";
    end if;
    set @ret = null;
    leave OuterLoop;
  end if;

  PollingLoop: loop
  while true do

    select t.source_id from trep_commit_seqno t into source_id;

    if source_id = @@hostname then
      if @tungsten_debug then
        select "Exiting because this is not a slave";
      end if;
      set @ret = null;
      leave PollingLoop;
    end if;
   
    select cast(substring_index(log_name, '.', -1) as UNSIGNED) into log_file_id;

    select cast(substring_index(t.eventid, ':', 1) as UNSIGNED) 
       from trep_commit_seqno t into tungsten_log_file_id;

    select cast(substring_index(substring_index(t.eventid, ':', -1), ';', 1) as UNSIGNED) 
       from trep_commit_seqno t into tungsten_log_pos; 

    if tungsten_log_file_id > log_file_id then
     if @tungsten_debug then
       select "Exiting because the tungsten_log_file_id .gt. log_file_id";
     end if;
     set @ret = 0;
     leave PollingLoop;
    end if;
 
    if tungsten_log_file_id = log_file_id then
       if tungsten_log_pos >= log_pos then
	 if @tungsten_debug then
           select "Exiting because we reached the position";
         end if;
         set @ret = time_waited;
         leave PollingLoop;
       end if;
    end if;
    
    if timeout <= 0 then
      if @tungsten_debug then
        select "Exiting because no timeout was specified";
      end if;
      set @ret = -1;
      leave PollingLoop;
    end if;

    set time_waited = time_waited + 1;
    select sleep(1) into nada;
    if time_waited >= timeout then
      if @tungsten_debug then
        select "Exiting because we exceeded the timeout";
      end if;
      set @ret = -1;
      leave PollingLoop;
    end if;

 end while;
 end loop PollingLoop;
 leave OuterLoop;
 end loop OuterLoop;
 select @ret as retval;
end;
//
delimiter ; ;

    

	


