These comments have been saved separately from the stored procedure
since the MySQL parser is so broken that the procedure can't
be compiled reliably with these comments inline with the
source. Consider yourself forewarned....
/*
** This stored procedure functions identically to the MySQL
** function, MASTER_POS_WAIT, except that it uses information
** in the tungsten replication system instead of the MySQL
** replication system.  The description below is directly from
** the MySQL 4.0 documentation:
** 
** MASTER_POS_WAIT(log_name,log_pos[,timeout])
**
** This function is useful for control of master/slave synchronization. 
** It blocks until the slave has read and applied all updates up to 
** the specified position in the master log. The return value is the 
** number of log events the slave had to wait for to advance to the 
** specified position. The function returns NULL if the slave SQL 
** thread is not started, the slave's master information is not 
** initialized, the arguments are incorrect, or an error occurs. It 
** returns -1 if the timeout has been exceeded. If the slave SQL 
** thread stops while MASTER_POS_WAIT() is waiting, the function 
** returns NULL. If the slave is past the specified position, the 
** function returns immediately.
**
** For the Tungsten implementation, the stored procedure basically
** polls the trep_commit_seqno table, which has the MySQL specific
** log id and log offset as well as Tungsten specific information.
**
** The stored procedure assumes the following:
**
** If the current log_name in the trep_commit_seqno table 
** is compared to the log_name passed into the procedure, and
** collates, alphabetically, to be greater than the log_name
** passed in, the procedure will immediately return with a zero
** result since if the log_name is greater, it means we've applied
** all of the log records for any log_name that is less than 
** the current log_name.
**
** We will be polling the trep_seqno_table on a one-second interval.
** This means that the practical response time of the procedure
** to reaching the specified log_pos can be as much as 2 seconds after
** the log_pos is reached. It's assumed that this 2 second latency
** does not affect the program that calls the procedure. Of course,
** the latency can be much lower depending on when the 'sample' is taken.
**
**
*/  