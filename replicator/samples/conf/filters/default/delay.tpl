# Time delay filter.  Should only be used on slaves, as it delays storage
# of new events on the master.  The time delay is in seconds. 
replicator.filter.delay=com.continuent.tungsten.replicator.filter.TimeDelayFilter
replicator.filter.delay.delay=300