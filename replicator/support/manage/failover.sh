#!/bin/bash
# 
# Fails over from master to slave. 
#

# Set variables. 
cd `dirname $0`
. variables.sh

# Start replicator process. 
echo "### Taking master and slaves offline"
$trep_ctl -host $MASTER offline
$trep_ctl -host $SLAVE offline

# Configure slave as master.  
echo "### Promoting former slave to master"
$trep_ctl -host $SLAVE setrole -role master

echo "### Bringing former slave back online"
$trep_ctl -host $SLAVE online
$trep_ctl -host $SLAVE wait -state ONLINE -limit 30

# Configure master as slave and restart. 
echo "### Starting old master as a slave"
$trep_ctl -host $MASTER setrole -role slave -uri thl://$SLAVE/
$trep_ctl -host $MASTER online

echo "Done!"
