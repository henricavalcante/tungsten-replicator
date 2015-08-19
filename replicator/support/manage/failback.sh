#!/bin/bash
# 
# Fails back from promoted slave to former master. 
#

# Set variables. 
cd `dirname $0`
. variables.sh

# Start replicator process. 
echo "### Taking master and slave offline"
$trep_ctl -host $MASTER offline
$trep_ctl -host $SLAVE offline

# Restore old master to master status. 
echo "### Reconfiguring old master to master again"
$trep_ctl -host $MASTER setrole -role master

echo "### Bringing old master online and to full master state"
$trep_ctl -host $MASTER online
$trep_ctl -host $MASTER wait -state ONLINE -limit 30

# Configure old slave as slave again and restart. 
echo "### Bringing old slave online again as a slave"
$trep_ctl -host $SLAVE setrole -role slave -uri thl://$MASTER
$trep_ctl -host $SLAVE online

echo "Done!"
