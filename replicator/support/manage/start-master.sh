#!/bin/bash
# 
# Starts the master replicator. 
#

# Set variables. 
cd `dirname $0`
. variables.sh

# Start replicator process. 
echo "### Starting master replicator"
ssh $MASTER $trep_start
sleep 5
$trep_ctl -host $MASTER wait -state ONLINE -limit 30

echo "Done!"
