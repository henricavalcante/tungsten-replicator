#!/bin/bash
# 
# Starts the slave replicator. 
#

# Set variables. 
cd `dirname $0`
. variables.sh

# Start replicator process. 
echo "### Starting slave replicator"
ssh $SLAVE $trep_start 
sleep 5
$trep_ctl -host $SLAVE wait -state ONLINE -limit 30

echo "Done!"
