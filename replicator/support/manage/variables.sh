#!/bin/bash
#
# Set your variables here.  This script is included by other demo scripts. 
#

# SET YOUR VARIABLES HERE.  
# ========
MASTER=tungsten1
SLAVE=tungsten2
TUNGSTEN_LOGIN=tungsten
TUNGSTEN_PASSWORD=secret
# ========

# DO NOT MAKE CHANGES BELOW THIS LINE. 

# Set home directory location. 
cd `dirname $0`
if [ -z $REPLICATOR_HOME ]; then
  REPLICATOR_HOME=`cd ../..; pwd`
fi

# Define tools and other handy locations. 
trepsvc=$REPLICATOR_HOME/bin/replicator
trep_stop="$trepsvc stop"
trep_start="$trepsvc start"
trep_ctl=$REPLICATOR_HOME/bin/trepctl
