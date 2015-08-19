#!/bin/bash
#
# VMware Continuent Tungsten Replicator
# Copyright (C) 2015 VMware, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Initial developer(s): Robert Hodges and Csaba Simon.
# Contributor(s):
#
#
# Description:
#
# This file contains routine to set Replicator Node Manager in MASTER state.
#
# Authors: Teemu Ollakka <teemu.ollakka@continuent.com>
#

set -e

REPLICATOR_BIN_DIR="$(dirname $0)"

. ${REPLICATOR_BIN_DIR}/env.sh

CTRL=${REPLICATOR_BIN_DIR}/replicatorctrl.sh

. ${REPLICATOR_BIN_DIR}/function.sh


state=`get_state`
if test -z $state
then
	echo "Failed to get state, is replicator.sh running?"
	exit 1
fi

set_param "replicator.thl.remote_uri" "thl:\/\/localhost\/" 

case $state in
	OFFLINE)
        configure
        set_slave
        set_master
        ;;
    SYNCHRONIZING)
        reconfigure
        wait_state SLAVE
        set_master
        ;;
    SLAVE)
        set_master
        ;;
    MASTER)
        ;;
esac
