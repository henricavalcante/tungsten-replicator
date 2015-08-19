#!/bin/bash

PREFIX="@{EXECUTABLE_PREFIX}"
if [ "$PREFIX" == "" ]; then
	exit 0
fi

BASE="@{CURRENT_RELEASE_DIRECTORY}"
alias ${PREFIX}_tpm="${BASE}/tools/tpm"
alias ${PREFIX}_replicator="${BASE}/tungsten-replicator/bin/replicator"
alias ${PREFIX}_trepctl="${BASE}/tungsten-replicator/bin/trepctl"
alias ${PREFIX}_thl="${BASE}/tungsten-replicator/bin/thl"
alias ${PREFIX}_tungsten_provision_slave="${BASE}/tungsten-replicator/scripts/tungsten_provision_slave"
alias ${PREFIX}_tungsten_provision_thl="${BASE}/tungsten-replicator/scripts/tungsten_provision_thl"
alias ${PREFIX}_tungsten_read_master_events="${BASE}/tungsten-replicator/scripts/tungsten_read_master_events"
alias ${PREFIX}_tungsten_get_position="${BASE}/tungsten-replicator/scripts/tungsten_get_position"
alias ${PREFIX}_tungsten_set_position="${BASE}/tungsten-replicator/scripts/tungsten_set_position"
alias ${PREFIX}_xtrabackup_to_slave="${BASE}/tungsten-replicator/scripts/xtrabackup_to_slave"
alias ${PREFIX}_mysqldump_to_slave="${BASE}/tungsten-replicator/scripts/mysqldump_to_slave"
alias ${PREFIX}_multi_trepctl="${BASE}/tungsten-replicator/scripts/multi_trepctl"
alias ${PREFIX}_manager="${BASE}/tungsten-manager/bin/manager"
alias ${PREFIX}_cctrl="${BASE}/tungsten-manager/bin/cctrl"
alias ${PREFIX}_connector="${BASE}/tungsten-connector/bin/connector"