#!/bin/bash 

set_tungsten_env() {
	export CONTINUENT_ROOT=@{HOME_DIRECTORY}
	
	EXECUTABLE_PREFIX="@{EXECUTABLE_PREFIX}"
	
	if [ -z $CONTINUENT_ROOT ];
	then
		echo "you must have the environment variable CONTINUENT_ROOT defined"
		echo "and it must point at a valid, readable directory"
		return 1
	fi

	if [ ! -d $CONTINUENT_ROOT ];
	then
		echo "the current value for CONTINUENT_ROOT, $CONTINUENT_ROOT"
		echo "must point at a valid, readable directory"
		return 1
	fi

	PREFERRED_PATH="@{PREFERRED_PATH}"
	if [ "$PREFERRED_PATH" != "" ]; then
		export PATH=${PREFERRED_PATH}:$PATH
	fi
	
	if [ -f $CONTINUENT_ROOT/share/aliases.sh ]; then
		. $CONTINUENT_ROOT/share/aliases.sh
	else
		export PATH=$PATH:$CONTINUENT_ROOT/tungsten/tungsten-manager/bin:$CONTINUENT_ROOT/tungsten/tungsten-replicator/bin:$CONTINUENT_ROOT/tungsten/tungsten-replicator/scripts:$CONTINUENT_ROOT/tungsten/cluster-home/bin:$CONTINUENT_ROOT/tungsten/tungsten-connector/bin:$CONTINUENT_ROOT/share:$CONTINUENT_ROOT/tungsten/tools
	fi
	
	_cctrl()
  {
    local cur prev opts
    COMPREPLY=()
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"
    opts="-admin -expert -host -logical -multi -no-history -physical -port -proxy"

    COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
    return 0
  }
	complete -F _cctrl cctrl
	if [ "$EXECUTABLE_PREFIX" != "" ]; then
		complete -F _cctrl ${EXECUTABLE_PREFIX}_cctrl
	fi
	
	_trepctl()
  {
    local cur prev opts
    COMPREPLY=()
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"
    trepctl_opts="-host -port -service -verbose -retry"
    trepctl_commands="version services capabilities shutdown kill backup clear configure flush heartbeat offline offline-deferred online purge reset restore setrole start status stop wait check shard"
    trepctl_shutdown="-y"
    trepctl_kill="-y"
    trepctl_backup="-backup -storage -limit"
    trepctl_flush="-limit"
    trepctl_heartbeat="-name"
    trepctl_offline="-immediate"
    trepctl_offline_deferred="-at-seqno -at-event -at-heartbeat"
    trepctl_online="-force -from-event -base-seqno -skip-seqno -until-seqno -until-event -until-heartbeat -until-time"
    trepctl_purge="-y -limit"
    trepctl_reset="-y"
    trepctl_restore="-uri -limit"
    trepctl_setrole="-role -uri"
    trepctl_status="-name channel-assignments services shards stages stores tasks watches"
    trepctl_stop="-y"
    trepctl_wait="-state -applied -limit"
    trepctl_check="-limit -method"
    trepctl_shard="-list -insert -update -delete"

    if [ $COMP_CWORD -eq 1 ]; then
      COMPREPLY=( $(compgen -W "${trepctl_opts} ${trepctl_commands}" -- ${cur}) )
      return 0
    else
      eval opts='$trepctl_'${COMP_WORDS[1]}
      COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
      return 0
    fi
  }
  complete -F _trepctl trepctl
	if [ "$EXECUTABLE_PREFIX" != "" ]; then
		complete -F _trepctl ${EXECUTABLE_PREFIX}_trepctl
	fi
  
  
	_thl()
  {
    local cur prev opts
    COMPREPLY=()
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"
    thl_options="-conf -service"
    thl_commands="list index purge info help"
    thl_list="-low -high -by -sql -charset -hex -file"
    thl_purge="-low -high -y -seqno"
    
    if [ $COMP_CWORD -eq 1 ]; then
      COMPREPLY=( $(compgen -W "${thl_commands} ${thl_options}" -- ${cur}) )
      return 0
    else
      eval opts='$thl_'${COMP_WORDS[1]}
      COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
      return 0
    fi
  }
  complete -F _thl thl
	if [ "$EXECUTABLE_PREFIX" != "" ]; then
		complete -F _thl ${EXECUTABLE_PREFIX}_thl
	fi
  
  if [ -f "${CONTINUENT_ROOT}/tungsten/tools/.tpm.complete" ]; then
    . "${CONTINUENT_ROOT}/tungsten/tools/.tpm.complete"
  fi
	
	return 0
}

set_tungsten_env