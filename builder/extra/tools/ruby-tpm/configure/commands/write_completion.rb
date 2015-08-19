class WriteCompletionCommand
  include ConfigureCommand
  
  def initialize(config)
    super(config)
    distribute_log?(false)
  end
  
  def run
    commands = {}
    ConfigureCommand.subclasses().each{
      |klass|
      begin
        if klass.display_command() == false
          next
        end
      rescue NoMethodError
      end
      
      begin
        cmd = klass.get_command_name()
      rescue NoMethodError
        next
      end
      
      begin
        desc = klass.get_command_description()
      rescue NoMethodError
        desc = ""
      end
      
      commands[cmd] = klass
    }
    
    completion_path = Configurator.instance.get_base_path() + "/tools/.tpm.complete"
    File.open(completion_path, 'w') do |file|
      file.printf <<EOF
_tpm()
{
  local cur prev opts
  COMPREPLY=()
  cur="${COMP_WORDS[COMP_CWORD]}"
  prev="${COMP_WORDS[COMP_CWORD-1]}"

  tpm_options="--skip-validation-check= --enable-validation-check= --skip-validation-warnings= --enable-validation-warnings= --property= --remove-property="
EOF
      file.printf("tpm_commands=\"#{commands.keys().sort().join(' ')}\"\n")
      commands.keys().sort().each{
        |cmd|
        cmd_object = commands[cmd].new(@config)
        klass_options = cmd_object.allowed_subcommands() + cmd_object.get_bash_completion_arguments().uniq().sort()
        
        file.printf("

#################
# tpm #{cmd}
#################
tpm_#{cmd.tr('-', '_')}=\"#{klass_options.join(' ')}\"

")
      }
      
      file.printf <<EOF

  if [ $COMP_CWORD -eq 1 ]; then
    COMPREPLY=( $(compgen -W "${tpm_commands} ${tpm_options}" -- ${cur}) )
    return 0
  else
    eval opts='$tpm_'${COMP_WORDS[1]}
    COMPREPLY=( $(compgen -W "${opts} ${tpm_options}" -- ${cur}) )
    return 0
  fi
}
complete -o nospace -F _tpm tpm
if [ "$EXECUTABLE_PREFIX" != "" ]; then
	complete -F _tpm ${EXECUTABLE_PREFIX}_tpm
fi
EOF

      scripts = [
        "cluster-home/bin/tungsten_monitor",
        "cluster-home/bin/tungsten_health_check",
        "cluster-home/bin/check_tungsten_backups",
        "cluster-home/bin/cluster_backup",
        "tungsten-replicator/scripts/tungsten_provision_slave",
        "tungsten-replicator/scripts/tungsten_provision_thl",
        "tungsten-replicator/scripts/tungsten_read_master_events",
        "tungsten-replicator/scripts/tungsten_get_position",
        "tungsten-replicator/scripts/tungsten_set_position",
        "tungsten-replicator/scripts/xtrabackup_to_slave",
        "tungsten-replicator/scripts/mysqldump_to_slave",
        "tungsten-replicator/scripts/multi_trepctl"
        ]
      scripts.each{
        |path|
        basename = File.basename(path)
        id = "_" + to_identifier(basename)
        
        if File.exist?("#{Configurator.instance.get_base_path()}/#{path}")
          script_args = cmd_result("#{Configurator.instance.get_base_path()}/#{path} --autocomplete")
          file.print("\n")
          file.print("#{id}()\n")
          file.printf <<EOF
{
  local cur prev opts
  COMPREPLY=()
  cur="${COMP_WORDS[COMP_CWORD]}"
  prev="${COMP_WORDS[COMP_CWORD-1]}"
EOF
        file.printf("script_options=\"#{script_args}\"")
        file.printf <<EOF

  COMPREPLY=( $(compgen -W "${script_options}" -- ${cur}) )
  return 0
}
EOF
          file.printf("complete -o nospace -F #{id} #{basename}\n")
          file.printf("if [ \"$EXECUTABLE_PREFIX\" != \"\" ]; then\n")
          file.printf("  complete -F #{id} ${EXECUTABLE_PREFIX}_#{basename}\n")
        	file.printf("fi\n")
        end
      }
    end
  end
  
  def self.display_command
    false
  end
  
  def self.get_command_name
    'write-completion'
  end
  
  def self.get_command_description
    "Write a bash-completion command"
  end
end