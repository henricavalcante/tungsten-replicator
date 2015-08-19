class PromoteCommand
  include ConfigureCommand
  include RemoteCommand
  include ClusterCommandModule
  
  def require_remote_config?
    if Configurator.instance.is_locked?()
      return false
    else
      return true
    end
  end
  
  def validate_commit
    super()
   
    if @no_connectors == true
      override_promotion_setting(RESTART_CONNECTORS, false)
    end
    
    is_valid?()
  end

  def parsed_options?(arguments)
    @no_connectors = false
    
    arguments = super(arguments)

    if display_help?() && !display_preview?()
      return arguments
    end
  
    # Define extra option to load event.  
    opts=OptionParser.new
    opts.on("--no-connectors") { @no_connectors = true }
    opts = Configurator.instance.run_option_parser(opts, arguments)

    # Return options. 
    opts
  end

  def output_command_usage
    super()
    output_usage_line("--no-connectors", "Do not restart any connectors running on the server")
  end
  
  def get_default_config_file
    if Configurator.instance.is_locked?()
      # This is a configured directory, we can promote it
      return "#{Configurator.instance.get_base_path()}/#{Configurator::HOST_CONFIG}"
    else
      # Search for a prepared directory under releases/install
      install_dir = "#{Configurator.instance.get_continuent_root()}/#{RELEASES_DIRECTORY_NAME}/#{PREPARE_RELEASE_DIRECTORY}"
      if File.directory?(install_dir)
        Dir[install_dir + "/*"].sort().each do |file| 
          if File.directory?(file)
            return "#{file}/#{Configurator::HOST_CONFIG}"
          end
        end
      end
      
      raise "Unable to find a configured directory to promote"
    end
  end
  
  def output_completion_text
    output_cluster_completion_text()

    super()
  end
  
  def validate_home_directory(target_home_directory, target_host, target_user)
    if ssh_result("if [ -d #{target_home_directory} ]; then if [ -f #{target_home_directory}/#{Configurator::HOST_CONFIG} ]; then echo 0; else echo 1; fi else echo 1; fi", target_host, target_user) == "0"
      return target_home_directory
    else
      ssh_result("find #{target_home_directory}/#{RELEASES_DIRECTORY_NAME}/#{PREPARE_RELEASE_DIRECTORY} -maxdepth 1 -type d 2>/dev/null", target_host, target_user).split("\n").each{
        |possible_promotion_dir|
        if ssh_result("if [ -d #{possible_promotion_dir} ]; then if [ -f #{possible_promotion_dir}/#{Configurator::HOST_CONFIG} ]; then echo 0; else echo 1; fi else echo 1; fi", target_host, target_user) == "0"
          return possible_promotion_dir
        end
      }
    end
    
    unless ssh_result("if [ -d #{target_home_directory} ]; then echo 0; else echo 1; fi", target_host, target_user) == "0"
      raise "Unable to find a promotable Tungsten directory at #{target_home_directory}"
    else
      raise "Unable to find #{target_home_directory}/#{Configurator::HOST_CONFIG}.  Make sure that you are specifying a Tungsten directory that is not currently active."
    end
  end
  
  def validate
    return true
  end
  
  def deploy
    return true
  end
  
  def self.get_command_name
    'promote'
  end
  
  def self.get_command_description
    "Take a previously prepared directory and make it active.  You should run the prepare command prior to running promote."
  end
end