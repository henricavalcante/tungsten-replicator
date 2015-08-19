class ConfigureValidationHandler
  @@skipped_classes = []
  @@enabled_classes = []
  @@skip_warnings = []
  @@enabled_warnings = []
  include ConfigureMessages
  attr_reader :deployment_checks, :local_checks
  
  def initialize()
    super()
    @local_checks = []
    @deployment_checks = []
    @commit_checks = []
    
    @config = Properties.new()
    initialize_validation_checks()
  end
  
  def initialize_validation_checks
    @local_checks = []
    @deployment_checks = []
    @commit_checks = []
    @post_validation_checks = []
    @post_validation_commit_checks = []
    
    register_checks(Configurator.instance.command.get_validation_checks())
  end
  
  def register_checks(check_objs)
    check_objs.each{|check_obj| register_check(check_obj)}
  end
  
  # Validate the prompt object and add it to the queue
  def register_check(check_obj)
    unless check_obj.is_a?(ValidationCheckInterface)
      raise "Attempt to register invalid check #{check_obj.class} failed " +
        "because it does not extend ValidationCheckInterface"
    end
    
    check_obj.set_config(@config)
    unless check_obj.is_initialized?()
      raise "#{class_name} cannot be used because it has not been properly initialized"
    end
    
    if check_obj.is_a?(LocalValidationCheck)
      @local_checks.push(check_obj)
    elsif check_obj.is_a?(CommitValidationCheck)
      @commit_checks.push(check_obj)
    elsif check_obj.is_a?(PostValidationCheck)
      @post_validation_checks.push(check_obj)
    elsif check_obj.is_a?(PostValidationCommitCheck)
      @post_validation_commit_checks.push(check_obj)
    else
      @deployment_checks.push(check_obj)
    end
  end
  
  # The preliminary checks look for ssh access, ruby and java
  def prevalidate(configs)
    reset_errors()
    @config.reset()
    
    if @local_checks.size() == 0
      trace("Skipping preliminary checks because none are set for this command")
      return is_valid?()
    end
    
    configs.each {
      |config|
      
      prevalidate_config(config)
    }
    
    is_valid?()
  end
  
  def prevalidate_config(config)
    @config.props = config.props
    
    Configurator.instance.write ""
    Configurator.instance.write_header "Preliminary checks for #{@config.getProperty(HOST)}:#{@config.getProperty(HOME_DIRECTORY)}"

    # Preliminary checks to ensure connectivity and transfer validation code
    @local_checks.each{
      |check|
      begin
        check.run()
        include_messages_object(check)

        unless check.is_valid?()
          if check.fatal_on_error?()
            break
          end
        end
      rescue ValidationError => ve
        Configurator.instance.exception(ve)
        @errors.push(ve)
      rescue Exception => e
        Configurator.instance.exception(e)
        @errors.push(ValidationError.new(e.to_s(), @config.getProperty(HOST), check))
      end
    }
  end
  
  # These checks are more in-depth
  def validate(configs)
    reset_errors()
    @config.reset()
    
    if @deployment_checks.size() == 0
      trace("Skipping checks because none are set for this command")
      return is_valid?()
    end
    
    configs.each{
      |config|
      @config.import(config)
      
      begin
        if run_locally?()
          Configurator.instance.write ""
          Configurator.instance.write_header "Local checks for #{@config.getProperty(HOME_DIRECTORY)}"
        
          validate_config(@config)
          debug("Finish: Local checks for #{@config.getProperty(HOME_DIRECTORY)}")
        else
          Configurator.instance.write ""
          Configurator.instance.write_header "Remote checks for #{@config.getProperty(HOST)}:#{@config.getProperty(HOME_DIRECTORY)}"
          
          # Invoke ValidationChecks on the remote server
          if Configurator.instance.command.use_remote_package?()
            validation_temp_directory = get_validation_temp_directory()
            command = "#{@config.getProperty(REMOTE_PACKAGE_PATH)}/tools/tpm validate-single-config --profile=#{validation_temp_directory}/#{Configurator::TEMP_DEPLOY_HOST_CONFIG} --command-class=#{Configurator.instance.command.class.name} #{Configurator.instance.get_remote_tpm_options().join(' ')}"
          else
            command = "#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tools/tpm validate-single-config --command-class=#{Configurator.instance.command.class.name} #{Configurator.instance.get_remote_tpm_options().join(' ')}"
          end
          
          result_dump = ssh_result(command, @config.getProperty(HOST), @config.getProperty(USERID), true)
          
          begin
            result = Marshal.load(result_dump)
            
            add_remote_result(result)
            
            debug("Finish: Remote checks for #{@config.getProperty(HOST)}:#{@config.getProperty(HOME_DIRECTORY)}")
          rescue ArgumentError => ae
            error("Unable to load the validation result.  This can happen due to a bug in Ruby.  Try updating to a newer version and retry the installation.")
          end
        end
      rescue => e
        exception(e)
      end
    }
    
    @config.reset()
    
    is_valid?()
  end
  
  def post_validate()
    begin
      @post_validation_checks.each{
        |check|
        
        check.run()
        include_messages_object(check)
        
        unless check.is_valid?()
          if check.fatal_on_error?()
            break
          end
        end
      }
    rescue => e  
      exception(e)
    end
    
    is_valid?()
  end
  
  def post_validate_commit()
    begin
      @post_validation_commit_checks.each{
        |check|
        
        check.run()
        include_messages_object(check)
        
        unless check.is_valid?()
          if check.fatal_on_error?()
            break
          end
        end
      }
    rescue => e  
      exception(e)
    end
    
    is_valid?()
  end
  
  # Handle the remote side of the validate function
  def validate_config(config)
    @config.import(config)
    Configurator.instance.command.build_topologies(@config)
    
    begin
      @deployment_checks.each{
        |check|
        
        check.run()
        include_messages_object(check)
        
        unless check.is_valid?()
          if check.fatal_on_error?()
            break
          end
        end
      }
    rescue => e  
      exception(e)
    end
    
    return get_remote_result()
  end
  
  # These checks are more in-depth
  def validate_commit(configs)
    reset_errors()
    @config.reset()
    
    configs.each{
      |config|
      
      @output_properties.setProperty(config.getProperty(DEPLOYMENT_HOST), {})
    }
    
    if @commit_checks.size() == 0
      trace("Skipping commit checks because none are set for this command")
      return is_valid?()
    end
    
    configs.each{
      |config|
      @config.import(config)
      
      begin
        if run_locally?()
          Configurator.instance.write ""
          Configurator.instance.write_header "Local commit checks for #{@config.getProperty(HOME_DIRECTORY)}"
        
          validate_commit_config(@config)
          debug("Finish: Local commit checks for #{@config.getProperty(HOME_DIRECTORY)}")
        else
          Configurator.instance.write ""
          Configurator.instance.write_header "Remote commit checks for #{@config.getProperty(HOST)}:#{@config.getProperty(HOME_DIRECTORY)}"
          
          # Invoke ValidationChecks on the remote server
          if Configurator.instance.command.use_remote_package?()
            validation_temp_directory = get_validation_temp_directory()
            command = "#{@config.getProperty(REMOTE_PACKAGE_PATH)}/tools/tpm validate-commit-config --profile=#{validation_temp_directory}/#{Configurator::TEMP_DEPLOY_HOST_CONFIG} --command-class=#{Configurator.instance.command.class.name} #{Configurator.instance.get_remote_tpm_options().join(' ')}"
          else
            command = "#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tools/tpm validate-commit-config --command-class=#{Configurator.instance.command.class.name} #{Configurator.instance.get_remote_tpm_options().join(' ')}"
          end

          result_dump = ssh_result(command, @config.getProperty(HOST), @config.getProperty(USERID), true)
          
          begin
            result = Marshal.load(result_dump)

            add_remote_result(result)
            
            debug("Finish: Remote commit checks for #{@config.getProperty(HOST)}:#{@config.getProperty(HOME_DIRECTORY)}")
          rescue ArgumentError => ae
            error("Unable to load the commit validation result.  This can happen due to a bug in Ruby.  Try updating to a newer version and retry the installation.")
          end
        end
      rescue => e
        exception(e)
      end
    }
    
    is_valid?()
  end
  
  # Handle the remote side of the validate function
  def validate_commit_config(config)
    @config.import(config)
    Configurator.instance.command.build_topologies(@config)
    
    begin
      @commit_checks.each{
        |check|
        
        check.run()
        include_messages_object(check)
        
        unless check.is_valid?()
          if check.fatal_on_error?()
            break
          end
        end
      }
    rescue => e  
      exception(e)
    end
    
    return get_remote_result()
  end
  
  def get_validation_temp_directory
    "#{@config.getProperty(TEMP_DIRECTORY)}/#{@config.getProperty(CONFIG_TARGET_BASENAME)}/"
  end
  
  def is_not_commit_validation_check(check)
    !(check.is_a?(CommitValidationCheck))
  end
  
  def is_commit_validation_check(check)
    (check.is_a?(CommitValidationCheck))
  end
  
  def run_locally?
    return Configurator.instance.is_localhost?(@config.getProperty(HOST)) && 
      Configurator.instance.whoami == @config.getProperty(USERID)
  end
  
  def get_message_hostname
    @config.getProperty(DEPLOYMENT_HOST)
  end
  
  def get_message_host_key
    @config.getProperty([DEPLOYMENT_CONFIGURATION_KEY])
  end
  
  def self.skip_validation_class?(klass, cfg)
    @@host_skip_classes ||= {}
    
    h_alias = cfg.getProperty(DEPLOYMENT_HOST)
    unless @@host_skip_classes.has_key?(h_alias)
      @@host_skip_classes[h_alias]= cfg.getTemplateValue(SKIPPED_VALIDATION_CLASSES)
    end
    
    return get_skipped_validation_classes().include?(klass) || (@@host_skip_classes[h_alias]||[]).include?(klass)
  end
  
  def self.mark_skipped_validation_class(klass)
    @@skipped_classes << klass
  end
  
  def self.get_skipped_validation_classes
    @@skipped_classes || []
  end
  
  def self.mark_enabled_validation_class(klass)
    @@enabled_classes << klass
  end
  
  def self.get_enabled_validation_classes
    @@enabled_classes || []
  end
  
  def self.skip_validation_warnings?(klass, cfg)
    @@host_skip_warnings ||= {}
    
    h_alias = cfg.getProperty(DEPLOYMENT_HOST)
    unless @@host_skip_warnings.has_key?(h_alias)
      @@host_skip_warnings[h_alias]= cfg.getTemplateValue(SKIPPED_VALIDATION_WARNINGS)
    end
    return get_skipped_validation_warnings().include?(klass) || @@host_skip_warnings[h_alias].include?(klass)
  end
  
  def self.mark_skipped_validation_warnings(klass)
    @@skip_warnings << klass
  end
  
  def self.get_skipped_validation_warnings
    @@skip_warnings || []
  end
  
  def self.mark_enabled_validation_warnings(klass)
    @@enabled_warnings << klass
  end
  
  def self.get_enabled_validation_warnings
    @@enabled_warnings || []
  end
end

class ValidationError < RemoteError
  attr_reader :check
  
  def initialize(message, host, check)
    super(message, host)
    @check = check
  end
  
  def get_message
    "#{@message} (#{@check.class.name})"
  end
  
  def get_help
    @check.get_help()
  end
  
  def clean
    check.set_config(nil)
    
    if check.is_a?(GroupValidationCheckMember)
      check.set_group(nil)
    end
  end
end