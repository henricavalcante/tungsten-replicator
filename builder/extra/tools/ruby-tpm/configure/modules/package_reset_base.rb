module ResetBasenamePackageModule
  def load_prompts
    unless @config.getNestedProperty([CONFIG_TARGET_BASENAME]) == Configurator.instance.get_base_path
      @config.setProperty(CONFIG_TARGET_BASENAME, nil)
      @config.setProperty([SYSTEM, CONFIG_TARGET_BASENAME], nil)
      @config.setProperty(CONFIG_TARGET_BASENAME, @config.getProperty(CONFIG_TARGET_BASENAME))
      @config.setProperty([SYSTEM, CONFIG_TARGET_BASENAME], @config.getProperty(CONFIG_TARGET_BASENAME))
    end
    
    super()
  end
end