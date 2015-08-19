class WatchFiles
  def self.watch_file(file, cfg)
    prepare_dir = cfg.getProperty(PREPARE_DIRECTORY)
    FileUtils.cp(file, WatchFiles.get_original_watch_file(file))
    if file =~ /#{prepare_dir}/
      file_to_watch = file.sub(prepare_dir, "")
      if file_to_watch[0, 1] == "/"
        file_to_watch.slice!(0)
      end 
    else
      file_to_watch = file
    end
    File.open("#{prepare_dir}/.watchfiles", "a") {
      |out|
      out.puts file_to_watch
    }

    if cfg.getProperty(PROTECT_CONFIGURATION_FILES) == "true"
      cmd_result("chmod o-rwx #{file}")
      cmd_result("chmod o-rwx #{WatchFiles.get_original_watch_file(file)}")
    end
  end
  
  def self.get_original_watch_file(file)
    File.dirname(file) + "/." + File.basename(file) + ".orig"
  end
  
  def self.show_differences(basedir)
    unless Configurator.instance.is_locked?()
      raise "Unable to show modified files because this is not the installed directory. If this is the staging directory, try running tpm from an installed Tungsten directory."
    end
    
    filename = basedir + "/.watchfiles"
    if File.exist?(filename)
      File.open(filename, 'r') do |file|
        file.read.each_line do |line|
          line.strip!

          unless line[0,1] == "/"
            line = "#{basedir}/#{line}"
          end
          current_file = line
          original_file = File.dirname(line) + "/." + File.basename(line) + ".orig"
          
          unless File.exist?(original_file)
            next
          end
          
          Configurator.instance.info("Compare #{current_file} to #{original_file}")
          begin
            file_differences = cmd_result("diff -u #{original_file} #{current_file}")
          rescue CommandError => ce
            if ce.rc == 1
              puts ce.result
            else
              raise ce
            end
          end
        end
      end
    else
      error("Unable to find #{filename}")
    end
  end
  
  def self.get_modified_files(basedir)
    filename = basedir + "/.watchfiles"
    if File.exist?(filename)
      modified_files = []
      File.open(filename, 'r') do |file|
        file.read.each_line do |line|
          line.strip!

          unless line[0,1] == "/"
            line = "#{basedir}/#{line}"
          end
          current_file = line
          original_file = File.dirname(line) + "/." + File.basename(line) + ".orig"
          
          unless File.exist?(original_file)
            next
          end
          
          Configurator.instance.debug("Compare #{current_file} to #{original_file}")
          begin
            file_differences = cmd_result("diff -u #{original_file} #{current_file}")
            # No differences
          rescue CommandError => ce
            if ce.rc == 1
              modified_files << line
            else
              raise ce
            end
          end
        end
      end
      
      return modified_files
    else
      return nil
    end
  end
end