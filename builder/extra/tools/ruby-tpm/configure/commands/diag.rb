class DiagnosticCommand
  include ConfigureCommand
  include RemoteCommand
  include ClusterCommandModule
  include ClusterConfigurationsModule
  include ClusterDiagnosticPackage
  
  def initialize(config)
    super(config)
    @skip_prompts = true
    @force = true
    @path = nil
  end
  
  def allow_command_line_cluster_options?
    false
  end
  
  def get_validation_checks
    [
      ActiveDirectoryIsRunningCheck.new(),
      ClusterStatusCheck.new(),
      ClusterDiagnosticCheck.new(),
      OldServicesRunningCheck.new()
    ]
  end
  
  def get_deployment_object_modules(config)
    [
    ]
  end
  
  def get_bash_completion_arguments
    super() + ["--to", "--path"]
  end
  
  def output_command_usage
    super()
  
    output_usage_line("--path", "Write the diagnostic package as a ZIP file in this path")
    output_usage_line("--to", "Send the diagnostic package as an attachment to this email address.")
  end
  
  def parsed_options?(arguments)
    arguments = super(arguments)
    
    @address = nil
    
    if display_help?() && !display_preview?()
      return arguments
    end
    
    opts=OptionParser.new
    opts.on("--to String") {|v| @address = v }
    opts.on("--path String") {|v| @path = v }
    remainder = Configurator.instance.run_option_parser(opts, arguments)
    
    unless @address == nil
      begin
        @sendmail_path = cmd_result("which sendmail")
      rescue CommandError
        error("Unable to find sendmail in the current path")
      end
    
      begin
        uuencode_path = cmd_result("which uuencode")
      rescue CommandError
        error("Unable to find uuencode in the current path")
      end
    
      unless @config.getProperty(ROOT_PREFIX) == "true"
        error("This command requires sudo access or must be run as root")
      end
    end
    
    return remainder
  end
  
  def validation_commit_error_header
    "Errors found during diagnostic test"
  end
  
  def output_completion_text
    notice("Diagnostic information written to #{get_diagnostic_file()}")
    
    unless @address == nil
      encodedcontent = [File.read(get_diagnostic_file())].pack("m")
      marker = "TUNGSTENPACKAGEMARKER"
      filename = File.basename(get_diagnostic_file())

      if @config.getProperty(ROOT_PREFIX) == "true"
        sendmail_prefix = "sudo -n"
      else
        sendmail_prefix = ""
      end

      body =<<EOF
From: #{@address}
To: #{@address}
Subject: Tungsten report package from #{@address}
MIME-Version: 1.0
Content-Type: multipart/mixed; boundary=#{marker}
--#{marker}
Content-Type: text/plain
Content-Transfer-Encoding:8bit

--#{marker}
Content-Type: application/zip; name=\"#{filename}\"
Content-Transfer-Encoding:base64
Content-Disposition: attachment; filename="#{filename}"

#{encodedcontent}
--#{marker}--
EOF

      report_tempfile = Tempfile.new("treport")
      report_tempfile.write(body)
      report_tempfile.close()

      cmd_result("cat #{report_tempfile.path()} | #{sendmail_prefix} #{@sendmail_path} -f#{@address} #{@address}")

      notice("Diagnostic package sent to #{@address}")
    end
    
    unless @path == nil
      begin
        FileUtils.cp(get_diagnostic_file(), @path)
      rescue => e
        exception(e)
        error("There was a problem copying the diagnostic file to #{@path}")
      end
    end
  end
  
  def self.get_command_name
    'diag'
  end
  
  def self.get_command_description
    "Package diagnostic information from the cluster to the current machine"
  end
end