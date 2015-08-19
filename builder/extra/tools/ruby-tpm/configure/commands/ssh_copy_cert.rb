class SSHCopyCertCommand
  include ConfigureCommand
  
  def run
    puts "mkdir -p ~/.ssh"
    puts "echo \"#{cmd_result("cat ~/.ssh/id_rsa")}\" > ~/.ssh/id_rsa"
    puts "echo \"#{cmd_result("cat ~/.ssh/id_rsa.pub")}\" > ~/.ssh/id_rsa.pub"
    puts "touch ~/.ssh/authorized_keys"
    puts "cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys"
    puts "chmod 700 ~/.ssh"
    puts "chmod 600 ~/.ssh/*"
  end
  
  def self.display_command
    false
  end
  
  def self.get_command_name
    'ssh-copy-cert'
  end
  
  def self.get_command_description
    "Display commands to install the local id_rsa and id_rsa.pub on another machine. Copy and paste the results into another terminal."
  end
end