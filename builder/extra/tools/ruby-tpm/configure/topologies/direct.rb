class DirectTopology
  include Topology
  
  def get_role(hostname)
    REPL_ROLE_DI
  end
  
  def self.get_name
    'direct'
  end
end