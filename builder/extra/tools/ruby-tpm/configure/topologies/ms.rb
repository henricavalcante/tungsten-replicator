class MasterSlaveTopology
  include Topology
  
  def self.get_name
    'master-slave'
  end
  
  def self.is_default?
    if Configurator.instance.is_enterprise?()
      false
    else
      true
    end
  end
end