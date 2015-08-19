class ClusterAliasTopology
  include Topology
  
  def use_replicator?
    false
  end
  
  def self.get_name
    'cluster-alias'
  end
end