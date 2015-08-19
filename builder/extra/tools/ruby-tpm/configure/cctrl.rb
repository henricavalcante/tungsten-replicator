class CCTRL
  COORDINATOR = "coordinator"
  ROUTERS = "routers"
  DATASOURCES = "datasources"
  MANAGER = "manager"
  REPLICATOR = "replicator"
  MASTER = "master"
  DATASERVER = "dataserver"
  STATUS = "status"
  STATE = "state"
  HOSTNAME = "hostname"
  ROLE = "role"
  SEQNO = "seqno"
  LATENCY = "latency"
  
  def initialize(cctrl_cmd)
    @cctrl_cmd = cctrl_cmd
    @props = nil
  end
  
  def parse
    if @props != nil
      return
    end
    
    @props = Properties.new()
    @props.use_prompt_handler = false
    
    ls_output = cmd_result("echo ls | #{@cctrl_cmd}")
    #@props.setProperty("raw", ls_output)
    
    coordinator = /COORDINATOR
      \[
      (\S+)
      :
      (\w+)
      :
      (\w+)
      \]/xm.match(ls_output)
    if coordinator
      @props.setProperty(COORDINATOR, {
        "host" => coordinator[1],
        "mode" => coordinator[2],
        "state" => coordinator[3]
      })
    end
    
    get_section(ls_output, 'ROUTERS').scan(/connector@
      (\S*)                       # hostname
      \[[0-9]*\]                  # process id
      \(
      (\S*)                       # status
      ,\s*
      created=([0-9]*),\s*
      active=([0-9]*)
      \)/xm){
      |router|
      
      @props.setProperty([ROUTERS, router[0]], {
        STATUS => router[1]
      })
    }
    
    get_compound_section(ls_output, 'DATASOURCES').split("\n\n").each{
      |ds|
      
      ds.scan(/
        \+-+\+\n
        (.+)
        \+-+\+\n
        (.+)
        \+-+\+
        /xm){
        |datasource|

        ds_hostname=nil
        ds_props={}
        datasource[0].scan(/
          \|(\S*)
          \(
          (\S*):(\S*),                # Role:Status
          [\s]progress=([\-0-9]*),      # Sequence number
          [\s](.*)=([0-9\.\-]*)         # Latency
          /xm){
          |m|
          
          ds_hostname=m[0]
          ds_props = {
            ROLE => m[1],
            STATUS => m[2],
            SEQNO => m[3],
            LATENCY => m[5]
          }
        }
        datasource[0].scan(/
          \|(\S*)
          \(
          ([a-zA-Z]*):([a-zA-Z]*)(:([a-zA-Z]*))?    # Role:Status[:State]
          /xm){
          |m|
          
          if ds_hostname == nil
            ds_hostname=m[0]
            ds_props = {
              ROLE => m[1],
              STATUS => m[2]
            }
          end
          
          if m[4].to_s() != ""
            ds_props[STATE] = m[4]
          end
        }
        datasource[0].scan(/
          STATUS[\s]?
          \[(\S*)\][\s]?    #
          \[(.*)\]          # Timestamp of data
          /xm){
          |m|
        }
        datasource[1].scan(/
          REPLICATOR\(role=(\S*),[\s]?state=(\S*)\)
          /xm){
          |m|
          
          ds_props[REPLICATOR] = {
            ROLE => m[0],
            STATUS => m[1]
          }
        }
        datasource[1].scan(/
          REPLICATOR\(role=(\S*),[\s]?master=(\S*),[\s]?state=(\S*)\)
          /xm){
          |m|
          
          ds_props[REPLICATOR] = {
            ROLE => m[0],
            MASTER => m[1],
            STATUS => m[2]
          }
        }
        datasource[1].scan(/
          MANAGER\(state=(\S*)\)
          /xm){
          |m|
          
          ds_props[MANAGER] = m[0]
        }
        datasource[1].scan(/
          DATASERVER\(state=(\S*)\)
          /xm){
          |m|
          
          ds_props[DATASERVER] = m[0]
        }
        
        @props.setProperty([DATASOURCES, ds_hostname], ds_props)
      }
    }
  end
  
  def get_section(ls_output, section)
    m = /#{section}:
      \s*\n               # optional space and EOL
      \+-+\+\n            # a dashed line
      (                   # capture
      .+?                 # many characters before the dashed line
      )                   # end capture
      \+-+\+\n            # a dashed_line
      /msx.match(ls_output)
    if m
      return m[1]
    else
      raise "Could not find #{section} section"
    end
  end
  
  def get_compound_section(ls_output, section)
    m = /#{section}:
      \n
      (
      \+
      -+
      .+
      -+\+\n
      )/msx.match(ls_output)
      
    if m
      return m[1]
    else
      raise "Could not find #{section} section"
    end
  end
  
  def to_s
    self.parse()
    return @props.to_s()
  end
  
  def to_a
    self.parse()
    return @props.props
  end
  
  def output()
    self.parse()
    Configurator.instance.output(self.to_s)
  end
  
  def force_output()
    self.parse()
    Configurator.instance.force_output(self.to_s)
  end
end