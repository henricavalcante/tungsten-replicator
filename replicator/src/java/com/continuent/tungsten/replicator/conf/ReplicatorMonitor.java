/**
 * VMware Continuent Tungsten Replicator
 * Copyright (C) 2015 VMware, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *      
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Initial developer(s): Robert Hodges
 * Contributor(s): Stephane Giron.
 */
package com.continuent.tungsten.replicator.conf;

import java.lang.management.ThreadInfo;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.continuent.tungsten.common.jmx.DynamicMBeanHelper;
import com.continuent.tungsten.common.jmx.JmxManager;
import com.continuent.tungsten.common.jmx.MethodDesc;

/**
 * This class implements JMX monitoring statistics for the replicator.  
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ReplicatorMonitor implements ReplicatorMonitorMBean
{
    // All values are volatile to ensure visibility across threads. 
    private volatile long startTimeMillis;

    //private static Logger      logger          = Logger.getLogger(ReplicatorMonitor.class);
    private volatile long applied;
    private volatile Timestamp appliedLastTargetTStamp;
    private volatile Timestamp appliedLastSourceTStamp;
    private volatile long appliedLastSeqNo;

    private volatile long extracted;
    private volatile long extractedLastSeqNo;
    
    private volatile long latestEpochNumber;
    private String latestEventId = "0:0";

    private volatile long received;
    private volatile Timestamp receivedLastTargetTStamp;
    private volatile Timestamp receivedLastSourceTStamp;
    private volatile long receivedLastSeqNo;

    private volatile int eventsTotalCacheSize = 0;
    private volatile int eventsUsedCacheSize = 0;

    private volatile long events = 0L; // events applied for applier, extracted for extractor
    private volatile long rows = 0L; // number of rows applied for applier, extractor for extractor

    private volatile boolean reportInterestingOnly = true;
    private volatile HashMap<Long, MonitorThreadInfo> threadInfos = new HashMap<Long, MonitorThreadInfo>();
    private volatile boolean detailEnabled = false;

    // NOTE: When adding a new member to these statistics, MonitorThreadInfo.java needs to be recompiled!!!
    static public final int      CPU_MSG_SERIAL    =  0; // Time spent serializing Events on master
    static public final int      CPU_MSG_DESERIAL  =  1; // Time spent deserializing Events on slave
    static public final int      CPU_DB_SERIAL     =  2; // Time spent serializing Events going into database
    static public final int      CPU_DB_DESERIAL   =  3; // Time spent deserializing Events coming out of database
    static public final int      CPU_EXTRACT       =  4; // Time spent extracting changes from log
    static public final int      CPU_INSERTTHL     =  5; // Time spent Ins into THL (insert NOT serialization part)
    static public final int      CPU_CHECKSUM      =  6; // Time spent computing checksum
    static public final int      CPU_APPLIERBUSQL  =  7; // Time spent building client SQL statement
    static public final int      CPU_APPLIERPRSQL  =  8; // Time spent preparing client SQL statement
    static public final int      CPU_APPLIERBISQL  =  9; // Time spent binding client SQL statement
    static public final int      CPU_APPLIEREXSQL  = 10; // Time spent execuing client SQL statement
    static public final int      CPU_APPLIERGEVENT = 11; // Time spent getting event
    static public final int      CPU_APPLIERAEVENT = 12; // Time spent applying event
    static public final int      CPU_APPLIERMEVENT = 13; // Time spent marking event
    static public final int      CPU_APPLIERSTRANS = 14; // Time spent starting transaction
    static public final int      CPU_COUNT         = 15; // Number of cpu time metrics
    static public final String[] cpuNames = {"MsgSerial", "MsgDeSer", "DBSerial", "DBDeSer", "Extract",
                                             "InsTHL", "CheckSum", "AplBuSQL", "AplPrSQL", "AplBiSQL",
                                             "AplExSQL", "AplGEvt", "AplAEvt", "AplMEvt", "AplBTran"};

    static public final int      REAL_THLSRVWAIT   =  0; // Time spent by THLSRV waiting for send to complete
    static public final int      REAL_THLCLIWAIT   =  1; // Time spent by THLCLI waiting for receive to complete 
    static public final int      REAL_WAIT4EXT     =  2; // Time spent waiting for extractor to fill THL
    static public final int      REAL_WAIT4THLCLI  =  3; // Time spent waiting for THLCli to fill THL
    static public final int      REAL_EXTHEAD      =  4; // Time spent reading binlog header
    static public final int      REAL_EXTBODY      =  5; // Time spent reading binlog body
    static public final int      REAL_INSWAIT      =  6; // Time spent waiting for Mysql to INSERT
    static public final int      REAL_UPDWAIT      =  7; // Time spent waiting for Mysql to UPDATE
    static public final int      REAL_DELWAIT      =  8; // Time spent waiting for Mysql to DELETE
    static public final int      REAL_COMMIT       =  9; // Time spent on waiting for commit in applier
    static public final int      REAL_APPLY        = 10; // Time spent applying event
    static public final int      REAL_EXTRACT      = 11; // Time spent extracting event
    static public final int      REAL_COUNT        = 12; // Number of real time metrics
    static public final String[] realNames = {"TSrvSnd", "TCliRcv", "ExtrWait", "THLWait", "ExtHead",
                                              "ExtBody", "InsWait", "UpdWait", "DelWait", "ComWait",
                                              "Apply", "Extract"};

    static public final int      COUNT_RECORDS_PER_EVENT  = 0; // Records for each event
    static public final int      COUNT_EVENTS_PER_MESSAGE = 1; // Events transmitted per message
    static public final int      COUNT_EVENTS_PER_COMMIT  = 2; // Events committed per commit
    static public final int      COUNT_COUNT              = 3; // Number of count metrics
    static public final String[] countNames = {"RowPerEvent", "EvtPerMess", "EvtPerCom"};
    
    
    static private final String INSTANCE_PRIMARY = "primary";
    
    static private Map<String, ReplicatorMonitor>  instanceMap = new HashMap<String, ReplicatorMonitor>();

    /**
     * Singleton implementation
     * 
     * @return singleton ReplicatorMonitor
     */
    public static synchronized ReplicatorMonitor getInstance(String instanceId, boolean doClear)
    {
        ReplicatorMonitor instance = instanceMap.get(instanceId);
        
        if (instance == null)
        {
            instance = new ReplicatorMonitor();
            instanceMap.put(instanceId, instance);
        }
        
        if (doClear)
        {
            instance.clearCounters();
        }
        
        return instance;
    }
    
    /**
     * 
     * Default method to use for normal operations
     * 
     * @return 'primary' instance of the monitor.
     */
    public static ReplicatorMonitor getInstance()
    {
        return getInstance(INSTANCE_PRIMARY, false);
    }
    
    /** Creates a new monitor, which automatically sets the start time. */
    private ReplicatorMonitor()
    {
        clearCounters();
    }

    public boolean getDetailEnabled()
    {
        return detailEnabled;
    }

    public void setDetailEnabled(boolean detailEnabled)
    {
        this.detailEnabled = detailEnabled;
    }

    public void incrementEvents(int numberOfRows)
    {
        events++;
        rows += (long)numberOfRows;
    }

    public long getEvents()
    {
        return events;
    }

    public long getRows()
    {
        return rows;
    }

    public void recordEvent(int eventID)
    {
        recordEvent(eventID, 1);
    }

    public void recordEvent(int eventID, long count)
    {
        long                              threadID;
        MonitorThreadInfo threadInfo;

        if (!detailEnabled) return;
        threadID = Thread.currentThread().getId();

        synchronized (threadInfos)
        {
          if ((threadInfo = threadInfos.get(threadID)) == null)
          {
             threadInfo = new MonitorThreadInfo(0, "", 0L);
             threadInfos.put(threadID, threadInfo);
          }
        }
        threadInfo.recordEvent(eventID, count);
    }

    public long startCPUEvent(int eventID)
    {
        java.lang.management.ThreadMXBean tmxbean;
        if (!detailEnabled) return 0L;
        tmxbean = java.lang.management.ManagementFactory.getThreadMXBean();
        return tmxbean.getCurrentThreadCpuTime();
    }

    public void stopCPUEvent(int eventID, long startingToken)
    {
        long                              threadID;
        java.lang.management.ThreadMXBean tmxbean;
        long stopCPUTime;
        MonitorThreadInfo threadInfo;
        long value;

        if (!detailEnabled) return;
        threadID = Thread.currentThread().getId();
        tmxbean = java.lang.management.ManagementFactory.getThreadMXBean();
       
        stopCPUTime = tmxbean.getCurrentThreadCpuTime();
        if (stopCPUTime < startingToken) return;

        synchronized (threadInfos)
        {
          if ((threadInfo = threadInfos.get(threadID)) == null)
          {
             threadInfo = new MonitorThreadInfo(0, "", 0L);
             threadInfos.put(threadID, threadInfo);
          }
        }
        value = stopCPUTime - startingToken;
        threadInfo.cpuTimeEvent(eventID, value);
    }

    public long startRealEvent(int eventID)
    {
        if (!detailEnabled) return 0L;
        long retval = System.nanoTime();
        return retval;
    }

    public void stopRealEvent(int eventID, long startingToken)
    {
        long                              threadID;
        long stopTime;
        MonitorThreadInfo threadInfo;
        long value;
        if (!detailEnabled) return;

        threadID = Thread.currentThread().getId();
       
        stopTime = System.nanoTime();

        if (stopTime < startingToken) return;

        synchronized (threadInfos)
        {
          if ((threadInfo = threadInfos.get(threadID)) == null)
          {
             threadInfo = new MonitorThreadInfo(0, "", 0L);
             threadInfos.put(threadID, threadInfo);
          }
        }
        value = stopTime - startingToken;
        threadInfo.realTimeEvent(eventID, value);
    }

    /** Increment the number of applied transactions. */
    public void incrementApplied()
    {
        applied++;
    }
    public void setAppliedLastTargetTStamp(Timestamp appliedLastTargetTStamp)
    {
        this.appliedLastTargetTStamp = appliedLastTargetTStamp;
    }
    public void setAppliedLastSourceTStamp(Timestamp appliedLastSourceTStamp)
    {
        this.appliedLastSourceTStamp = appliedLastSourceTStamp;
    }
    public void setAppliedLastSeqNo(long lastSeqNoApplied)
    {
        this.appliedLastSeqNo = lastSeqNoApplied;
    }

    public void incrementExtracted()
    {
        this.extracted++;
    }
    public void setExtractedLastSeqNo(long lastSeqNoExtracted)
    {
        this.extractedLastSeqNo = lastSeqNoExtracted;
    }

    /** Increment the number of received transactions. */
    public void incrementReceived()
    {
        received++;
    }
    public void setReceivedLastSeqNo(long lastSeqNoReceived)
    {
        this.receivedLastSeqNo = lastSeqNoReceived;
    }
    public void setReceivedLastTargetTStamp(Timestamp receivedLastTargetTStamp)
    {
        this.receivedLastTargetTStamp = receivedLastTargetTStamp;
    }
    public void setReceivedLastSourceTStamp(Timestamp receivedLastSourceTStamp)
    {
        this.receivedLastSourceTStamp = receivedLastSourceTStamp;
    }

    public void clearCounters()
    {
        startTimeMillis = System.currentTimeMillis();

        applied = 0;
        appliedLastTargetTStamp = null;
        appliedLastSourceTStamp = null;
        appliedLastSeqNo = -1;

        extracted = 0;
        extractedLastSeqNo = -1;

        received = 0;
        receivedLastTargetTStamp = null;
        receivedLastSourceTStamp = null;
        receivedLastSeqNo = -1;
        
        // Not sure it makes sense to clear these during performance runs
        // since, once cleared, they will never be right again.
        // eventsTotalCacheSize = 0;
        // eventsUsedCacheSize = 0;
    }
    public long getStartTimeMillis()
    {
        return startTimeMillis;
    }
    public double getMonitoringIntervalSecs()
    {
        return (System.currentTimeMillis() - startTimeMillis) / 1000.0;
    }

    public long getApplied()
    {
        return applied;
    }
    public double getAppliedLatency()
    {
        if (appliedLastTargetTStamp == null || appliedLastSourceTStamp == null)
            return 0.0;
        else
        {
            // Latency could be less than 0 if clocks are off. 
            double latency = (appliedLastTargetTStamp.getTime() - appliedLastSourceTStamp
                    .getTime()) / 1000.0;
            if (latency < 0.0)
                latency = 0;
            return latency;
        }
    }
    public double getAppliedPerSec()
    {
        return applied / this.getMonitoringIntervalSecs();
    }
    public Timestamp getAppliedLastTargetTStamp()
    {
        return appliedLastTargetTStamp;
    }
    public Timestamp getAppliedLastSourceTStamp()
    {
        return appliedLastSourceTStamp;
    }
    public long getAppliedLastSeqNo()
    {
        return appliedLastSeqNo;
    }

    public long getExtracted()
    {
        return extracted;
    }
    public long getExtractedLastSeqNo()
    {
        return extractedLastSeqNo;
    }
    public double getExtractedPerSec()
    {
        return extracted / getMonitoringIntervalSecs();
    }
    
    public long getReceived()
    {
        return received;
    }
    public long getReceivedLastSeqNo()
    {
        return receivedLastSeqNo;
    }
    public double getReceivedLatency()
    {
        if (receivedLastTargetTStamp == null || receivedLastSourceTStamp == null)
            return 0.0;
        else
        {
            // Latency could be less than 0 if clocks are off. 
            double latency = (receivedLastTargetTStamp.getTime() - receivedLastSourceTStamp
                    .getTime()) / 1000.0;
            if (latency < 0.0)
                latency = 0;
            return latency;
        }
    }
    public double getReceivedPerSec()
    {
        return received / this.getMonitoringIntervalSecs();
    }
    public Timestamp getReceivedLastTargetTStamp()
    {
        return receivedLastTargetTStamp;
    }
    public Timestamp getReceivedLastSourceTStamp()
    {
        return receivedLastSourceTStamp;
    }
    public void setCacheSize(int size)
    {
        eventsTotalCacheSize = size;
    }
    public synchronized void increaseUsedCacheSize()
    {
        eventsUsedCacheSize++;
    }
    public synchronized void decreaseUsedCacheSize()
    {
        eventsUsedCacheSize--;
    }
    public int getEventsTotalCacheSize()
    {
        return eventsTotalCacheSize;
    }
    public int getEventsUsedCacheSize()
    {
        return eventsUsedCacheSize;
    }
    public int getUsedCacheRatio()
    {
        return (int) (100 * (double) eventsUsedCacheSize / (double) eventsTotalCacheSize);
    }
    public synchronized ArrayList<MonitorThreadInfo> getCPUTimes()
    {
        java.lang.management.ThreadMXBean tmxbean;
        ArrayList<MonitorThreadInfo> retval = new ArrayList<MonitorThreadInfo>();
        long[] threadIDs;
        ThreadInfo threadInfo;
        long cpuTime;
        long threadID;
        MonitorThreadInfo mti;
        String threadName;

        tmxbean = java.lang.management.ManagementFactory.getThreadMXBean();
        threadIDs = tmxbean.getAllThreadIds();

        for (int idx = 0; idx < threadIDs.length; idx++)
        {
            threadID = threadIDs[idx];
            cpuTime = tmxbean.getThreadCpuTime(threadID);
            threadInfo = tmxbean.getThreadInfo(threadID);
            threadName = threadInfo.getThreadName();
          
            if (reportInterestingOnly &&
                !threadName.equals(MonitorThreadInfo.THLSRV) &&
                !threadName.equals(MonitorThreadInfo.THLCLI) &&
                !threadName.equals(MonitorThreadInfo.APPLIER) &&
                !threadName.equals(MonitorThreadInfo.EXTRACTOR)) continue;

            if ((mti = threadInfos.get(threadID)) == null)
            {
               mti = new MonitorThreadInfo(threadID, threadName, cpuTime);
            } else {
               mti.setName(threadName);
               mti.setThreadID(threadID);
               mti.setCPUTime(cpuTime);
            }

            retval.add(mti);
        }
        return retval;
    }
    
    public long getLatestEpochNumber()
    {
        return latestEpochNumber;
    }

    public void setLatestEpochNumber(long epochNumber)
    {
        this.latestEpochNumber = epochNumber;
    }

    public String getLatestEventId()
    {
        return latestEventId;
    }

    public void setLatestEventId(String eventId)
    {
        this.latestEventId = eventId;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.conf.ReplicatorMonitorMBean#createHelper()
     */
    @MethodDesc(description = "Returns a DynamicMBeanHelper to facilitate dynamic JMX calls", usage = "createHelper")
    public DynamicMBeanHelper createHelper() throws Exception
    {
        return JmxManager.createHelper(getClass());
    }

   
}





