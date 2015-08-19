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
 * Contributor(s): Alex Yurchenko, Teemu Ollakka, Stephane Giron.
 */

package com.continuent.tungsten.replicator.conf;

import java.sql.Timestamp;
import java.util.ArrayList;

import com.continuent.tungsten.common.jmx.DynamicMBeanHelper;

/**
 * Replicator monitoring interface.  This interface contains in memory counters
 * used to show replication performance.  They reset whenever the replicator is 
 * restarted. 
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface ReplicatorMonitorMBean
{
    /**
     * Clear monitoring counters and timing intervals.  This ensures that values are
     * current when looking at transaction processing rates. 
     */
    public void clearCounters();

    /**
     * return TRUE IFF detailed statistics are enabled
     */
    public boolean getDetailEnabled();

    /**
     * set value for detail statistics gathering
     */
    public void setDetailEnabled(boolean detailEnabled);

    /**
     * Returns the monitoring start time in milliseconds.  
     */
    public long getStartTimeMillis(); 
    
    /**
     * Gets the length of the current monitoring interval in seconds. 
     */
    public double getMonitoringIntervalSecs();

    /**
     * Returns the number of events (extracted for extractor, applied for applier).
     */
    public long getEvents();

    /**
     * Returns the number of rows (extracted for extrator, applied for applier).
     */
    public long getRows();

    /**
     * Returns the number of transactions applied to target.
     */
    public long getApplied();

    /**
     * Returns the last sequence number that was applied.
     */
    public long getAppliedLastSeqNo();

    /**
     * Returns the latency of the last event processed in seconds, including
     * heartbeat events.
     */
    public double getAppliedLatency();
    
    /** 
     * 
     * @return the latest epoch number, the context of which is determined
     * by whether the replicator's role is master or slave.
     */
    public long getLatestEpochNumber();
   
    /** 
     * 
     * @return the latest event id, the context of which is determined
     * by whether the replicator's role is master or slave.
     */
    public String getLatestEventId();
   

    /**
     * Returns the average events applied per second.
     */
    public double getAppliedPerSec();

    /**
     * Returns the source timestamp of the last event that was applied. 
     */
    public Timestamp getAppliedLastSourceTStamp();
    
    /**
     * Returns the timestamp when the last event was applied. 
     */
    public Timestamp getAppliedLastTargetTStamp();
    
    /**
     * Returns the total events extracted. 
     */
    public long getExtracted();

    /**
     * Returns the last sequence number that was extracted.  
     */
    public long getExtractedLastSeqNo();

    /**
     * Returns the average events extracted per second.
     */
    public double getExtractedPerSec();

    /**
     * Returns the number of events received and placed in the THL. 
     */
    public long getReceived();

    /**
     * Returns the sequence number of the last event received. 
     */
    public long getReceivedLastSeqNo();

    /**
     * Returns the latency of the last event received in seconds. 
     */
    public double getReceivedLatency();

    /**
     * Returns the average events received per second.
     */
    public double getReceivedPerSec();

    /**
     * Returns the timestamp when the last event was received. 
     */
    public Timestamp getReceivedLastTargetTStamp();
    
    /**
     * Returns the source timestamp of the last event that was received. 
     */
    public Timestamp getReceivedLastSourceTStamp();
    
    /**
     * Returns the events cache total size.
     */
    public int getEventsTotalCacheSize();

    /**
     * Returns the events cache used size.
     */
    public int getEventsUsedCacheSize();

    /**
     * Returns used vs. total cache size ratio
     */
    public int getUsedCacheRatio();

    /**
     * Returns hash map of CPUTimes per thread
     */
    public ArrayList<MonitorThreadInfo> getCPUTimes();
    
    public DynamicMBeanHelper createHelper() throws Exception;
}
