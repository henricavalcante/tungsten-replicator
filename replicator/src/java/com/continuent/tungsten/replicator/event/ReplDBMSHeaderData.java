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
 * Contributor(s): Linas Virbalas
 */

package com.continuent.tungsten.replicator.event;

import java.sql.Timestamp;

/**
 * An implementation of replicator header information used to track position.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ReplDBMSHeaderData implements ReplDBMSHeader
{
    private final long      seqno;
    private final short     fragno;
    private final boolean   lastFrag;
    private final String    sourceId;
    private final long      epochNumber;
    private final String    eventId;
    private final String    shardId;
    private final Timestamp extractedTstamp;
    private final long      appliedLatency;

    // Not all DBMS types have the following fields:
    private Timestamp       updateTstamp = null;
    private Long            taskId       = null;

    /**
     * Create extensive header instance from component parts.
     */
    public ReplDBMSHeaderData(long seqno, short fragno, boolean lastFrag,
            String sourceId, long epochNumber, String eventId, String shardId,
            Timestamp extractedTstamp, long latency, Timestamp updateTimestamp,
            long taskId)
    {
        this.seqno = seqno;
        this.fragno = fragno;
        this.lastFrag = lastFrag;
        this.sourceId = sourceId;
        this.epochNumber = epochNumber;
        this.eventId = eventId;
        this.shardId = shardId;
        this.extractedTstamp = extractedTstamp;
        this.appliedLatency = latency;
        this.updateTstamp = updateTimestamp;
        this.taskId = taskId;
    }
    
    /**
     * Create header instance from component parts.
     */
    public ReplDBMSHeaderData(long seqno, short fragno, boolean lastFrag,
            String sourceId, long epochNumber, String eventId, String shardId,
            Timestamp extractedTstamp, long latency)
    {
        this.seqno = seqno;
        this.fragno = fragno;
        this.lastFrag = lastFrag;
        this.sourceId = sourceId;
        this.epochNumber = epochNumber;
        this.eventId = eventId;
        this.shardId = shardId;
        this.extractedTstamp = extractedTstamp;
        this.appliedLatency = latency;
    }

    public ReplDBMSHeaderData(ReplDBMSHeader event)
    {
        this.seqno = event.getSeqno();
        this.fragno = event.getFragno();
        this.lastFrag = event.getLastFrag();
        this.sourceId = event.getSourceId();
        this.epochNumber = event.getEpochNumber();
        this.eventId = event.getEventId();
        this.shardId = event.getShardId();
        this.extractedTstamp = event.getExtractedTstamp();
        this.appliedLatency = event.getAppliedLatency();
    }

    public long getSeqno()
    {
        return seqno;
    }

    public String getEventId()
    {
        return eventId;
    }

    public long getEpochNumber()
    {
        return epochNumber;
    }

    public short getFragno()
    {
        return fragno;
    }

    public boolean getLastFrag()
    {
        return lastFrag;
    }

    public String getSourceId()
    {
        return sourceId;
    }

    public String getShardId()
    {
        return shardId;
    }

    public Timestamp getExtractedTstamp()
    {
        return extractedTstamp;
    }

    @Override
    public long getAppliedLatency()
    {
        return appliedLatency;
    }

    public Timestamp getUpdateTstamp()
    {
        return updateTstamp;
    }

    public Long getTaskId()
    {
        return taskId;
    }
}