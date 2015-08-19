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
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.pipeline;

/**
 * Tracks statistics for an individual shard, which is identified by a shard ID.
 *
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ShardProgress
{
    private final String shardId;
    private final String stageName;
    private long         lastSeqno          = -1;
    private String       lastEventId;
    private long         eventCount         = 0;
    private long         applyLatencyMillis = 0;

    /**
     * Defines a new task progress tracker for the given shard
     *
     * @param shardId ID of the shard
     * @param stageName Name of stage that processes this shard
     * @param taskId Task ID number
     */
    ShardProgress(String shardId, String stageName)
    {
        this.shardId = shardId;
        this.stageName = stageName;
    }

    public String getShardId()
    {
        return this.shardId;
    }

    public String getStageName()
    {
        return this.stageName;
    }

    public long getLastSeqno()
    {
        return lastSeqno;
    }

    public void setLastSeqno(long seqno)
    {
        this.lastSeqno = seqno;
    }

    public String getLastEventId()
    {
        return lastEventId;
    }

    public void setLastEventId(String lastEventId)
    {
        this.lastEventId = lastEventId;
    }

    public long getEventCount()
    {
        return eventCount;
    }

    public void setEventCount(long eventCount)
    {
        this.eventCount = eventCount;
    }

    public void incrementEventCount()
    {
        this.eventCount++;
    }

    /** Return apply latency in milliseconds. Sub-zero values are rounded to 0. */
    public long getApplyLatencyMillis()
    {
        // Latency may be sub-zero due to clock differences.
        if (applyLatencyMillis < 0)
            return 0;
        else
            return applyLatencyMillis;
    }

    /** Return apply latency in seconds. */
    public double getApplyLatencySeconds()
    {
        long applyLatencyMillis = getApplyLatencyMillis();
        return applyLatencyMillis / 1000.0;
    }

    public void setApplyLatencyMillis(long applyLatencyMillis)
    {
        this.applyLatencyMillis = applyLatencyMillis;
    }

    /**
     * Returns a shallow copy of this instance.
     */
    public ShardProgress clone()
    {
        ShardProgress clone = new ShardProgress(shardId, stageName);
        clone.setLastSeqno(lastSeqno);
        clone.setLastEventId(lastEventId);
        clone.setApplyLatencyMillis(applyLatencyMillis);
        clone.setEventCount(eventCount);
        return clone;
    }
}
