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

package com.continuent.tungsten.replicator.event;

import java.sql.Timestamp;

/**
 * Denotes header data used for replication. This is the core information used
 * to remember the replication position so that restart is possible.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface ReplDBMSHeader
{
    /**
     * Returns the log sequence number, a monotonically increasing whole number
     * starting at 0 that denotes a single transaction.
     */
    public long getSeqno();

    /**
     * Returns the event fragment number, a monotonically increasing whole
     * number starting at 0.
     */
    public short getFragno();

    /**
     * Returns true if this fragment is the last one.
     */
    public boolean getLastFrag();

    /**
     * Returns the ID of the data source from which this event was originally
     * extracted.
     */
    public String getSourceId();

    /**
     * Returns the epoch number, a number that identifies a continuous sequence
     * of events from the time a master goes online until it goes offline.
     */
    public long getEpochNumber();

    /**
     * Returns the native event ID corresponding to this log sequence number.
     */
    public String getEventId();

    /**
     * Returns the shard ID for this transaction.
     */
    public String getShardId();

    /**
     * Returns the extractedTstamp value.
     */
    public Timestamp getExtractedTstamp();

    /**
     * Returns the applied latency in seconds.
     */
    public long getAppliedLatency();
    
    /**
     * Not all DBMS types have this field for position, hence might be null.
     */
    public Timestamp getUpdateTstamp();

    /**
     * Not all DBMS types have this field for position, in which case -1 is
     * returned.
     */
    public Long getTaskId();
}