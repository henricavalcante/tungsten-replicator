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
 * Initial developer(s): Stephane Giron
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.replicator.event;

import java.sql.Timestamp;

/**
 * Implements a filtered event, which represents a gap in the transaction
 * sequence. Missing sequence numbers can result in ambiguities in processing
 * the log--for example, are we missing a seqno because the log is corrupt or
 * because it was filtered out. This class holds a record of the missing
 * transactions in order to remove such ambiguity.
 * <p/>
 * Generally speaking this class marks position using the first event in the
 * beginning of the range. We also store the end seqno and fragno to show the
 * extent of the range. Both beginning and end seqno/frago are inclusive.
 * <p/>
 * For now filtering less-than-whole transactions is not support and may cause
 * unpredictable replicator behavior.
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class ReplDBMSFilteredEvent extends ReplDBMSEvent
{
    private long  seqnoEnd  = -1;
    private short fragnoEnd = -1;

    /**
     * Simple method to instantiate.  This should only be used for testing as
     * well as cases where we do not have full restart information as it 
     * will lead to restart problems if stored in the log or in the applier
     * table (usually trep_commit_seqno) used to restart replication. 
     */
    public ReplDBMSFilteredEvent(String lastFilteredId,
            Long firstFilteredSeqno, Long lastFilteredSeqno, Short lastFragno)
    {
        super(firstFilteredSeqno, new DBMSEvent(lastFilteredId));
        this.seqnoEnd = lastFilteredSeqno;
        this.fragnoEnd = lastFragno;
    }

    /**
     * Method to instantiate a filtered event from header data of the first
     * event in the sequence plus the end seqno and fragno of the last event.
     * 
     * @param firstFilteredSeqno First filtered seqno
     * @param firstFilteredFragno First filtered fragno
     * @param lastFilteredSeqno Last seqno that is filtered
     * @param lastFilteredFragno Last fragno on that event
     * @param lastFrag Whether this is the last fragment or not -- does not seem
     *            to mean much for filtered transactions
     * @param eventId Restart point in log
     * @param sourceId Source of the first transaction
     * @param timestamp Commit timestamp
     * @param epochNumber Epoch number of the first event
     */
    public ReplDBMSFilteredEvent(Long firstFilteredSeqno,
            Short firstFilteredFragno, Long lastFilteredSeqno,
            Short lastFilteredFragno, boolean lastFrag, String eventId,
            String sourceId, Timestamp timestamp, long epochNumber)
    {
        super(firstFilteredSeqno, new DBMSEvent(eventId, null, timestamp));
        this.seqnoEnd = lastFilteredSeqno;
        this.fragno = firstFilteredFragno;
        this.fragnoEnd = lastFilteredFragno;
        this.lastFrag = lastFrag;
        this.sourceId = sourceId;
        this.extractedTstamp = timestamp;
        this.epochNumber = epochNumber;
    }

    /**
     * Standard way to instantiate a filtered event using the headers from the
     * first and last event.
     * 
     * @param firstFilteredEvent First filtered event in stream
     * @param lastFilteredEvent Last filtered event in stream
     */
    public ReplDBMSFilteredEvent(ReplDBMSHeader firstFilteredEvent,
            ReplDBMSHeader lastFilteredEvent)
    {
        super(firstFilteredEvent.getSeqno(), new DBMSEvent(
                lastFilteredEvent.getEventId()));
        this.seqnoEnd = lastFilteredEvent.getSeqno();
        this.fragno = firstFilteredEvent.getFragno();
        this.fragnoEnd = lastFilteredEvent.getFragno();
        this.lastFrag = lastFilteredEvent.getLastFrag();
        this.sourceId = firstFilteredEvent.getSourceId();
        this.epochNumber = firstFilteredEvent.getEpochNumber();
        this.shardId = firstFilteredEvent.getShardId();
        // Use last extracted timestamp so we get a more accurate
        // latency in case we skip many transactions.
        this.extractedTstamp = lastFilteredEvent.getExtractedTstamp();
    }

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Returns the seqnoEnd value.
     * 
     * @return Returns the seqnoEnd.
     */
    public long getSeqnoEnd()
    {
        return seqnoEnd;
    }

    public void updateCommitSeqno()
    {
        this.seqno = this.seqnoEnd;
    }

    /**
     * Returns the fragnoEnd value.
     * 
     * @return Returns the fragnoEnd.
     */
    public short getFragnoEnd()
    {
        return fragnoEnd;
    }

}
