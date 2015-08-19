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
 * Initial developer(s): Teemu Ollakka
 * Contributor(s): Robert Hodges, Linas Virbalas
 */

package com.continuent.tungsten.replicator.event;

import java.sql.Timestamp;
import java.util.ArrayList;

import com.continuent.tungsten.replicator.dbms.DBMSData;

/**
 * Storage class for replication events implementing full event management
 * metadata such as timestamp, source ID, epoch number, and event fragment
 * protocol.
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class ReplDBMSEvent extends ReplEvent implements ReplDBMSHeader
{
    static final long serialVersionUID = 1300;

    long              seqno;
    short             fragno;
    boolean           lastFrag;
    Timestamp         extractedTstamp;
    String            sourceId;
    String            shardId;
    long              epochNumber;
    DBMSEvent         event;

    /**
     * Construct a new replication event.
     * 
     * @param seqno Log sequence number
     * @param fragno Fragment number
     * @param lastFrag True if this is the last fragment
     * @param sourceId Originating source of data
     * @param epochNumber Epoch number on data
     * @param extractedTstamp Time of extraction
     * @param event Raw event data, which must always be supplied.
     */
    public ReplDBMSEvent(long seqno, short fragno, boolean lastFrag,
            String sourceId, long epochNumber, Timestamp extractedTstamp,
            DBMSEvent event)
    {
        // All fields must exist to protect against failures. We therefore
        // validate object instances.
        this.seqno = seqno;
        this.fragno = fragno;
        this.lastFrag = lastFrag;
        this.epochNumber = epochNumber;
        if (sourceId == null)
            this.sourceId = "NONE";
        else
            this.sourceId = sourceId;
        if (extractedTstamp == null)
            this.extractedTstamp = new Timestamp(System.currentTimeMillis());
        else
            this.extractedTstamp = extractedTstamp;
        if (event == null)
            this.event = new DBMSEvent();
        else
            this.event = event;
    }

    /**
     * Short constructor.
     */
    public ReplDBMSEvent(long seqno, DBMSEvent event)
    {
        this(seqno, (short) 0, true, "NONE", 0, new Timestamp(
                System.currentTimeMillis()), event);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.event.ReplDBMSHeader#getSeqno()
     */
    public long getSeqno()
    {
        return seqno;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.event.ReplDBMSHeader#getFragno()
     */
    public short getFragno()
    {
        return fragno;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.event.ReplDBMSHeader#getLastFrag()
     */
    public boolean getLastFrag()
    {
        return lastFrag;
    }

    /**
     * Gets the event data for this replicated event.
     */
    public ArrayList<DBMSData> getData()
    {
        if (event != null)
            return event.getData();
        else
            return new ArrayList<DBMSData>();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.event.ReplDBMSHeader#getSourceId()
     */
    public String getSourceId()
    {
        return sourceId;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.event.ReplDBMSHeader#getEpochNumber()
     */
    public long getEpochNumber()
    {
        return epochNumber;
    }

    /**
     * Returns the extractedTstamp value.
     * 
     * @return Returns the extractedTstamp.
     */
    public Timestamp getExtractedTstamp()
    {
        return extractedTstamp;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.event.ReplDBMSHeader#getEventId()
     */
    public String getEventId()
    {
        return event.getEventId();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.event.ReplDBMSHeader#getShardId()
     */
    public String getShardId()
    {
        String shardId = getDBMSEvent().getMetadataOptionValue(
                ReplOptionParams.SHARD_ID);
        if (shardId == null)
            return ReplOptionParams.SHARD_ID_UNKNOWN;
        else
            return shardId;
    }

    /**
     * Sets the shard ID. This can be assigned after the event is created.
     */
    public void setShardId(String shardId)
    {
        this.getDBMSEvent().setMetaDataOption(ReplOptionParams.SHARD_ID,
                shardId);
    }

    /**
     * Returns the raw DBMS event containing SQL data.
     */
    public DBMSEvent getDBMSEvent()
    {
        return event;
    }

    @Override
    public long getAppliedLatency()
    {
        return 0;
    }

    /**
     * Returns the value of a metadata option, if present.
     */
    public String getMetadataOption(String name)
    {
        DBMSEvent dbmsEvent = getDBMSEvent();
        if (dbmsEvent == null)
            return null;
        else
            return dbmsEvent.getMetadataOptionValue(name);
    }

    /**
     * Sets the value of a metadata option.
     */
    public void setMetadataOption(String name, String value)
    {
        DBMSEvent dbmsEvent = getDBMSEvent();
        if (dbmsEvent != null)
            dbmsEvent.setMetaDataOption(name, value);
    }

    /**
     * Not applicable.
     */
    public Timestamp getUpdateTstamp()
    {
        return null;
    }

    /**
     * Not applicable.
     */
    public Long getTaskId()
    {
        return null;
    }
}