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

package com.continuent.tungsten.replicator.thl.log;

import java.io.DataInputStream;
import java.io.IOException;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.thl.THLEvent;
import com.continuent.tungsten.replicator.thl.THLException;
import com.continuent.tungsten.replicator.thl.serializer.Serializer;

/**
 * This class encapsulates operations to read a log record header and serialized
 * THLEvent for an event. It automatically reads the header but does not
 * deserialize the event until asked to. You should call done() after use to
 * free resources.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class LogEventReplReader
{
    // Inputs
    private LogRecord       logRecord;
    private Serializer      serializer;
    private boolean         checkCRC;

    // Stream used to read the event.
    private DataInputStream dis;

    // Header fields
    private byte            recordType;
    private long            seqno;
    private short           fragno;
    private boolean         lastFrag;
    private long            epochNumber;
    private String          sourceId;
    private String          eventId;
    private String          shardId;
    private Long            sourceTStamp;

    /**
     * Instantiate the reader and load header information.
     */
    public LogEventReplReader(LogRecord logRecord, Serializer serializer,
            boolean checkCRC) throws ReplicatorException
    {
        this.logRecord = logRecord;
        this.serializer = serializer;
        this.checkCRC = checkCRC;
        try
        {
            load();
        }
        catch (IOException e)
        {
            throw new THLException(
                    "I/O error while loading log record header: offset="
                            + logRecord.getOffset(), e);
        }
    }

    // Load header fields.
    private void load() throws ReplicatorException, IOException
    {
        // Check CRC if requested.
        if (checkCRC)
        {
            logRecord.verifyChecksum();
        }

        // Read the header fields.
        dis = new DataInputStream(logRecord.read());
        recordType = dis.readByte();
        if (recordType != LogRecord.EVENT_REPL)
            throw new THLException("Invalid log record type reader: offset="
                    + logRecord.getOffset() + " type=" + recordType);
        seqno = dis.readLong();
        fragno = dis.readShort();
        lastFrag = (dis.readByte() == 1);
        epochNumber = dis.readLong();
        sourceId = dis.readUTF();
        eventId = dis.readUTF();
        shardId = dis.readUTF();
        sourceTStamp = dis.readLong();
    }

    public LogRecord getLogRecord()
    {
        return logRecord;
    }

    public byte getRecordType()
    {
        return recordType;
    }

    public long getSeqno()
    {
        return seqno;
    }

    public short getFragno()
    {
        return fragno;
    }

    public boolean isLastFrag()
    {
        return lastFrag;
    }

    public long getEpochNumber()
    {
        return epochNumber;
    }

    public String getSourceId()
    {
        return sourceId;
    }

    public String getEventId()
    {
        return eventId;
    }

    public String getShardId()
    {
        return shardId;
    }

    public Long getSourceTStamp()
    {
        return sourceTStamp;
    }

    /** Deserialize and return the event. */
    public THLEvent deserializeEvent() throws ReplicatorException
    {
        try
        {
            THLEvent thlEvent = serializer.deserializeEvent(dis);
            return thlEvent;
        }
        catch (IOException e)
        {
            throw new THLException("Unable to deserialize event", e);
        }
    }

    /** Release the log record. */
    public void done()
    {
        logRecord.done();
        logRecord = null;
    }
}