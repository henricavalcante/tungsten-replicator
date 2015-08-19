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

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.thl.THLEvent;
import com.continuent.tungsten.replicator.thl.THLException;
import com.continuent.tungsten.replicator.thl.serializer.Serializer;

/**
 * This class encapsulates operations to write a log record header and
 * serialized THLEvent.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class LogEventReplWriter
{
    // Inputs
    private THLEvent   event;
    private Serializer serializer;
    private boolean    checkCRC;
    private File       file;

    /**
     * Instantiate the writer.
     */
    public LogEventReplWriter(THLEvent event, Serializer serializer,
            boolean checkCRC, File file) throws ReplicatorException
    {
        this.event = event;
        this.serializer = serializer;
        this.checkCRC = checkCRC;
        this.file = file;
    }

    /**
     * Write and return the log record.
     */
    public LogRecord write() throws ReplicatorException
    {
        LogRecord logRecord = new LogRecord(file, -1, checkCRC);
        try
        {
            DataOutputStream dos = new DataOutputStream(logRecord.write());
            dos.writeByte(LogRecord.EVENT_REPL);
            dos.writeLong(event.getSeqno());
            dos.writeShort(event.getFragno());
            dos.writeByte((event.getLastFrag() ? 1 : 0));
            dos.writeLong(event.getEpochNumber());
            dos.writeUTF(event.getSourceId());
            dos.writeUTF(event.getEventId());
            dos.writeUTF(event.getShardId());
            dos.writeLong(event.getSourceTstamp().getTime());

            serializer.serializeEvent(event, dos);
            dos.flush();
            logRecord.done();

            if (checkCRC)
                logRecord.storeCrc(LogRecord.CRC_TYPE_32);
        }
        catch (IOException e)
        {
            throw new THLException("Error writing log record data: "
                    + e.getMessage(), e);
        }
        finally
        {
            logRecord.done();
        }

        return logRecord;
    }
}