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

/**
 * This class encapsulates operations to read a log rotate event.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class LogEventRotateReader
{
    // Inputs
    private LogRecord       logRecord;
    private boolean         checkCRC;

    // Stream used to read the event.
    private DataInputStream dis;

    // Fields
    private byte            recordType;
    private long            index;

    /**
     * Instantiate the reader and load header information.
     */
    public LogEventRotateReader(LogRecord logRecord, boolean checkCRC)
            throws ReplicatorException, IOException
    {
        this.logRecord = logRecord;
        this.checkCRC = checkCRC;
        load();
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
        index = dis.readLong();

        if (recordType != LogRecord.EVENT_ROTATE)
            throw new IOException("Invalid log record type reader: offset="
                    + logRecord.getOffset() + " type=" + recordType);
    }

    public byte getRecordType()
    {
        return recordType;
    }

    public long getIndex()
    {
        return index;
    }

    /** Release the log record. */
    public void done()
    {
        logRecord.done();
        logRecord = null;
    }
}