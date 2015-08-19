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
import com.continuent.tungsten.replicator.thl.THLException;

/**
 * This class encapsulates operations to write a log rotate event.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class LogEventRotateWriter
{
    // Inputs
    private File    file;
    private long    index;
    private boolean checkCRC;

    /**
     * Instantiate the writer.
     */
    public LogEventRotateWriter(File file, long index, boolean checkCRC)
            throws ReplicatorException
    {
        this.file = file;
        this.index = index;
        this.checkCRC = checkCRC;
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
            dos.writeByte(LogRecord.EVENT_ROTATE);
            dos.writeLong(index);
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