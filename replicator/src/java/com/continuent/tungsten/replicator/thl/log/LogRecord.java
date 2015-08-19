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
 *
 */

package com.continuent.tungsten.replicator.thl.log;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

/**
 * Encapsulates a log record from the Tungsten disk log.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class LogRecord
{
    /**
     * Number of bytes in length field plus CRC. The record length is this
     * number plus the number of bytes of data (currently 4 + 1 + 8).
     */
    public static final int       NON_DATA_BYTES = 13;

    /** Denotes record header information. */
    public static final byte      EVENT_REPL     = 0x01;

    /** Denotes a replication event */
    public static final byte      EVENT_ROTATE   = 0x02;

    /** Record does not have a CRC computed. */
    public static final byte      CRC_TYPE_NONE  = 0x00;

    /** Record uses conventional CRC-32 computed by Java CRC32 class. */
    public static final byte      CRC_TYPE_32    = 0x01;

    private File                  file;
    private byte[]                data;
    private long                  offset;
    private byte                  crcType;
    private long                  crc;
    private boolean               truncated      = false;

    // Computed CRC from checkCRC() call.
    private long                  computedCrc    = -1;

    private ByteArrayInputStream  read;
    private ByteArrayOutputStream write;

    /**
     * Creates an empty record, which is optionally truncated.
     * 
     * @param file File to which this record belongs
     * @param offset Offset of this record in the file
     * @param truncated If true this record is truncated rather than merely
     *            empty
     */
    public LogRecord(File file, long offset, boolean truncated)
    {
        this.file = file;
        this.offset = offset;
        this.data = null;
        this.crc = 0;
        this.crcType = CRC_TYPE_NONE;
        this.truncated = truncated;
    }

    /**
     * Creates a readable record with indicated content.
     * 
     * @param offset File offset at which this record was read
     * @param bytes Data in record
     * @param crcType Type of CRC check to use
     * @param crc CRC value
     */
    public LogRecord(File file, long offset, byte[] bytes, byte crcType,
            long crc)
    {
        this.file = file;
        this.offset = offset;
        this.data = bytes;
        this.crcType = crcType;
        this.crc = crc;
        this.truncated = false;
    }

    /**
     * Returns the computed length of this record in the file, including length
     * field, data, and CRC.
     */
    public long getRecordLength()
    {
        if (data == null)
            return 0;
        else
            return data.length + NON_DATA_BYTES;
    }

    /**
     * Returns the offset into the source file of this record.
     */
    public long getOffset()
    {
        return offset;
    }

    /**
     * Returns the underlying byte buffer. Must call done() when writing before
     * calling this method.
     */
    public byte[] getData()
    {
        return data;
    }

    /**
     * Returns the CRC type.
     */
    public byte getCrcType()
    {
        return crcType;
    }

    /**
     * Returns the CRC value.
     */
    public long getCrc()
    {
        return crc;
    }

    /** Returns true if the record is truncated. */
    public boolean isTruncated()
    {
        return truncated;
    }

    /**
     * Returns true if the record is empty.
     */
    public boolean isEmpty()
    {
        return data == null;
    }

    /**
     * Compute and return CRC on data.
     */
    public long computeCrc() throws IOException
    {
        if (data == null || crcType == CRC_TYPE_NONE)
            computedCrc = 0;
        else if (crcType == CRC_TYPE_32)
        {
            computedCrc = computeCrc32(data);
        }
        else
        {
            throw new IOException("Invalid crc type: " + crcType);
        }

        return computedCrc;
    }

    /**
     * Compute and store CRC. This is used to populate CRC in a record to which
     * we are writing.
     */
    public void storeCrc(byte crcType) throws IOException
    {
        if (data == null || crcType == CRC_TYPE_NONE)
        {
            this.crc = 0;
            this.crcType = crcType;
        }
        else if (crcType == CRC_TYPE_32)
        {
            this.crc = computeCrc32(data);
            this.crcType = crcType;
        }
        else
        {
            throw new IOException("Invalid crc type: " + crcType);
        }
    }

    /**
     * Computes the CRC value and compares to the CRC stored in the record,
     * returning true only if the CRC values match.
     */
    public boolean checkCrc() throws IOException
    {
        if (computedCrc == -1)
            computeCrc();
        return computedCrc == crc;
    }

    /**
     * Verifies the CRC value. This generates an exception if the exception is
     * bad.
     * 
     * @throws LogConsistencyException Thrown if checksum verification fails
     */
    public void verifyChecksum() throws LogConsistencyException
    {
        try
        {
            if (!checkCrc())
            {
                String fileName;
                if (file == null)
                    fileName = "unknown";
                else
                    fileName = file.getName();
                String message = "Log record CRC failure: file=" + fileName
                        + " offset=" + getOffset() + " crc type="
                        + getCrcType() + " stored crc=" + crc
                        + " computed crc=" + computedCrc;
                throw new LogConsistencyException(message);
            }
        }
        catch (IOException e)
        {
            String fileName;
            if (file == null)
                fileName = "unknown";
            else
                fileName = file.getName();
            String message = "CRC computation failure: file=" + fileName
                    + " offset=" + getOffset() + " crc type=" + getCrcType()
                    + " stored crc=" + getCrc();
            throw new LogConsistencyException(message, e);
        }
    }

    /**
     * Static routine to compute CRC 32.
     */
    public static long computeCrc32(byte[] bytes) throws IOException
    {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        try
        {
            CheckedInputStream cis = new CheckedInputStream(bais, new CRC32());

            byte[] buf = new byte[128];
            while (cis.read(buf) >= 0)
            {
            }

            return cis.getChecksum().getValue();
        }
        finally
        {
            bais.close();
        }

    }

    /** Returns a stream to read record contents. */
    public InputStream read()
    {
        return new ByteArrayInputStream(data);
    }

    /** Returns a stream to write record contents. */
    public OutputStream write()
    {
        data = null;
        write = new ByteArrayOutputStream();
        return write;
    }

    /**
     * Deallocate resources and in the case of a writable log record write data.
     */
    public void done()
    {
        if (read != null)
        {
            try
            {
                read.close();
            }
            catch (IOException e)
            {
            }
            read = null;
        }
        if (write != null)
        {
            try
            {
                data = write.toByteArray();
                write.close();
            }
            catch (IOException e)
            {
            }
            write = null;
        }
    }

    /**
     * Print log record as string.
     */
    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        sb.append(this.getClass().getSimpleName());
        sb.append(": offset=").append(offset);
        if (data == null)
        {
            sb.append(" data=[] length=0");
        }
        else
        {
            sb.append(" data=");
            for (int i = 0; i < 10 && i < data.length; i++)
            {
                sb.append(String.format("%2X", data[i]));
            }
            if (data.length >= 10)
                sb.append("...");
            sb.append("] length=").append(data.length);
        }
        sb.append(" crcType=").append(crcType);
        sb.append(" crc=").append(crc);
        sb.append(" truncated=").append(truncated);

        return sb.toString();
    }

    /**
     * Return true if two records are equal, which means that offset, byte
     * array, and CRC all match.
     */
    public boolean equals(Object o)
    {
        if (!(o instanceof LogRecord))
            return false;
        LogRecord that = (LogRecord) o;
        if (offset != that.getOffset())
            return false;
        if (data == null)
        {
            if (that.getData() != null)
                return false;
        }
        else
        {
            if (data.length != that.getData().length)
                return false;
            for (int i = 0; i < data.length; i++)
            {
                if (data[i] != that.getData()[i])
                    return false;
            }
        }
        if (crcType != that.getCrcType())
            return false;
        if (crc != that.getCrc())
            return false;

        // Everything matches!
        return true;
    }
}