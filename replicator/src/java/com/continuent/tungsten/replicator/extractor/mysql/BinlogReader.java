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

package com.continuent.tungsten.replicator.extractor.mysql;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.io.BufferedFileDataInput;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.extractor.mysql.conversion.LittleEndianConversion;

/**
 * Encapsulates logic to open, read data, and clone positions on MySQL binlog
 * files. This file takes care of validating the header and also eliminates much
 * of the tangled logic of validating headers and cloning positions present in
 * former implementations. It also leans on the {@link BufferedFileDataInput}
 * class to ensure reads are buffered and minimize use of disk metadata calls
 * that kill performance on network-attached storage.
 * <p>
 * The new implementation replaces the old BinlogPosition class authored by
 * Seppo Jaakola; a few fields are copied into this implementation but the logic
 * is quite different.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class BinlogReader implements FilenameFilter, Cloneable
{
    static Logger                 logger                 = Logger.getLogger(MySQLExtractor.class);

    // Stream from which we are reading.
    private BufferedFileDataInput bfdi;

    // Binlog file name and directory.
    private String                fileName;
    private String                directory;

    // Binlog file base name.
    private String                baseName;

    // Start position. We seek to this after open if greater than 0.
    private long                  startPosition;

    // Binlog version. This must be set externally by clients after reading
    // the header.
    private int                   version                = MysqlBinlog.VERSION_NONE;

    // Id of last event read.
    private int                   eventID;

    // Buffer size for reads.
    private int                   bufferSize             = 64000;

    // Delay in milliseconds to wait for binlog writes to flush fully.
    private int                   binlogFlushDelayMillis = 5000;

    /**
     * Defines only binlog directory and binlog file base name.
     * 
     * @param directory Directory path where binlog files should reside
     * @param baseName File name pattern for binlog files: basenName001.bin
     */
    public BinlogReader(String directory, String baseName, int bufferSize)
    {
        this(0, null, directory, baseName, bufferSize);
    }

    /**
     * Defines all possible binlog parameters .
     * 
     * @param start Start location in the file, will skip until that
     * @param fileName Full file path name to open
     * @param directory Directory path where binlog files should reside
     * @param baseName File name pattern for binlog files: baseName-000001.bin
     * @param bufferSize Size of buffer to read
     */
    public BinlogReader(long start, String fileName, String directory,
            String baseName, int bufferSize)
    {
        this.bfdi = null;
        this.startPosition = start;
        this.eventID = 0;
        this.fileName = fileName;
        this.directory = directory;
        this.baseName = baseName;
        this.bufferSize = bufferSize;
    }

    /**
     * Clones the current reader position. Clients call open() on the resulting
     * file to create an alternate read stream.
     * 
     * @see java.lang.Object#clone()
     */
    public BinlogReader clone()
    {
        long offset = bfdi == null ? 0 : bfdi.getOffset();
        BinlogReader cloned = new BinlogReader(offset, fileName, directory,
                baseName, bufferSize);

        // Set last ID read.
        cloned.setEventID(eventID);

        return cloned;
    }

    /**
     * Opens up the binlog file, validates the magic number, and checks the
     * header for the binlog version. If this call succeeds the binlog is ready
     * for reading.
     */
    void open() throws ReplicatorException, InterruptedException
    {
        try
        {
            // Check for safety conditions.
            if (getFileName() == null)
            {
                throw new MySQLExtractException("No binlog file specified");
            }
            if (bfdi != null)
            {
                throw new MySQLExtractException(
                        "Attempt to open binlog twice: " + this.fileName);
            }

            // Hack to avoid crashing during log rotate. MySQL seems to write
            // log rotate event in the old file before creating new file. We
            // wait for a few seconds, polling file every 10 msecs.
            File file = new File(getDirectory() + File.separator
                    + getFileName());
            int tryCnt = 0;
            while (file.exists() == false && tryCnt++ < 500)
            {
                Thread.sleep(10);
            }

            if (logger.isDebugEnabled())
                logger.debug("Opening file " + file.getName()
                        + " with buffer = " + bufferSize);

            bfdi = new BufferedFileDataInput(file, bufferSize);

            // Validate the file magic number.
            byte magic[] = new byte[MysqlBinlog.BIN_LOG_HEADER_SIZE];
            try
            {
                waitAvailable(magic.length, binlogFlushDelayMillis);
                if (available() < magic.length)
                {
                    throw new MySQLExtractException(
                            "Failed reading header;  Probably an empty file or very slow file system: "
                                    + getBaseName());
                }
                read(magic);
                if (!Arrays.equals(magic, MysqlBinlog.BINLOG_MAGIC))
                {
                    throw new MySQLExtractException(
                            "File is not a binary log file - found : "
                                    + LogEvent.hexdump(magic)
                                    + " / expected : "
                                    + LogEvent
                                            .hexdump(MysqlBinlog.BINLOG_MAGIC));
                }
            }
            catch (IOException e)
            {
                throw new MySQLExtractException(
                        "Failed reading binlog file header: " + getBaseName(),
                        e);
            }

            // Figure out the binlog format and mark accordingly. Here are the
            // rules for distinguishing formats.
            //
            // Binlog V1 - MySQL 3.23. Starts with 69-byte START_EVENT_V3.
            // Binlog V3 - MySQL 4.0/4.1. Starts with 75-byte START_EVENT_V3.
            // Binlog V4 - MySQL 5.0+. Starts with FORMAT_DESCRIPTION_EVENT.
            //
            // Detailed rules for determining type can be found at the following
            // URL:
            // http://forge.mysql.com/wiki/MySQL_Internals_Binary_Log#Determining_the_Binary_Log_Version

            // Mark a reset point to return to after readahead to deduce the
            // binlog version.
            mark(2048);

            // Read first for fields, which are common to all events.
            byte[] header = new byte[MysqlBinlog.PROBE_HEADER_LEN];
            waitAvailable(header.length, binlogFlushDelayMillis);
            if (available() < header.length)
            {
                throw new MySQLExtractException(
                        "Failed reading header;  You may have an incomplete log file or a very slow file system: "
                                + getBaseName());
            }
            bfdi.readFully(header);
            int typeCode = header[4];
            int eventLength = (int) LittleEndianConversion.convert4BytesToLong(
                    header, MysqlBinlog.EVENT_LEN_OFFSET);

            // Apply rules for distinguishing binlog type, generating
            // exceptions if we seem to be confused.
            if (typeCode == MysqlBinlog.START_EVENT_V3)
            {
                // Check event length to distinguish between V1 and V3.
                if (eventLength == 69)
                {
                    version = MysqlBinlog.BINLOG_V1;
                    if (logger.isDebugEnabled())
                        logger.debug("Binlog format is V1");
                }
                else if (eventLength == 75)
                {
                    version = MysqlBinlog.BINLOG_V3;
                    if (logger.isDebugEnabled())
                        logger.debug("Binlog format is V3");
                }
                else
                {
                    throw new MySQLExtractException(
                            "Unexpected start event length: file="
                                    + this.fileName + " length=" + eventLength);
                }
            }
            else if (typeCode == MysqlBinlog.FORMAT_DESCRIPTION_EVENT)
            {
                version = MysqlBinlog.BINLOG_V4;
                if (logger.isDebugEnabled())
                    logger.debug("Binlog format is V4");
            }
            else if (typeCode == MysqlBinlog.ROTATE_EVENT)
            {
                version = MysqlBinlog.BINLOG_V3;
                if (logger.isDebugEnabled())
                    logger.debug("Binlog format is V3 (special case w/ rotate event)");
            }
            else
            {
                throw new MySQLExtractException(
                        "Unexpected start event type code: file="
                                + this.fileName + " type code=" + typeCode);
            }

            // If we have predefined position to start from, let's skip until
            // the position. This situation can happen if the extractor is
            // cloned and we need to seek to the correct read position.
            // Otherwise just reset.
            if (startPosition >= bfdi.getOffset())
            {
                bfdi.seek(startPosition);
            }
            else
            {
                bfdi.reset();
            }
        }
        catch (FileNotFoundException e)
        {
            throw new MySQLExtractException("Unable to open binlog file", e);
        }
        catch (IOException e)
        {
            throw new MySQLExtractException("Unable to scan binlog file", e);
        }
    }

    /**
     * Returns true if a binlog log file is currently open.
     */
    public boolean isOpen()
    {
        return (bfdi != null);
    }

    /**
     * Closes the stream but leaves directory and base name intact
     */
    public void close() throws ReplicatorException
    {
        if (bfdi != null)
        {
            bfdi.close();
            bfdi = null;
        }
        setStartPosition(0);
        setEventID(0);
        setFileName(null);
    }

    /**
     * Returns the number of bytes available in the current log file. WARNING:
     * this is a potentially expensive call as it will fetch disk metadata over
     * a network if necessary.
     * 
     * @throws IOException Thrown if call fails
     */
    public long available() throws IOException, InterruptedException
    {
        return bfdi.available();
    }

    /**
     * Wait for a specific number of bytes to be available or until we time out
     * 
     * @param requested Number of bytes requested
     * @param waitMillis Timeout in milliseconds
     * @return Number of bytes available for non-blocking read
     * @throws IOException Thrown if wait fails
     * @throws InterruptedException Thrown if interrupted during wait
     */
    public long waitAvailable(int requested, int waitMillis)
            throws IOException, InterruptedException
    {
        return bfdi.waitAvailable(requested, waitMillis);
    }

    /**
     * Skips a given number of bytes.
     * 
     * @return Actual number of bytes skipped
     * @throws IOException Thrown if bytes cannot be skipped
     */
    public long skip(long bytes) throws IOException
    {
        return bfdi.skip(bytes);
    }

    /**
     * Mark binlog to read up to limit.
     * 
     * @param readLimit Number of bytes that may be read before resetting
     */
    public void mark(int readLimit)
    {
        bfdi.mark(readLimit);
    }

    /**
     * Reset binlog back to last mark.
     * 
     * @throws IOException Thrown if mark has been invalidated or not set
     * @throws InterruptedException Thrown if we are interrupted
     */
    public void reset() throws IOException, InterruptedException
    {
        bfdi.reset();
    }

    /**
     * Read bytes fully into a byte buffer. This call blocks until data are
     * available.
     * 
     * @param buf Buffer into which to read bytes
     * @throws IOException Thrown if read fails.
     */
    public void read(byte[] buf) throws IOException
    {
        bfdi.readFully(buf);
    }

    /**
     * Read bytes fully into a byte buffer. This call blocks until data are
     * available.
     * 
     * @param buf Buffer into which to transfer bytes
     * @param offset Point at which to write bytes in array
     * @param len Number of bytes to read
     * @throws IOException Thrown if read fails.
     */
    public void read(byte[] buf, int offset, int len) throws IOException
    {
        bfdi.readFully(buf, offset, len);
    }

    /**
     * Read an 8-byte long from binlog.
     * 
     * @throws IOException Thrown if read fails.
     */
    public long readLong() throws IOException
    {
        return bfdi.readLong();
    }

    /**
     * Read a 4-byte int from binlog.
     * 
     * @throws IOException Thrown if read fails.
     */
    public int readInt() throws IOException
    {
        return bfdi.readInt();
    }

    /**
     * Read a single byte from binlog.
     * 
     * @throws IOException Thrown if read fails.
     */
    public byte readByte() throws IOException
    {
        return bfdi.readByte();
    }

    /* member getters and setters */
    public void setStartPosition(long newPosition)
    {
        startPosition = newPosition;
    }

    public long getPosition()
    {
        if (bfdi != null)
            return bfdi.getOffset();
        else
            return startPosition;
    }

    public void setFileName(String fileName)
    {
        this.fileName = fileName;
    }

    public String getFileName()
    {
        return (fileName);
    }

    public void setDirectory(String directory)
    {
        this.directory = directory;
    }

    public String getDirectory()
    {
        return (directory);
    }

    public boolean accept(File dir, String name)
    {
        if (name.startsWith(baseName))
            return true;
        return false;
    }

    public String getBaseName()
    {
        return baseName;
    }

    public void setBaseName(String baseName)
    {
        this.baseName = baseName;
    }

    public int getEventID()
    {
        return eventID;
    }

    public void setEventID(int eventID)
    {
        this.eventID = eventID;
    }

    public int getVersion()
    {
        return version;
    }

    public void setVersion(int version)
    {
        this.version = version;
    }

    @Override
    public String toString()
    {
        return fileName + " (" + getPosition() + ")";
    }
}
