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

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.io.BufferedFileDataInput;
import com.continuent.tungsten.common.io.BufferedFileDataOutput;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.thl.THLException;

/**
 * This class manages I/O on a physical log file. It handles streams to read or
 * write from the underlying file.
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class LogFile
{
    static Logger              logger             = Logger.getLogger(LogFile.class);

    // Header fields values.
    private static final int   MAGIC_NUMBER       = 0xC001CAFE;
    private static final short MAJOR_VERSION      = 0x0001;
    private static final short MINOR_VERSION      = 0x0001;
    private static final int   RECORD_LENGTH_SIZE = 4;
    // Length of header in bytes.
    public static final int    HEADER_LENGTH      = 16;
    // Length of time to wait for a partially written header to appear.
    private static final int   HEADER_WAIT_MILLIS = 5000;

    /**
     * Maximum value of a single record. Larger values indicate file corruption.
     */
    private static final int   MAX_RECORD_LENGTH  = 1000000000;

    /** Return immediately from write when there are no data. */
    public static final int    NO_WAIT            = 0;

    /** Current mode of file, namely read or write. */
    private enum AccessMode
    {
        write, read
    };

    // Log file parameters.

    /** Log file name. */
    private final File             file;
    /** Buffer size used for I/O operations. */
    private int                    bufferSize          = 65536;
    /**
     * Flush (or fsync) after this many milliseconds. Higher values defer flush
     */
    private long                   flushIntervalMillis = 0;
    /** If true, fsync when flushing. */
    private boolean                fsyncOnFlush        = false;

    // Log sync task.
    private LogFlushTask           logFlushTask        = null;

    // Current access mode.
    private AccessMode             mode                = null;

    // Input control data.
    private BufferedFileDataInput  dataInput;

    // Output parameters.
    private BufferedFileDataOutput dataOutput;
    private long                   nextFlushMillis     = 0;
    private long                   baseSeqno;
    private boolean                needsFlush;

    /**
     * Creates a file from a parent directory and child filename. The file must
     * exist.
     * 
     * @param parentDirectory Log file directory
     * @param fileName Log file name
     */
    public LogFile(File parentDirectory, String fileName)
    {
        this(new File(parentDirectory, fileName));
    }

    /**
     * Creates a log file from a simple file. The file must exist.
     * 
     * @param file Log file specification
     */
    public LogFile(File file)
    {
        this.file = file;
    }

    /**
     * Returns the log file.
     */
    public synchronized File getFile()
    {
        return file;
    }

    /** Returns the current log flush task, if any. */
    public synchronized LogFlushTask getLogFlushTask()
    {
        return logFlushTask;
    }

    /**
     * Sets the log flush task. This enables delayed flush/fsync using
     * flushIntervalMillis.
     */
    public synchronized void setLogSyncTask(LogFlushTask logFlushTask)
    {
        this.logFlushTask = logFlushTask;
    }

    public synchronized long getFlushIntervalMillis()
    {
        return flushIntervalMillis;
    }

    public synchronized void setFlushIntervalMillis(long flushIntervalMillis)
    {
        this.flushIntervalMillis = flushIntervalMillis;
    }

    public synchronized boolean isFsyncOnFlush()
    {
        return fsyncOnFlush;
    }

    public synchronized void setFsyncOnFlush(boolean fsyncOnFlush)
    {
        this.fsyncOnFlush = fsyncOnFlush;
    }

    public synchronized int getBufferSize()
    {
        return bufferSize;
    }

    public synchronized void setBufferSize(int bufferSize)
    {
        this.bufferSize = bufferSize;
    }

    // API Calls for opening and closing log files.

    /**
     * Open the log file for reading. The log cannot be written.
     * 
     * @throws ReplicatorException Thrown if file cannot be opened
     * @throws InterruptedException Thrown if thread is interrupted
     */
    public synchronized void openRead() throws ReplicatorException,
            InterruptedException
    {
        // Confirm that file exists.
        if (!file.exists())
        {
            throw new THLException(
                    "Cannot open log file for reading; file does not exist: "
                            + file.getName());
        }

        // Open and read the file header so we are correctly positioned in the
        // file to begin reading.
        try
        {
            dataInput = new BufferedFileDataInput(file, bufferSize);
        }
        catch (IOException e)
        {
            throw new THLException("Unable to open file for reading: "
                    + file.getName(), e);
        }
        mode = AccessMode.read;
        checkFileHeader(dataInput);
    }

    /**
     * Prepare the log file for writing. The write offset is automatically set
     * to the end of the file.
     * 
     * @throws ReplicatorException Thrown if file cannot be opened
     * @throws InterruptedException Thrown if we are interrupted
     */
    public synchronized void openWrite() throws ReplicatorException,
            InterruptedException
    {
        // Confirm file exists.
        if (!file.exists())
        {
            throw new THLException(
                    "Cannot open log file for writing; file does not exist: "
                            + file.getName());
        }

        // Validate the file header, then open for write.
        try
        {
            BufferedFileDataInput bfdi = new BufferedFileDataInput(file,
                    bufferSize);
            checkFileHeader(bfdi);
            bfdi.close();

            dataOutput = new BufferedFileDataOutput(file, bufferSize);
        }
        catch (IOException e)
        {
            throw new THLException("Failed to open existing file for writing: "
                    + file.getName(), e);
        }

        // Set access mode.
        mode = AccessMode.write;

        // Register with log sync task.
        if (logFlushTask != null)
            logFlushTask.addLogFile(this);
    }

    /**
     * Create a new log file. We must write a header with a base sequence
     * number. NOTE: The file offset is positioned after the header.
     * 
     * @param seqno Base sequence number of this file (written to header)
     */
    public synchronized void create(long seqno) throws ReplicatorException,
            InterruptedException
    {
        // Confirm file does not already exist.
        if (file.exists())
        {
            throw new THLException(
                    "Cannot create new log file; file already exists: "
                            + file.getName());
        }

        // Open new file and write header.
        try
        {
            dataOutput = new BufferedFileDataOutput(file, bufferSize);
        }
        catch (IOException e)
        {
            throw new THLException("Failed to open new file for writing: "
                    + file.getName(), e);
        }

        // Write the header.
        mode = AccessMode.write;
        try
        {
            write(MAGIC_NUMBER);
            write(MAJOR_VERSION);
            write(MINOR_VERSION);
            write(seqno);
            flush();
        }
        catch (IOException e)
        {
            throw new THLException("Unable to write file header: "
                    + file.getName(), e);
        }

        // Set base sequence number.
        baseSeqno = seqno;

        // Register with log sync task.
        if (logFlushTask != null)
            logFlushTask.addLogFile(this);
    }

    /**
     * Flush and close file. It should be called after all other methods as part
     * of a clean shutdown.
     */
    public synchronized void close()
    {
        // Release only once.
        if (mode != null)
        {
            if (mode == AccessMode.read)
            {
                if (dataInput != null)
                {
                    dataInput.close();
                    dataInput = null;
                }

            }
            else if (mode == AccessMode.write)
            {
                if (dataOutput != null)
                {
                    if (logFlushTask != null)
                        logFlushTask.removeLogFile(this);
                    dataOutput.close();
                    dataOutput = null;
                }
            }
            mode = null;
        }
    }

    /**
     * Read the file header and return the log sequence number stored in the
     * file header.
     */
    private long checkFileHeader(BufferedFileDataInput bfdi)
            throws ReplicatorException, InterruptedException
    {
        int magic = 0;
        short major = 0;
        short minor = 0;

        try
        {
            bfdi.waitAvailable(HEADER_LENGTH, HEADER_WAIT_MILLIS);
            magic = bfdi.readInt();
            major = bfdi.readShort();
            minor = bfdi.readShort();
            baseSeqno = bfdi.readLong();
        }
        catch (IOException e)
        {
            throw new THLException("Failed to read file header from  "
                    + file.getAbsolutePath(), e);
        }

        if (magic != MAGIC_NUMBER)
            throw new THLException("Could not open file "
                    + file.getAbsolutePath() + " : invalid magic number");
        if (major != MAJOR_VERSION)
            throw new THLException("Could not open file "
                    + file.getAbsolutePath() + " : incompatible major version");
        if (minor != MINOR_VERSION)
            logger.warn("Minor version mismatch : file "
                    + file.getAbsolutePath() + " using format " + major + "."
                    + minor + " - Tungsten running version " + MAJOR_VERSION
                    + "." + MINOR_VERSION);
        return baseSeqno;
    }

    // File access management functions

    /** Returns only if log file is in read or write mode. */
    protected void assertAnyMode() throws ReplicatorException
    {
        if (mode == null)
            throw new THLException("Log file not initialized for access: file="
                    + file.getName());
    }

    /**
     * Returns only if log file is in write mode. If we are in read mode, this
     * will switch over to write mode and position at the end of the file.
     */
    protected void assertWriteMode() throws ReplicatorException,
            InterruptedException
    {
        if (mode != AccessMode.write)
        {
            // Close and re-open in write mode.
            close();
            openWrite();
        }
    }

    /** Returns only if log file is in read mode. */
    protected void assertReadMode() throws ReplicatorException,
            InterruptedException
    {
        if (mode != AccessMode.read)
        {
            // Close and re-open in read mode.
            close();
            openRead();
        }

    }

    // Generic operations to return file contents, position, and length.

    /**
     * Returns the base sequence number from the file header.
     */
    public synchronized long getBaseSeqno()
    {
        return baseSeqno;
    }

    /**
     * Returns the length of the file, including any unbuffered writes.
     */
    public synchronized long getLength() throws ReplicatorException
    {
        assertAnyMode();
        try
        {
            if (mode == AccessMode.read)
                return file.length();
            else
                return dataOutput.getOffset();
        }
        catch (IOException e)
        {
            throw new THLException("Unable to determine log file length: name="
                    + this.file.getAbsolutePath(), e);
        }
    }

    /**
     * Returns the current position in the log file.
     */
    public synchronized long getOffset() throws ReplicatorException
    {
        assertAnyMode();
        try
        {
            if (mode == AccessMode.read)
                return dataInput.getOffset();
            else
                return dataOutput.getOffset();
        }
        catch (IOException e)
        {
            throw new THLException("Unable to determine log file offset: name="
                    + this.file.getAbsolutePath(), e);
        }
    }

    // Read mode operations.

    /**
     * Seeks to a particular offset in the file.
     * 
     * @throws IOException If positioning results in an error.
     */
    public synchronized void seekOffset(long offset) throws IOException,
            ReplicatorException, InterruptedException
    {
        assertReadMode();
        dataInput.seek(offset);
        if (logger.isDebugEnabled())
        {
            logger.debug("Skipping to position " + offset + " into file "
                    + this.file.getName());
        }
    }

    /**
     * Reads a record from the file into a byte array. We may encounter a number
     * of unpredictable conditions at this point that we need to report
     * accurately to layers above us that will decide whether it represents a
     * problem.
     * 
     * @param waitMillis Number of milliseconds to wait for data to be
     *            available. 0 means do not wait.
     * @return A log record if we can read one before timing out
     * @throws IOException Thrown if there is an I/O error
     * @throws InterruptedException Thrown if we are interrupted
     * @throws LogTimeoutException Thrown if we timeout while waiting for data
     *             to appear
     */
    public synchronized LogRecord readRecord(int waitMillis)
            throws IOException, InterruptedException, LogTimeoutException,
            ReplicatorException
    {
        assertReadMode();
        long offset = dataInput.getOffset();
        if (logger.isDebugEnabled())
            logger.debug("Reading log file position=" + offset);

        // Mark position so we can reset on failure.
        dataInput.mark(65636);

        // Read record length.
        long startIntervalMillis = System.currentTimeMillis();
        long available = dataInput
                .waitAvailable(RECORD_LENGTH_SIZE, waitMillis);
        if (available < RECORD_LENGTH_SIZE)
        {
            // Reset input.
            dataInput.reset();

            if (waitMillis > 0)
            {
                // If we were waiting for data, timeout.
                throw new LogTimeoutException("Log read timeout: waitMillis="
                        + waitMillis + " file=" + file.getName() + " offset="
                        + offset);
            }
            else if (available == 0)
            {
                // If there is nothing to read at this point, just return an
                // empty record.
                if (logger.isDebugEnabled())
                    logger.debug("Read empty record");
                return new LogRecord(file, offset, false);
            }
            else if (available < RECORD_LENGTH_SIZE)
            {
                // If there is not enough to read the length, and we don't want
                // to wait, this is a truncated record.
                if (logger.isDebugEnabled())
                    logger.debug("Length is truncated; returning immediately");
                return new LogRecord(file, offset, true);
            }
        }

        // Read the length. Check for corrupt data.
        int recordLength = dataInput.readInt();
        if (recordLength < LogRecord.NON_DATA_BYTES
                || recordLength > MAX_RECORD_LENGTH)
        {
            logger.warn("Record length is invalid, log may be corrupt: offset="
                    + offset + " record length=" + recordLength);
            dataInput.reset();
            return new LogRecord(file, offset, true);
        }
        if (logger.isDebugEnabled())
            logger.debug("Record length=" + recordLength);

        // See if there is enough data to read the rest of the record.
        waitMillis = waitMillis
                + (int) (startIntervalMillis - System.currentTimeMillis());
        int remainingRecordLength = recordLength - RECORD_LENGTH_SIZE;
        available = dataInput.waitAvailable(remainingRecordLength, waitMillis);

        if (available < remainingRecordLength)
        {
            // Reset input.
            dataInput.reset();

            if (waitMillis > 0)
            {
                // If we were waiting for data, timeout.
                throw new LogTimeoutException("Log read timeout: waitMillis="
                        + waitMillis + " file=" + file.getName() + " offset="
                        + offset);
            }
            else
            {
                // Not enough data, so return a partial record.
                return new LogRecord(file, offset, true);
            }
        }

        // Finally, there's enough to read a record, so get it.
        byte[] bytesToRead = new byte[recordLength - LogRecord.NON_DATA_BYTES];
        dataInput.readFully(bytesToRead);
        byte crcType = dataInput.readByte();
        long crc = dataInput.readLong();
        return new LogRecord(file, offset, bytesToRead, crcType, crc);
    }

    /** Reads a single short. */
    protected short readShort() throws IOException, ReplicatorException,
            InterruptedException
    {
        assertReadMode();
        return dataInput.readShort();
    }

    /** Read a single integer. */
    protected int readInt() throws IOException, ReplicatorException,
            InterruptedException
    {
        assertReadMode();
        return dataInput.readInt();
    }

    /** Reads a single long. */
    protected long readLong() throws IOException, ReplicatorException,
            InterruptedException
    {
        assertReadMode();
        return dataInput.readLong();
    }

    // Write-mode operations.

    /**
     * Truncate the file to the provided length. Performs an automatic fsync.
     * 
     * @param length New file length
     */
    public synchronized void setLength(long length) throws ReplicatorException,
            InterruptedException
    {
        assertWriteMode();
        try
        {
            dataOutput.setLength(length);
        }
        catch (IOException e)
        {
            throw new THLException("Unable to set log file length: "
                    + file.getName(), e);
        }
    }

    protected void write(int myInt) throws IOException, ReplicatorException,
            InterruptedException
    {
        assertWriteMode();
        dataOutput.writeInt(myInt);
    }

    protected void write(long seqno) throws IOException, ReplicatorException,
            InterruptedException
    {
        assertWriteMode();
        dataOutput.writeLong(seqno);
    }

    protected void write(short myShort) throws IOException,
            ReplicatorException, InterruptedException
    {
        assertWriteMode();
        dataOutput.writeShort(myShort);
    }

    /**
     * Writes a buffer to the log file and returns true if we have exceeded the
     * log file size.
     * 
     * @param record Log record to write
     * @param logFileSize Maximum log file size
     * @return true if log file size exceeded
     */
    public synchronized boolean writeRecord(LogRecord record, int logFileSize)
            throws IOException, InterruptedException, ReplicatorException
    {
        // Write the length followed by the code.
        assertWriteMode();
        dataOutput.writeInt((int) record.getRecordLength());
        dataOutput.write(record.getData());
        dataOutput.writeByte(record.getCrcType());
        dataOutput.writeLong(record.getCrc());

        // Record that we need a flush.
        needsFlush = true;

        // See if we have exceeded the maximum number of bytes per log file.
        if (logFileSize > 0 && dataOutput.getOffset() > logFileSize)
            return true;
        else
            return false;
    }

    /**
     * Synchronizes file writes using flush with optional fsync. You must call
     * this method to commit data.
     */
    public synchronized void flush() throws IOException, ReplicatorException,
            InterruptedException
    {
        // Only proceed if we need flush.
        if (!needsFlush)
        {
            return;
        }

        // Perform fsync checks.
        assertWriteMode();
        if (flushIntervalMillis == 0)
        {
            // Issue flush now.
            flushPrivate();
        }
        else if (nextFlushMillis == 0)
        {
            // Delayed fsync enabled but timer has never been set.
            // Initialize but do not fsync. (Ensures consistent
            // delayed fsync after first write to log file.)
            nextFlushMillis = System.currentTimeMillis()
                    + this.flushIntervalMillis;
        }
        else if (System.currentTimeMillis() >= nextFlushMillis)
        {
            // Timer is expired. Issue fsync call.
            flushPrivate();
        }
    }

    // Perform actual flush/fsync call.
    private void flushPrivate() throws IOException
    {
        if (fsyncOnFlush)
            dataOutput.fsync();
        else
            dataOutput.flush();

        nextFlushMillis = System.currentTimeMillis() + this.flushIntervalMillis;
        needsFlush = false;
    }

    /**
     * Returns a nicely formatting description of the file.
     */
    public synchronized String toString()
    {
        StringBuffer sb = new StringBuffer();
        sb.append(this.getClass().getSimpleName()).append(": ");
        sb.append("name=").append(file.getName());
        sb.append(" mode=").append(mode);
        if (dataInput != null)
        {
            sb.append(" open=y size=").append(file.length());
            sb.append(" offset=").append(dataInput.getOffset());
        }
        else if (dataOutput != null)
        {
            sb.append(" open=y size=").append(file.length());
            try
            {
                sb.append(" offset=").append(dataOutput.getOffset());
            }
            catch (IOException e)
            {
                sb.append(" [unable to get offset due to i/o error]");
            }
        }
        else
        {
            sb.append(" open=n");
        }
        return sb.toString();
    }
}