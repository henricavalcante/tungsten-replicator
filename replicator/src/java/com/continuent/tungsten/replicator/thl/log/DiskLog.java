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
 * Initial developer(s): Stephane Giron, Robert Hodges
 * Contributor(s): 
 */

package com.continuent.tungsten.replicator.thl.log;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.thl.THLException;
import com.continuent.tungsten.replicator.thl.serializer.ProtobufSerializer;
import com.continuent.tungsten.replicator.thl.serializer.Serializer;

/**
 * This class implements a multi-thread disk log store.
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class DiskLog
{
    static Logger                logger                     = Logger.getLogger(DiskLog.class);

    // Dummy start index for a new log.
    private static long          FIRST                      = 0;

    // Various operational variables.
    LogCursorManager             cursorManager;
    private Serializer           eventSerializer            = null;
    private File                 logDir;

    // Connection pools.
    private LogConnectionManager connectionManager          = new LogConnectionManager();

    // Variables used to maintain index on log files.
    private LogIndex             index                      = null;
    private long                 fileIndex                  = 1;
    private static final int     fileIndexSize              = Integer
                                                                    .toString(
                                                                            Integer.MAX_VALUE)
                                                                    .length();
    private static final String  DATA_FILENAME_PREFIX       = "thl.data.";

    /** Store and compare checksum values on the log. */
    private boolean              doChecksum                 = true;

    /** Name of the log directory. */
    protected String             logDirName                 = "/opt/tungsten/logs";

    /** Name of the class used to serialize events. */
    protected String             eventSerializerClass       = ProtobufSerializer.class
                                                                    .getName();

    /** Log file maximum size in bytes. */
    protected int                logFileSize                = 1000000000;

    /** Wait timeout. This is used for testing to prevent infinite timeouts. */
    protected int                timeoutMillis              = Integer.MAX_VALUE;

    /**
     * Special timeout when waiting for a new log file after a rotate log. This
     * timeout will normally only expire when there is a corrupt log.
     */
    protected int                logRotateMillis            = 60000;

    /** Number of milliseconds to retain old logs. */
    protected long               logFileRetainMillis        = 0;

    /**
     * Number of milliseconds before timing out idle log connections. Defaults
     * to 8 hours.
     */
    protected int                logConnectionTimeoutMillis = 28800000;

    /**
     * I/O buffer size for log file access. Larger is better.
     */
    protected int                bufferSize                 = 65536;

    /** Write lock to prevent log file corruption by concurrent access. */
    protected WriteLock          writeLock;

    /** Indicates whether access should be read only or not */
    protected boolean            readOnly                   = true;

    /**
     * Flush data after this many milliseconds. 0 flushes after every write.
     */
    private long                 flushIntervalMillis        = 0;

    /**
     * If true, fsync when flushing.
     */
    private boolean              fsyncOnFlush               = false;

    /**
     * Log flush task; enabled if asynchronous flush interval is greater than 0.
     */
    private LogFlushTask         logSyncTask;
    private Thread               logSyncThread;

    /**
     * Creates a new log instance.
     */
    public DiskLog()
    {
    }

    // Log parameters.

    /**
     * Sets the directory that will be used to store the log files
     * 
     * @param path directory to be used. Last / is optional.
     */
    public void setLogDir(String path)
    {
        this.logDirName = path.trim();
        if (this.logDirName.charAt(this.logDirName.length() - 1) != '/')
        {
            this.logDirName = this.logDirName.concat("/");
        }
    }

    /**
     * Returns the log directory.
     */
    public String getLogDir()
    {
        return this.logDirName;
    }
    
    /**
     * Returns THL log filename prefix.
     */
    public String getFilePrefix()
    {
        return DATA_FILENAME_PREFIX;
    }

    /**
     * Sets the log file size. This is approximate as rotation will occur after
     * storing an event that made the file grow above the given limit.
     * 
     * @param size file size
     */
    public void setLogFileSize(int size)
    {
        this.logFileSize = size;
    }

    /**
     * Returns the log file size.
     */
    public int getLogFileSize()
    {
        return logFileSize;
    }

    /**
     * Determines whether to checksum log records.
     * 
     * @param doChecksum If true use checksums
     */
    public void setDoChecksum(boolean doChecksum)
    {
        this.doChecksum = doChecksum;
    }

    /**
     * Return true if checksums are enabled.
     */
    public boolean isDoChecksum()
    {
        return this.doChecksum;
    }

    /**
     * Set the number of milliseconds to retain old log files.
     * 
     * @param logFileRetainMillis If other than 0, logs are retained for this
     *            amount of time
     */
    public void setLogFileRetainMillis(long logFileRetainMillis)
    {
        this.logFileRetainMillis = logFileRetainMillis;
    }

    /**
     * Set the number of milliseconds before timing out idle log connections.
     * 
     * @param logConnectionTimeoutMillis Time in milliseconds
     */
    public void setLogConnectionTimeoutMillis(int logConnectionTimeoutMillis)
    {
        this.logConnectionTimeoutMillis = logConnectionTimeoutMillis;
    }

    /**
     * Sets the event serializer class name.
     */
    public void setEventSerializerClass(String eventSerializerClass)
    {
        this.eventSerializerClass = eventSerializerClass;
    }

    /**
     * Returns the event serializer instance.
     */
    public Serializer getEventSerializer()
    {
        return eventSerializer;
    }

    /**
     * Sets the timeout value for blocking reads. This value is only changed
     * when testing.
     */
    public void setTimeoutMillis(int timeoutMillis)
    {
        this.timeoutMillis = timeoutMillis;
    }

    /**
     * Returns the current timeout value for blocking reads.
     */
    public int getTimeoutMillis()
    {
        return timeoutMillis;
    }

    /**
     * Sets the timeout value for reading a new file after a log rotation.
     */
    public void setLogRotateMillis(int logRotateMillis)
    {
        this.logRotateMillis = logRotateMillis;
    }

    /**
     * Returns the current timeout value for log rotation.
     */
    public int getLogRotateMillis()
    {
        return logRotateMillis;
    }

    /**
     * Sets the log buffer size.
     */
    public void setBufferSize(int bufferSize)
    {
        this.bufferSize = bufferSize;
    }

    /**
     * Set write flush interval in milliseconds. 0 means flush on every write.
     * This lowers latency.
     */
    public void setFlushIntervalMillis(long flushIntervalMillis)
    {
        this.flushIntervalMillis = flushIntervalMillis;
    }

    /**
     * Return flush interval in milliseconds.
     */
    public long getFlushIntervalMillis()
    {
        return flushIntervalMillis;
    }

    /**
     * If set to true, perform an fsync with every flush. Warning: fsync is very
     * slow, so you want a long flush interval in this case.
     */
    public synchronized void setFsyncOnFlush(boolean fsyncOnFlush)
    {
        this.fsyncOnFlush = fsyncOnFlush;
    }

    // Administrative API calls.
    public void setReadOnly(boolean readOnly)
    {
        this.readOnly = readOnly;
    }

    // Log initialization and release.

    /**
     * Prepare the log for use, which includes ensuring that the log is created
     * automatically on first use and building an index of log file contents.
     */
    public void prepare() throws ReplicatorException, InterruptedException
    {
        logger.info(String.format("Using directory '%s' for replicator logs",
                logDirName));
        logger.info("Checksums enabled for log records: " + doChecksum);

        // Ensure log directory is ready for use, which includes creating
        // a new log directory if desired.
        if (logger.isDebugEnabled())
        {
            logger.debug("logFileSize = " + logFileSize);
        }

        logDir = new File(logDirName);

        // Ensure the log directory exists or can be created.
        if (!logDir.exists())
        {
            if (readOnly)
            {
                // If read-only, do not create log directory. Just fail.
                throw new ReplicatorException("Log directory does not exist : "
                        + logDir.getAbsolutePath());
            }
            else
            {
                // Otherwise create the directory.
                logger.info("Log directory does not exist; creating now:"
                        + logDir.getAbsolutePath());
                if (!logDir.mkdirs())
                {
                    throw new ReplicatorException(
                            "Unable to create log directory: "
                                    + logDir.getAbsolutePath());
                }
            }
        }

        // Ensure we have a directory and not some other type of file.
        if (!logDir.isDirectory())
        {
            throw new ReplicatorException("Log directory is not a directory: "
                    + logDir.getAbsolutePath());
        }

        // Ensure we have appropriate access to the file.
        if (readOnly)
        {
            logger.info("Using read-only log connection");
        }
        else
        {
            // Ensure the directory is writable.
            if (!logDir.canWrite())
            {
                // Check write permission only when not read only
                throw new ReplicatorException("Log directory is not writable: "
                        + logDir.getAbsolutePath());
            }
            // Attempt to acquire write lock when write access is required.
            File lockFile = new File(logDir, "disklog.lck");
            if (logger.isDebugEnabled())
            {
                logger.debug("Attempting to acquire lock on write lock file: "
                        + lockFile.getAbsolutePath());
            }
            writeLock = new WriteLock(lockFile);
            writeLock.acquire();
            if (writeLock.isLocked())
                logger.info("Acquired write lock; log is writable");
            else
                logger.info("Unable to acquire write lock; log is read-only");
        }

        // Load event serializer.
        try
        {
            eventSerializer = (Serializer) Class.forName(eventSerializerClass)
                    .newInstance();
        }
        catch (Exception e)
        {
            throw new ReplicatorException(
                    "Unable to load event serializer class: "
                            + eventSerializerClass, e);
        }
        logger.info("Loaded event serializer class: "
                + eventSerializer.getClass().getName());

        // If the log does not have any files, initialize the first log file
        // now.
        if (listLogFiles(logDir, DATA_FILENAME_PREFIX).length == 0)
        {
            if (readOnly)
            {
                throw new ReplicatorException(
                        "Attempting to read a non-existent log; is log initialized? dirName="
                                + logDir.getAbsolutePath());
            }
            else
            {
                String logFileName = getDataFileName(fileIndex);
                LogFile logFile = new LogFile(logDir, logFileName);
                logFile.setBufferSize(bufferSize);
                logger.info("Initializing logs: logDir="
                        + logDir.getAbsolutePath() + " file="
                        + logFile.getFile().getName());
                logFile.create(-1);
                logFile.close();
            }
        }

        // Create an index on the log. This validates the log and will clean up
        // the last file if it has a partial header.
        if (logger.isDebugEnabled())
            logger.debug("Preparing index");
        index = new LogIndex(logDir, DATA_FILENAME_PREFIX, logFileRetainMillis,
                bufferSize, isWritable());

        // Open the last index file and parse the name to get the index of the
        // next file to be created. This ensures new files will be properly
        // created.
        String logFileName = index.getLastFile();
        int logFileIndexPos = logFileName.lastIndexOf(".");
        fileIndex = Long.valueOf(logFileName.substring(logFileIndexPos + 1));

        LogFile logFile = null;
        boolean recoveryComplete = false;
        try
        {
            // Starting with the final index file, read to find the final
            // full transaction. We may have to read 2 files, as the last
            // file in a multi-file log may be empty, in which case we
            // clean it up and move back to the previous file.
            int iteration = 0;
            while (!recoveryComplete && iteration < 2)
            {
                // Starting with the sequence number stored in the file header,
                // which is max seqno at time of the log file creation, scan
                // forward through the file and find the last sequence number.
                // At the end of the file, clean up any partially written
                // record(s) to prepare the file for use.
                iteration++;
                logFile = openLastFile(readOnly);
                long maxSeqno = logFile.getBaseSeqno();
                long lastCompleteEventOffset = LogFile.HEADER_LENGTH;
                boolean logFileIsEmpty = true;
                boolean lastFrag = true;

                if (logger.isDebugEnabled())
                    logger.debug("Starting max seqno is " + maxSeqno);

                // Read until we find an empty record.
                logger.info("Validating last log file: "
                        + logFile.getFile().getAbsolutePath());
                LogRecord currentRecord = null;

                currentRecord = logFile.readRecord(0);
                byte lastRecordType = -1;
                while (!currentRecord.isEmpty())
                {
                    // See what kind of event we have.
                    lastRecordType = currentRecord.getData()[0];
                    if (lastRecordType == LogRecord.EVENT_REPL)
                    {
                        LogEventReplReader eventReader = new LogEventReplReader(
                                currentRecord, eventSerializer, doChecksum);
                        lastFrag = eventReader.isLastFrag();

                        // If we are on a last fragment of an event, update the
                        // last complete transaction offset and store the
                        // sequence number.
                        if (lastFrag)
                        {
                            logFileIsEmpty = false;
                            maxSeqno = eventReader.getSeqno();
                            lastCompleteEventOffset = logFile.getOffset();
                        }
                        eventReader.done();
                    }
                    else if (lastRecordType == LogRecord.EVENT_ROTATE)
                    {
                        // This means the replicator stopped on a rotate log
                        // record with no file beyond it. It is a rare corner
                        // case but we need to handle it or the replicator will
                        // not be able to start writing.
                        String fileName = logFile.getFile().getName();

                        logger.info("Last log file ends on rotate log event: "
                                + fileName);
                        logFile.close();
                        if (isWritable())
                        {
                            // Ensure that last log file is not just a header
                            // plus a rotate event. This would indicate some
                            // kind of bug that should be investigated.
                            if (maxSeqno <= -1)
                            {
                                throw new LogConsistencyException(
                                        "Last log file consists of header plus rotate log event only: "
                                                + fileName);
                            }

                            // Update the index as this completes the current
                            // index entry.
                            index.setMaxIndexedSeqno(maxSeqno);

                            // Create the next file.
                            logFileIndexPos = fileName.lastIndexOf(".");
                            fileIndex = Long.valueOf(fileName
                                    .substring(logFileIndexPos + 1));
                            fileIndex = (fileIndex + 1) % Integer.MAX_VALUE;
                            logFile = this.startNewLogFile(maxSeqno + 1);
                            logFileIsEmpty = false;
                        }

                        // Nothing left to read, so we break from inner
                        // loop. We also have
                        recoveryComplete = true;
                        break;
                    }

                    // Read next record.
                    currentRecord = logFile.readRecord(0);
                }

                // If recovery is complete at this point, let the loop logic
                // take over as our work is done.
                if (recoveryComplete)
                    continue;

                // Update the index with the max sequence number we found. If
                // the log is empty, this should end up as -1.
                index.setMaxIndexedSeqno(maxSeqno);

                // If the last transaction was not terminated, we need to
                // truncate the log to the end of the last full transaction.
                if (!lastFrag)
                {
                    if (isWritable())
                    {
                        logger.warn("Log file contains partially written transaction; "
                                + "truncating to last full transaction: seqno="
                                + maxSeqno
                                + " length="
                                + lastCompleteEventOffset);
                        logFile.setLength(lastCompleteEventOffset);
                    }
                    else
                    {
                        logger.warn("Log ends with a partially written "
                                + "transaction, but this log is read-only.  "
                                + "It is possible that the process that "
                                + "owns the write lock is still writing it.");
                    }
                }
                // If we have a complete transaction but a record is truncated,
                // we have a corrupt file. Fix it now.
                else if (currentRecord.isTruncated())
                {
                    if (isWritable())
                    {
                        logger.warn("Log file contains partially written record: offset="
                                + currentRecord.getOffset()
                                + " partially written bytes="
                                + (logFile.getLength() - currentRecord
                                        .getOffset()));
                        logFile.setLength(currentRecord.getOffset());
                        logger.info("Log file truncated to end of last good record: length="
                                + logFile.getLength());
                    }
                    else
                    {
                        logger.warn("Log ends with a partially written record "
                                + "at end, but this log is read-only.  "
                                + "It is possible that the process that "
                                + "owns the write lock is still writing it.");
                        break;
                    }
                }

                // If following these cleanups we have an empty file at the
                // end of a multi-file log, we need to remove it and try again,
                // which we do at most once and only if we are writable.
                if (logFileIsEmpty && index.size() > 1)
                {
                    if (isWritable())
                    {
                        // We are writable and should clean up if on the
                        // first iteration. This is the only case that does not
                        // result in recovery being complete.
                        if (iteration == 1)
                        {
                            File emptyFile = logFile.getFile();
                            logger.warn("Log ends with an empty file: name="
                                    + emptyFile.getAbsolutePath());
                            logFile.close();
                            logFile = null;
                            if (!emptyFile.delete())
                            {
                                throw new LogConsistencyException(
                                        "Unable to delete empty log file: "
                                                + emptyFile.getAbsolutePath());
                            }
                            index.removeFile(index.getLastFile());
                        }
                        else
                        {
                            // After first iteration we conclude log is corrupt.
                            throw new LogConsistencyException(
                                    "Unable to clean up multiple empty files at end of log; use 'thl index' to check logs");
                        }
                    }
                    else
                    {
                        // We must be readable. We cannot clean up.
                        logger.warn("Ignoring empty file at end of log as we are not writable");
                        recoveryComplete = true;
                    }
                }
                // Otherwise if we have a valid sequence number things are all
                // cleaned up.
                else if (maxSeqno >= 0)
                {
                    recoveryComplete = true;
                }
                // If we have an empty log, that's good too.
                else if (index.size() <= 1)
                {
                    recoveryComplete = true;
                }
            }
        }
        catch (IOException e)
        {
            throw new ReplicatorException(
                    "I/O error while scanning log file: name="
                            + logFile.getFile().getAbsolutePath() + " offset="
                            + logFile.getOffset(), e);
        }
        finally
        {
            // Make sure the log file is closed.
            if (logFile != null)
                logFile.close();
        }

        // If this log is writable, compute the write flush interval.
        logger.info("Setting up log flush policy: fsyncIntervalMillis="
                + flushIntervalMillis + " fsyncOnFlush=" + this.fsyncOnFlush);
        if (!this.readOnly)
        {
            startLogSyncTask();
        }

        // Open up the connection manager for business.
        this.cursorManager = new LogCursorManager();
        cursorManager.setTimeoutMillis(logConnectionTimeoutMillis);
        logger.info(String.format("Idle log connection timeout: %dms",
                logConnectionTimeoutMillis));

        logger.info("Log preparation is complete");
    }

    /**
     * Releases the log resources. This should be called after use to ensure log
     * sync task termination.
     */
    public void release() throws ReplicatorException, InterruptedException
    {
        // Release all connections.
        connectionManager.releaseAll();

        // Free lock on log file.
        if (!readOnly)
            writeLock.release();

        // Terminate the log flush thread.
        stopLogSyncTask();
    }

    // Start log sync task.
    private void startLogSyncTask()
    {
        if (flushIntervalMillis > 0)
        {
            logSyncTask = new LogFlushTask(flushIntervalMillis);
            logSyncThread = new Thread(logSyncTask, "log-sync-"
                    + logDir.getName());
            logSyncThread.start();
            logger.info("Started deferred log sync thread: "
                    + logSyncThread.getName());
        }
    }

    // Stop log sync task.
    private void stopLogSyncTask() throws InterruptedException
    {
        if (logSyncThread != null)
        {
            logger.info("Stopping deferred log sync thread: "
                    + logSyncThread.getName());
            logSyncTask.cancel();
            logSyncThread.interrupt();
            try
            {
                logSyncThread.join(5000);
            }
            finally
            {
                if (logSyncThread.isAlive())
                    logger.warn("Unable to terminate log sync thread: "
                            + logSyncThread.getName());
                logSyncThread = null;
            }
        }
    }

    /**
     * Ensure the log sync tasks is running.
     */
    void checkLogSyncTask() throws InterruptedException
    {
        // Ensure that sync thread is healthy. If not, restart it.
        if (flushIntervalMillis > 0 && logSyncTask.isFinished())
        {
            stopLogSyncTask();
            startLogSyncTask();
        }
    }

    // Log metadata.

    /**
     * Updates the active sequence number. Log files will be retained if they
     * contain this number or above.
     */
    public void setActiveSeqno(long activeSeqno)
    {
        index.setActiveSeqno(activeSeqno);
    }

    /**
     * Returns the active sequence number.
     */
    public long getActiveSeqno()
    {
        if (index != null)
            return index.getActiveSeqno();
        else
            return -1;
    }

    /**
     * Return the maximum sequence number stored in the log.
     */
    public long getMaxSeqno()
    {
        if (logger.isDebugEnabled())
            logger.debug("Getting max seqno for thread "
                    + Thread.currentThread().getName() + "("
                    + Thread.currentThread().getId() + ") using " + this);
        return index.getMaxIndexedSeqno();
    }

    /**
     * Sets the maximum sequence number stored in the log.
     */
    public void setMaxSeqno(long seqno)
    {
        index.setMaxIndexedSeqno(seqno);
    }

    /**
     * Return the minimum sequence number stored in the log.
     */
    public long getMinSeqno()
    {
        return index.getMinIndexedSeqno();
    }

    /**
     * Returns the count of files in the log.
     */
    public int fileCount()
    {
        return index.size();
    }

    /**
     * Returns an array of log files.
     */
    public String[] getLogFileNames()
    {
        return index.getFileNames();
    }

    /**
     * Returns true if this log is writable.
     */
    public boolean isWritable()
    {
        return (!readOnly && writeLock.isLocked());
    }

    // Log connection API.

    /**
     * Creates a new log connection.
     * 
     * @param readonly If true, for read only. Only one active connection may
     *            write at any given time.
     * @return A new log client.
     */
    public LogConnection connect(boolean readonly) throws ReplicatorException
    {
        // Allocate, store, and return the connection.
        LogConnection client = new LogConnection(this, readonly);
        if (logger.isDebugEnabled())
            logger.debug("Client connect to log: connection="
                    + client.toString());
        connectionManager.store(client);
        return client;
    }

    /**
     * Releases a log connection.
     * 
     * @param connection Connection to release
     */
    public void release(LogConnection connection)
    {
        connectionManager.release(connection);
    }

    // New log API with methods to support client connections.

    /**
     * Rotate to the next file to store data : write the rotate event, close the
     * file and prepare the new one, if it does not exists
     * 
     * @dataFile Data file to be rotated
     * @seqno Sequence number of first event in new file
     */
    LogFile rotate(LogFile dataFile, long seqno) throws IOException,
            ReplicatorException, InterruptedException
    {
        // Increment the log index here.
        fileIndex = (fileIndex + 1) % Integer.MAX_VALUE;

        // Write the new record into the log.
        try
        {
            LogEventRotateWriter writer = new LogEventRotateWriter(
                    dataFile.getFile(), fileIndex, doChecksum);
            LogRecord logRec = writer.write();
            dataFile.writeRecord(logRec, 0);
        }
        catch (IOException e)
        {
            throw new THLException(
                    "Error writing rotate log event to log file: name="
                            + dataFile.getFile().getName(), e);
        }

        return startNewLogFile(seqno);
    }

    /**
     * Returns the log file containing a particular seqno or null if it does not
     * exist.
     */
    LogFile getLogFile(long seqno)
    {
        // Find the log file name.
        String name;
        if (seqno == FIRST)
            name = index.getFirstFile();
        else
            name = index.getFile(seqno);

        // Create and return a matching log file instance.
        if (name == null)
            return null;
        else
        {
            LogFile logFile = new LogFile(logDir, name);
            logFile.setBufferSize(bufferSize);
            return logFile;
        }
    }

    /**
     * Returns the log file corresponding to the log file name.
     */
    LogFile getLogFile(String name)
    {
        if (index.fileNameExists(name))
        {
            LogFile logFile = new LogFile(logDir, name);
            logFile.setBufferSize(bufferSize);
            return logFile;
        }
        else
            return null;
    }

    /**
     * Returns the name of a log file based on an index
     * 
     * @return a file name corresponding to the given index
     */
    String getDataFileName(long index)
    {
        return DATA_FILENAME_PREFIX
                + String.format("%0" + fileIndexSize + "d", index);
    }

    /**
     * Opens a log file for reading if it exists. Caller must release the log
     * file.
     * 
     * @param newFileName Name of the file null if the file does not exist
     * @throws ReplicatorException If file exists but cannot be opened
     * @throws InterruptedException Thrown if we are interrupted
     */
    LogFile getLogFileForReading(String newFileName)
            throws ReplicatorException, InterruptedException
    {
        File newFile = new File(logDir, newFileName);
        if (newFile.exists())
        {
            LogFile logFile = new LogFile(newFile);
            logFile.setBufferSize(bufferSize);
            logFile.openRead();
            return logFile;
        }
        else
        {
            return null;
        }
    }

    /**
     * Validates the log to ensure there are no inconsistencies.
     * 
     * @throws LogConsistencyException Thrown if log is not consistent
     */
    public void validate() throws LogConsistencyException
    {
        index.validate(logDir);
    }

    /**
     * Deletes a portion of the log. This operation requires a file lock to
     * accomplish.
     * 
     * @param client Disk log client used for deletion
     * @param low Sequence number specifying the beginning of the range. Leave
     *            null to start from the current beginning of the log.
     * @param high Sequence number specifying the end of the range. Leave null
     *            to delete to the end of the log.
     * @throws ReplicatorException Thrown if delete fails
     */
    public void delete(LogConnection client, Long low, Long high)
            throws ReplicatorException, InterruptedException
    {
        // Ensure the log is writable.
        if (readOnly || !writeLock.isLocked())
        {
            throw new THLException("Attempt to delete from read-only log");
        }

        // Determine the range of sequence numbers to delete.
        long lowSeqno;
        long highSeqno;
        if (low == null)
            lowSeqno = index.getMinIndexedSeqno();
        else
            lowSeqno = low;
        if (high == null)
            highSeqno = index.getMaxIndexedSeqno();
        else
            highSeqno = high;

        // For now we don't permit logs to be deleted from the middle as this
        // would result in corruption.
        if (highSeqno != index.getMaxIndexedSeqno()
                && lowSeqno != index.getMinIndexedSeqno())
        {
            throw new THLException("Deletion range invalid; "
                    + "must include one or both log end points: low seqno="
                    + lowSeqno + " high seqno=" + highSeqno);
        }

        // Start reading through the available log files one index at a time.
        for (LogIndexEntry lie : index.getIndexCopy())
        {
            if (lie.startSeqno >= lowSeqno && lie.endSeqno <= highSeqno)
            {
                logger.info("Deleting log file: " + lie.toString());
                purgeFile(lie);
            }
            else if (lie.startSeqno < lowSeqno && lie.endSeqno >= lowSeqno)
            {
                // Upper end of file is in delete range, so we truncate.
                logger.info("Truncating log file at seqno " + lowSeqno + ": "
                        + lie.toString());
                truncateFile(client, lie, lowSeqno);
            }
        }
    }

    // Drops a file completely.
    private void purgeFile(LogIndexEntry entry)
    {
        index.removeFile(entry.fileName);
        File f = new File(logDir, entry.fileName);
        if (!f.delete())
        {
            logger.warn("Unable to delete log file: " + f.getAbsolutePath());
        }
    }

    // Truncates the file at a particular sequence number.
    private void truncateFile(LogConnection client, LogIndexEntry entry,
            long seqno) throws ReplicatorException, InterruptedException
    {
        LogFile logFile = null;
        try
        {
            // This operation is going to invalidate the current log file
            // connection,if any, as we are going to truncate the file.
            // If there is a current log file connection, close it.
            cursorManager.releaseConnection(client);

            // Open a new log file and get to work.
            logFile = openFile(entry.fileName, false);
            long offset = logFile.getOffset();
            LogRecord currentRecord = logFile.readRecord(0);
            while (!currentRecord.isEmpty())
            {

                // See what kind of event we have.
                byte recordType = currentRecord.getData()[0];
                if (recordType == LogRecord.EVENT_REPL)
                {
                    LogEventReplReader eventReader = new LogEventReplReader(
                            currentRecord, eventSerializer, doChecksum);
                    long currentSeqno = eventReader.getSeqno();
                    eventReader.done();

                    if (currentSeqno >= seqno)
                    {
                        // This means we found the truncation point.
                        logger.info("Truncating log file after sequence number: file="
                                + entry.fileName + " seqno=" + seqno);
                        logFile.setLength(offset);
                        index.setMaxIndexedSeqno(seqno - 1);
                        break;
                    }
                }
                else if (recordType == LogRecord.EVENT_ROTATE)
                {
                    // This means we hit the end of the file without truncating.
                    logger.warn("Unable to truncate log file at intended sequence number: file="
                            + entry.fileName + " seqno=" + seqno);
                    break;
                }

                // Remember current offset and read the next record.
                offset = logFile.getOffset();
                currentRecord = logFile.readRecord(0);
            }
        }
        catch (IOException e)
        {
            throw new THLException(
                    "Unable to read log file: " + entry.fileName, e);
        }
        catch (ReplicatorException e)
        {
            throw new THLException("Unable to process log file: "
                    + entry.fileName, e);
        }
        finally
        {
            if (logFile != null)
                logFile.close();
        }
    }

    /**
     * Open the last log file for writing. The file is assumed to exist as the
     * log must be initialized at this point.
     * 
     * @param readOnly
     * @return a {@link LogFile} object referencing the last indexed file
     * @throws ReplicatorException if an error occurs
     * @throws InterruptedException Thrown if we are interrupted
     */
    LogFile openLastFile(boolean readOnly) throws ReplicatorException,
            InterruptedException
    {
        String logFileName = index.getLastFile();
        return openFile(logFileName, readOnly);
    }

    /**
     * Open a specific log file for writing.
     * 
     * @param logFileName Log file name
     * @param readOnly
     * @return a {@link LogFile} object referencing the last indexed file
     * @throws ReplicatorException if an error occurs
     * @throws InterruptedException Thrown if we are interrupted
     */
    private LogFile openFile(String logFileName, boolean readOnly)
            throws ReplicatorException, InterruptedException
    {
        // Open a LogFile instance. Set log sync task if we are writing and
        // deferred sync is enabled.
        LogFile data = new LogFile(logDir, logFileName);
        if (!readOnly)
        {
            data.setLogSyncTask(logSyncTask);
            data.setFlushIntervalMillis(flushIntervalMillis);
            data.setFsyncOnFlush(readOnly);
        }
        data.setBufferSize(bufferSize);

        // Ensure the file exists.
        if (!data.getFile().exists())
        {
            throw new ReplicatorException(
                    "Last log file does not exist; index may be corrupt: "
                            + data.getFile().getName());
        }

        // Open for writing. The file exists so we pass in -1 for the
        // sequence number because we won't write it in the header. This
        // is hacky but should work.
        if (logger.isDebugEnabled())
            logger.debug("Opening log file: "
                    + data.getFile().getAbsolutePath());

        if (readOnly)
            data.openRead();
        else
            data.openWrite();

        return data;
    }

    /**
     * Start a new log file.
     * 
     * @seqno Sequence number of first event in the file
     */
    private LogFile startNewLogFile(long seqno) throws ReplicatorException,
            IOException, InterruptedException
    {
        // Open new log file and update index. 
        String logFileName = getDataFileName(fileIndex);
        LogFile dataFile = new LogFile(logDir, logFileName);
        dataFile.setBufferSize(bufferSize);
        if (dataFile.getFile().exists())
        {
            throw new THLException("New log file exists already: "
                    + dataFile.getFile().getName());
        }
        dataFile.create(seqno);

        // Add the file to the volatile index.
        index.addNewFile(seqno, logFileName);

        return dataFile;
    }

    /**
     * getIndex returns a String representation of the index, built from the
     * configured log directory.
     * 
     * @return a string representation of the index
     */
    public String getIndex()
    {
        return index.toString();
    }
    
    /**
     * Returns the first index file or null if no such file exists.
     */
    public String getFirstFile()
    {
        return index.getFirstFile();
    }

    /**
     * Returns the last index file or null if no such file exists.
     */
    public String getLastFile()
    {
        return index.getLastFile();
    }

    /**
     * Returns a sorted list of log files.
     * 
     * @param logDir Directory containing logs
     * @param logFilePrefix Prefix for log file names
     * @return Array of logfiles (zero-length if log is not initialized)
     */
    public static File[] listLogFiles(File logDir, String logFilePrefix)
    {
        // Find the log files and sort into file name order.
        ArrayList<File> logFiles = new ArrayList<File>();
        for (File f : logDir.listFiles())
        {
            if (!f.isDirectory() && f.getName().startsWith(logFilePrefix))
            {
                logFiles.add(f);
            }
        }
        File[] logFileArray = new File[logFiles.size()];
        return logFiles.toArray(logFileArray);
    }
}