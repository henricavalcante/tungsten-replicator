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
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.commands.FileCommands;
import com.continuent.tungsten.common.config.Interval;
import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * Implements an in-memory index showing the starting sequence number of each
 * index file. Index operations are fully synchronized to ensure there are no
 * issues due to concurrent access across threads.
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class LogIndex
{
    static Logger                    logger      = Logger.getLogger(LogIndex.class);

    private ArrayList<LogIndexEntry> index;
    private File                     logDir;
    private String                   filePrefix;
    private long                     retentionMillis;
    private long                     activeSeqno = 0;
    private int                      bufferSize;

    /**
     * Creates a new in-memory instance on all log files in a particular
     * directory.
     * 
     * @param logDir Log directory
     * @param filePrefix Prefix for log files
     * @param retentionMillis Amount of time to retain log files before
     *            auto-deleting
     * @param bufferSize Buffer size for reading log files
     * @param isWritable True if the log is writable and we can clean up
     * @throws ReplicatorException Thrown in the event of an error constructing
     *             the index
     */
    public LogIndex(File logDir, String filePrefix, long retentionMillis,
            int bufferSize, boolean isWritable) throws ReplicatorException,
            InterruptedException
    {
        index = new ArrayList<LogIndexEntry>();
        this.logDir = logDir;
        this.filePrefix = filePrefix;
        this.retentionMillis = retentionMillis;
        this.bufferSize = bufferSize;
        build(isWritable);
    }

    /**
     * Builds the index. This routine checks for basic log corruption due to
     * missing or partial header records. If we are writable and the header is
     * missing at the end, we clean the last file. It is best to do this when
     * building the index as we have all information in hand.
     * 
     * @param isWritable If true the log is writable.
     * @throws ReplicatorException Thrown if there is an error reading or fixing
     *             up the log
     * @throws InterruptedException Thrown if we are interrupted
     */
    private synchronized void build(boolean isWritable)
            throws ReplicatorException, InterruptedException
    {
        logger.info("Building file index on log directory: " + logDir);

        // Find the log files and sort into file name order.
        FileFilter fileFilter = new FileFilter()
        {
            public boolean accept(File file)
            {
                return !file.isDirectory()
                        && file.getName().startsWith(filePrefix);
            }
        };
        File[] files = logDir.listFiles(fileFilter);
        Arrays.sort(files);

        // Scan each file to get the base sequence number of the file. This
        // is incremented to give the starting index number of this file.
        // We use the starting number of the next index entry to compute the
        // ending index entry.
        LogIndexEntry lastEntry = null;
        for (int i = 0; i < files.length; i++)
        {
            // Select the file.
            File file = files[i];

            // Make sure the file has a full header.
            long fileSize = file.length();
            if (fileSize < LogFile.HEADER_LENGTH)
            {
                // If we have a header and are on the last file, this is due to
                // unclean shutdown.
                if ((i + 1) >= files.length)
                {
                    if (isWritable)
                    {
                        // Delete the file if we are writable.
                        logger.info("Cleaning up partially written tail log file: file="
                                + file.getAbsolutePath()
                                + " length="
                                + fileSize);
                        if (!file.delete())
                        {
                            throw new LogConsistencyException(
                                    "Unable to clean up partially written log file: "
                                            + file.getAbsolutePath());
                        }
                    }
                    else
                    {
                        // Try to ignore the file if we are read-only.
                        logger.warn("Ignoring partially written tail log file: file="
                                + file.getAbsolutePath()
                                + " length="
                                + fileSize);
                    }
                    break;
                }
                else
                {
                    // This is a log consistency error--there is a file within
                    // the log that has a partial header. It should be removed.
                    throw new LogConsistencyException(
                            "Found invalid log file header; log must be purged up to this file to open: "
                                    + file.getAbsolutePath());
                }
            }

            // Try to read the base sequence number. Any file that cannot be
            // read is ignored for indexing purposes.
            if (logger.isDebugEnabled())
                logger.debug("Checking " + file.getName());
            LogFile lf = new LogFile(file);
            lf.setBufferSize(bufferSize);
            lf.openRead();
            long seqno = lf.getBaseSeqno();

            // If we get -1 it means we have the first file in the
            // index. Try to read the header of the first record to
            // get the correct starting sequence.
            if (seqno < 0)
            {
                try
                {
                    LogRecord record1 = lf.readRecord(0);
                    if (!record1.isEmpty() && !record1.isTruncated())
                    {
                        if (record1.getData()[0] == LogRecord.EVENT_REPL)
                        {
                            LogEventReplReader eventReader = new LogEventReplReader(
                                    record1, null, false);
                            seqno = eventReader.getSeqno();
                            eventReader.done();
                        }
                        else
                        {
                            logger.warn("Unexpected record type in first log record: type="
                                    + record1.getData()[0]
                                    + " file="
                                    + lf.getFile().getAbsolutePath());
                        }
                    }
                }
                catch (IOException e)
                {
                    logger.warn(
                            "Unable to read sequence number of first log record: file="
                                    + lf.getFile().getAbsolutePath(), e);
                }
            }

            // Decrement to set the end seqno of the previous index entry.
            if (lastEntry != null)
            {
                lastEntry.endSeqno = seqno - 1;
                if (logger.isDebugEnabled())
                    logger.debug("Updating " + lastEntry);
            }

            // Create the next index entry.
            LogIndexEntry ie = new LogIndexEntry(seqno, Long.MAX_VALUE, lf
                    .getFile().getName());
            index.add(ie);
            if (logger.isDebugEnabled())
                logger.debug("Adding index entry: " + ie);

            // Remember this entry and release the log file.
            lastEntry = ie;
            lf.close();
        }
        Collections.sort(index);
        logger.info("Constructed index; total log files added=" + index.size());
    }

    /**
     * Returns the current active sequence number.
     */
    public synchronized long getActiveSeqno()
    {
        return activeSeqno;
    }

    /**
     * Sets the active sequence number, which is the lowest sequence number
     * known to be in use by clients of this log. Log files can only be deleted
     * if they are before this sequence number.
     */
    public synchronized void setActiveSeqno(long activeSeqno)
    {
        this.activeSeqno = activeSeqno;
    }

    /**
     * Returns true if the index is empty.
     */
    public synchronized boolean isEmpty()
    {
        return index.isEmpty();
    }

    /**
     * Returns the number of files in the index.
     */
    public synchronized int size()
    {
        return index.size();
    }

    /**
     * Releases resources in the index.
     */
    public synchronized void release()
    {
        index.clear();
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        StringBuffer ind = new StringBuffer();
        for (LogIndexEntry entry : index)
        {
            ind.append(entry);
            ind.append('\n');
        }
        return ind.toString();
    }

    /**
     * Sets the maximum sequence number this index knows about by updating the
     * last index entry. This routine takes care of patching up the first index
     * entry in a new log, which starts with the default value -1 until we find
     * the correct sequence number.
     */
    public synchronized void setMaxIndexedSeqno(long seqno)
    {
        if (index != null && !index.isEmpty())
        {
            LogIndexEntry entry = index.get(index.size() - 1);
            entry.endSeqno = seqno;
            if (entry.startSeqno < 0)
            {
                // Patch up start entry on new index.
                entry.startSeqno = seqno;
            }
            else if (entry.startSeqno > entry.endSeqno)
            {
                // Patch up if recovery caused the initial seqno to be wiped
                // out.
                entry.startSeqno = seqno;
            }
        }
    }

    /**
     * Returns the maximum committed sequence number this index knows about. If
     * the currently open file does not have anything written to it, return the
     * end value from the previous entry.
     * 
     * @return -1 if index is empty, otherwise, return the max value
     */
    public synchronized long getMaxIndexedSeqno()
    {
        // If the index is empty return -1.
        if (index == null || index.isEmpty())
            return -1;

        // If the index has been written, get the last committed value.
        LogIndexEntry entry = index.get(index.size() - 1);
        if (entry.startSeqno < 0)
        {
            // If the index is empty, return -1.
            return -1;
        }
        else if (entry.endSeqno < Long.MAX_VALUE)
        {
            // Last entry has a committed value. Return that.
            return entry.endSeqno;
        }
        else if (index.size() >= 2)
        {
            // Last entry has no committed value. Return the previous log
            // last committed value.
            entry = index.get(index.size() - 2);
            return entry.endSeqno;
        }
        else
        {
            // We should not be able to get here, but return -1 to avoid
            // breaking things in the event that there is a case we did
            // not think of.
            return -1;
        }
    }

    /**
     * Returns the minimum sequence number this index knows about. This number
     * might not be committed if we are just writing a new log.
     */
    public synchronized long getMinIndexedSeqno()
    {
        if (index == null || index.isEmpty())
            return -1;
        else
            return index.get(0).startSeqno;
    }

    /**
     * Locates and returns the file that contains a given sequence number. The
     * implementation uses a linear search, so we assume this is a comparatively
     * rare operation on connection startup.
     */
    public synchronized String getFile(long seqno)
    {
        if (logger.isDebugEnabled())
            logger.debug("Request to find seqno in index: seqno=" + seqno
                    + " index size=" + index.size());

        // If the value is less than the smallest indexed sequence number,
        // we don't have a file to offer. Return a null in this case.
        if (seqno < getMinIndexedSeqno() || seqno < 0)
            return null;

        // Search the current file index.
        for (LogIndexEntry indexEntry : index)
        {
            if (indexEntry.contains(seqno))
                return indexEntry.fileName;
        }

        // We did not find a log file with the value currently present. We now
        // return the last log file or nothing if there are no log files.
        int last = index.size() - 1;
        if (last >= 0)
            return index.get(last).fileName;
        else
            return null;
    }

    /**
     * Returns true if the name of a log file exists in the index.
     */
    public synchronized boolean fileNameExists(String name)
    {
        // Search the current file index.
        for (LogIndexEntry indexEntry : index)
        {
            if (indexEntry.fileName.equals(name))
                return true;
        }
        return false;
    }

    /**
     * Returns a copy of the index entries in sorted order.
     */
    public synchronized List<LogIndexEntry> getIndexCopy()
    {
        return new ArrayList<LogIndexEntry>(index);
    }

    /**
     * Returns the first index file or null if no such file exists.
     */
    public synchronized String getFirstFile()
    {
        if (index.size() == 0)
            return null;
        else
            return index.get(0).fileName;
    }

    /**
     * Returns the last index file or null if no such file exists.
     */
    public synchronized String getLastFile()
    {
        if (index.size() == 0)
            return null;
        else
            return index.get(index.size() - 1).fileName;
    }

    /**
     * Returns an array containing all file names.
     */
    public synchronized String[] getFileNames()
    {
        String[] fileNames = new String[this.size()];
        int i = 0;
        for (LogIndexEntry lie : index)
        {
            fileNames[i++] = lie.fileName;
        }
        return fileNames;
    }

    /**
     * Adds a new file to the index.
     * 
     * @param seqno Starting sequence number in the file
     * @param fileName Name of the log file
     */
    public synchronized void addNewFile(long seqno, String fileName)
    {
        // Add the entry.
        logger.info("Adding new index entry for " + fileName
                + " starting at seqno " + seqno);
        index.add(new LogIndexEntry(seqno, Long.MAX_VALUE, fileName));

        // If retentions are enabled, this is a good time to check for files
        // to purge. Note that we always retain the last two files in the
        // index to prevent unhappy accidents due to deleting a file that is
        // currently active. Also we never delete a file that contains
        // sequence numbers at or before the active sequence number.
        if (retentionMillis > 0)
        {
            String activeFile = getFile(activeSeqno);
            File[] purgeCandidates = FileCommands
                    .filesOverRetentionAndInactive(logDir, filePrefix, 2,
                            activeFile);
            File[] filesToPurge = FileCommands.filesOverModDate(
                    purgeCandidates, new Interval(retentionMillis));
            if (filesToPurge.length > 0)
            {
                for (File file : filesToPurge)
                    removeFile(file.getName());

                FileCommands.deleteFiles(filesToPurge, false);
            }
        }
    }

    /**
     * Remove a file from the index.
     */
    public synchronized void removeFile(String fileName)
    {
        for (LogIndexEntry entry : index)
        {
            if (fileName.equals(entry.fileName))
            {
                index.remove(entry);
                logger.info("Removed file from disk log index: " + fileName);
                return;
            }
        }
        logger.warn("Attempt to remove non-existent file from disk log index: "
                + fileName);
    }

    /**
     * Validates the index by ensuring that each file exists and that the log
     * entries have matching start and end dates.
     */
    public void validate(File logDir) throws LogConsistencyException
    {
        long prevEndSeqno = -1;
        for (LogIndexEntry entry : this.index)
        {
            // Check for file existence.
            File f = new File(logDir, entry.fileName);
            if (!f.exists())
            {
                throw new LogConsistencyException("Indexed file is missing: "
                        + entry.toString());
            }

            // Ensure that there is no gap between sequence numbers on
            // index entries.
            if (prevEndSeqno >= 0 && (prevEndSeqno + 1) != entry.startSeqno)
            {
                throw new LogConsistencyException(
                        "Start seqno does not match previous entry's end seqno value: prev end seqno="
                                + prevEndSeqno + " " + entry.toString());
            }

            // Ensure that end sequence number is greater than or equal to
            // the start.
            if (entry.startSeqno > entry.endSeqno)
            {
                throw new LogConsistencyException(
                        "Start seqno greater than end seqno: "
                                + entry.toString());
            }

            prevEndSeqno = entry.endSeqno;
        }
    }
}