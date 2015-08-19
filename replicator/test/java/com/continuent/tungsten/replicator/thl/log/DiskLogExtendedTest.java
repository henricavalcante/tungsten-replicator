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

package com.continuent.tungsten.replicator.thl.log;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.HashMap;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.thl.THLEvent;

/**
 * Tests concurrency operations on the disk log. This test is an extension of
 * cases in DiskLogTest and uses a similar approach.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class DiskLogExtendedTest extends TestCase
{
    private static Logger logger = Logger.getLogger(DiskLogExtendedTest.class);

    /**
     * Setup.
     * 
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
    }

    /**
     * Teardown.
     * 
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

    /**
     * Confirm that we can handle multiple readers accessing the log at
     * different locations while simultaneously writing. We need to make the
     * logs small enough to pick off problems with log rotate events not
     * appearing in time.
     */
    public void testConcurrentAccess() throws Exception
    {
        // Create the log and write multiple events.
        File logDir = prepareLogDir("testConcurrentAccess");
        DiskLog log = new DiskLog();
        log.setDoChecksum(true);
        log.setReadOnly(false);
        log.setLogDir(logDir.getAbsolutePath());
        log.setLogFileSize(10000);
        log.setFlushIntervalMillis(100);
        log.prepare();

        // Create and start readers.
        HashMap<Thread, SimpleLogReader> tasks = new HashMap<Thread, SimpleLogReader>();
        for (int i = 0; i < 10; i++)
        {
            SimpleLogReader reader = new SimpleLogReader(log, 0, 100000);
            Thread thread = new Thread(reader);
            tasks.put(thread, reader);
            thread.start();
        }

        // Write a bunch of events to the log.
        writeEventsToLog(log, 0, 100000);

        // Wait for readers to finish. 30 seconds should be sufficient.
        for (Thread thread : tasks.keySet())
        {
            thread.join(30000);
            thread.interrupt();

            SimpleLogReader reader = tasks.get(thread);
            if (reader.error != null)
            {
                throw new Exception(
                        "Reader thread failed with exception after "
                                + reader.eventsRead + " events", reader.error);
            }
            assertEquals("Checking events read", 100000, reader.eventsRead);
        }

        // Release the log.
        log.release();
    }

    /**
     * Test the effect of varying fsync intervals from 20 to 2000ms and using
     * buffer sizes from 64k to 512k.
     */
    public void testVaryingFsync() throws Exception
    {
        // Define fsync intervals and buffer sizes for test.
        long[] fsyncIntervals = {20, 100, 500, 2000};
        int[] bufferSizes = {65536, 524288};

        // Perform a test for each interval.
        int run = 0;
        for (long fsync : fsyncIntervals)
        {
            for (int buffer : bufferSizes)
            {
                run++;

                // Initialize log.
                File logDir = prepareLogDir("testVaryingFsync");
                DiskLog log = new DiskLog();
                log.setDoChecksum(false);
                log.setReadOnly(false);
                log.setLogDir(logDir.getAbsolutePath());
                log.setLogFileSize(1000000);
                log.setFlushIntervalMillis(fsync);
                log.setBufferSize(buffer);

                // Write a bunch of events to the log.
                long start = System.currentTimeMillis();
                log.prepare();
                writeEventsToLog(log, 0, 100000);
                log.release();
                long end = System.currentTimeMillis();

                // Print elapsed time.
                double elapsed = (end - start) / 1000.0;
                String msg = String.format(
                        "Test %d: fsync ms=%d, buffer=%d, elapsed secs=%f\n",
                        run, fsync, buffer, elapsed);
                logger.info(msg);
            }
        }
    }

    /**
     * Confirm that if the log retention is set we will purge files after the
     * specified interval but that we always retain at least the last two log
     * files.
     */
    public void testLogRetention() throws Exception
    {
        // Create the log with with 5K log files and a 5 second retention.
        File logDir = prepareLogDir("testLogRetention");
        DiskLog log = new DiskLog();
        log.setLogDir(logDir.getAbsolutePath());
        log.setReadOnly(false);
        log.setLogFileSize(3000);
        log.setTimeoutMillis(10000);
        log.setLogFileRetainMillis(2000);

        log.prepare();
        writeEventsToLog(log, 200);

        // Collect the log file count and ensure it is greater than two.
        int fileCount = log.fileCount();
        assertTrue("More than two logs generated", fileCount > 2);

        // Wait for the retention to expire.
        Thread.sleep(4000);

        // Write enough events to force log rotation by computing how many
        // events on average go into a single log.
        int logEvents = (200 / fileCount) * 2;
        writeEventsToLog(log, 200, logEvents);

        // Give the deletion thread time to do its work.
        Thread.sleep(3000);

        // Confirm no logs are expired. The active seqno points to the start
        // of the log so it would be a bug if they were dropped.
        int fileCount2 = log.fileCount();
        assertTrue("Aging out should delete no files", fileCount2 > fileCount);

        // Now set the active sequence number in the log to the last event
        // written.
        log.setActiveSeqno(log.getMaxSeqno());

        // Write enough events to force log rotation by computing how many
        // events on average go into a single log.
        writeEventsToLog(log, log.getMaxSeqno() + 1, logEvents);

        // Give the deletion thread time to do its work.
        Thread.sleep(3000);

        // We should now have 3 logs because the old logs will age out. We
        // have the file with the active seqno, plus two additional log files.
        // (It turns out that the last set of transactions may write two
        // files.)
        int fileCount3 = log.fileCount();
        assertEquals("Aging out should result in 3 logs", 3, fileCount3);

        // All done!
        log.release();
    }

    /**
     * Confirm that if a log file goes missing while the log is open, readers
     * fail after the log rotation timeout expires.
     */
    public void testMissingLogFile() throws Exception
    {
        // Create the log with with 3K log files.
        File logDir = prepareLogDir("testLogRetention");
        DiskLog log = new DiskLog();
        log.setLogDir(logDir.getAbsolutePath());
        log.setReadOnly(false);
        log.setLogFileSize(3000);
        log.setLogRotateMillis(2000);

        log.prepare();
        writeEventsToLog(log, 200);

        // Prove we can read the log.
        long lastSeqno = scanLog(log);
        assertEquals("scanned to end of log", log.getMaxSeqno(), lastSeqno);

        // Get the log files and delete a file from the middle of the list.
        String[] logFileNames = log.getLogFileNames();
        int fileCount = logFileNames.length;
        assertTrue("More than two logs generated", fileCount > 2);
        int middle = (fileCount / 2) + 1;
        deleteLogFile(logDir, logFileNames[middle]);

        // Now read from the log again. We should get a timeout failure.
        try
        {
            long lastSeqno2 = scanLog(log);
            throw new Exception("Able to scan a broken log!! last seqno="
                    + lastSeqno2);
        }
        catch (LogTimeoutException e)
        {
            logger.info("Caught expected timeout: " + e);
        }

        // All done!
        log.release();
    }

    /**
     * Confirm that if the last file is zero length on log startup we clean up
     * if the log is writable and allow further writes. If the log is readable,
     * we ignore the bad file and allow reads.
     */
    public void testZeroLengthTail() throws Exception
    {
        // Create the log with with 5K log files and a 5 second retention.
        File logDir = prepareLogDir("testZeroLengthTail");
        DiskLog log = new DiskLog();
        log.setLogDir(logDir.getAbsolutePath());
        log.setReadOnly(false);
        log.setLogFileSize(3000);

        log.prepare();
        writeEventsToLog(log, 200);
        long maxSeqno = log.getMaxSeqno();
        log.release();

        // Get the log files and make the last log file 0 length.
        String[] logFileNames = log.getLogFileNames();
        int fileCount = logFileNames.length;
        assertTrue("More than two logs generated", fileCount > 2);
        String lastLogName = logFileNames[fileCount - 1];
        File lastLog = new File(logDir, lastLogName);
        assertTrue("lastLog is non-zero length: " + lastLog.toString(),
                lastLog.length() > 0);
        truncate(lastLog);
        assertEquals("lastLog is zero length: " + lastLog.toString(), 0,
                lastLog.length());

        // Open a writable log and get the beginning and end positions.
        DiskLog log2 = new DiskLog();
        log2.setLogDir(logDir.getAbsolutePath());
        log2.setReadOnly(false);
        log2.setLogRotateMillis(2000);

        // Open the log and write a few more events.
        log2.prepare();
        long maxSeqno2 = log2.getMaxSeqno();
        logger.info("Number of events after cleanup of bad header: "
                + maxSeqno2);

        assertTrue("Truncated log must be shorter than old log",
                maxSeqno > maxSeqno2);
        writeEventsToLog(log2, maxSeqno2 + 1, 200);

        // Scan and confirm that the end of the log is the same as
        // the reported maxSeqno value.
        long scanSeqno2 = scanLog(log2);
        assertEquals("Last event scanned must be maxSeqno", log2.getMaxSeqno(),
                scanSeqno2);
        logger.info("Number of events read after writing to cleaned-up log: "
                + scanSeqno2);
        log2.release();

        // Truncate the log again.
        truncate(lastLog);
        assertEquals("lastLog is zero length: " + lastLog.toString(), 0,
                lastLog.length());

        // Open a read-only log.
        DiskLog log3 = new DiskLog();
        log3.setLogDir(logDir.getAbsolutePath());
        log3.setReadOnly(true);
        log3.setLogRotateMillis(2000);
        log3.prepare();

        // Scan the log and prove it is shorter. This should also generate a
        // warning but we cannot test for that.
        long scanSeqno3 = scanLog(log3);
        assertTrue("Log must be readable and shorter", scanSeqno3 < scanSeqno2);
        logger.info("Number of events read after ignoring bad header: "
                + scanSeqno3);

        // All done!
        log3.release();
    }

    /**
     * Confirm that if a middle file is zero length on log startup we signal an
     * error. This is a sign of log corruption.
     */
    public void testZeroLengthMiddleFile() throws Exception
    {
        // Create the log with with 5K log files and a 5 second retention.
        File logDir = prepareLogDir("testZeroLengthMiddleFile");
        DiskLog log = new DiskLog();
        log.setLogDir(logDir.getAbsolutePath());
        log.setReadOnly(false);
        log.setLogFileSize(3000);

        log.prepare();
        writeEventsToLog(log, 200);
        log.release();

        // Get the log files and make a middle file 0 length.
        String[] logFileNames = log.getLogFileNames();
        int fileCount = logFileNames.length;
        int middleFileIndex = fileCount / 2;
        assertTrue("More than two logs generated", fileCount > 2);
        assertTrue("Middle > 0", middleFileIndex > 0);

        String middleLogName = logFileNames[middleFileIndex];
        File middleLog = new File(logDir, middleLogName);
        assertTrue("middleLog is non-zero length: " + middleLog.toString(),
                middleLog.length() > 0);
        truncate(middleLog);
        assertEquals("lastLog is zero length: " + middleLog.toString(), 0,
                middleLog.length());

        // Open a writable log and get the beginning and end positions.
        DiskLog log2 = new DiskLog();
        log2.setLogDir(logDir.getAbsolutePath());
        log2.setReadOnly(false);
        log2.setLogRotateMillis(2000);

        // Open the log.
        try
        {
            log2.prepare();
            throw new Exception(
                    "Able to open log with zero-length file in middle: "
                            + middleLog.getAbsolutePath());
        }
        catch (LogConsistencyException e)
        {
            logger.info("Got expected exception: " + e.getMessage());
        }
        log2.release();
    }

    // Truncate a file to 0 bytes length.
    private void truncate(File f) throws IOException
    {
        FileOutputStream fis = new FileOutputStream(f);
        fis.flush();
        fis.close();
    }

    // Create an empty log directory or if the directory exists remove
    // any files within it.
    private File prepareLogDir(String logDirName)
    {
        File logDir = new File(logDirName);
        if (logDir.exists())
        {
            for (File f : logDir.listFiles())
            {
                f.delete();
            }
        }
        else
        {
            logDir.mkdirs();
        }
        return logDir;
    }

    // Write a prescribed number of events to the log starting at zero.
    private void writeEventsToLog(DiskLog log, int howMany)
            throws ReplicatorException, InterruptedException
    {
        writeEventsToLog(log, 0, howMany);
    }

    // Write a prescribed number of events to the log starting at a
    // specified sequence number. Last event will be committed.
    private void writeEventsToLog(DiskLog log, long seqno, int howMany)
            throws ReplicatorException, InterruptedException
    {
        // Should match incremented seqno on the final iteration.
        long lastSeqno = seqno + howMany;

        // Write a series of events to the log.
        LogConnection conn = log.connect(false);
        for (int i = 0; i < howMany; i++)
        {
            THLEvent e = this.createTHLEvent(seqno++);
            conn.store(e, (seqno == lastSeqno));
        }
        conn.commit();
        conn.release();
        assertEquals("Should have stored requested events", (seqno - 1),
                log.getMaxSeqno());
        logger.info("Final seqno: " + (seqno - 1));
    }

    // Create a dummy THL event.
    private THLEvent createTHLEvent(long seqno, short fragno, boolean lastFrag,
            String sourceId)
    {
        String eventId = new Long(seqno).toString();
        ReplDBMSEvent replEvent = new ReplDBMSEvent(seqno, fragno, lastFrag,
                sourceId, 1, new Timestamp(System.currentTimeMillis()),
                new DBMSEvent());
        return new THLEvent(eventId, replEvent);
    }

    private THLEvent createTHLEvent(long seqno)
    {
        return createTHLEvent(seqno, (short) 0, true, "test");
    }

    // Scan the entire log and return the last sequence number.
    private long scanLog(DiskLog log) throws ReplicatorException,
            InterruptedException
    {
        LogConnection conn = log.connect(true);
        long maxSeqno = log.getMaxSeqno();
        long minSeqno = log.getMinSeqno();
        long lastSeqno = -1;

        assertTrue("Seeking to min log position", conn.seek(minSeqno));
        for (long i = minSeqno; i <= maxSeqno; i++)
        {
            THLEvent e = conn.next(true);
            lastSeqno = e.getSeqno();
        }
        conn.release();

        return lastSeqno;
    }

    // Deletes a log file with suitable assertions.
    private void deleteLogFile(File logDir, String logFileName)
            throws Exception
    {
        File fileToDelete = new File(logDir, logFileName);
        if (!fileToDelete.exists())
        {
            throw new Exception("File to delete does not exist: "
                    + fileToDelete.getAbsolutePath());
        }

        if (!fileToDelete.delete())
        {
            throw new Exception("Unable to delete file: "
                    + fileToDelete.getAbsolutePath());
        }

        if (fileToDelete.exists())
        {
            throw new Exception("Deleted file exists: "
                    + fileToDelete.getAbsolutePath());
        }
    }
}