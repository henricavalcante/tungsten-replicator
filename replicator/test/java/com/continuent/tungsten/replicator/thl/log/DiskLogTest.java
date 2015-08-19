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
import java.io.FileWriter;
import java.io.RandomAccessFile;
import java.sql.Timestamp;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSFilteredEvent;
import com.continuent.tungsten.replicator.event.ReplEvent;
import com.continuent.tungsten.replicator.thl.THLEvent;
import com.continuent.tungsten.replicator.thl.THLException;
import com.continuent.tungsten.replicator.thl.serializer.ProtobufSerializer;

/**
 * Tests public methods on the disk log. The tests in this suite require a
 * directory in which to write log records. The default serializer for this test
 * is currently the ProtobufSerializer. To use the Java serializer define the
 * following macro: <code><pre>
 *   -Dserializer=com.continuent.tungsten.enterprise.replicator.thl.serializer.JavaSerializer
 * </pre></code> The serializer class will be loaded.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class DiskLogTest extends TestCase
{
    private static Logger logger     = Logger.getLogger(DiskLogTest.class);
    private Class<?>      serializer = ProtobufSerializer.class;

    /**
     * Setup.
     * 
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        String serializerClass = System.getProperty("serializer");
        if (serializerClass != null)
        {
            serializer = Class.forName(serializerClass);
        }
        logger.info("Using serializer: " + serializer.getName());
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
     * Confirm that we can prepare a log with defaults and release it.
     */
    public void testLogPreparation() throws Exception
    {
        // Ensure log directory does not exist yet.
        File logDir = prepareLogDir("testLogPreparation");
        DiskLog log = new DiskLog();
        log.setLogDir(logDir.getAbsolutePath());
        log.setReadOnly(false);
        log.prepare();
        log.release();
    }

    /**
     * Confirm that read-only log preparation fails if the log does not exist.
     */
    public void testLogReadonlyNonExistent() throws Exception
    {
        File logDir = prepareLogDir("testLogReadonlyNonExistent");
        DiskLog log = new DiskLog();
        log.setReadOnly(true);
        log.setLogDir(logDir.getAbsolutePath());
        try
        {
            log.prepare();
            throw new Exception("Able to prepare r/o log that has no files");
        }
        catch (ReplicatorException e)
        {
        }
    }

    /**
     * Confirm that log preparation succeeds if the log is writable but does not
     * exist.
     */
    public void testLogWritableNonExistent() throws Exception
    {
        // Delete existing log file, if any.
        File logDir = new File("testLogWritableNonExistent");
        if (logDir.exists())
        {
            for (File f : logDir.listFiles())
            {
                f.delete();
            }
            logDir.delete();
        }
        assertFalse("Log dir does not exist", logDir.exists());

        // Create disk log and confirm we can prepare the same.
        DiskLog log = new DiskLog();
        log.setReadOnly(false);
        log.setLogDir(logDir.getAbsolutePath());
        log.prepare();
        log.release();

        // Confirm log directory now exists.
        assertTrue("Log dir exists", logDir.exists() && logDir.isDirectory());
    }

    /**
     * Confirm that we can write to the log and read back what we wrote using
     * sequence numbers.
     */
    public void testReadWriteBasic() throws Exception
    {
        // Create the log.
        File logDir = prepareLogDir("testReadWriteBasic");
        DiskLog log = openLog(logDir, false);

        // Add three records.
        LogConnection conn = log.connect(false);
        for (int i = 0; i < 3; i++)
        {
            THLEvent e = this.createTHLEvent(i);
            conn.store(e, false);
        }
        conn.commit();
        assertEquals("Should have seqnos 0-2", 0, log.getMinSeqno());
        assertEquals("Should have seqnos 0-2", 2, log.getMaxSeqno());

        // Find the records again.
        assertTrue("Find first record", conn.seek(0));
        for (int i = 0; i < 3; i++)
        {
            THLEvent e = conn.next();
            assertNotNull("Should find an event", e);
            assertEquals("Expect seqno: " + i, i, e.getSeqno());
        }
        conn.release();
        log.release();
    }

    /**
     * Confirm that if a log record in the tail file has a bad checksum
     * attempting to open the log results in a LogConsistencyException. Confirm
     * that we can still open the log and find the record with the bad checksum
     * by turning off checksums.
     */
    public void testOpenWithChecksumFailure() throws Exception
    {
        // Create the log.
        File logDir = prepareLogDir("testOpenWithChecksumFailure");
        DiskLog log = openLog(logDir, false);

        // Add three records.
        writeEventsToLog(log, 3);

        // Get the last log file and corrupt the checksum on the last record.
        // This is the last 8 bytes of the file--we set it to an
        // incorrect value.
        LogFile lf = log.getLogFile(2);
        RandomAccessFile raf = new RandomAccessFile(lf.getFile(), "rw");
        long len = raf.length();
        raf.seek(len - 8);
        raf.writeLong(99);
        raf.close();
        log.release();

        // Try to open the log file and confirm that a log consistency
        // exception results.
        try
        {
            log = openLog(logDir, false);
            throw new Exception("Able to open log with bad checksum!");
        }
        catch (LogConsistencyException e)
        {
            logger.info("Received expected exception" + e.toString());
        }

        // Confirm that if we disable CRCs we can open the log.
        findSeqnoWithoutChecksums(logDir, 2);
    }

    /**
     * Confirm that if a log record in the tail file has a bad CRC type
     * attempting to open the log results in a LogConsistencyException. Confirm
     * that we can still open the log and find the record with the bad type by
     * turning off checksums.
     */
    public void testOpenWithBadChecksumType() throws Exception
    {
        // Create the log.
        File logDir = prepareLogDir("testOpenWithBadChecksumType");
        DiskLog log = openLog(logDir, false);

        // Add three records.
        writeEventsToLog(log, 4);

        // Get the last log file and corrupt the checksum type on the last
        // record.
        // This is located in the 9th byte from the end of the file.
        LogFile lf = log.getLogFile(3);
        RandomAccessFile raf = new RandomAccessFile(lf.getFile(), "rw");
        long len = raf.length();
        raf.seek(len - 9);
        raf.writeByte(101);
        raf.close();
        log.release();

        // Try to open the log file and confirm that a log consistency
        // exception results.
        try
        {
            log = openLog(logDir, false);
            throw new Exception("Able to open log with bad checksum!");
        }
        catch (LogConsistencyException e)
        {
            logger.info("Received expected exception" + e.toString());
        }

        // Confirm that if we disable CRCs we can open the log.
        findSeqnoWithoutChecksums(logDir, 3);
    }

    // Find log sequence number in log opened without checksums.
    private void findSeqnoWithoutChecksums(File logDir, long seqno)
            throws ReplicatorException, InterruptedException
    {
        // Confirm that if we disable CRCs we can open the log.
        DiskLog log2 = new DiskLog();
        log2.setDoChecksum(false);
        log2.setLogDir(logDir.getAbsolutePath());
        log2.prepare();
        // Connect and find the record with the bad seqno.
        LogConnection conn2 = log2.connect(true);
        assertTrue("Find first record", conn2.seek(seqno));
        THLEvent e = conn2.next(false);
        assertNotNull("Should find an event", e);
        assertEquals("Expect seqno: " + seqno, seqno, e.getSeqno());
        conn2.release();
        log2.release();
    }

    /**
     * Confirm that only one writer is permitted per log.
     */
    public void testOneWriter() throws Exception
    {
        File logDir = prepareLogDir("testOneWriter");
        DiskLog log = openLog(logDir, false);

        // Confirm that a second write connection causes an exception.
        LogConnection conn = log.connect(false);
        LogConnection conn2 = null;
        try
        {
            conn2 = log.connect(false);
            throw new Exception("Able to connect with 2nd writer");
        }
        catch (THLException e)
        {
        }

        // Release the first write connection and show that we can now connect.
        conn.release();
        conn2 = log.connect(false);
        assertNotNull("Expect to get a connection", conn2);
        assertFalse("Should be writable", conn2.isReadonly());
        conn2.release();
        log.release();
    }

    /**
     * Confirm that read-only connections cannot write to the log.
     */
    public void testReadonlyNoWrite() throws Exception
    {
        // Create the log.
        File logDir = prepareLogDir("testReadonlyNoWrite");
        DiskLog log = openLog(logDir, false);

        // Confirm that we can write to the log, then close the connection.
        LogConnection conn = log.connect(false);
        THLEvent e0 = this.createTHLEvent(0);
        conn.store(e0, false);
        conn.commit();
        conn.release();

        // Reopen as readonly connection, and try to write to the log.
        conn = log.connect(true);
        THLEvent e1 = this.createTHLEvent(1);
        try
        {
            conn.store(e1, false);
            throw new Exception("Read-only connection writes to log");
        }
        catch (THLException e)
        {
        }

        try
        {
            conn.commit();
            throw new Exception("Read-only connection commits to log");
        }
        catch (THLException e)
        {
        }
        conn.release();
        log.release();

        // Reopen with a writable connection and readonly log. Confirm that
        // writes fail here as well.
        log = this.openLog(logDir, true);
        conn = log.connect(false);
        try
        {
            conn.store(e1, false);
            throw new Exception("Read-only connection writes to log");
        }
        catch (THLException e)
        {
        }

        try
        {
            conn.commit();
            throw new Exception("Read-only connection commits to log");
        }
        catch (THLException e)
        {
        }
        conn.release();
        log.release();
    }

    /**
     * Confirm that the log will not accept a write that causes the seqno to
     * decrease, e.g., writing seqno 25 followed by seqno 24.
     */
    public void testSeqnoOrdering() throws Exception
    {
        // Create the log.
        File logDir = prepareLogDir("testSeqnoOrdering");
        DiskLog log = openLog(logDir, false);

        // Write and commit a transaction to start.
        LogConnection conn = log.connect(false);
        THLEvent e25 = createTHLEvent(25);
        conn.store(e25, true);

        // Write a transaction that decreases the seqno and confirm
        // we get an exception.
        THLEvent e24 = createTHLEvent(24);
        try
        {
            conn.store(e24, true);
            throw new Exception(
                    "Able to store a decreasing seqno on same connection");
        }
        catch (LogConsistencyException e)
        {
        }
        conn.release();

        // Reopen connection and confirm that the same thing happens if
        // we do this on a new connection.
        LogConnection conn2 = log.connect(false);
        THLEvent e24b = createTHLEvent(24);
        try
        {
            conn2.store(e24b, true);
            throw new Exception(
                    "Able to store a decreasing seqno on new connection");
        }
        catch (LogConsistencyException e)
        {
        }
        conn2.release();
        log.release();

        // Reopen log and confirm that the same thing happens if
        // we do this on a new connection on a reopened log.
        DiskLog log2 = openLog(logDir, false);
        LogConnection conn3 = log2.connect(false);
        THLEvent e24c = createTHLEvent(24);
        try
        {
            conn3.store(e24c, true);
            throw new Exception(
                    "Able to store a decreasing seqno on newly opened log");
        }
        catch (LogConsistencyException e)
        {
        }
        conn3.release();
        log2.release();
    }

    /**
     * Confirm that the log will not accept a write in which the fragno is the
     * same or decreases from the previously written log record.
     */
    public void testFragnoOrdering() throws Exception
    {
        // Create the log.
        File logDir = prepareLogDir("testFragnoOrdering");
        DiskLog log = openLog(logDir, false);

        // Write two transaction fragments to start.
        LogConnection conn = log.connect(false);
        THLEvent e25_1 = createTHLEvent(25, (short) 0, false, "x");
        conn.store(e25_1, false);
        THLEvent e25_2 = createTHLEvent(25, (short) 1, false, "x");
        conn.store(e25_2, false);

        // Try to write a fragment that repeats the fragment number.
        THLEvent e25_same = createTHLEvent(25, (short) 1, false, "x");
        try
        {
            conn.store(e25_same, true);
            throw new Exception("Able to store the same fragment number again");
        }
        catch (LogConsistencyException e)
        {
            logger.info("Caught expected exception: " + e.getMessage());
        }

        // Now write a fragment with a lesser fragment number.
        THLEvent e25_less = createTHLEvent(25, (short) 0, false, "x");
        try
        {
            conn.store(e25_less, true);
            throw new Exception("Able to store a lower fragment number");
        }
        catch (LogConsistencyException e)
        {
            logger.info("Caught expected exception: " + e.getMessage());
        }

        // All done.
        conn.release();
        log.release();
    }

    /**
     * Confirm that seeking a non-existent seqno returns false.
     */
    public void testNonexistingSeqno() throws Exception
    {
        // Create a log and write three events to it.
        File logDir = prepareLogDir("testNonexistingSeqno");
        DiskLog log = openLog(logDir, false);
        writeEventsToLog(log, 0, 3);

        // Confirm that seek finds all three events.
        LogConnection conn = log.connect(true);
        for (int i = 0; i < 3; i++)
        {
            assertTrue(conn.seek(i));
        }

        // Confirm that we seek the next number but it is not present.
        assertTrue("can seek to end", conn.seek(3));
        assertNull("cannot read past end", conn.next(false));

        // Confirm that seek does not find events with higher sequence numbers.
        for (int i = 4; i < 10; i++)
        {
            assertTrue("seek past end", conn.seek(i));
            assertNull("find non-existing event", conn.next(false));
        }

        conn.release();
        log.release();
    }

    /**
     * Confirm that seeking a non-existent log file results in an exception.
     */
    public void testNonexistingLogFile() throws Exception
    {
        // Create a log and write three events to it.
        File logDir = prepareLogDir("testNonexistingLogFile");
        DiskLog log = openLog(logDir, false);
        writeEventsToLog(log, 0, 3);

        // Confirm that we can seek to the first log file.
        LogConnection conn = log.connect(true);
        String[] names = log.getLogFileNames();
        for (String name : names)
        {
            assertTrue(conn.seek(name));
        }

        // Confirm that seek does not a non-existent file.
        assertFalse(conn.seek("does-not-exist.dat"));

        conn.release();
        log.release();
    }

    /**
     * Confirm that a next() call on a non-existent seqno blocks if the call is
     * blocking and returns null if it is non-blocking.
     */
    public void testBlockingNext() throws Exception
    {
        // Create a log and write 1000 events to it. Set the timeout interval to
        // 1 second.
        File logDir = prepareLogDir("testNonexistingLogFile");
        DiskLog log = openLog(logDir, false);
        log.setTimeoutMillis(1000);
        writeEventsToLog(log, 0, 1000);

        // See to the final seqno and confirm we can read it.
        LogConnection conn = log.connect(true);
        assertTrue("Seeking last event", conn.seek(999));
        THLEvent e999 = conn.next();
        assertNotNull("Last event is found", e999);

        // Confirm we timeout on the next event if the call is blocking.
        try
        {
            THLEvent e1000 = conn.next();
            throw new Exception("Found non-existent event: " + e1000.toString());
        }
        catch (LogTimeoutException e)
        {
        }

        // Confirm that we return null on the next event if the call is
        // non-blocking.
        THLEvent e1000a = conn.next(false);
        assertNull("Non-blocking call returns null", e1000a);

        // Close up.
        conn.release();
        log.release();
    }

    /**
     * Confirm that we can set a read timeout that is less than the timeout on
     * the log and that blocking reads return null within or around that time.
     */
    public void testReadTimeout() throws Exception
    {
        int bigLogTimeout = 5000;

        // Create the log.
        File logDir = prepareLogDir("testReadWriteBasic");
        DiskLog log = new DiskLog();
        log.setReadOnly(false);
        log.setLogDir(logDir.getAbsolutePath());
        log.setTimeoutMillis(bigLogTimeout);
        log.prepare();

        // Add 1 record to log.
        writeEventsToLog(log, 1);

        // Confirm that normal reading from the log times out after 5 seconds.
        LogConnection conn = log.connect(true);
        assertTrue("Seek first record", conn.seek(0));
        assertNotNull("Return first record", conn.next());
        long startMillis = System.currentTimeMillis();
        try
        {
            conn.next();
            throw new Exception("Read #1 did not timeout!");
        }
        catch (LogTimeoutException e)
        {
        }
        long intervalMillis = System.currentTimeMillis() - startMillis;
        assertTrue("Took at least 5 seconds to complete",
                5000 <= intervalMillis);

        // Now repeat with log connection timeout reduced to 10ms. Show that
        // read times
        // out within a second.
        // WARNING: THIS WILL NOT WORK IN A DEBUGGER IF YOU STOP DURING READ.
        conn.setTimeoutMillis(10);
        assertTrue("Seek first record", conn.seek(0));
        assertNotNull("Return first record", conn.next());
        startMillis = System.currentTimeMillis();
        try
        {
            conn.next();
            throw new Exception("Read #2 did not timeout!");
        }
        catch (LogTimeoutException e)
        {
        }
        intervalMillis = System.currentTimeMillis() - startMillis;
        assertTrue("Took less than 1 second to complete", 1000 > intervalMillis);

        // Clean up.
        conn.release();
        log.release();
    }

    /**
     * Confirm that we can seek to the sequenced number *after* the last
     * sequence number currently stored and read the next event when it appears.
     */
    public void testSeekLast() throws Exception
    {
        // Create a log and write 10 events to it. Set the timeout interval to
        // 1 second.
        File logDir = prepareLogDir("testNonexistingLogFile");
        DiskLog log = openLog(logDir, false);
        log.setTimeoutMillis(1000);
        writeEventsToLog(log, 0, 10);

        // Confirm that 9 is the last sequence number, then seek to 10.
        assertEquals("expected last sequence number", 9, log.getMaxSeqno());
        LogConnection conn = log.connect(true);
        assertTrue("Seeking last event", conn.seek(10));

        // Confirm we timeout on the next event if the call is blocking.
        try
        {
            THLEvent e10 = conn.next();
            throw new Exception("Found non-existent event: " + e10.toString());
        }
        catch (LogTimeoutException e)
        {
        }

        // Confirm that we return null on the next event if the call is
        // non-blocking.
        THLEvent e10a = conn.next(false);
        assertNull("Non-blocking call returns null", e10a);

        // Close up.
        conn.release();
        log.release();
    }

    /**
     * Confirm that we can seek a non-zero entry in an empty log and read it
     * when it appears.
     */
    public void testSeekGoodNonZeroFirst() throws Exception
    {
        // Create a log with a short timeout.
        File logDir = prepareLogDir("testSeekGoodNonZeroFirst");
        DiskLog log = openLog(logDir, false);
        log.setTimeoutMillis(1000);

        // Confirm that seek to future non-zero position returns true even
        // though no such position exists.
        LogConnection conn = log.connect(true);
        assertTrue("Seeking future event that will exist", conn.seek(10));

        // Confirm that a seek to that position waits (using a timeout).
        try
        {
            THLEvent e = conn.next();
            throw new Exception(
                    "Able to seek and read non-existent position in empty log: "
                            + e);
        }
        catch (LogTimeoutException e)
        {
            logger.info("Returned expected timeout exception");
        }

        // Write data and confirm we can read them.
        writeEventsToLog(log, 10, 1);
        THLEvent e = conn.next();
        assertNotNull("Found an event", e);
        assertEquals("Has expected seqno", 10, e.getSeqno());
        assertEquals("Log min should be same as written event", 10,
                log.getMinSeqno());
        assertEquals("Log max should be same as written event", 10,
                log.getMaxSeqno());

        // Close up.
        conn.release();
        log.release();
    }

    /**
     * Confirm that we can seek a non-zero entry that has yet to arrive in the
     * log.
     */
    public void testSeekNotArrivedEvent() throws Exception
    {
        // Create a log with a short timeout.
        File logDir = prepareLogDir("testSeekNotArrivedEvent");
        DiskLog log = openLog(logDir, false);
        log.setTimeoutMillis(1000);

        // Put 10 events into the log.
        writeEventsToLog(log, 0, 10);

        // Confirm that seek to future non-zero position returns true even
        // though no such position exists.
        LogConnection conn = log.connect(true);
        assertTrue("Seeking future event that will exist", conn.seek(19));

        // Confirm that a seek to that position waits (using a timeout).
        try
        {
            THLEvent e = conn.next();
            throw new Exception(
                    "Able to seek and read non-existent position in empty log: "
                            + e);
        }
        catch (LogTimeoutException e)
        {
            logger.info("Returned expected timeout exception");
        }

        // Write more data to the log.
        writeEventsToLog(log, 10, 10);

        // Fetch from the log and confirm we get the expected event without
        // seeing intervening events.
        THLEvent e = conn.next();
        assertNotNull("Found an event", e);
        assertEquals("Has expected seqno", 19, e.getSeqno());

        // Close up.
        conn.release();
        log.release();
    }

    /**
     * Confirm that if we seek a future non-zero event it will succeed but if
     * the first event returned is less than the future seqno the next() call
     * fails. Note: We permit event to be greater to allow for seeking in the
     * log where the last recorded event is filtered.
     */
    public void testSeekBadNonZeroFirst() throws Exception
    {
        // Create a log and write 10 events to it. Set the timeout interval to
        // 1 second.
        File logDir = prepareLogDir("testSeekGoodNonZeroFirst");
        DiskLog log = openLog(logDir, false);
        log.setTimeoutMillis(1000);

        // Confirm that seek to future non-zero position returns true even
        // though no such position exists.
        LogConnection conn = log.connect(true);
        assertTrue("Seeking future event that will not exist", conn.seek(9));

        // Write event at seqno 10 and confirm our next call fails.
        writeEventsToLog(log, 10, 1);
        try
        {
            THLEvent e = conn.next();
            throw new Exception(
                    "Able to seek and read non-existent position in empty log: "
                            + e);
        }
        catch (LogPositionException e)
        {
            logger.info("Received expected exception");
        }

        // Close up.
        conn.release();
        log.release();
    }

    /**
     * Verify that we can seek and return a filtered event by looking for any
     * seqno value from the starting to ending seqno. Confirm also that we are
     * not confused by bracketing non-filtered events.
     */
    public void testSeekFilteredEvent() throws Exception
    {
        // Create the log.
        File logDir = prepareLogDir("testSeekFilteredEvent");
        DiskLog log = openLog(logDir, false);

        // Write 5 non-filtered events.
        long seqno = 0;
        LogConnection conn = log.connect(false);
        logger.info("Writing five unfiltered events");
        for (int i = 0; i < 5; i++)
        {
            THLEvent e = this.createTHLEvent(seqno++);
            conn.store(e, false);
        }

        // Filter five events.
        long endSeqno = seqno + 4;
        logger.info("Filtering 5 events: start=" + seqno + " end=" + endSeqno);
        THLEvent fe = this.createFilteredTHLEvent(seqno, endSeqno, (short) 0);
        conn.store(fe, false);
        seqno += 5;

        // Write 5 non-filtered events.
        logger.info("Writing five unfiltered events");
        for (int i = 0; i < 5; i++)
        {
            THLEvent e = this.createTHLEvent(seqno++);
            conn.store(e, false);
        }
        conn.commit();
        conn.release();
        log.release();

        // Ensure all events are stored.
        assertEquals("Should have stored requested events", (seqno - 1),
                log.getMaxSeqno());
        logger.info("Final seqno: " + (seqno - 1));

        // Seek for and find first event before filtered events.
        DiskLog log2 = openLog(logDir, true);
        log2.validate();
        LogConnection conn2 = log2.connect(true);

        conn2.seek(4);
        THLEvent eBefore = conn2.next(false);
        ReplDBMSEvent reBefore = (ReplDBMSEvent) eBefore.getReplEvent();
        assertTrue("Expect a ReplDBMSEvent", reBefore instanceof ReplDBMSEvent);

        // Prove we can see and find filtered event using any seqno within
        // the filtered range.
        for (int i = 5; i <= 9; i++)
        {
            logger.info("Seeking filtered event: seqno=" + i);
            assertTrue("Seeking sequence number: seqno=" + i,
                    conn2.seek(i, (short) 0));
            THLEvent eFiltered = conn2.next(false);
            this.validateFilteredEvent(eFiltered, 5, 9);
        }

        // Find non-filtered event following.
        conn2.seek(10);
        THLEvent eAfter = conn2.next(false);
        ReplDBMSEvent reAfter = (ReplDBMSEvent) eAfter.getReplEvent();
        assertTrue("Expect a ReplDBMSEvent", reAfter instanceof ReplDBMSEvent);

        // All done.
        log2.release();
    }

    /**
     * Verify that if the log contains a mix of filtered and non-filtered events
     * we can seek to any position and read all remaining events. Note: the log
     * may not end with a filtered event.
     */
    public void testSeekFilteredEvent2() throws Exception
    {
        // Create the log.
        File logDir = prepareLogDir("testSeekFilteredEvent2");
        DiskLog log = openLog(logDir, false);

        // Boundaries of sequence numbers in the log.
        long firstSeqno = 25;
        long lastSeqno = -1;

        // Write a sequence of alternating filtered and non-filtered events.
        long seqno = 25;
        LogConnection conn = log.connect(false);
        for (int i = 0; i < 4; i++)
        {
            THLEvent e;
            if (i % 2 == 0)
            {
                // Generate a filtered event.
                lastSeqno = seqno + 4;
                logger.info("Creating filtered event: start=" + seqno + " end="
                        + lastSeqno);
                e = this.createFilteredTHLEvent(seqno, lastSeqno, (short) 0);
                seqno += 5;
            }
            else
            {
                // Generate a non-filtered event.
                logger.info("Creating normal event: seqno=" + seqno);
                lastSeqno = seqno;
                e = this.createTHLEvent(seqno++);
            }
            conn.store(e, false);
        }
        conn.commit();
        conn.release();
        log.release();

        // Seek for and find first event before filtered events.
        DiskLog log2 = openLog(logDir, true);
        log2.validate();
        LogConnection conn2 = log2.connect(true);

        // Loop through starting with each seqno and ensure we can read
        // continuously to the end of the log.
        for (long s = firstSeqno; s <= lastSeqno; s++)
        {
            // Seek the current seqno.
            logger.info("Seeking seqno: " + s);
            boolean found = conn2.seek(s);
            Assert.assertTrue("Found initial seqno with seek: " + s, found);

            // Set markers to allow us to march through the log and
            // also confirm we made it to the end.
            long nextExpectedSeqno = s;
            long lastFoundSeqno = -1;

            // Read the log to the end.
            THLEvent thlEvent;
            while ((thlEvent = conn2.next(false)) != null)
            {
                ReplDBMSEvent event = (ReplDBMSEvent) thlEvent.getReplEvent();
                if (event instanceof ReplDBMSFilteredEvent)
                {
                    // If we have a filtered event it should contain the
                    // expected next seqno. Record found seqno to show the gap.
                    ReplDBMSFilteredEvent filteredEvent = (ReplDBMSFilteredEvent) event;
                    logger.info("Found filtered event: start seqno="
                            + filteredEvent.getSeqno() + " end seqno="
                            + filteredEvent.getSeqnoEnd());
                    String msg = String
                            .format("Filtered event contains expected seqno: [%d <= %d <= %d] (Starting seqno: %s)",
                                    filteredEvent.getSeqno(),
                                    nextExpectedSeqno,
                                    filteredEvent.getSeqnoEnd(), s);
                    Assert.assertTrue(
                            msg,
                            filteredEvent.getSeqno() <= nextExpectedSeqno
                                    && nextExpectedSeqno <= filteredEvent
                                            .getSeqnoEnd());
                    lastFoundSeqno = filteredEvent.getSeqnoEnd();
                }
                else
                {
                    // If we have a normal event it should have the expected
                    // next seqno, which is also the found seqno.
                    logger.info("Found normal event: seqno=" + event.getSeqno());
                    Assert.assertEquals("Seqno of normal event",
                            nextExpectedSeqno, event.getSeqno());
                    lastFoundSeqno = event.getSeqno();
                }
                nextExpectedSeqno = lastFoundSeqno + 1;
            }

            // Having read the log, we expect the last found seqno to be the
            // maximum in the log.
            Assert.assertEquals("Expect to be at last seqno of log", lastSeqno,
                    lastFoundSeqno);
        }

        // All done.
        log2.release();
    }

    /**
     * Confirm that we can seek on a log where the final event in the log is a
     * filtered event and there are no further events in the log. Moreover, if
     * we all additional events later on they are found by LogConnection.next()
     * calls.
     */
    public void testSeekFinalFilteredEvent() throws Exception
    {
        // Create the log.
        File logDir = prepareLogDir("testSeekFinalFilteredEvent");
        DiskLog log = openLog(logDir, false);

        // Write a non-filtered event followed by a filtered event. This
        // is a common log prefix in real logs.
        long seqno = 30;
        LogConnection conn = log.connect(false);
        THLEvent e1 = this.createTHLEvent(seqno++);
        conn.store(e1, true);
        long lastSeqno = seqno + 4;
        THLEvent e2 = this.createFilteredTHLEvent(seqno, lastSeqno, (short) 0);
        conn.store(e2, true);

        // Seek to the filtered event and confirm we have it.
        DiskLog log2 = openLog(logDir, true);
        log2.validate();
        LogConnection conn2 = log2.connect(true);

        logger.info("Seeking seqno: " + seqno);
        boolean found = conn2.seek(seqno);
        Assert.assertTrue("Found initial seqno with seek: " + seqno, found);
        THLEvent foundEvent = conn2.next(false);
        Assert.assertEquals("Expect the filtered event", foundEvent.getSeqno(),
                seqno);

        // Add an event to the log and confirm we can read it.
        seqno = lastSeqno + 1;
        THLEvent e3 = this.createTHLEvent(seqno);
        conn.store(e3, true);
        THLEvent foundEvent3 = conn2.next();
        Assert.assertEquals("Expect the filtered event", e3.getSeqno(),
                foundEvent3.getSeqno());

        // All done.
        conn.release();
        log.release();

        conn2.release();
        log2.release();
    }

    /**
     * Confirm that if the final event in the log is filtered and we seek within
     * that event, then events are added to the log, we will return the next
     * event after the filtered event. (This test case recapitulates Google Code
     * Issue 1018.)
     */
    public void testSeekAfterFinalFilteredEvent() throws Exception
    {
        // Create the log.
        File logDir = prepareLogDir("testSeekAfterFinalFilteredEvent");
        DiskLog log = openLog(logDir, false);

        // Open the log and write a filtered event.
        long seqno = 30;
        LogConnection conn = log.connect(false);
        long lastSeqno = seqno + 4;
        THLEvent e1 = this.createFilteredTHLEvent(seqno, lastSeqno, (short) 0);
        conn.store(e1, true);

        // Seek to a position after the filtered event.
        DiskLog log2 = openLog(logDir, true);
        log2.validate();
        LogConnection conn2 = log2.connect(true);
        seqno++;
        logger.info("Seeking seqno: " + seqno);
        boolean found = conn2.seek(seqno);
        Assert.assertTrue("Found a trailing filtered event", found);

        // Confirm we can find the filtered event.
        THLEvent foundEvent1 = conn2.next();
        Assert.assertEquals("Expect the filtered event", e1.getSeqno(),
                foundEvent1.getSeqno());

        // Add an event to the log and confirm it is returned by next().
        seqno = lastSeqno + 1;
        THLEvent e3 = this.createTHLEvent(seqno);
        conn.store(e3, true);
        THLEvent foundEvent3 = conn2.next();
        Assert.assertEquals("Expect the filtered event", e3.getSeqno(),
                foundEvent3.getSeqno());

        // All done.
        conn.release();
        log.release();

        conn2.release();
        log2.release();
    }

    /**
     * Confirm that after seeking on a log file next() returns each event in log
     * file followed by a null after the rotate log event. This behavior should
     * be identical for both blocking and non-blocking connections.
     */
    public void testLogFileRead() throws Exception
    {
        // Create a log with 1000 events spread over multiple files.
        File logDir = prepareLogDir("testLogFileRead");
        DiskLog log = new DiskLog();
        log.setReadOnly(false);
        log.setLogDir(logDir.getAbsolutePath());
        log.setLogFileSize(3000);
        log.setTimeoutMillis(1000);
        log.prepare();
        writeEventsToLog(log, 0, 1000);

        // Find log files and ensure they are legion.
        String[] logFileNames = log.getLogFileNames();
        assertTrue("Need at least two log files", logFileNames.length > 2);

        // Ensure that that if we read we only read a single file with a null
        // at the end of each.
        long nextSeqno = 0;
        int nulls = 0;
        for (String name : logFileNames)
        {
            // Find next file start.
            logger.info("Opening file: " + name);
            LogConnection conn = log.connect(true);
            assertTrue("Seeking next file: " + name, conn.seek(name));

            // Read all the events in the log.
            THLEvent e = null;
            while ((e = conn.next(false)) != null)
            {
                assertEquals("Checking seqno", nextSeqno++, e.getSeqno());
            }
            logger.info("End of file: seqno=" + (nextSeqno - 1));
            nulls++;
            conn.release();
        }

        // Expect as many nulls read as there are files.
        assertEquals("Nulls match file number", logFileNames.length, nulls);
        assertEquals("Read all sequence numbers", log.getMaxSeqno(),
                nextSeqno - 1);
    }

    /**
     * Confirm that if you seek to the beginning of a newly initialized log
     * next() returns null if the connection is non-blocking and otherwise
     * blocks and then times out. (Using a timeout enables us to test blocking
     * without using extra threads.)
     */
    public void testFirstRead() throws Exception
    {
        // Create an empty log.
        File logDir = prepareLogDir("testFirstRead");
        DiskLog log = openLog(logDir, false);
        log.setTimeoutMillis(500);

        // Seek to first event and test non-blocking and blocking reads.
        LogConnection conn = log.connect(true);
        assertTrue("Found beginning", conn.seek(LogConnection.FIRST));
        assertNull("Non-blocking read returns null", conn.next(false));

        try
        {
            THLEvent e = conn.next(true);
            throw new Exception("Blocking read returned event from empty log: "
                    + e.toString());
        }
        catch (LogTimeoutException e)
        {

        }
        log.release();
    }

    /**
     * Confirm that log correctly identifies the max sequence number when
     * starting new log with sequence number above 0.
     */
    public void testLogNonZeroStart() throws Exception
    {
        // Create the log and write an event that starts with seqno > 0.
        File logDir = prepareLogDir("testLogNonZeroStart");
        DiskLog log = openLog(logDir, false);
        writeEventsToLog(log, 100, 1);
        assertEquals("Should show correct max seqno from single record", 100,
                log.getMaxSeqno());
        log.release();

        // Reopen the log and read back.
        DiskLog log2 = openLog(logDir, false);
        log.validate();
        assertEquals("Should show correct max seqno from single record", 100,
                log.getMaxSeqno());
        readBackStoredEvents(log2, 100, 1);
        log2.release();
    }

    /**
     * Confirm that we create the log directory automatically if it is possible
     * to do so and fail horribly if we cannot.
     */
    public void testLogDirectoryCreation() throws Exception
    {
        // Ensure log directory does not exist yet.
        File logDir = new File("testLogDirectoryCreation");
        if (logDir.exists())
        {
            for (File f : logDir.listFiles())
            {
                f.delete();
            }
            logDir.delete();
        }
        if (logDir.exists())
            throw new Exception(
                    "Unable to delete log directory prior to test: "
                            + logDir.getAbsolutePath());

        // Create and release log.
        DiskLog log = new DiskLog();
        log.setDoChecksum(true);
        log.setEventSerializerClass(ProtobufSerializer.class.getName());
        log.setLogDir(logDir.getAbsolutePath());
        log.setLogFileSize(1000000);
        log.setReadOnly(false);

        log.prepare();
        log.validate();
        log.release();
        assertTrue("Log directory must now exist", logDir.exists());

        // Create a file that will prevent the log directory from being created,
        // then try to instantiate the log.
        File logDir2 = new File("testLogDirectoryCreation2");
        FileWriter fw = new FileWriter(logDir2);
        fw.write("test data");
        fw.close();

        // Create log. Prepare should now fail.
        log = new DiskLog();
        log.setDoChecksum(true);
        log.setEventSerializerClass(ProtobufSerializer.class.getName());
        log.setLogDir(logDir2.getAbsolutePath());
        log.setLogFileSize(1000000);

        try
        {
            log.prepare();
            throw new Exception("Able to open log on invalid directory: "
                    + logDir2.getAbsolutePath());
        }
        catch (ReplicatorException e)
        {
        }
    }

    /**
     * Confirm that we can write to and read back from the log.
     */
    public void testLogReadback() throws Exception
    {
        // Create the log and write 50 events.
        File logDir = prepareLogDir("testLogReadback");
        DiskLog log = openLog(logDir, false);
        this.writeEventsToLog(log, 10000);
        assertEquals("Should have stored 10000 events", 9999, log.getMaxSeqno());
        log.release();

        // Reopen the log and read back.
        DiskLog log2 = openLog(logDir, true);
        log.validate();
        assertEquals("Should have stored 10000 events", 9999,
                log2.getMaxSeqno());
        this.readBackStoredEvents(log2, 0, 10000);
        log2.release();
    }

    /**
     * Confirm that we can write to and read from the log a stream of events
     * that include filtered events.
     */
    public void testLogReadbackWithFiltering() throws Exception
    {
        // Create the log and write groups of events interspersed with
        // filtered events.
        File logDir = prepareLogDir("testLogReadbackWithFiltering");
        DiskLog log = openLog(logDir, false);
        long seqno = 0;
        for (int i = 1; i <= 3; i++)
        {
            // Write 7 events.
            this.writeEventsToLog(log, seqno, 7);
            seqno += 7;

            // Filter three events. Have to use a new connection for this each
            // time as log only allows one writer.
            LogConnection conn = log.connect(false);
            THLEvent e = this.createFilteredTHLEvent(seqno, seqno + 2,
                    (short) i);
            conn.store(e, false);
            conn.commit();
            conn.release();
            seqno += 3;
        }

        // 27 is start seqno of last event.
        assertEquals("Should have seqno 27 as last entry", 27,
                log.getMaxSeqno());
        log.validate();
        log.release();

        // Reopen the log and read back events.
        DiskLog log2 = openLog(logDir, true);
        log2.validate();
        LogConnection conn2 = log2.connect(true);
        assertEquals("Should have seqno 27 as last entry on reopen", 27,
                log.getMaxSeqno());
        seqno = 0;

        assertTrue("Seeking sequence number 1", conn2.seek(seqno, (short) 0));
        for (int i = 1; i <= 24; i++)
        {
            THLEvent e = conn2.next();
            ReplDBMSEvent replEvent = (ReplDBMSEvent) e.getReplEvent();
            if (i % 8 == 0)
            {
                // Every 8th event is a filtered event. It should filter from
                // seqno to seqno + 2.
                assertTrue("Expect a ReplDBMSFilteredEvent",
                        replEvent instanceof ReplDBMSFilteredEvent);
                ReplDBMSFilteredEvent filterEvent = (ReplDBMSFilteredEvent) replEvent;
                assertEquals("Expected start seqno of filtered events", seqno,
                        filterEvent.getSeqno());
                assertEquals("Expected end seqno of filtered events",
                        seqno + 2, filterEvent.getSeqnoEnd());
                seqno += 3;
            }
            else
            {
                // All other events are normal and have the current sequence
                // number.
                assertEquals("Expected seqno of next event", seqno,
                        replEvent.getSeqno());
                seqno++;
            }
        }
        log2.release();
    }

    /**
     * Confirm that we can write to and read back from the log with lots of
     * intervening open and close operations on the file. This checks that we
     * can handle restarts of log clients while preserving log integrity.
     */
    public void testStutteringLogReadback() throws Exception
    {
        // Create the log.
        File logDir = prepareLogDir("testStutteringLogReadback");
        DiskLog log = openLog(logDir, false);
        LogConnection conn = log.connect(false);

        // Write a series of events to the log.
        for (int i = 0; i < 100; i++)
        {
            THLEvent e = this.createTHLEvent(i);
            conn.store(e, false);

            // Restart the log every 10th call.
            if (i > 0 && i % 10 == 0)
            {
                log.release();
                log = openLog(logDir, false);
                log.validate();
                conn = log.connect(false);
            }
        }
        conn.commit();
        conn.release();

        assertEquals("Should have stored 100 events", 99, log.getMaxSeqno());

        // Release and reopen the existing log.
        log.release();
        log = null;
        DiskLog log2 = openLog(logDir, true);
        assertEquals("Should have stored 100 events", 99, log2.getMaxSeqno());

        // Read back events from the log.
        LogConnection conn2 = log2.connect(true);
        assertTrue("Seeking first event", conn2.seek(0));
        for (int i = 0; i < 100; i++)
        {
            THLEvent e = conn2.next();
            assertNotNull("Returned event must not be null! i=" + i, e);
            assertEquals("Test expected seqno", i, e.getSeqno());
            assertEquals("Test expected fragno", (short) 0, e.getFragno());
            assertEquals("Test expected eventId", new Long(i).toString(),
                    e.getEventId());

            // Restart the log every 15th call just to be different from
            // previous loop.
            if (i > 0 && i % 15 == 0)
            {
                log2.release();
                log2 = openLog(logDir, true);
                conn2 = log2.connect(true);
                assertTrue("Seeking first event", conn2.seek(i + 1));
            }
        }

        // Close the log.
        log2.release();
    }

    /**
     * Confirm that trying to find a non-existent sequence number results in a
     * null.
     */
    public void testFindNonExistent() throws Exception
    {
        // Part 1: Look for events on an empty log. This is the trivial
        // case but still important.
        File logDir = prepareLogDir("testStutteringLogReadback");
        DiskLog log = openLog(logDir, false);
        log.release();
        DiskLog logR = openLog(logDir, true, 1000000, 1000, 0, 0);

        // Ensure that we don't find anything when the number is less than what
        // is in the log.
        LogConnection conn = logR.connect(true);
        assertFalse("Cannot find non-existent value", conn.seek(-2, (short) 0));

        // Ensure that higher values time out.
        long[] seqno1 = {1, 2, 100};
        for (long seqno : seqno1)
        {
            assertTrue("Seeking non-existent value in empty log",
                    conn.seek(seqno));
            assertNull("No value in empty log", conn.next(false));
        }
        conn.release();
        logR.release();

        // Part 2: Look for events on a log with events in it. This is the
        // more realistic case.
        log = openLog(logDir, false);
        writeEventsToLog(log, 50);
        log.release();
        logR = openLog(logDir, false, 1000000, 1000, 0, 0);

        // Ensure that we don't find anything when the number is less than what
        // is in the log.
        conn = logR.connect(true);
        assertFalse("Cannot find non-existent value", conn.seek(-2, (short) 0));

        // Ensure that higher values fail.
        long[] seqno2 = {51, 52, 100000};
        for (long seqno : seqno2)
        {
            assertTrue("Can seek non-existent seqno",
                    conn.seek(seqno, (short) 0));
            assertNull("Cannot find non-existing event", conn.next(false));
        }
        logR.release();
    }

    /**
     * Confirm that we can write and read across multiple logs with rotation
     * events.
     */
    public void testMultipleLogs() throws Exception
    {
        // Create the log and write multiple events.
        File logDir = prepareLogDir("testMultipleLogs");
        DiskLog log = openLog(logDir, false, 3000);
        writeEventsToLog(log, 200);

        // Assert that we stored the proper number of events in multiple files.
        logger.info("Log file count: " + log.fileCount());
        assertTrue("More than one log file", log.fileCount() > 1);
        log.validate();
        log.release();

        // Reopen and read back.
        DiskLog log2 = openLog(logDir, true);
        log.validate();
        assertEquals("Should have stored 200 events", 199, log2.getMaxSeqno());

        // Read back expected number of events.
        readBackStoredEvents(log2, 0, 200);

        // Close the log.
        log2.release();
    }

    /**
     * Confirm that we can write and seek across multiple logs with rotation
     * events when the logs contain only filtered values. This catches possible
     * corner cases where filtered events are present at the beginning and end
     * of log files.
     */
    public void testMultipleLogsFiltered() throws Exception
    {
        // Create the log and write multiple events.
        File logDir = prepareLogDir("testMultipleLogsFiltered");
        DiskLog log = openLog(logDir, false, 1000);

        // Fill log with unfiltered events.
        LogConnection conn = log.connect(false);
        long seqno = 0;
        for (int i = 0; i < 100; i++)
        {
            THLEvent fe = this.createFilteredTHLEvent(seqno, seqno + 4,
                    (short) 0);
            conn.store(fe, false);
            seqno += 5;
        }
        // Add a final event that filters only one seqno. This is necessary to
        // avoid ambiguity at the end of the log. Then commit and release.
        conn.store(createFilteredTHLEvent(seqno, seqno, (short) 0), false);
        conn.commit();
        conn.release();

        // Assert that we stored multiple files.
        logger.info("Log file count: " + log.fileCount());
        assertTrue("More than one log file", log.fileCount() > 1);
        log.release();

        // Reopen and read back all filtered events.
        DiskLog log2 = openLog(logDir, true);
        LogConnection conn2 = log2.connect(true);
        assertTrue("Find first filtered event", conn2.seek(0));

        for (int i = 0; i < 100; i++)
        {
            THLEvent e = conn2.next(false);
            long start = i * 5;
            long end = start + 4;
            validateFilteredEvent(e, start, end);
            ReplDBMSFilteredEvent fe = (ReplDBMSFilteredEvent) e.getReplEvent();
            logger.info("Found filtered event: start=" + fe.getSeqno()
                    + " end=" + fe.getSeqnoEnd());
            if (i > 0 && i % 100 == 0)
                logger.info("Reading filtered events from disk: start=" + start
                        + " end=" + end);
        }

        // Should be a single event at the end.
        THLEvent e2 = conn2.next(false);
        validateFilteredEvent(e2, seqno, seqno);

        // Prove that we can seek to and read each seqno value. This
        // hits corner cases at the start and end of log files.
        for (int i = 0; i < 100; i++)
        {
            long start = i * 5;
            long end = start + 4;
            for (long j = start; j <= end; j++)
            {
                assertTrue("Find filtered event for seqno: " + j, conn2.seek(j));
                THLEvent e = conn2.next(false);
                validateFilteredEvent(e, start, end);
                if (j > 0 && j % 250 == 0)
                    logger.info("Seeking single filtered events from disk: seqno="
                            + j + " start=" + start + " end=" + end);
            }
        }

        // Should be a single event at the end.
        assertTrue("Find last filtered event for seqno: " + seqno,
                conn2.seek(seqno));
        THLEvent e3 = conn2.next(false);
        validateFilteredEvent(e3, seqno, seqno);

        // Close the log.
        log2.release();
    }

    /**
     * Confirm that if the last log file ends with a rotate log event with no
     * log file thereafter that non-blocking reads return null and a blocking
     * read times out. This test covers two important cases: (a) a log that has
     * been truncated by removing one or more files and (b) a reader tries to
     * read past the end of the log file before a writer can add a new file.
     */
    public void testTruncateEndFile() throws Exception
    {
        // Create the log and write multiple events.
        File logDir = prepareLogDir("testTruncateEndFile");
        DiskLog log = openLog(logDir, false, 3000);
        writeEventsToLog(log, 200);
        log.validate();
        log.release();

        // Delete the last log file.
        String[] logFiles = log.getLogFileNames();
        String lastLogName = logFiles[logFiles.length - 1];
        File lastLog = new File(logDir, lastLogName);
        logger.info("Deleting last log: " + lastLog.getAbsolutePath());
        assertTrue("Delete last log: " + lastLogName, lastLog.delete());

        // Confirm that a non-blocking read returns null.
        DiskLog log2 = openLog(logDir, false, 3000);
        LogConnection conn1 = log2.connect(true);
        conn1.seek(0);
        long maxSeqnoNonBlocking = -1;
        THLEvent e;
        while ((e = conn1.next(false)) != null)
        {
            maxSeqnoNonBlocking = e.getSeqno();
        }
        logger.info("Non-blocking reads find " + maxSeqnoNonBlocking
                + " events");
        assertTrue("Found more than 0 events", maxSeqnoNonBlocking > 0);
        conn1.release();

        // Confirm that a blocking read times out.
        LogConnection conn2 = log2.connect(true);
        conn2.setTimeoutMillis(500);
        conn2.seek(0);
        long maxSeqnoBlocking = -1;
        try
        {
            while (maxSeqnoBlocking <= maxSeqnoNonBlocking)
            {
                e = conn2.next(true);
                maxSeqnoBlocking = e.getSeqno();
            }
            throw new Exception(
                    "Read failed to time out on missing log file after rotation");
        }
        catch (LogTimeoutException ex)
        {
            logger.info("Read timed out as expected: " + ex.getMessage());
        }
        logger.info("Blocking reads find " + maxSeqnoBlocking + " events");
        assertEquals("Blocking and non-blocking find same number of reads",
                maxSeqnoNonBlocking, maxSeqnoBlocking);
        conn2.release();

        // Write events to the end of the file to round it out to 300 events.
        long startSeqno = maxSeqnoNonBlocking + 1;
        writeEventsToLog(log2, startSeqno, 300);
        log2.validate();

        // Read back to confirm log now has 300 events.
        readBackStoredEvents(log2, 0, 300);

        // Close the log.
        log2.release();
    }

    /**
     * Confirm that we can delete log files from the bottom to the top.
     */
    public void testDeleteUp() throws Exception
    {
        // Create the log and write multiple events.
        File logDir = prepareLogDir("testDelete");
        DiskLog log = openLog(logDir, false, 3000);
        writeEventsToLog(log, 200);

        // Delete from the bottom up.
        LogConnection conn = log.connect(false);
        for (int i = 0; i < 200; i++)
        {
            conn.delete(null, new Long(i));
        }

        // Confirm that we have no log files left.
        log.validate();
        assertEquals("Should have no log files", 0, log.fileCount());
        log.release();

        // Reopen and check.
        DiskLog log2 = openLog(logDir, false);
        log.validate();

        // Write more events to the log and validate.
        writeEventsToLog(log2, 100);
        readBackStoredEvents(log2, 0, 100);
        log.validate();

        // Close the log.
        log2.release();
    }

    /**
     * Confirm that we can delete log files from the top back down to the
     * bottom.
     */
    public void testDeleteDown() throws Exception
    {
        // Create the log and write multiple events.
        File logDir = prepareLogDir("testDeleteDown");
        DiskLog log = openLog(logDir, false, 3000);
        writeEventsToLog(log, 200);

        // Delete from the top down.
        LogConnection conn = log.connect(false);
        for (int i = 199; i >= 0; i--)
        {
            conn.delete(new Long(i), null);
            if (i > 0)
            {
                long newMaxSeqno = i - 1;
                assertEquals("Expected maximum after truncation", newMaxSeqno,
                        log.getMaxSeqno());
                assertTrue("Can find max seqno", conn.seek(newMaxSeqno));
                THLEvent e = conn.next();
                assertNotNull("Last event must not be null", e);
            }
        }
        conn.release();

        // Confirm that we have no log files left.
        log.validate();
        assertEquals("Should have no log files", 0, log.fileCount());
        log.release();

        // Reopen and check.
        DiskLog log2 = openLog(logDir, false);
        log.validate();

        // Write more events to the log and validate.
        writeEventsToLog(log2, 100);
        readBackStoredEvents(log2, 0, 100);
        log.validate();

        // Close the log.
        log2.release();
    }

    /**
     * Confirm that logs do not rotate until the last fragment of a transaction.
     */
    public void testFragmentsAndRotation() throws Exception
    {
        // Create log with short file size.
        File logDir = prepareLogDir("testFragmentsAndRotation");
        DiskLog log = openLog(logDir, false, 3000);
        LogConnection conn = log.connect(false);

        // Write fragmented events to the log and confirm that the number
        // of files never changes during a single fragment.
        for (int i = 0; i < 5; i++)
        {
            for (int j = 0; j < 100; j++)
            {
                int fileCount = log.fileCount();

                // Write next fragment.
                THLEvent e = createTHLEvent(i, (short) j, false, "test");
                conn.store(e, false);
                assertEquals("Seqno should be invariant for fragments", i,
                        log.getMaxSeqno());

                // Log file may not rotate except on the first fragment,
                // which can force a rotation from previous event.
                if (j > 0)
                {
                    assertEquals("Must not rotate log file", fileCount,
                            log.fileCount());
                }
            }

            // Write final fragment.
            THLEvent e = createTHLEvent(i, (short) 100, true, "test");
            conn.store(e, true);
            assertEquals("Seqno should be invariant for fragments", i,
                    log.getMaxSeqno());
        }

        // Write a terminating fragment, then confirm the number of fragments
        // is greater than the number of large transactions.
        THLEvent e = createTHLEvent(5, (short) 0, true, "test");
        conn.store(e, true);
        assertTrue("Number of fragments >= max seqno", log.fileCount() >= 5);

        // Check and release the log.
        log.validate();
        log.release();

        // Reopen and check size. Ensure we can find the last fragments.
        DiskLog log2 = openLog(logDir, true);
        log2.validate();
        assertEquals("Should have stored 6 events", 5, log2.getMaxSeqno());
        LogConnection conn2 = log2.connect(true);
        assertTrue("Find end fragment", conn2.seek(4, (short) 100));
        THLEvent e2 = conn2.next();
        assertNotNull("Last frag should not be null", e2);
        assertTrue("Find end fragment", conn2.seek(5, (short) 0));
        THLEvent e3 = conn2.next();
        assertNotNull("Last frag should not be null", e3);
        log2.release();
    }

    /**
     * Confirm that the log correctly drops a partial fragment at the beginning
     * of the log and sets the seqno to -1.
     */
    public void testSimpleFragmentCleanup() throws Exception
    {
        // Create log with short file size.
        File logDir = prepareLogDir("testSimpleFragmentCleanup");
        DiskLog log = openLog(logDir, false, 3000);

        // Perform a test that writes a few properly terminated transactions
        // into the log and then writes a partial transaction. Reopen the log
        // and confirm that the log is cleaned up.
        LogConnection conn = log.connect(false);
        for (short i = 0; i < 3; i++)
        {
            // Write next fragment and confirm no rotation occurs.
            // Terminate and commit the last fragment property.
            THLEvent e = createTHLEvent(0, i, false, "test");
            conn.store(e, false);
        }
        // Do not commit--let the data flush automatically when file closes.
        // (Why? Maybe something unexpected will happen.)
        log.release();
        log = null;
        conn = null;

        // Close and reopen the log for writing. Confirm there are no
        // transactions.
        DiskLog log2 = openLog(logDir, false);
        assertEquals("Max seqno in log should be -1", -1, log2.getMaxSeqno());
        log2.validate();
        LogConnection conn2 = log2.connect(false);

        // Should be able to seek for first transaction but we don't expect
        // to find anything.
        assertTrue("Seek last full xact", conn2.seek(0, (short) 0));
        THLEvent e = conn2.next(false);
        if (e != null)
        {
            throw new Exception(
                    "Found transaction that should have been cleaned up!  seqno="
                            + e.getSeqno() + " fragno=" + e.getFragno());
        }

        log2.release();
    }

    /**
     * Confirm that the log drops partial transactions from the end of the log
     * on open. This cleans up the log after a transaction failure and prevents
     * restart failures.
     */
    public void testComplexFragmentCleanup() throws Exception
    {
        // Create log with short file size.
        File logDir = prepareLogDir("testComplexFragmentCleanup");
        DiskLog log = openLog(logDir, false, 3000);

        // Perform a test that writes a few properly terminated transactions
        // into the log and then writes a partial transaction. Reopen the log
        // and confirm that the log is cleaned up.
        long seqno = -1;

        for (int i = 0; i < 20; i++)
        {
            LogConnection conn = log.connect(false);

            // Write one new sequence number per loop iteration.
            seqno++;

            // Write a properly terminated transaction. We key off the value
            // of the index i to make them gradually increase in size.
            for (int j = 0; j <= i; j++)
            {
                // Write next fragment and confirm no rotation occurs.
                // Terminate and commit the last fragment property.
                THLEvent e = createTHLEvent(seqno, (short) j, (j == i), "test");
                conn.store(e, (j == i));
            }

            // Now write an unterminated fragment.
            for (int j = 0; j <= i; j++)
            {
                THLEvent e = createTHLEvent(seqno + 1, (short) j, false, "test");
                conn.store(e, true);
            }

            // Close and reopen the log. Confirm that we can find the last full
            // transaction but not the next partial transaction.
            log.release();
            log = openLog(logDir, false);
            log.validate();
            conn = log.connect(true);

            assertTrue("Seek last full xact", conn.seek(seqno, (short) i));
            THLEvent eLastFrag = conn.next();
            assertNotNull("Last full xact frag should not be null", eLastFrag);
            assertTrue(
                    "Max seqno in log should be same as last unterminated Xact",
                    log.getMaxSeqno() == eLastFrag.getSeqno());

            // Close and reopen the log to ready for next iteration of writes.
            log.release();
            log = openLog(logDir, false);
        }

        log.release();
    }

    /**
     * Confirm that if a tail log file is empty due to truncation of partial
     * data that we can still open the log for reading and writing and that
     * moreover the log correctly reports the number of transactions in the log.
     */
    public void testEmptyFileCleanup() throws Exception
    {
        File logDir = prepareLogDir("testEmptyFileCleanup");

        // Cycle through writing partial transactions, then testing cleanup.
        long lastSeqno = -1;
        for (int i = 0; i < 100; i++)
        {
            // Open the log using very limited file size and connect. Validate
            // every tenth transaction.
            DiskLog log = openLog(logDir, false, 1000);
            if (i % 10 == 0)
                log.validate();
            LogConnection conn = log.connect(false);

            // Confirm the log maximum seqno matches the last properly committed
            // transaction.
            assertEquals("Checking log max seqno", lastSeqno, log.getMaxSeqno());

            // Write a good event and commit.
            THLEvent e_next = createTHLEvent(i, (short) 0, true, "good");
            conn.store(e_next, true);
            lastSeqno = e_next.getSeqno();
            logger.info("Wrote good complete event: seqno=" + e_next.getSeqno());

            // Write a partial event and attempt to commit.
            THLEvent e_partial = createTHLEvent(i + 1, (short) 0, false, "bad");
            conn.store(e_partial, true);
            logger.info("Wrote incomplete event: seqno=" + e_partial.getSeqno());

            // Close the log.
            log.release();
        }
    }

    /**
     * Confirm that we can read backwards across multiple logs that include log
     * rotation events.
     */
    public void testMultipleLogsBackwardScan() throws Exception
    {
        // Create the log and write multiple events.
        File logDir = prepareLogDir("testMultipleLogsBackwards");
        DiskLog log = openLog(logDir, false, 3000);
        writeEventsToLog(log, 50);

        // Assert that we stored the proper number of events in multiple files.
        logger.info("Log file count: " + log.fileCount());
        log.release();

        // Reopen the log and read events back in reverse order.
        DiskLog log2 = openLog(logDir, true);
        LogConnection conn2 = log2.connect(true);
        for (int i = 49; i >= 0; i--)
        {
            assertTrue("Looking for seqno=" + i, conn2.seek(i));
            THLEvent e = conn2.next();
            assertNotNull("Returned event must not be null!", e);
            assertEquals("Test expected seqno", i, e.getSeqno());
            assertEquals("Test expected fragno", (short) 0, e.getFragno());
            assertEquals("Test expected eventId", new Long(i).toString(),
                    e.getEventId());
        }

        // Close the log.
        log2.release();
    }

    /**
     * Confirm that records written to the log do not appear to clients until
     * commit.
     */
    public void testCommitVisibility() throws Exception
    {
        // Create the log.
        File logDir = prepareLogDir("testCommitVisibility");
        DiskLog log = new DiskLog();
        log.setLogDir(logDir.getAbsolutePath());
        log.setReadOnly(false);
        log.setLogFileSize(1000000);
        log.setTimeoutMillis(Integer.MAX_VALUE);
        log.prepare();

        LogConnection conn = log.connect(false);

        // Create and start a reader. It will read two events and exit.
        SimpleLogReader reader = new SimpleLogReader(log, LogConnection.FIRST,
                4);
        Thread thread = new Thread(reader);
        thread.start();

        // Write but do not commit to the log. Confirm that reader does not see
        // it after 5 seconds.
        logger.info("Writing message #0, no commit");
        THLEvent e = this.createTHLEvent(0);
        conn.store(e, false);
        reader.lastSeqno.waitSeqnoGreaterEqual(0, 2000);
        assertEquals("Reader does not see", 0, reader.eventsRead);

        // Write with implicit commit. Confirm that reader sees both events
        // within a very short duration.
        logger.info("Writing message #1, implicit commit");
        e = this.createTHLEvent(1);
        long commitStart = System.currentTimeMillis();
        conn.store(e, true);
        reader.lastSeqno.waitSeqnoGreaterEqual(1, 2000);
        long commitEnd = System.currentTimeMillis();
        assertEquals("Reader does see", 2, reader.eventsRead);
        logger.info("Saw committed value #1: elapsed millis="
                + (commitEnd - commitStart));

        // Write again with no commit.
        logger.info("Writing message #2, no commit");
        e = this.createTHLEvent(2);
        conn.store(e, false);
        reader.lastSeqno.waitSeqnoGreaterEqual(2, 2000);
        assertEquals("Reader does not see uncommitted #2", 2, reader.eventsRead);

        // Finally, write with an explicit commit. Check that it is read very
        // quickly.
        logger.info("Writing message #3, explicit commit");
        e = this.createTHLEvent(3);
        conn.store(e, false);
        commitStart = System.currentTimeMillis();
        conn.commit();
        commitEnd = System.currentTimeMillis();
        reader.lastSeqno.waitSeqnoGreaterEqual(3, 5000);
        assertEquals("Reader sees all events", 4, reader.eventsRead);
        logger.info("Saw committed value #3: elapsed millis="
                + (commitEnd - commitStart));

        // Validate reader is done and no errors occurred.
        assertTrue("Reader is done", reader.waitFinish(1000));
        if (reader.error != null)
        {
            throw new Exception("Reader thread failed with exception after "
                    + reader.eventsRead + " events", reader.error);
        }

        // Release the log.
        log.release();
    }

    /**
     * Confirm that committed data are quickly visible to multiple readers.
     */
    public void testCommitMultiReader() throws Exception
    {
        int numberOfEvents = 25;

        // Create the log.
        File logDir = prepareLogDir("testCommitMultiReader");
        DiskLog log = new DiskLog();
        log.setLogDir(logDir.getAbsolutePath());
        log.setReadOnly(false);
        log.prepare();

        LogConnection conn = log.connect(false);

        // Create and start 25 readers.
        SimpleLogReader[] readers = new SimpleLogReader[25];
        for (int r = 0; r < readers.length; r++)
        {
            readers[r] = new SimpleLogReader(log, LogConnection.FIRST,
                    numberOfEvents);
            Thread thread = new Thread(readers[r]);
            thread.start();
        }

        // Write to the log multiple times and measure how long it takes for
        // every reader to get the committed event.
        long readTotalMillis = 0;

        for (int i = 0; i < numberOfEvents; i++)
        {
            // Write an event.
            THLEvent e = this.createTHLEvent(i);
            conn.store(e, true);

            // Measure time to read across all readers.
            long startMillis = System.currentTimeMillis();
            for (int r = 0; r < readers.length; r++)
            {
                boolean found = readers[r].lastSeqno.waitSeqnoGreaterEqual(i,
                        5000);
                assertTrue("Found event: reader=" + r + " seqno=" + i, found);
            }
            long endMillis = System.currentTimeMillis();
            long readMillis = endMillis - startMillis;
            readTotalMillis += readMillis;

            // Report results every 10 records.
            if (i > 0 && i % 10 == 0)
            {
                logger.info("Read subtotals: iteration=" + i + " readMillis="
                        + readMillis + " avg readMillis=" + readTotalMillis / i);
            }
        }

        // Report full average.
        logger.info("Read totals:  avg readMillis=" + readTotalMillis / 50
                + " total readMillis=" + readTotalMillis);

        for (int r = 0; r < readers.length; r++)
        {
            assertTrue("Reader is done", readers[r].waitFinish(1000));
            if (readers[r].error != null)
            {
                throw new Exception("Reader thread " + r
                        + " failed with exception after "
                        + readers[r].eventsRead + " events", readers[r].error);
            }
        }

        // Release the log.
        log.release();
    }

    /**
     * Confirm that records written to the log become visible automatically when
     * implicit commit is enabled by setting the flush interval to a value
     * greater than zero.
     */
    public void testImplicitCommit() throws Exception
    {
        // Create the log.
        File logDir = prepareLogDir("testCommitVisibility");
        DiskLog log = new DiskLog();
        log.setLogDir(logDir.getAbsolutePath());
        log.setReadOnly(false);
        log.setFlushIntervalMillis(500);
        log.prepare();

        LogConnection conn = log.connect(false);

        // Create and start a reader. It will read 5 events and exit.
        SimpleLogReader reader = new SimpleLogReader(log, LogConnection.FIRST,
                5);
        new Thread(reader).start();

        // Loop through 5 times writing events without commit. Confirm that the
        // reader sees the events in due time.
        for (int i = 0; i < 5; i++)
        {
            // Create an event and write without commit.
            THLEvent e = this.createTHLEvent(i);
            conn.store(e, false);

            // Confirm that the event becomes visible.
            boolean visible = reader.lastSeqno.waitSeqnoGreaterEqual(i, 2000);
            assertTrue("Event is visible: seqno=" + i, visible);
        }

        // Validate reader is done and no errors occurred.
        assertTrue("Reader is done", reader.waitFinish(1000));
        if (reader.error != null)
        {
            throw new Exception("Reader thread failed with exception after "
                    + reader.eventsRead + " events", reader.error);
        }

        // Release the log.
        log.release();
    }

    /**
     * Confirm that basic read filtering on header fields such as seqno and
     * shard ID returns only those records that match the filter predicate.
     */
    public void testReadFiltering() throws Exception
    {
        // Open the log.
        File logDir = prepareLogDir("testReadFiltering");
        DiskLog log = openLog(logDir, false);

        // Write 30 events to the log using different shard IDs.
        LogConnection conn = log.connect(false);
        for (int i = 0; i < 30; i++)
        {
            String eventId = new Long(i).toString();
            ReplDBMSEvent replEvent = new ReplDBMSEvent(i, (short) 0, true,
                    "local", 1, new Timestamp(System.currentTimeMillis()),
                    new DBMSEvent());
            replEvent.setShardId("shard_" + i);
            conn.store(new THLEvent(eventId, replEvent), false);
        }
        conn.commit();
        conn.release();

        // Create a filter for events with sequence numbers between 10 and 19
        // inclusive.
        LogConnection conn1 = log.connect(true);
        LogEventReadFilter filter1 = new LogEventReadFilter()
        {
            public boolean accept(LogEventReplReader reader)
                    throws ReplicatorException
            {
                long seqno = reader.getSeqno();
                return seqno >= 10 && seqno <= 19;
            }
        };
        conn1.setReadFilter(filter1);

        // Confirm that the filter reads only the selected events.
        int readCount = 0;
        int selectCount = 0;
        conn1.seek(0);
        THLEvent e = null;
        while ((e = conn1.next(false)) != null)
        {
            ReplEvent re = e.getReplEvent();
            readCount++;
            if (re != null)
            {
                assertTrue("Seqno matches: " + re.getSeqno(),
                        re.getSeqno() >= 10);
                assertTrue("Seqno matches: " + re.getSeqno(),
                        re.getSeqno() <= 19);
                selectCount++;
            }
        }
        assertEquals("Selected correct number", 10, selectCount);
        assertEquals("Read correct number", 30, readCount);

        // Release the connection and log.
        conn1.release();
        log.release();
    }

    // Create an empty log directory or if the directory exists remove
    // any files within it.
    private File prepareLogDir(String logDirName) throws Exception
    {
        File logDir = new File(logDirName);
        // Delete old log directory.
        if (logDir.exists())
        {
            for (File f : logDir.listFiles())
            {
                f.delete();
            }
            logDir.delete();
        }

        // Create a new directory.
        if (!logDir.mkdirs())
        {
            throw new Exception("Unable to create log directory: "
                    + logDir.getAbsolutePath());
        }
        return logDir;
    }

    // Open a new or existing log.
    private DiskLog openLog(File logDir, boolean readonly, int fileSize,
            int timeoutMillis, int logFileRetainMillis, int flushIntervalMillis)
            throws ReplicatorException, InterruptedException
    {
        // Create the log directory if this is a new log.
        DiskLog log = new DiskLog();
        log.setDoChecksum(true);
        log.setReadOnly(readonly);
        log.setEventSerializerClass(this.serializer.getName());
        log.setLogDir(logDir.getAbsolutePath());
        log.setLogFileSize(fileSize);
        log.setTimeoutMillis(timeoutMillis);
        log.setLogFileRetainMillis(logFileRetainMillis);
        log.setFlushIntervalMillis(flushIntervalMillis);
        log.prepare();

        return log;
    }

    // Default open to create log with 10 second read timeout.
    private DiskLog openLog(File logDir, boolean readonly, int fileSize)
            throws ReplicatorException, InterruptedException
    {
        return openLog(logDir, readonly, fileSize, 10000, 0, 0);
    }

    // Open new or existing with default size of 1M bytes.
    private DiskLog openLog(File logDir, boolean readonly)
            throws ReplicatorException, InterruptedException
    {
        return openLog(logDir, readonly, 1000000);
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
            if (i > 0 && i % 1000 == 0)
                logger.info("Writing events to disk: seqno=" + i);
        }
        conn.release();

        assertEquals("Should have stored requested events", (seqno - 1),
                log.getMaxSeqno());
        logger.info("Final seqno: " + (seqno - 1));
    }

    // Read back a prescribed number of events.
    private void readBackStoredEvents(DiskLog log, long fromSeqno, long count)
            throws ReplicatorException, InterruptedException
    {
        // Read back events from the log.
        LogConnection conn = log.connect(true);
        assertTrue("Looking for seqno=" + fromSeqno, conn.seek(fromSeqno));

        for (long i = fromSeqno; i < fromSeqno + count; i++)
        {
            THLEvent e = conn.next();
            assertNotNull("Returned event must not be null!", e);
            assertNotNull("Returned event must not be null!", e);
            assertEquals("Test expected seqno", i, e.getSeqno());
            assertEquals("Test expected fragno", (short) 0, e.getFragno());
            assertEquals("Test expected eventId", new Long(i).toString(),
                    e.getEventId());
            if (i > 0 && i % 1000 == 0)
                logger.info("Reading events from disk: seqno=" + i);
        }
    }

    // Validate that the proferred event is within expected range.
    private void validateFilteredEvent(THLEvent fe, long start, long end)
    {
        assertNotNull(
                "Not null filtered event: start=" + start + " end=" + end, fe);
        assertTrue("Expect a ReplDBMSFilteredEvent",
                fe.getReplEvent() instanceof ReplDBMSFilteredEvent);
        ReplDBMSFilteredEvent filterEvent = (ReplDBMSFilteredEvent) fe
                .getReplEvent();
        assertEquals("Expected start seqno of filtered events", start,
                filterEvent.getSeqno());
        assertEquals("Expected end seqno of filtered events", end,
                filterEvent.getSeqnoEnd());
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

    // Create a dummy THL event that contains a filtered event.
    private THLEvent createFilteredTHLEvent(long fromSeqno, long toSeqno,
            short toFragno)
    {
        String eventId = new Long(toSeqno).toString();
        ReplDBMSFilteredEvent filterEvent = new ReplDBMSFilteredEvent(eventId,
                fromSeqno, toSeqno, toFragno);
        return new THLEvent(eventId, filterEvent);
    }

    private THLEvent createTHLEvent(long seqno)
    {
        return createTHLEvent(seqno, (short) 0, true, "test");
    }

}