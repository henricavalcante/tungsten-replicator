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

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;

/**
 * Test capabilities of tungsten log files. This test is fully self-contained
 * but creates files on the file system.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class LogFileTest extends TestCase
{
    private static Logger logger = Logger.getLogger(LogFileTest.class);

    /**
     * Setup.
     * 
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        logger.info("Test starting");
        System.out.println("test");
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
     * Confirm that we can open and close a log file.
     */
    public void testOpenClose() throws Exception
    {
        // Assign log file name.
        File logfile = new File("testCreate.dat");
        logfile.delete();

        // Create new file.
        LogFile tfrw = new LogFile(logfile);
        tfrw.create(2);
        tfrw.close();

        // Reopen the file to ensure it really exists.
        LogFile tfro = new LogFile(logfile);
        tfro.openRead();
        long seqno = tfro.getBaseSeqno();
        assertEquals("Sequence number should be 2", 2, seqno);
        tfro.close();
    }

    /**
     * Confirm that reading from a new file returns an empty record if you don't
     * wait or a timeout exception if you do.
     */
    public void testReadEmpty1() throws Exception
    {
        LogFile tf = LogHelper.createLogFile("testReadEmpty1.dat", 3);
        tf.close();
        LogFile tfro = LogHelper.openExistingFileForRead("testReadEmpty1.dat");

        // Read with no wait returns empty record immediately.
        LogRecord logRec = tfro.readRecord(LogFile.NO_WAIT);
        assertTrue("Record from empty log is empty", logRec.isEmpty());
        assertFalse("Record is not truncated", logRec.isTruncated());

        // Read with 2 second wait generates a timeout exception.
        try
        {
            LogRecord logRec2 = tfro.readRecord(2000);
            throw new Exception("Timeout did not occur with no data: "
                    + logRec2.toString());
        }
        catch (LogTimeoutException e)
        {
        }

        tfro.close();
    }

    /**
     * Confirm that we can write an event to a file and then read it back again.
     */
    public void testReadWrite() throws Exception
    {
        // Create file.
        LogFile tf = LogHelper.createLogFile("testReadWrite.dat", 5);

        // Write byte-encoded string as a record.
        String testData1 = "Test characters";
        byte[] testBytes1 = testData1.getBytes();
        long firstPos = tf.getOffset();
        LogRecord record1 = new LogRecord(tf.getFile(), firstPos, testBytes1,
                LogRecord.CRC_TYPE_NONE, 0);
        tf.writeRecord(record1, 100);
        long lastPos = tf.getOffset();
        assertTrue("Position advanced after write", firstPos < lastPos);
        tf.flush();

        // Reopen file read only, reread, and compare.
        LogFile tf2 = LogHelper.openExistingFileForRead("testReadWrite.dat");
        long firstPos2 = tf2.getOffset();
        LogRecord record2 = tf2.readRecord(0);
        long lastPos2 = tf2.getOffset();
        assertEquals("Start of record matches between logs", firstPos,
                firstPos2);
        assertEquals("End of record matches between logs", lastPos, lastPos2);
        assertEquals("Record contents match", record1, record2);

        // Release resources
        tf.close();
        tf2.close();
    }

    /**
     * Confirm that we can detect and repair a truncated file. By truncated we
     * mean that the last record is only partially written.
     */
    public void testFileTruncationAndRepair() throws Exception
    {
        // Create file.
        LogFile tf = LogHelper.createLogFile("testFileTruncationAndRepair.dat",
                5);

        // Populate file with a few records.
        byte[] testBytes = "test bytes".getBytes();
        LogRecord lastGoodRecord = null;
        for (int i = 0; i < 10; i++)
        {
            lastGoodRecord = new LogRecord(tf.getFile(), -1, testBytes,
                    LogRecord.CRC_TYPE_NONE, 0);
            tf.writeRecord(lastGoodRecord, 10000);
        }

        // Add a record to be truncated.
        long lastRecordOffset = tf.getOffset();
        byte[] lastBytes = "last bytes".getBytes();
        LogRecord recordToTruncate = new LogRecord(tf.getFile(),
                lastRecordOffset, lastBytes, LogRecord.CRC_TYPE_NONE, 0);
        tf.writeRecord(recordToTruncate, 10000);

        // Confirm we can read the last record back and that it is different
        // from previous record.
        tf.seekOffset(lastRecordOffset);
        LogRecord lastRecord = tf.readRecord(0);
        assertEquals("Last record matches orginal input record",
                recordToTruncate, lastRecord);
        assertFalse("Last record different from previous record",
                lastRecord.equals(lastGoodRecord));

        // Truncate the last record within the byte array. (I.e., over 4 bytes
        // after the start.) Confirm it reads back as a truncated record.
        tf.setLength(lastRecordOffset + 8);
        tf.seekOffset(lastRecordOffset);
        lastRecord = tf.readRecord(0);
        assertTrue("Record truncated in the middle of byte array",
                lastRecord.isTruncated());

        // Truncate record within length field. Confirm it reads back as
        // truncated record.
        tf.setLength(lastRecordOffset + 2);
        tf.seekOffset(lastRecordOffset);
        lastRecord = tf.readRecord(0);
        assertTrue("Record truncated in the middle of length",
                lastRecord.isTruncated());

        // Truncate record cleanly at record offset. Confirm it reads back as
        // empty record.
        tf.setLength(lastRecordOffset);
        tf.seekOffset(lastRecordOffset);
        lastRecord = tf.readRecord(0);
        assertTrue("Record is empty", lastRecord.isEmpty());

        // Release resources
        tf.close();
    }

    /**
     * Confirm that we correctly write and read checksums
     */
    public void testCheckSum() throws Exception
    {
        // Populate file with 100 records containing random data.
        LogFile tf = LogHelper.createLogFile("testCheckSum.dat", 5);
        for (int i = 0; i < 100; i++)
        {
            byte[] data = new byte[100];
            for (int j = 0; j < 100; j++)
                data[j] = (byte) (Math.random() * 255);
            long crc32 = LogRecord.computeCrc32(data);
            LogRecord rec = new LogRecord(tf.getFile(), -1, data,
                    LogRecord.CRC_TYPE_32, crc32);
            tf.writeRecord(rec, 100000);
            if (logger.isDebugEnabled())
            {
                logger.debug("Offset:" + tf.getOffset() + " Record: "
                        + rec.toString());
            }
        }
        tf.close();

        // Read records and ensure checksums are the same.
        LogFile tfro = LogHelper.openExistingFileForRead("testCheckSum.dat");
        assertTrue("File length is at least 10000 bytes",
                tfro.getLength() > 10000);

        for (int i = 0; i < 100; i++)
        {
            LogRecord rec = tfro.readRecord(0);
            long storedCrc = rec.getCrc();
            long computedCrc = rec.computeCrc();
            if (logger.isDebugEnabled())
            {
                logger.debug("Computed CRC: " + computedCrc + " Record: " + rec);
            }
            assertFalse("Record must not be empty", rec.isEmpty());
            assertEquals("Expect CRC-32 type", LogRecord.CRC_TYPE_32,
                    rec.getCrcType());
            assertEquals("Stored and computed CRC must match", storedCrc,
                    computedCrc);
        }

        // Release resources
        tf.close();
    }

    /**
     * Confirm that we can write and read concurrently.
     */
    public void testConcurrentReadWrite() throws Exception
    {
        // Open up file and put in header.
        LogFile tf = LogHelper.createLogFile("testConcurrentReadWrite.dat", -1);
        tf.close();

        // Open read and write files.
        LogFile tfwr = LogHelper
                .openExistingFileForWrite("testConcurrentReadWrite.dat");
        LogFile tfro = LogHelper
                .openExistingFileForRead("testConcurrentReadWrite.dat");
        assertEquals("File lengths must match", tfwr.getLength(),
                tfro.getLength());

        // Start read thread.
        SimpleLogFileReader lr = new SimpleLogFileReader(tfro, 100000);
        Thread reader = new Thread(lr);
        reader.start();

        // Start writing log records into the file.
        long bytesWritten = 0;
        for (int i = 0; i < 100000; i++)
        {
            byte[] data = new byte[100];
            for (int j = 0; j < 100; j++)
                data[j] = (byte) (Math.random() * 255);
            long crc32 = LogRecord.computeCrc32(data);
            LogRecord rec = new LogRecord(tf.getFile(), -1, data,
                    LogRecord.CRC_TYPE_32, crc32);
            tfwr.writeRecord(rec, 100000000);
            bytesWritten += rec.getRecordLength();
            if (i % 10000 == 0)
                logger.info("Records written: " + i);
        }
        tfwr.close();

        // Wait for the reader to get done.
        try
        {
            reader.join(5000);
        }
        catch (InterruptedException e)
        {
        }

        // Ensure we read all records.
        assertEquals("Checking records read", 100000, lr.recordsRead);
        assertEquals("Checking bytes read", bytesWritten, lr.bytesRead);
        assertEquals("Checking CRC failures", 0, lr.crcFailures);
        if (lr.error != null)
            throw lr.error;
    }
}