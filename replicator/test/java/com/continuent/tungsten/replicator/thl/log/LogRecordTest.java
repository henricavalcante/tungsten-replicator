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

import java.io.RandomAccessFile;
import java.sql.Timestamp;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;

import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.thl.THLEvent;
import com.continuent.tungsten.replicator.thl.serializer.ProtobufSerializer;
import com.continuent.tungsten.replicator.thl.serializer.Serializer;

/**
 * Tests ability to write and read back more or less realistic log records
 * containing replication and log rotation events.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class LogRecordTest extends TestCase
{
    private static Logger logger = Logger.getLogger(LogRecordTest.class);

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
     * Confirm that we can write and then read back a replication event.
     */
    public void testReplicationEvents() throws Exception
    {
        // Open log file.
        Serializer serializer = new ProtobufSerializer();
        LogFile tfrw = LogHelper.createLogFile("testReplicationEvents.dat", 3);

        // Add a THL event.
        Timestamp now = new Timestamp(System.currentTimeMillis());
        ReplDBMSEvent replEvent = new ReplDBMSEvent(31, (short) 2, true,
                "unittest", 1, now, new DBMSEvent());
        THLEvent inputEvent = new THLEvent("dummy", replEvent);

        LogEventReplWriter writer = new LogEventReplWriter(inputEvent,
                serializer, true, null);
        LogRecord logRec = writer.write();
        tfrw.writeRecord(logRec, 10000);
        tfrw.close();

        // Read the same THL event back.
        LogFile tfro = LogHelper
                .openExistingFileForRead("testReplicationEvents.dat");
        LogRecord logRec2 = tfro.readRecord(0);
        LogEventReplReader reader = new LogEventReplReader(logRec2, serializer,
                true);
        THLEvent outputEvent = reader.deserializeEvent();
        reader.done();

        // Check header fields.
        assertEquals("Checking recordType", LogRecord.EVENT_REPL,
                reader.getRecordType());
        assertEquals("Checking setno", 31, reader.getSeqno());
        assertEquals("Checking fragment", 2, reader.getFragno());
        assertEquals("Checking last frag", true, reader.isLastFrag());
        assertEquals("Checking epoch", 1, reader.getEpochNumber());
        assertEquals("Checking sourceId", "unittest", reader.getSourceId());
        assertEquals("Checking eventId", "dummy", reader.getEventId());
        assertEquals("Checking shardId", "#UNKNOWN", reader.getShardId());
        assertEquals("Checking source tstamp", inputEvent.getSourceTstamp(),
                new Timestamp(reader.getSourceTStamp()));

        // Check THLEvent.
        assertNotNull("Event deserialized", outputEvent);
        assertEquals("Event seqno", inputEvent.getSeqno(),
                outputEvent.getSeqno());

        tfro.close();
    }

    /**
     * Confirm that a record that has corrupted bytes triggers a checksum
     * failure resulting in a LogConsistencyException.
     */
    public void testChecksumFailure() throws Exception
    {
        // Write log file.
        Serializer serializer = new ProtobufSerializer();
        ReplDBMSEvent replEvent = new ReplDBMSEvent(31, (short) 0, true,
                "unittest", 1, new Timestamp(System.currentTimeMillis()),
                new DBMSEvent());
        LogFile tfrw = writeToLogFile("testChecksumFailure.dat", replEvent);

        // Corrupt a byte 10 bytes from the end of the file.
        RandomAccessFile raf = new RandomAccessFile(tfrw.getFile(), "rw");
        long len = raf.length();
        raf.seek(len - 20);
        raf.writeShort(0);
        raf.close();

        // Read the same THL event back.
        LogFile tfro = LogHelper
                .openExistingFileForRead("testChecksumFailure.dat");
        LogRecord logRec2 = tfro.readRecord(0);
        try
        {
            LogEventReplReader reader = new LogEventReplReader(logRec2,
                    serializer, true);
            throw new Exception(
                    "Able to instantiate reader on corrupt file: reader="
                            + reader.toString());
        }
        catch (LogConsistencyException e)
        {
            logger.info("Got expected exception: " + e.toString());
        }
        finally
        {
            tfro.close();
        }
    }

    /**
     * Confirm that a record with a corrupt checksum type value triggers a
     * LogConsistencyException and that we can overcome this by reading with
     * checksums disabled. The checksum type is byte #9 from the end.
     */
    public void testChecksumTypeFailure() throws Exception
    {
        // Write log file.
        Serializer serializer = new ProtobufSerializer();
        ReplDBMSEvent replEvent = new ReplDBMSEvent(32, (short) 0, true,
                "unittest", 1, new Timestamp(System.currentTimeMillis()),
                new DBMSEvent());
        LogFile tfrw = writeToLogFile("testChecksumTypeFailure.dat", replEvent);

        // Corrupt the 9th byte from the end of the file. This should be the CRC
        // type.
        RandomAccessFile raf = new RandomAccessFile(tfrw.getFile(), "rw");
        long len = raf.length();
        raf.seek(len - 9);
        raf.writeByte(25);
        raf.close();

        // Read the same THL event back with checksums enabled.
        LogFile tfro = LogHelper
                .openExistingFileForRead("testChecksumTypeFailure.dat");
        LogRecord logRec2 = tfro.readRecord(0);
        try
        {
            LogEventReplReader reader = new LogEventReplReader(logRec2,
                    serializer, true);
            throw new Exception(
                    "Able to instantiate reader with corrupt CRC type: reader="
                            + reader.toString());
        }
        catch (LogConsistencyException e)
        {
            logger.info("Got expected exception: " + e.toString(), e);
        }
        finally
        {
            tfro.close();
        }

        // Read again with checksums disabled.
        tfro = LogHelper.openExistingFileForRead("testChecksumTypeFailure.dat");
        logRec2 = tfro.readRecord(0);
        try
        {
            LogEventReplReader reader = new LogEventReplReader(logRec2,
                    serializer, false);
            reader.deserializeEvent();
            reader.done();

            // Check a couple of header fields to be sure we have our data back.
            assertEquals("Checking recordType", LogRecord.EVENT_REPL,
                    reader.getRecordType());
            assertEquals("Checking setno", 32, reader.getSeqno());
            logger.info("Able to ignore bad checksum type with checksums disabled...");
        }
        finally
        {
            tfro.close();
        }
    }

    /**
     * Confirm that a record with a corrupt checksum value triggers a
     * LogConsistencyException and that we can overcome this by reading with
     * checksums disabled. The checksum value is byte 1-8 from the end.
     */
    public void testChecksumValueMismatch() throws Exception
    {
        // Write log file.
        Serializer serializer = new ProtobufSerializer();
        ReplDBMSEvent replEvent = new ReplDBMSEvent(33, (short) 0, true,
                "unittest", 1, new Timestamp(System.currentTimeMillis()),
                new DBMSEvent());
        LogFile tfrw = writeToLogFile("testChecksumValueMismatch.dat", replEvent);

        // Overwrite the CRC value, which is bytes 1-8 from the end of the file.
        RandomAccessFile raf = new RandomAccessFile(tfrw.getFile(), "rw");
        long len = raf.length();
        raf.seek(len - 8);
        raf.writeLong(25);
        raf.close();

        // Read the same THL event back with checksums enabled.
        LogFile tfro = LogHelper
                .openExistingFileForRead("testChecksumValueMismatch.dat");
        LogRecord logRec2 = tfro.readRecord(0);
        try
        {
            LogEventReplReader reader = new LogEventReplReader(logRec2,
                    serializer, true);
            throw new Exception(
                    "Able to instantiate reader with corrupt CRC type: reader="
                            + reader.toString());
        }
        catch (LogConsistencyException e)
        {
            logger.info("Got expected exception: " + e.toString(), e);
        }
        finally
        {
            tfro.close();
        }

        // Read again with checksums disabled.
        tfro = LogHelper.openExistingFileForRead("testChecksumValueMismatch.dat");
        logRec2 = tfro.readRecord(0);
        try
        {
            LogEventReplReader reader = new LogEventReplReader(logRec2,
                    serializer, false);
            reader.deserializeEvent();
            reader.done();

            // Check a couple of header fields to be sure we have our data back.
            assertEquals("Checking recordType", LogRecord.EVENT_REPL,
                    reader.getRecordType());
            assertEquals("Checking setno", 33, reader.getSeqno());
            logger.info("Able to ignore bad checksum type with checksums disabled...");
        }
        finally
        {
            tfro.close();
        }
    }

    // Write event to logFile.
    private LogFile writeToLogFile(String fileName, ReplDBMSEvent replEvent)
            throws Exception
    {
        Serializer serializer = new ProtobufSerializer();
        LogFile tfrw = LogHelper.createLogFile(fileName, 3);
        THLEvent inputEvent = new THLEvent("dummy", replEvent);
        LogEventReplWriter writer = new LogEventReplWriter(inputEvent,
                serializer, true, null);
        LogRecord logRec = writer.write();
        tfrw.writeRecord(logRec, 10000);
        tfrw.close();
        return tfrw;
    }

    /**
     * Confirm that we can write and then read back a log rotation event.
     */
    public void testRotationEvents() throws Exception
    {
        // Open log file.
        LogFile tfrw = LogHelper.createLogFile("testRotationEvents.dat", 3);

        // Add a log rotation event.
        LogEventRotateWriter writer = new LogEventRotateWriter(tfrw.getFile(),
                45, true);
        LogRecord logRec = writer.write();
        tfrw.writeRecord(logRec, 10000);
        tfrw.close();

        // Read the rotation event back.
        LogFile tfro = LogHelper
                .openExistingFileForRead("testRotationEvents.dat");
        LogRecord logRec2 = tfro.readRecord(0);
        LogEventRotateReader reader = new LogEventRotateReader(logRec2, true);

        // Check header fields.
        assertEquals("Checking recordType", LogRecord.EVENT_ROTATE,
                reader.getRecordType());
        assertEquals("Checking index", 45, reader.getIndex());

        tfro.close();
    }
}
