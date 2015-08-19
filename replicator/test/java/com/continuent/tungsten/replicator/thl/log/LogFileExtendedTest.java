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
 * Test large-scale writing and reading of log files.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class LogFileExtendedTest extends TestCase
{
    private static Logger logger = Logger.getLogger(LogFileExtendedTest.class);

    /**
     * Setup.
     * 
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        logger.info("Test starting");
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
     * Confirm that we can write and read concurrently a very large file with
     * many readers. This test case consumes substantial quantifies of disk
     * space.
     */
    public void testConcurrentReadWriteMulti() throws Exception
    {
        long maxRecords = 50000;

        // Open up file and put in header.
        LogFile tf = LogHelper.createLogFile(
                "testConcurrentReadWriteMulti.dat", -1);
        tf.close();

        // Start read thread.
        SimpleLogFileReader[] readers = new SimpleLogFileReader[10];
        Thread threads[] = new Thread[10];
        for (int i = 0; i < readers.length; i++)
        {
            LogFile tfro = LogHelper
                    .openExistingFileForRead("testConcurrentReadWriteMulti.dat");
            readers[i] = new SimpleLogFileReader(tfro, maxRecords);
            threads[i] = new Thread(readers[i]);
            threads[i].start();
        }

        // Start writing log records into the file.
        LogFile tfwr = LogHelper
                .openExistingFileForWrite("testConcurrentReadWriteMulti.dat");
        long bytesWritten = 0;
        long recordsWritten = 0;

        for (int i = 0; i < maxRecords; i++)
        {
            byte[] data = new byte[100];
            for (int j = 0; j < 100; j++)
                data[j] = (byte) (Math.random() * 255);
            LogRecord rec = new LogRecord(tfwr.getFile(), -1, data,
                    LogRecord.CRC_TYPE_NONE, 0);
            tfwr.writeRecord(rec, 1000000000);
            bytesWritten += rec.getRecordLength();
            recordsWritten++;
            if (recordsWritten % 10000 == 0)
                logger.info("Records written: " + recordsWritten);
        }
        tfwr.close();

        // Wait for the reader to get done.
        for (Thread t : threads)
        {
            try
            {
                t.join(25000);
            }
            catch (InterruptedException e)
            {
            }
        }

        // Ensure we read all records.
        for (SimpleLogFileReader lr : readers)
        {
            assertEquals("Checking records read", recordsWritten,
                    lr.recordsRead);
            assertEquals("Checking bytes read", bytesWritten, lr.bytesRead);
            assertEquals("Checking CRC failures", 0, lr.crcFailures);
            if (lr.error != null)
                throw lr.error;

        }

        // Remove the test file, as it is fairly large.
        new File("testConcurrentReadWriteMulti.dat").delete();
    }
}