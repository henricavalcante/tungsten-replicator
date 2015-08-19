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
import org.junit.Assert;
import org.junit.Before;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.ReplEvent;
import com.continuent.tungsten.replicator.thl.THLEvent;

/**
 * Tests upwards compatibility of older log versions. Each test case reads a log
 * file from a different version of the replicator. The test cases prove that
 * regardless of replicator version the current classes can still properly
 * deserialize and process log data.
 * <p/>
 * This test depends on use of log files generated from specific replicator
 * versions. Logs are located in directory test/data/thl-files/<version> where
 * version is the name of the particular replicator log. See the README.txt for
 * instructions generating test files for new versions of the replicator.
 * <p/>
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class DiskLogCompabilityTest extends TestCase
{
    private static Logger logger = Logger.getLogger(DiskLogCompabilityTest.class);

    private static File   masterLogDir;

    /**
     * Find the master directory for all THL data, which are arranged in
     * subdirectories beneath the log file. File path differs according to
     * whether we are running debug session from Eclipse on code or ant unit
     * tests, which copy files into build directory.
     * 
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        // Possible file paths.
        String[] paths = {"test/data/thl-files", "thl-files"};
        for (String path : paths)
        {
            masterLogDir = new File(path);
            logger.info("Seeking master log directory: "
                    + masterLogDir.getAbsolutePath());
            if (masterLogDir.isDirectory() && masterLogDir.canRead())
            {
                return;
            }
        }

        // If we get here, we cannot find the directory.
        throw new Exception("Unable to find master log directory: "
                + masterLogDir.getAbsolutePath());
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
     * Verifies that we can read version 2.2.1 logs.
     */
    public void testVersion_2_2_1() throws Exception
    {
        verifyLog("tungsten-replicator-2.2.1-403");
    }

    /**
     * Verifies that we can read version 3.1.0 logs.
     */
    public void testVersion_3_1_0() throws Exception
    {
        verifyLog("tungsten-replicator-3.1.0");
    }

    /** Open and read all events in the log. */
    public void verifyLog(String version) throws Exception
    {
        // Find the log directory.
        File logDir = new File(masterLogDir, version);
        if (logDir.isDirectory() && logDir.canRead())
        {
            logger.info("Reading from log directory: "
                    + logDir.getAbsolutePath());
        }
        else
        {
            throw new Exception("Unable to find log directory: "
                    + logDir.getAbsolutePath());
        }

        // Open the log.
        DiskLog log = new DiskLog();
        log.setReadOnly(true);
        log.setLogDir(logDir.getAbsolutePath());
        log.prepare();

        // Read and count all events in the log.
        long maxSeqno = scanLog(log);

        // Test that we read the expected minimum number of events.
        logger.info("Maximum seqno=" + maxSeqno);
        Assert.assertEquals("Checking max seqno from log", 7, maxSeqno);

        // Release the log.
        log.release();
    }

    // Scan the entire log printing seqnos and types.
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
            ReplEvent replEvent = e.getReplEvent();
            logger.info("Reading event: seqno=" + e.getSeqno() + " type="
                    + replEvent.getClass().getName());
            lastSeqno = e.getSeqno();
        }
        conn.release();

        return lastSeqno;
    }
}