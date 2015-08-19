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

package com.continuent.tungsten.replicator.pipeline;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;

/**
 * This class implements simple performance tests of Java operations.  We
 * use the results to benchmark operations like fetching the current time
 * from the VM. 
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class JavaOpsTest extends TestCase
{
    private static Logger logger = Logger.getLogger(JavaOpsTest.class);

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
     * Test the time required to read time from the VM.
     */
    public void testReadTime() throws Exception
    {
        long currentTimeMillis;
        long count = 0;
        for (int i = 0; i < 5; i++)
        {
            logger.info("System.currentTimeMillis() invocation count: " + count);
            for (int j = 0; j < 1000000; j++)
            {
                // Make call with usage to prevent it from being optimized out. 
                count++;
                currentTimeMillis = 0;
                currentTimeMillis = System.currentTimeMillis();
                assertTrue(currentTimeMillis > 0);
            }
        }
    }
    
    /** 
     * Test the time to inquire about the health of a thread. 
     */
    public void testThreadIsInterrupted() throws Exception
    {
        long count = 0;
        for (int i = 0; i < 5; i++)
        {
            logger.info("Thread. invocation count: " + count);
            for (int j = 0; j < 1000000; j++)
            {
                // Make call with usage to prevent it from being optimized out. 
                count++;
                boolean isInterrupted = true;
                isInterrupted = Thread.currentThread().isInterrupted();
                assertFalse(isInterrupted);
            }
        }
    }
}