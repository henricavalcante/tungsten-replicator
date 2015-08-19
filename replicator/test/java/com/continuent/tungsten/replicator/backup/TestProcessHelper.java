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

package com.continuent.tungsten.replicator.backup;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;

import com.continuent.tungsten.replicator.util.ProcessHelper;

/**
 * This class tests the process helper using dummy commands.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class TestProcessHelper extends TestCase
{
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
     * Tests execution with non-prefixed command.
     */
    public void testNonPrefixedCommand() throws Exception
    {
        ProcessHelper processHelper = new ProcessHelper();
        processHelper.configure();
        processHelper.exec("Running an un-prefixed echo command",
                "echo 'hello!'");
    }

    /**
     * Tests execution with prefixed command.
     */
    public void testPrefixedCommand() throws Exception
    {
        ProcessHelper processHelper = new ProcessHelper();
        processHelper.setCmdPrefix("bash -c");
        processHelper.configure();
        processHelper.exec("Running a prefixed echo command", "echo");
    }

    /**
     * Verify we get an exception if the command fails.
     */
    public void testFailingCommand() throws Exception
    {
        // Try with a bad prefix.
        ProcessHelper processHelper = new ProcessHelper();
        processHelper.setCmdPrefix("bad");
        processHelper.configure();
        try
        {
            processHelper.exec("Running a command with a bad prefix", "echo");
            throw new Exception("Command runs with bad prefix!");
        }
        catch (BackupException e)
        {
        }

        // Try with good prefix and a bad command.
        processHelper.setCmdPrefix("bash -c");
        processHelper.configure();
        try
        {
            processHelper.exec("Running a command with a bad base command",
                    "bad");
            throw new Exception("Command runs with bad prefix!");
        }
        catch (BackupException e)
        {
        }
    }
}