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

package com.continuent.tungsten.common.exec.test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Test;

import com.continuent.tungsten.common.exec.ProcessExecutor;

/**
 * Implements a basic unit test of executing operating system commands.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ProcessExecutorTest
{
    /**
     * Tests a very simple command that succeeds and generates output.
     */
    @Test
    public void testEchoOutput()
    {
        ProcessExecutor pe = new ProcessExecutor();
        pe.setCommands(new String[]{"echo", "hi!"});
        pe.run();

        assertSuccessfulStdout(pe, "hi!");
    }

    /**
     * Tests a very simple command that echoes the value of an environmental
     * variable that we set.
     */
    @Test
    public void testEchoEnvironmentalVar()
    {
        // Warning--this command will not work on Windows.
        ProcessExecutor pe = new ProcessExecutor();
        pe.setCommands(new String[]{"sh", "-c", "echo $myenvvar"});
        pe.setEnv("myenvvar", "hi!");
        pe.run();

        assertSuccessfulStdout(pe, "hi!");
    }

    /**
     * Tests a very simple command that prints out from stdin. This is the same
     * test as before except that stdout is now copied from stdin.
     */
    @Test
    public void testCatStdin()
    {
        // Warning--this command might not work on Windows.
        ProcessExecutor pe = new ProcessExecutor();
        pe.setCommands(new String[]{"cat"});
        pe.setStdin(new ByteArrayInputStream("hi!".getBytes()));
        pe.run();

        assertSuccessfulStdout(pe, "hi!");
    }

    /**
     * Tests that a command that times out is properly terminated and marked as
     * unsuccessful.
     */
    @Test
    public void testTimeout()
    {
        // Warning--this command might not work on Windows.
        ProcessExecutor pe = new ProcessExecutor();
        pe.setCommands(new String[]{"sleep", "60"});
        pe.setTimeout(1000);
        pe.run();

        Assert.assertEquals("Checking stdout", "", pe.getStdout());
        Assert.assertEquals("Checking empty stderr", "", pe.getStderr());
        Assert.assertFalse("Checking exit code equal 0",
                (pe.getExitValue() == 0));
        Assert.assertNull("Ensuring no error was generated", pe.getError());
        Assert.assertTrue("Checking for timeout", pe.isTimedout());
        Assert.assertFalse("Checking success", pe.isSuccessful());
    }

    /**
     * Tests a command that fails miserably.
     */
    @Test
    public void testFailedCommand()
    {
        ProcessExecutor pe = new ProcessExecutor();
        pe.setCommands(new String[]{"x9999", "arg1", "arg2", "arg3"});
        pe.run();

        Assert.assertEquals("Checking stdout", "", pe.getStdout());
        Assert.assertEquals("Checking empty stderr", "", pe.getStderr());
        Assert.assertFalse("Checking exit code not equal 0",
                (pe.getExitValue() == 0));
        Assert.assertFalse("Checking success", pe.isSuccessful());
    }

    /**
     * Verify that we can handle binary output from stdout by cat'ing an
     * executable from the operating system and then comparing the stdout with
     * the original.
     */
    @Test
    public void testBinaryFileStdout() throws Exception
    {
        // Construct a list of files to try to cat by searching for the
        // on-disk locations of popular
        String[] executables = {"/bin/bash", "/bin/ls", "/bin/sh"};
        ArrayList<File> binaryFiles = new ArrayList<File>();
        for (String executable : executables)
        {
            File exeFile = new File(executable);
            if (exeFile.canRead())
                binaryFiles.add(exeFile);
        }
        if (binaryFiles.size() == 0)
            throw new Exception("Could not find any binary files to test");

        // For each file, run a process to cat the file into another file,
        // then diff.
        for (File binaryFile : binaryFiles)
        {
            // Run process.
            File copyBinFile = File.createTempFile("test-", ".dat");
            ProcessExecutor pe = new ProcessExecutor();
            pe.setCommands(new String[]{"cat"});
            pe.setStdin(new FileInputStream(binaryFile));
            pe.setStdOut(copyBinFile);
            pe.run();

            // Ensure process was successful.
            Assert.assertEquals("Checking exit code equal 0", 0,
                    pe.getExitValue());
            Assert.assertNull("Ensuring no error was generated", pe.getError());
            Assert.assertTrue("Checking success", pe.isSuccessful());

            // Compare files.
            if (binaryFile.length() != copyBinFile.length())
                throw new Exception(
                        "Original bin file and temp copy do not match lengths: "
                                + " orig file=" + binaryFile.getAbsolutePath()
                                + ":" + binaryFile.length() + " copy file="
                                + copyBinFile.getAbsolutePath() + ":"
                                + copyBinFile.length());

            FileInputStream origStream = new FileInputStream(binaryFile);
            FileInputStream copyStream = new FileInputStream(copyBinFile);
            long length = binaryFile.length();

            try
            {
                for (long i = 1; i <= length; i++)
                {
                    int origByte = origStream.read();
                    int copyByte = copyStream.read();
                    if (origByte != copyByte)
                    {
                        throw new Exception(
                                "Original bin file and temp copy differ at byte "
                                        + i + " orig file="
                                        + binaryFile.getAbsolutePath()
                                        + " copy file="
                                        + copyBinFile.getAbsolutePath());
                    }
                }
            }
            finally
            {
                origStream.close();
                copyStream.close();
            }
        }
    }

    /**
     * Verify that we can append stdout to an existing file.
     */
    @Test
    public void testStdOutAppend() throws Exception
    {
        // Create a file with data in it.
        File tmp = File.createTempFile("testStdOutAppend-", ".dat");
        FileWriter fw = new FileWriter(tmp);
        fw.write("prefix");
        fw.close();

        // Cat more data to stdout with appending.
        ProcessExecutor pe = new ProcessExecutor();
        pe.setCommands(new String[]{"echo", "output"});
        pe.setStdOut(tmp);
        pe.setStdOutAppend(true);
        pe.run();

        // Read the file contents and confirm that both the original
        // and appended output are present.
        FileReader fr = new FileReader(tmp);
        BufferedReader br = new BufferedReader(fr);
        try
        {
            String contents = br.readLine();
            Assert.assertTrue("Looking for prefix: ",
                    contents.startsWith("prefix"));
            Assert.assertTrue("Looking for output string: ",
                    contents.indexOf("output") > 0);
        }
        finally
        {
            br.close();
        }
    }

    // Utility routine to check stdout string from a successful execution.
    private void assertSuccessfulStdout(ProcessExecutor pe,
            String expectedStdout)
    {
        if ("".equals(expectedStdout))
            Assert.assertEquals("Checking empty stdout", "", pe.getStdout());
        else
            Assert.assertEquals("Checking stdout", expectedStdout,
                    pe.getStdoutByLine().get(0));

        Assert.assertEquals("Checking empty stderr", "", pe.getStderr());
        Assert.assertEquals("Checking exit code", 0, pe.getExitValue());
        Assert.assertNull("Checking exception is null", pe.getError());
        Assert.assertFalse("Checking for no timeout", pe.isTimedout());
        Assert.assertEquals("Checking success", true, pe.isSuccessful());
    }
}
