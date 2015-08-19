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

package com.continuent.tungsten.common.file;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.junit.Test;

/**
 * Implements generic unit test for file IO operations designed to be invoked by
 * different implementations.
 */
public abstract class AbstractFileIOTest
{
    private static Logger logger = Logger.getLogger(AbstractFileIOTest.class);

    // Must be provided by subclasses or test cases will not run.
    protected FileIO      fileIO;

    /**
     * Verify that every unit test has an empty test directory that exists, is
     * readable, and is writable.
     */
    @Test
    public void testTestDir() throws Exception
    {
        if (!assertFileIO())
            return;

        FilePath testDir = prepareTestDir("testTestDir");
        Assert.assertTrue("Exists: " + testDir, fileIO.exists(testDir));
        Assert.assertTrue("Is directory: " + testDir,
                fileIO.isDirectory(testDir));
        Assert.assertFalse("Is file: " + testDir, fileIO.isFile(testDir));
        Assert.assertTrue("Is writable: " + testDir, fileIO.writable(testDir));
        Assert.assertTrue("Is readable: " + testDir, fileIO.readable(testDir));
        Assert.assertEquals("Has no children: " + testDir, 0,
                fileIO.list(testDir).length);
    }

    /**
     * Verify that we can write a file using a stream and read it back.
     */
    @Test
    public void testWriteReadStreams() throws Exception
    {
        if (!assertFileIO())
            return;

        FilePath testDir = prepareTestDir("testWriteReadStreams");

        // Ensure no file exists to begin with.
        FilePath fp = new FilePath(testDir, "wrtest");
        Assert.assertFalse("File does not exist: " + fp, fileIO.exists(fp));

        // Write data to the file and close same.
        OutputStream os = fileIO.getOutputStream(fp);
        PrintWriter pw = new PrintWriter(new OutputStreamWriter(os));
        pw.write("test data!");
        pw.flush();
        pw.close();

        // Ensure the file now exists.
        Assert.assertTrue("File exists: " + fp, fileIO.exists(fp));
        Assert.assertTrue("Is a file: " + fp, fileIO.isFile(fp));

        // Read back and compare.
        InputStream is = fileIO.getInputStream(fp);
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        String contents = br.readLine();
        br.close();

        Assert.assertEquals("File contents should match what we wrote",
                "test data!", contents);
    }

    /**
     * Verify that we can write a string value to a file and read it back.
     */
    @Test
    public void testWriteReadValues() throws Exception
    {
        if (!assertFileIO())
            return;

        FilePath testDir = prepareTestDir("testWriteReadValues");

        // Write to the file and ensure it exists thereafter.
        FilePath fp = new FilePath(testDir, "foo");
        Assert.assertFalse("File does not exist: " + fp, fileIO.exists(fp));
        fileIO.write(fp, "bar");
        Assert.assertTrue("File exists: " + fp, fileIO.exists(fp));
        Assert.assertTrue("Is a file: " + fp, fileIO.isFile(fp));
        Assert.assertFalse("Is a directory: " + fp, fileIO.isDirectory(fp));

        // Read the value back and ensure it matches.
        String contents = fileIO.read(fp);
        Assert.assertEquals("File contents should match what we wrote", "bar",
                contents);
    }

    /**
     * Verify that we can can create children of a directory, correctly list
     * them, and then delete them.
     */
    @Test
    public void testDirectoryChildren() throws Exception
    {
        if (!assertFileIO())
            return;

        FilePath testDir = prepareTestDir("testDirectoryChildren");

        // Create a directory.
        FilePath dir1 = new FilePath(testDir, "dir1");
        boolean createdDir1 = fileIO.mkdir(dir1);
        Assert.assertTrue("Created dir: " + dir1, createdDir1);
        Assert.assertEquals("Has no children: " + dir1, 0,
                fileIO.list(dir1).length);

        // Add another directory and a file.
        FilePath dir1File1 = new FilePath(dir1, "file1");
        fileIO.write(dir1File1, "file1 contents");

        FilePath dir1Dir2 = new FilePath(dir1, "dir2");
        fileIO.mkdirs(dir1Dir2);

        // Ensure we have expected counts of children.
        int dir1Children = fileIO.list(dir1).length;
        Assert.assertEquals("dir 1 has children", 2, dir1Children);

        int dir1File1Children = fileIO.list(dir1File1).length;
        Assert.assertEquals("dir 1 file1 has no children", 0, dir1File1Children);

        int dir1Dir2Children = fileIO.list(dir1Dir2).length;
        Assert.assertEquals("dir 1 dir2 has no children", 0, dir1Dir2Children);

        // Ensure that we cannot delete dir1 if the delete is not recursive.
        boolean deleted1 = fileIO.delete(dir1, false);
        Assert.assertFalse("Unable to delete: " + dir1, deleted1);
        Assert.assertTrue("Exists: " + dir1, fileIO.exists(dir1));

        // Ensure that we delete everything if the dir1 delete is recursive.
        boolean deleted2 = fileIO.delete(dir1, true);
        Assert.assertTrue("Able to delete: " + dir1, deleted2);
        Assert.assertFalse("Does not exist: " + dir1, fileIO.exists(dir1));
    }

    // Sets up a test directory.
    protected FilePath prepareTestDir(String dirName) throws Exception
    {
        FilePath testDir = new FilePath(dirName);
        fileIO.delete(testDir, true);
        if (fileIO.exists(testDir))
            throw new Exception("Unable to clear test directory: " + dirName);
        fileIO.mkdirs(testDir);
        if (!fileIO.exists(testDir))
            throw new Exception("Unable to create test directory: " + dirName);
        return testDir;
    }

    // Returns false if the fileIO instances has not be set and test case should
    // return immediately.
    protected boolean assertFileIO()
    {
        if (fileIO == null)
        {
            logger.warn("FileIO is not set; test case will not be run");
            return false;
        }
        else
            return true;
    }
}