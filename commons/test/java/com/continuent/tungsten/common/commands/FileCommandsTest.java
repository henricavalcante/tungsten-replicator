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

package com.continuent.tungsten.common.commands;

import java.io.File;
import java.io.FileWriter;

import junit.framework.TestCase;

import com.continuent.tungsten.common.commands.FileCommands;
import com.continuent.tungsten.common.config.Interval;

/**
 * Implements a unit test for file commands.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class FileCommandsTest extends TestCase
{
    /**
     * Verify ability to delete a list of files.
     */
    public void testFileDeletion() throws Exception
    {
        // Create 10 files in two lists.
        File testDir = createTestDir("testFileDeletion");
        String prefix1 = "testFileDeletion1";
        File[] files1 = createFiles(testDir, prefix1, 5);
        String prefix2 = "testFileDeletion2";
        File[] files2 = createFiles(testDir, prefix2, 5);

        // Delete the first list synchronously and confirm they are gone.
        FileCommands.deleteFiles(files1, true);
        assertNotExists(files1);

        // Delete the second list asynchronously and ensure they are also gone
        // after a suitable period of waiting.
        FileCommands.deleteFiles(files2, false);
        Thread.sleep(1000);
        assertNotExists(files2);
    }

    /**
     * Ensure that we can properly identify files for purging using numeric and
     * date retention criteria. This case must show that we identify the files
     * to be purged correctly and also do not include extraneous files.
     */
    public void testFileRetention() throws Exception
    {
        // Create 10 files in two lists.
        File testDir = createTestDir("testFileRetention");
        String prefix1 = "testfile";
        File[] files1 = createFiles(testDir, prefix1, 5);
        String prefix2 = "testfil";
        File[] files2 = createFiles(testDir, prefix2, 5);

        // Confirm that all files are retained if we have a 3s interval.
        File[] purgeCandidates1 = FileCommands.filesOverRetention(testDir,
                "testfile", 1);
        assertEquals("4 of 5 eligible", 4, purgeCandidates1.length);
        File[] toPurge1 = FileCommands.filesOverModDate(purgeCandidates1,
                new Interval("10s"));
        assertEquals("Nobody is expired yet", 0, toPurge1.length);

        // Wait for 5 seconds.
        Thread.sleep(5000);

        // Confirm that with 3s interval & 2 file retention we would purge 3
        // files
        // from first group.
        File[] purgeCandidates2 = FileCommands.filesOverRetention(testDir,
                "testfile", 2);
        assertEquals("3 of 5 eligible", 3, purgeCandidates2.length);
        File[] toPurge2 = FileCommands.filesOverModDate(purgeCandidates2,
                new Interval("3s"));
        assertEquals("Files to purge", 3, toPurge2.length);

        // Purge the files thus identified and assert that the last two files
        // from the original
        // list were retained (tests ordering).
        FileCommands.deleteFiles(toPurge2, true);
        assertNotExists(toPurge2);
        for (int i = files1.length - 2; i < files1.length; i++)
        {
            if (!files1[i].exists())
                throw new Exception("File was unexpectedly deleted: "
                        + files1[i].getName());
        }

        // Ensure that the second group of files is untouched.
        assertExists(files2);
    }

    /**
     * Ensure that we can properly identify files for purging using numeric
     * retention and an active file marker. Only files that are inactive *and*
     * lexically less than the active file will removed.
     */
    public void testFileRetentionAndInactive() throws Exception
    {
        // Create 5 files for deletion.
        File testDir = createTestDir("testFileRetentionAndInactive");
        String prefix1 = "testfile";
        File[] files1 = createFiles(testDir, prefix1, 5);

        // Confirm that if the retention is 1 we always retain everything
        // at the same level or above the active file.
        for (int i = 0; i < files1.length; i++)
        {
            File[] purgeCandidates2 = FileCommands.filesOverRetentionAndInactive(testDir,
                    "testfile", 1, files1[i].getName());
            assertEquals("Expect to purge files below active only", i,
                    purgeCandidates2.length);
        }
    }

    // Create a clean test directory.
    private File createTestDir(String name)
    {
        File dir = new File(name);
        if (dir.exists())
        {
            for (File f : dir.listFiles())
            {
                f.delete();
            }
        }
        else
        {
            dir.mkdirs();
        }
        return dir;
    }

    // Create a sorted list of files and ensure each exists.
    private File[] createFiles(File dir, String prefix, int count)
            throws Exception
    {
        File[] files = new File[count];
        for (int i = 0; i < files.length; i++)
        {
            File f = new File(dir, prefix + "_" + i);
            FileWriter fw = new FileWriter(f);
            fw.write(f.getName());
            fw.close();
            assertTrue("Ensuring file exists: " + f.getName(), f.exists());
            files[i] = f;
        }
        return files;
    }

    // Ensure each file in a list exists.
    private void assertNotExists(File[] files)
    {
        for (File f : files)
        {
            assertFalse("Ensuring file does not exist: " + f.getName(), f
                    .exists());
        }
    }

    // Ensure each file in a list exists.
    private void assertExists(File[] files)
    {
        for (File f : files)
        {
            assertTrue("Ensuring file exists: " + f.getName(), f.exists());
        }
    }
}