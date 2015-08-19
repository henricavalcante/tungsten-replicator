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
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.Interval;

/**
 * This class contains utilities for managing file used in logs.
 */
public class FileCommands
{
    /**
     * Delete a list of one or more files in array order. If a particular file
     * is deleted in the array, we guarantee that any previous files are also
     * deleted. Files after may or may not be deleted due to failures.
     * 
     * @param files Files to be deleted
     * @param wait If true, execute synchronously
     */
    public static void deleteFiles(File[] files, boolean wait)
    {
        FileDeleteTask task = new FileDeleteTask(files);
        if (wait)
            task.run();
        else
        {
            Thread t = new Thread(task);
            t.start();
        }
    }

    /**
     * Compute and return a list of files matching a particular prefixed that
     * exceed the number of files to retain. The files sorted in ascending
     * order. The list is returned minus the last N files, where N is the
     * retention.
     * 
     * @param dir Directory in which to see files
     * @param prefix File prefix against which to match
     * @param retention Number of files to retain.
     * @return Array of files that exceed the retention. The array is empty if
     *         the directory does not exist.
     */
    public static File[] filesOverRetention(File dir, String prefix,
            int retention)
    {
        return filesOverRetentionAndInactive(dir, prefix, retention, null);
    }

    /**
     * Returns only those files that are over retention and whose names sort
     * lexically below an active file. This allows us to delete files whose
     * retention is expired and that have been processed.
     * 
     * @param dir Directory in which to see files
     * @param prefix File prefix against which to match
     * @param retention Number of files to retain.
     * @param activeFile Currently active file; this and higher files should not
     *            be deleted. Ignored if null.
     * @return Array of files that exceed the retention. The array is empty if
     *         the directory does not exist.
     */
    public static File[] filesOverRetentionAndInactive(File dir, String prefix,
            int retention, String activeFile)
    {
        // Find matching files and sort into file name order.
        FileFilter fileFilter = new PrefixFileFilter(prefix);
        File[] files = dir.listFiles(fileFilter);
        if (files == null)
        {
            return new File[0];
        }
        Arrays.sort(files);

        // Compute the retention and add that many files to
        // the list of files over retention.
        int discard = files.length - retention;
        if (discard < 0)
            discard = 0;
        ArrayList<File> overRetentionFiles = new ArrayList<File>();
        for (int i = 0; i < discard; i++)
        {
            String name = files[i].getName();
            if (activeFile == null || name.compareToIgnoreCase(activeFile) < 0)
                overRetentionFiles.add(files[i]);
        }

        // Return a list of files.
        File[] overFileArray = new File[overRetentionFiles.size()];
        return overRetentionFiles.toArray(overFileArray);
    }

    /**
     * Returns members of a file array whose last modification dates are older
     * than a specific retention period.
     * 
     * @param files List of files to evaluate
     * @param retention Retention period
     * @return an array of files modified before the given date
     */
    public static File[] filesOverModDate(File[] files, Interval retention)
    {
        ArrayList<File> overFiles = new ArrayList<File>();
        long now = System.currentTimeMillis();
        for (File file : files)
        {
            long modDate = file.lastModified();
            if (modDate > 0 && retention.overInterval(modDate, now))
                overFiles.add(file);
        }

        File[] fileArray = new File[overFiles.size()];
        return overFiles.toArray(fileArray);
    }
}

// Local class to delete files.
class FileDeleteTask implements Runnable
{
    private static Logger logger = Logger.getLogger(FileDeleteTask.class);
    private File[]        deleteFiles;

    FileDeleteTask(File[] deleteFiles)
    {
        this.deleteFiles = deleteFiles;
    }

    /** Delete all files in the list. */
    public void run()
    {
        for (File file : deleteFiles)
        {
            if (file.delete())
                logger.info("File deleted: " + file.getAbsolutePath());
            else
                logger.warn("Unable to delete file: " + file.getAbsolutePath());
        }
    }
}
