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

package com.continuent.tungsten.replicator.extractor.mysql;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * Reads and lists binlog files in the binlog index
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class BinlogIndex
{
    private static Logger logger = Logger.getLogger(BinlogIndex.class);

    // binlog directory and base name
    private final File    binlogDirectory;
    private final File    indexFile;

    // List of files in index order.
    List<File>            binlogFiles;

    /**
     * Creates a new <code>BinlogIndex</code> instance.
     * 
     * @param directory Binlog directory
     * @param baseName Binlog base name pattern
     * @param readIndex If true, read the index immediately
     * @throws ReplicatorException Throw if there is any kind of error
     */
    public BinlogIndex(String directory, String baseName, boolean readIndex)
            throws ReplicatorException
    {
        this.binlogDirectory = new File(directory);
        if (!binlogDirectory.canRead())
        {
            throw new MySQLExtractException(
                    "Binlog index missing or unreadable; check binlog directory and file pattern settings: "
                            + binlogDirectory.getAbsolutePath());
        }

        indexFile = new File(binlogDirectory, baseName + ".index");
        if (!indexFile.canRead())
        {
            throw new MySQLExtractException(
                    "Binlog index missing or unreadable; check binlog directory and file pattern settings: "
                            + indexFile.getAbsolutePath());
        }

        if (readIndex)
            readIndex();
    }

    /**
     * Refreshes the list of binlog files from current index contents.
     * 
     * @throws ReplicatorException Thrown if there is an error when reading the
     *             file
     */
    public void readIndex() throws ReplicatorException
    {
        if (logger.isDebugEnabled())
            logger.debug("Reading binlog index: " + indexFile.getAbsolutePath());

        // Open and read the index file.
        FileInputStream fis = null;
        try
        {
            fis = new FileInputStream(this.indexFile);
            InputStreamReader reader = new InputStreamReader(fis);
            @SuppressWarnings("resource")
            BufferedReader bufferedReader = new BufferedReader(reader);

            // Read and store each successive line.
            binlogFiles = new ArrayList<File>();
            String binlogName = null;
            while ((binlogName = bufferedReader.readLine()) != null)
            {
                File binlogFile = new File(this.binlogDirectory,
                        binlogName.trim());
                binlogFiles.add(binlogFile);
            }
        }
        catch (FileNotFoundException e)
        {
            throw new MySQLExtractException("Binlog index file not found: "
                    + indexFile.getAbsolutePath(), e);
        }
        catch (IOException e)
        {
            throw new MySQLExtractException("Error reading binlog index file: "
                    + indexFile.getAbsolutePath(), e);
        }
        finally
        {
            // Free FileInputStream to prevent resource leak.
            if (fis != null)
            {
                try
                {
                    fis.close();
                }
                catch (IOException e)
                {
                }
            }
        }
    }

    /**
     * Return the next binlog file following the named file. Return null if the
     * filename either cannot be found or is the last file in the list.
     * 
     * @param binlogName Basename of binlog file, e.g., mysql-bin.000023.
     * @return Next binlog file or null if not found
     */
    public File nextBinlog(String binlogName)
    {
        if (this.binlogFiles == null)
            throw new IllegalStateException(
                    "Attempt to find next binlog before reading index");

        // Locate the file in the index after converting to a file to
        // ensure we compare base name to base name.
        int index = -1;
        File binlog = new File(this.binlogDirectory, binlogName);
        for (index = 0; index < binlogFiles.size(); index++)
        {
            if (binlog.getName().equals(binlogFiles.get(index).getName()))
            {
                break;
            }
        }
        if (index == -1)
        {
            // This might mean we have a corrupt index file or are confused.
            logger.warn("Index lookup on non-existent binlog file: "
                    + binlogName);
            return null;
        }

        // Return the next file in the index if it exists.
        int nextIndex = index + 1;
        if ((nextIndex) < binlogFiles.size())
        {
            return (binlogFiles.get(nextIndex));
        }
        else
        {
            return null;
        }
    }

    /** Returns the current list of binlog files. */
    public List<File> getBinlogFiles()
    {
        return this.binlogFiles;
    }
}