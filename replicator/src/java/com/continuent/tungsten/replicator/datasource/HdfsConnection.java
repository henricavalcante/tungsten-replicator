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

package com.continuent.tungsten.replicator.datasource;

import java.io.BufferedWriter;
import java.io.IOException;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.csv.CsvSpecification;
import com.continuent.tungsten.common.csv.CsvWriter;
import com.continuent.tungsten.common.file.FileIOException;
import com.continuent.tungsten.common.file.FileIOUtils;
import com.continuent.tungsten.common.file.FilePath;
import com.continuent.tungsten.common.file.HdfsFileIO;
import com.continuent.tungsten.common.file.JavaFileIO;
import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * Implements a connection for HDFS with methods that mimic the 'hadoop fs'
 * command verbs.
 */
public class HdfsConnection implements UniversalConnection
{
    private static final Logger    logger = Logger.getLogger(HdfsConnection.class);
    private final HdfsFileIO       hdfsFileIO;
    private final CsvSpecification csvSpecification;

    /**
     * Creates a new instance.
     */
    public HdfsConnection(HdfsFileIO fileIO, CsvSpecification csvSpecification)
    {
        this.hdfsFileIO = fileIO;
        this.csvSpecification = csvSpecification;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.UniversalConnection#getCsvWriter(java.io.BufferedWriter)
     */
    public CsvWriter getCsvWriter(BufferedWriter writer)
    {
        return csvSpecification.createCsvWriter(writer);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.UniversalConnection#commit()
     */
    public void commit() throws Exception
    {
        // Do nothing.
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.UniversalConnection#rollback()
     */
    public void rollback() throws Exception
    {
        // Do nothing.
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.UniversalConnection#setAutoCommit(boolean)
     */
    public void setAutoCommit(boolean autoCommit) throws Exception
    {
        // Do nothing.
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.UniversalConnection#setLogged(boolean)
     */
    public void setLogged(boolean logged)
    {
        // Do nothing.
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.UniversalConnection#setPrivileged(boolean)
     */
    public void setPrivileged(boolean privileged)
    {
        // Do nothing.
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.UniversalConnection#close()
     */
    public void close()
    {
        // Do nothing.
    }

    /**
     * Creates a directory.
     * 
     * @param path Directory path to create.
     * @param ignoreErrors If true, ignore errors if directory exists.
     */
    public void mkdir(String path, boolean ignoreErrors)
            throws ReplicatorException
    {
        FilePath remote = new FilePath(path);
        try
        {
            if (ignoreErrors)
                hdfsFileIO.mkdirs(remote);
            else
                hdfsFileIO.mkdir(remote);
        }
        catch (FileIOException e)
        {
            throw new ReplicatorException(
                    "Unable to create directory: hdfs path=" + path
                            + " message=" + e.getMessage(), e);
        }
    }

    /**
     * Delete a file or directory.
     * 
     * @param path Directory path to remove.
     * @param recursive If true, delete recursively
     * @param ignoreErrors If true, ignore errors
     */
    public void rm(String path, boolean recursive, boolean ignoreErrors)
            throws ReplicatorException
    {
        FilePath remote = new FilePath(path);
        try
        {
            hdfsFileIO.delete(remote, recursive);
        }
        catch (FileIOException e)
        {
            if (ignoreErrors)
            {
                if (logger.isDebugEnabled())
                {
                    logger.debug("Ignoring delete error: path=" + path, e);
                }
            }
            else
            {
                throw new ReplicatorException(
                        "Unable to delete file or directory: hdfs path=" + path
                                + " recursive=" + recursive + " message="
                                + e.getMessage(), e);
            }
        }
    }

    /**
     * Move a local file to HDFS.
     * 
     * @param localPath Path of input file on local file system
     * @param hdfsPath Path of file in HDFS
     */
    public void put(String localPath, String hdfsPath)
            throws ReplicatorException
    {
        JavaFileIO localFileIO = new JavaFileIO();
        FilePath local = new FilePath(localPath);
        FilePath remote = new FilePath(hdfsPath);
        try
        {
            FileIOUtils.copyBytes(localFileIO.getInputStream(local),
                    hdfsFileIO.getOutputStream(remote), 1024, true);
        }
        catch (IOException e)
        {
            throw new ReplicatorException("Unable to copy file: local path="
                    + localPath + " hdfs path=" + hdfsPath + " message="
                    + e.getMessage(), e);
        }
    }
}