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
 * Contributor(s): Linas Virbalas, Stephane Giron
 */

package com.continuent.tungsten.replicator.scripting;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.datasource.HdfsConnection;

/**
 * Provides a simple wrapper for HDFS connections that is suitable for exposure
 * in scripted environments. This is a delegate on the underlying HdfsConnection
 * class.
 */
public class HdfsWrapper
{
    private static Logger        logger = Logger.getLogger(HdfsWrapper.class);
    private final HdfsConnection connection;

    /** Creates a new instance. */
    public HdfsWrapper(HdfsConnection connection)
    {
        this.connection = connection;
    }

    /**
     * Delegate method on {@link HdfsConnection#put(String, String)}
     */
    public void put(String localFsPath, String hdfsPath)
            throws ReplicatorException
    {
        if (logger.isDebugEnabled())
            logger.debug(String.format(
                    "Copying from local file: %s to HDFS file: %s",
                    localFsPath, hdfsPath));
        connection.put(localFsPath, hdfsPath);
    }

    /**
     * Delegate method on {@link HdfsConnection#mkdir(String, boolean)}
     */
    public void mkdir(String path, boolean ignoreErrors)
            throws ReplicatorException
    {
        connection.mkdir(path, ignoreErrors);
    }

    /**
     * Delegate method on {@link HdfsConnection#rm(String, boolean, boolean)}
     */
    public void rm(String path, boolean recursive, boolean ignoreErrors)
            throws ReplicatorException
    {
        connection.rm(path, recursive, ignoreErrors);
    }

    /**
     * Releases the connection.
     */
    public void close()
    {
        connection.close();
    }
}