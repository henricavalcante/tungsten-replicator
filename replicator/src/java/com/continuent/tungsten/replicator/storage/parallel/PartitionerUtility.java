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

package com.continuent.tungsten.replicator.storage.parallel;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * Utility functions for partitioning operations.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class PartitionerUtility
{
    private static Logger logger = Logger.getLogger(PartitionerUtility.class);

    /**
     * Find and load shard properties.
     * 
     * @param shardMap File containing shard properties
     */
    public static TungstenProperties loadShardProperties(File shardMap)
            throws ReplicatorException
    {
        // Locate shard map file.
        if (shardMap == null)
        {
            shardMap = new File(System.getProperty("replicator.home.dir")
                    + File.separatorChar + "conf" + File.separatorChar
                    + "shard.list");
        }
        if (!shardMap.isFile() || !shardMap.canRead())
        {
            throw new ReplicatorException(
                    "Shard map file missing or unreadable: "
                            + shardMap.getAbsolutePath());
        }
        logger.info("Loading shard map file: " + shardMap.getAbsolutePath());

        // Load properties from the file.
        FileInputStream fis = null;
        TungstenProperties shardMapProperties = null;
        try
        {
            fis = new FileInputStream(shardMap);
            shardMapProperties = new TungstenProperties();
            shardMapProperties.load(fis);
        }
        catch (IOException e)
        {
            throw new ReplicatorException("Unable to load shard map file: "
                    + shardMap.getAbsolutePath(), e);
        }
        finally
        {
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

        return shardMapProperties;
    }
}