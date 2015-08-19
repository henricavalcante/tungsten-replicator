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

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplOptionParams;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Implements a simple shard partitioner that hashes on the shard name. #UNKNOWN
 * shards are assumed to be critical, hence must be serialized.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class HashPartitioner implements Partitioner
{
    private int availablePartitions;

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.storage.parallel.Partitioner#setPartitions(int)
     */
    public synchronized void setPartitions(int availablePartitions)
    {
        this.availablePartitions = availablePartitions;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.storage.parallel.Partitioner#setContext(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void setContext(PluginContext context)
    {
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.storage.parallel.Partitioner#partition(com.continuent.tungsten.replicator.event.ReplDBMSHeader,
     *      int)
     */
    public PartitionerResponse partition(ReplDBMSHeader event, int taskId)
            throws ReplicatorException
    {
        String shardId = event.getShardId();

        // Shard IDs may not be a blank string. Ensure that these
        // serialize.
        if ("".equals(shardId.trim()))
        {
            shardId = ReplOptionParams.SHARD_ID_UNKNOWN;
        }

        // Compute the partition.
        if (taskId > availablePartitions)
            throw new ReplicatorException(
                    "Task ID exceeds available partitions: taskId=" + taskId
                            + " availablePartitions=" + availablePartitions);
        int partition = new Integer(Math.abs(event.getShardId().hashCode())
                % availablePartitions);

        // Compute whether this is a critical shard.
        boolean critical = ReplOptionParams.SHARD_ID_UNKNOWN.equals(shardId);

        // Finally, return a response.
        return new PartitionerResponse(partition, critical);
    }
}