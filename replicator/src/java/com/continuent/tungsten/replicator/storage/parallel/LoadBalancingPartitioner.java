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

import java.util.List;

import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Partitions event by assigning to the least loaded queue.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class LoadBalancingPartitioner implements StatefulPartitioner
{
    private List<PartitionMetadata> partitionList;

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.storage.parallel.Partitioner#setPartitions(int)
     */
    public synchronized void setPartitions(int availablePartitions)
    {
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.storage.parallel.StatefulPartitioner#setPartitionMetadata(java.util.List)
     */
    public void setPartitionMetadata(List<PartitionMetadata> partitions)
    {
        partitionList = partitions;
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
     * Returns the partition with the smallest current size or the first
     * partition that has a current size of zero.
     */
    public synchronized PartitionerResponse partition(ReplDBMSHeader event,
            int taskId)
    {
        long minSize = Long.MAX_VALUE;
        int partition = 0;
        for (PartitionMetadata meta : partitionList)
        {
            long size = meta.getCurrentSize();
            if (size == 0)
            {
                partition = meta.getPartitionNumber();
                break;
            }
            else if (size < minSize)
            {
                minSize = size;
                partition = meta.getPartitionNumber();
            }
        }

        return new PartitionerResponse(partition, false);
    }
}