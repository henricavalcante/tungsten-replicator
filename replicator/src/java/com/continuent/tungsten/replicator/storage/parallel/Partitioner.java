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
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Implements an algorithm to divide replicator events into partitions.  This
 * is used to support channel assignment in parallel apply.  The partitioning
 * algorithm must be idempotent and must result in the same channel assignment
 * for a specific shard.  The algorithm may change only after a clean 
 * shutdown, i.e., following an offline operation. <p/>
 * 
 * Channels were called "partitions" in the original parallel apply 
 * implementation.  The terms partition and channel are equivalent in the code. 
 * Only channel should be used for user-visible interfaces, configuration 
 * files, and documentation.  
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface Partitioner
{
    /**
     * Sets the number of available partitions.
     * 
     * @param availablePartitions Number of partitions available
     */
    public void setPartitions(int availablePartitions);

    /**
     * Assigns the current runtime context in case the partitioner needs to
     * refer to replicator state.
     * 
     * @param context Replicator runtime context
     */
    public void setContext(PluginContext context);

    /**
     * Assign an event to a particular partition. All fragments of a particular
     * sequence number must go to the same partition.
     * 
     * @param event Event to be assigned a partition
     * @param taskId Task id of input thread
     * @return Response containing partition ID and whether event requires a
     *         critical section
     */
    public PartitionerResponse partition(ReplDBMSHeader event, int taskId)
            throws ReplicatorException;
}