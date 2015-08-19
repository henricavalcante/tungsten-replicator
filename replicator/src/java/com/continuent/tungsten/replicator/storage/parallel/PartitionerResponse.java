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

/**
 * Contains partitioning response data.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class PartitionerResponse
{
    private final int     partition;
    private final boolean critical;

    /**
     * Generates a new response.
     * 
     * @param partition Partition to which current event should be assigned
     * @param critical If true, this event requires a critical section
     */
    public PartitionerResponse(int partition, boolean critical)
    {
        this.partition = partition;
        this.critical = critical;
    }

    public int getPartition()
    {
        return partition;
    }

    public boolean isCritical()
    {
        return critical;
    }
}