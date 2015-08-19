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
 * Denotes information describing a partition. This information is available to
 * partitioners so that they can assign based on current state of partition
 * themselves (i.e. parallel queues). This allows support for load balancing and
 * other context sensitive decisions.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface PartitionMetadata
{
    /**
     * Returns the number of the partition to which the metadata applies.
     */
    public int getPartitionNumber();

    /**
     * Returns the number of events currently in the partition. The
     * implementation must be non-blocking and thread-safe. This number may be
     * an estimate.
     */
    public long getCurrentSize();
}