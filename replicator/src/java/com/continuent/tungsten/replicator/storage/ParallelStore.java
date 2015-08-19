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

package com.continuent.tungsten.replicator.storage;

import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.util.WatchPredicate;

/**
 * Denotes a storage component that partitions transactions into disjoint sets.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface ParallelStore extends Store
{
    /** Returns the maximum size of individual queues. */
    public void setMaxSize(int size);

    /** Sets the number of queue partitions, i.e., channels. */
    public void setPartitions(int partitions);

    /** Returns the number of partitions for events, i.e., channels. */
    public int getPartitions();

    /** Returns the class used for partitioning transactions across queues. */
    public String getPartitionerClass();

    /** Sets the class used for partitioning transactions across queues. */
    public void setPartitionerClass(String partitionerClass);

    /** Returns the number of events between sync intervals. */
    public int getSyncInterval();

    /**
     * Sets the number of events to process before generating an automatic
     * control event if sync is enabled.
     */
    public void setSyncInterval(int syncInterval);

    /** Returns the maximum number of seconds to do a clean shutdown. */
    public int getMaxOfflineInterval();

    /** Sets the maximum number of seconds for a clean shutdown. */
    public void setMaxOfflineInterval(int maxOfflineInterval);

    /**
     * Inserts stop control event after next complete transaction.
     */
    public void insertStopEvent() throws InterruptedException;

    /**
     * Inserts watch synchronization event after next complete transaction that
     * matches the provided predicate.
     */
    public void insertWatchSyncEvent(WatchPredicate<ReplDBMSHeader> predicate)
            throws InterruptedException;
}