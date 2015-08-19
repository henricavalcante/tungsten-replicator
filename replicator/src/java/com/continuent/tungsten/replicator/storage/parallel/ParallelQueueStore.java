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

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.DBMSEmptyEvent;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplControlEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplEvent;
import com.continuent.tungsten.replicator.event.ReplOptionParams;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.storage.ParallelStore;
import com.continuent.tungsten.replicator.util.AtomicCounter;
import com.continuent.tungsten.replicator.util.WatchPredicate;

/**
 * Implements an parallel event store. This queue has no memory beyond its
 * current contents.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ParallelQueueStore implements ParallelStore
{
    private static Logger                                       logger             = Logger.getLogger(ParallelQueueStore.class);
    private String                                              name;
    private List<LinkedBlockingQueue<ReplEvent>>                queues;
    private List<PartitionMetadata>                             queueMetadata;
    private ReplDBMSHeader[]                                    lastHeaders;
    private ReplDBMSEvent                                       lastInsertedEvent;

    // Partitioner configuration variables.
    private Partitioner                                         partitioner;
    private String                                              partitionerClass   = SimplePartitioner.class
                                                                                           .getName();
    private long                                                transactionCount   = 0;
    private long                                                serializationCount = 0;
    private long                                                discardCount       = 0;

    // Queue for predicates belonging to pending wait synchronization requests.
    private LinkedBlockingQueue<WatchPredicate<ReplDBMSHeader>> watchPredicates;

    // Flag to insert stop synchronization event at next transaction boundary.
    private boolean                                             stopRequested      = false;

    // Queue parameters.
    private int                                                 maxSize            = 1;
    private int                                                 partitions         = 1;
    private boolean                                             syncEnabled        = true;
    private int                                                 syncInterval       = 100;

    // Counter to force synchronization events at intervals so all queues remain
    // up-to-date.
    private int                                                 syncCounter        = 1;

    // Control information for event serialization to support dependent shard
    // processing.
    private int                                                 criticalPartition  = -1;
    private AtomicCounter                                       activeSize         = new AtomicCounter(
                                                                                           0);

    // Implements partition metadata for an in-memory queue.
    public class QueueMetadataImpl implements PartitionMetadata
    {
        int      partition;
        Queue<?> q;

        QueueMetadataImpl(int partition, Queue<?> q)
        {
            this.partition = partition;
            this.q = q;
        }

        public int getPartitionNumber()
        {
            return partition;
        }

        public long getCurrentSize()
        {
            return q.size();
        }
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    /** Maximum size of individual queues. */
    public int getMaxSize()
    {
        return maxSize;
    }

    public void setMaxSize(int size)
    {
        this.maxSize = size;
    }

    /** Sets the number of queue partitions. */
    public void setPartitions(int partitions)
    {
        this.partitions = partitions;
    }

    /** Returns the number of partitions for events. */
    public int getPartitions()
    {
        return partitions;
    }

    public Partitioner getPartitioner()
    {
        return partitioner;
    }

    public void setPartitioner(Partitioner partitioner)
    {
        this.partitioner = partitioner;
    }

    public String getPartitionerClass()
    {
        return partitionerClass;
    }

    public void setPartitionerClass(String partitionerClass)
    {
        this.partitionerClass = partitionerClass;
    }

    /** Returns the number of events between sync intervals. */
    public int getSyncInterval()
    {
        return syncInterval;
    }

    /**
     * Sets the number of events to process before generating an automatic
     * control event if sync is enabled.
     */
    public void setSyncInterval(int syncInterval)
    {
        this.syncInterval = syncInterval;
    }

    /**
     * Returns true if automatic control events for synchronization are enabled.
     */
    public boolean isSyncEnabled()
    {
        return syncEnabled;
    }

    /**
     * Enables/disables automatic generation of control events to ensure queue
     * consumers have up-to-date positions in the log. This feature is mostly
     * used for testing, as it makes it easier to count queue contents if sync
     * control events are turned off.
     * 
     * @param syncEnabled If true sync control events are generated
     */
    public void setSyncEnabled(boolean syncEnabled)
    {
        this.syncEnabled = syncEnabled;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.storage.ParallelStore#setMaxOfflineInterval(int)
     */
    public void setMaxOfflineInterval(int maxOfflineInterval)
    {
        // NO-OP for this queue type.
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.storage.ParallelStore#getMaxOfflineInterval()
     */
    public int getMaxOfflineInterval()
    {
        // NO-OP for this queue type.
        return 0;
    }

    /**
     * Returns the current number of events across all queues of store.
     */
    public long getStoreSize()
    {
        return activeSize.getSeqno();
    }

    /** Sets the last header processed. This is required for restart. */
    public void setLastHeader(int taskId, ReplDBMSHeader header)
            throws ReplicatorException
    {
        assertTaskIdWithinRange(taskId);
        lastHeaders[taskId] = header;
    }

    /** Returns the last header processed. */
    public ReplDBMSHeader getLastHeader(int taskId) throws ReplicatorException
    {
        assertTaskIdWithinRange(taskId);
        return lastHeaders[taskId];
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.storage.Store#getMaxStoredSeqno()
     */
    public long getMaxStoredSeqno()
    {
        return -1;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.storage.Store#getMinStoredSeqno()
     */
    public long getMinStoredSeqno()
    {
        return -1;
    }

    /**
     * Puts an event in the queue, blocking if it is full. Putting events into
     * parallel queues needs to occur atomically so this method is synchronized.
     * (Getting/peeking from queues on the other hand must not be synchronized
     * or we would deadlock due to critical sectio processing.)
     */
    public synchronized void put(int taskId, ReplDBMSEvent event)
            throws InterruptedException, ReplicatorException
    {
        boolean needsSync = false;

        // Discard empty events.
        DBMSEvent dbmsEvent = event.getDBMSEvent();
        if (dbmsEvent == null | dbmsEvent instanceof DBMSEmptyEvent
                || dbmsEvent.getData().size() == 0)
        {
            discardCount++;
            return;
        }

        // Partition the event. Handle critical sections by "blocking to zero"
        // under the following circumstances:
        // 1.) Event is critical and we are not in a critical section.
        // 2.) We are in a critical section but the shard ID has changed.
        // 3.) Event is not critical and we are in a critical section.
        PartitionerResponse response = partitioner.partition(event, taskId);
        if (response.isCritical()
                && (criticalPartition != response.getPartition()))
        {
            // Covers cases 1 & 2. We have to serialize here.
            serializationCount++;
            blockToZero();
            criticalPartition = response.getPartition();
            if (logger.isDebugEnabled())
            {
                logger.debug("Enabling critical partition: partition="
                        + criticalPartition + " seqno=" + event.getSeqno());
            }
        }
        else if (!response.isCritical() && criticalPartition > 0)
        {
            // Covers case 3.
            blockToZero();
            criticalPartition = -1;
            if (logger.isDebugEnabled())
            {
                logger.debug("Ending critical partition: seqno="
                        + event.getSeqno());
            }
        }

        // Add event to the queue, increment the active store size, and remember
        // the event.
        queues.get(response.getPartition()).put(event);
        long size = activeSize.incrAndGetSeqno();
        transactionCount++;
        if (logger.isDebugEnabled())
        {
            logger.debug("Placed event in queue: seqno=" + event.getSeqno()
                    + " partition=" + response.getPartition() + " activeSize="
                    + size);
            if (transactionCount % 10000 == 0)
                logger.debug("Queue store: xacts=" + transactionCount
                        + " size=" + queues.size() + " activeSize=" + size);
        }
        this.lastInsertedEvent = event;

        // Fulfill stop request if we have one.
        if (event.getLastFrag() && stopRequested)
        {
            putControlEvent(ReplControlEvent.STOP, event);
            stopRequested = false;
            if (logger.isDebugEnabled())
            {
                logger.debug("Added stop control event after log event: seqno="
                        + event.getSeqno());
            }
        }

        // If we have pending predicate matches, try to fulfill them as well.
        if (event.getLastFrag() && watchPredicates.size() > 0)
        {
            // Scan for matches and add control events for each.
            List<WatchPredicate<ReplDBMSHeader>> removeList = new ArrayList<WatchPredicate<ReplDBMSHeader>>();
            for (WatchPredicate<ReplDBMSHeader> predicate : watchPredicates)
            {
                if (predicate.match(event))
                {
                    needsSync = true;
                    removeList.add(predicate);
                }
            }

            // Remove matching predicates.
            watchPredicates.removeAll(removeList);
        }

        // See if we need to send a sync event.
        if (syncEnabled && syncCounter >= syncInterval)
        {
            needsSync = true;
            syncCounter = 1;
        }
        else
            syncCounter++;

        // Even if we are not waiting for a heartbeat, these should always
        // generate a sync control event to ensure all tasks receive it.
        if (!needsSync
                && event.getDBMSEvent().getMetadataOptionValue(
                        ReplOptionParams.HEARTBEAT) != null)
        {
            needsSync = true;
        }

        // Now generate a sync event if we need one.
        if (needsSync)
        {
            putControlEvent(ReplControlEvent.SYNC, event);
            if (logger.isDebugEnabled())
            {
                logger.debug("Added sync control event after log event: seqno="
                        + event.getSeqno());
            }
        }
    }

    // Block until all queues are empty.
    private void blockToZero() throws InterruptedException
    {
        activeSize.waitSeqnoLessEqual(0);
    }

    // Inserts a control event in all queues.
    private void putControlEvent(int type, ReplDBMSEvent event)
            throws InterruptedException
    {
        long ctrlSeqno;
        if (event == null)
            ctrlSeqno = 0;
        else
            ctrlSeqno = event.getSeqno();
        ReplControlEvent ctrl = new ReplControlEvent(type, ctrlSeqno, event);

        for (LinkedBlockingQueue<ReplEvent> queue : queues)
        {
            queue.put(ctrl);
            activeSize.incrAndGetSeqno();
        }
    }

    /**
     * Removes and returns next event from the queue, blocking if empty.
     */
    public ReplEvent get(int taskId) throws InterruptedException,
            ReplicatorException
    {
        assertTaskIdWithinRange(taskId);
        ReplEvent event = queues.get(taskId).take();
        long size = activeSize.decrAndGetSeqno();
        if (logger.isDebugEnabled())
        {
            if (event instanceof ReplDBMSEvent)
            {
                logger.debug("Returning event from queue: seqno="
                        + ((ReplDBMSEvent) event).getSeqno() + " taskId="
                        + taskId + " activeSize=" + size);
            }
            else
            {
                logger.debug("Returning control event from queue: taskId="
                        + taskId + " event=" + event.toString()
                        + " activeSize=" + size);
            }
        }
        return event;
    }

    /**
     * Returns but does not remove next event from the queue if it exists or
     * returns null if queue is empty.
     */
    public ReplEvent peek(int taskId) throws ReplicatorException,
            InterruptedException
    {
        assertTaskIdWithinRange(taskId);
        return queues.get(taskId).peek();
    }

    /**
     * Returns the current queue size.
     */
    public int size(int taskId)
    {
        return queues.get(taskId).size();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public synchronized void configure(PluginContext context)
            throws ReplicatorException
    {
        // Instantiate partitioner class.
        if (partitioner == null)
        {
            try
            {
                partitioner = (Partitioner) Class.forName(partitionerClass)
                        .newInstance();
                partitioner.setContext(context);
                partitioner.setPartitions(partitions);
            }
            catch (Exception e)
            {
                throw new ReplicatorException(
                        "Unable to instantiated partitioner: class="
                                + partitionerClass, e);
            }
        }

        // Instantiate queue list, followed by array of last sequence numbers to
        // permit propagation of restart points from each output task.
        queues = new ArrayList<LinkedBlockingQueue<ReplEvent>>(partitions);
        queueMetadata = new ArrayList<PartitionMetadata>(partitions);
        lastHeaders = new ReplDBMSHeader[partitions];
        this.watchPredicates = new LinkedBlockingQueue<WatchPredicate<ReplDBMSHeader>>();

    }

    /**
     * Allocate an in-memory queue.
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public synchronized void prepare(PluginContext context)
            throws ReplicatorException
    {
        // Create queues.
        for (int i = 0; i < partitions; i++)
        {
            queues.add(new LinkedBlockingQueue<ReplEvent>(maxSize));
        }

        // Add queue metadata required by stateful partitioners.
        if (partitioner instanceof StatefulPartitioner)
        {
            logger.info("Generating queue metadata for stateful partitioner");
            for (int i = 0; i < partitions; i++)
            {
                queueMetadata.add(new QueueMetadataImpl(i, queues.get(i)));
            }
            ((StatefulPartitioner) partitioner)
                    .setPartitionMetadata(queueMetadata);
        }
    }

    /**
     * Release queue. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public synchronized void release(PluginContext context)
            throws ReplicatorException
    {
        queues = null;
        lastHeaders = null;
    }

    // Validate that the taskId is in the accepted range of partitions.
    private void assertTaskIdWithinRange(int taskId) throws ReplicatorException
    {
        if (taskId >= partitions)
            throw new ReplicatorException(
                    "Task ID is out of range, must be less than partition size: taskId="
                            + taskId + " partitions=" + partitions);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.storage.ParallelStore#insertStopEvent()
     */
    public synchronized void insertStopEvent() throws InterruptedException
    {
        if (lastInsertedEvent == null || lastInsertedEvent.getLastFrag())
            putControlEvent(ReplControlEvent.STOP, lastInsertedEvent);
        else
            stopRequested = true;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.storage.ParallelStore#insertWatchSyncEvent(com.continuent.tungsten.replicator.util.WatchPredicate)
     */
    public void insertWatchSyncEvent(WatchPredicate<ReplDBMSHeader> predicate)
            throws InterruptedException
    {
        this.watchPredicates.add(predicate);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.storage.Store#status()
     */
    public TungstenProperties status()
    {
        TungstenProperties props = new TungstenProperties();
        props.setLong("storeSize", getStoreSize());
        props.setLong("maxSize", maxSize);
        props.setLong("eventCount", transactionCount);
        props.setLong("discardCount", discardCount);
        props.setInt("queues", partitions);
        props.setBoolean("syncEnabled", syncEnabled);
        props.setInt("syncInterval", syncInterval);
        props.setBoolean("serialized", this.criticalPartition >= 0);
        props.setLong("serializationCount", serializationCount);
        props.setBoolean("stopRequested", stopRequested);
        props.setInt("criticalPartition", criticalPartition);
        props.setString("partitionerClass", partitionerClass);
        for (int i = 0; i < queues.size(); i++)
        {
            props.setInt("store.queueSize." + i, queues.get(i).size());
        }
        return props;
    }
}