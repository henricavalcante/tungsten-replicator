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

package com.continuent.tungsten.replicator.thl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

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
import com.continuent.tungsten.replicator.storage.parallel.Partitioner;
import com.continuent.tungsten.replicator.storage.parallel.PartitionerResponse;
import com.continuent.tungsten.replicator.storage.parallel.SimplePartitioner;
import com.continuent.tungsten.replicator.storage.parallel.StatefulPartitioner;
import com.continuent.tungsten.replicator.util.AtomicCounter;
import com.continuent.tungsten.replicator.util.AtomicIntervalGuard;
import com.continuent.tungsten.replicator.util.WatchPredicate;

/**
 * Implements a parallel event store based on on-disk queues. On-disk queues
 * work by managing a set of threads that read from the THL in parallel on
 * behalf of the next stage and populate queues for each applier task. This
 * class is responsible for setting up the task threads as well as the queues
 * they feed, ensuring that queues do not get too far apart when executing, and
 * to handle control events including getting apply tasks to commit their
 * restart position both at regular intervals as well as prior to clean
 * shutdown.
 * <p/>
 * Applier tasks are known as "channels" in replicator end-user documentation.
 * <p/>
 * This class makes a very strong assumption that shard IDs are correctly
 * assigned in prior stages. If not, parallelization may fail due to conflicts
 * when transactions are assigned to incorrect channels. This in turn can lead
 * to apply failing on slaves in such a way that the slave must be
 * resynchronized with its master to continue.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class THLParallelQueue implements ParallelStore
{
    private static Logger             logger              = Logger.getLogger(THLParallelQueue.class);

    // Queue parameters.
    private String                    name;
    private int                       maxSize             = 100;
    private int                       maxControlEvents    = 1000;
    private int                       partitions          = 1;
    private int                       syncInterval        = 5000;
    private int                       maxOfflineInterval  = 10;
    private int                       maxDelayInterval    = 60;
    private String                    thlStoreName        = "thl";

    // Plugin context in case we need to make inquiries.
    private PluginContext             context;

    // THL for which we are implementing a parallel queue.
    private THL                       thl;

    // Read task control information.
    private List<THLParallelReadTask> readTasks;
    private ReplDBMSEvent             lastInsertedEvent;

    // Headers used to track the restart position from downstream tasks.
    private ReplDBMSHeader[]          lastHeaders;
    private int                       partitionsReporting = 0;

    // Counter of head sequence number.
    private AtomicCounter             headSeqnoCounter    = new AtomicCounter(
                                                                  -1);

    // Partitioner configuration variables.
    private Partitioner               partitioner;
    private String                    partitionerClass    = SimplePartitioner.class
                                                                  .getName();
    private long                      transactionCount    = 0;
    private long                      serializationCount  = 0;
    private long                      discardCount        = 0;

    // Flag to insert stop synchronization event at next transaction boundary.
    private boolean                   stopRequested       = false;

    // Control information for event serialization to support shard processing.
    private int                       criticalPartition   = -1;
    private AtomicCounter             activeSize          = new AtomicCounter(0);

    // Control data to enforce maximum offline interval. These variables limit
    // the time interval between most and least advanced read threads.
    private AtomicIntervalGuard<?>    intervalGuard;
    private long                      maxOfflineMillis;
    private long                      maxDelayMillis;

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

    /**
     * Sets the instance used to assign events to partitions (channels).
     */
    public void setPartitioner(Partitioner partitioner)
    {
        this.partitioner = partitioner;
    }

    public String getPartitionerClass()
    {
        return partitionerClass;
    }

    /**
     * Sets the name of the class used to assign events to partitions
     * (channels).
     */
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

    /** Returns the maximum number of seconds to do a clean shutdown. */
    public int getMaxOfflineInterval()
    {
        return maxOfflineInterval;
    }

    /**
     * Sets the maximum number of seconds for a clean shutdown. This is
     * maintained by keeping the THL read tasks from getting too far apart from
     * each other.
     */
    public void setMaxOfflineInterval(int maxOfflineInterval)
    {
        this.maxOfflineInterval = maxOfflineInterval;
    }

    public int getMaxDelayInterval()
    {
        return maxDelayInterval;
    }

    /**
     * Sets the maximum number of seconds to delay before allowing transactions
     * to continue even when they would cause maxOfflineInterval to be exceeded.
     */
    public void setMaxDelayInterval(int maxDelayInterval)
    {
        this.maxDelayInterval = maxDelayInterval;
    }

    /** Returns the current head seqno to which read tasks may advance. */
    public long getHeadSeqno()
    {
        return this.headSeqnoCounter.getSeqno();
    }

    /** Returns the interval guard structure that tracks positions of channels. */
    public AtomicIntervalGuard<?> getIntervalGuard()
    {
        return intervalGuard;
    }

    /** Sets the last header processed. This is required for restart. */
    public void setLastHeader(int taskId, ReplDBMSHeader header)
            throws ReplicatorException
    {
        // Check the taskId range and record the header.
        assertTaskIdWithinRange(taskId);
        lastHeaders[taskId] = header;
        partitionsReporting++;

        // If all downstream tasks have reported in, we can now set the restart
        // point. All tasks must start from the same position or we will get
        // confused about the active size of the queue when restarting after a
        // crash.
        if (partitionsReporting == partitions)
        {
            ReplDBMSHeader restartHeader = getMinLastHeader();
            if (restartHeader != null)
            {
                for (int i = 0; i < partitions; i++)
                {
                    readTasks.get(i).setRestartHeader(restartHeader);
                }
            }
        }
    }

    /** Returns the last header processed. */
    private ReplDBMSHeader getLastHeader(int taskId) throws ReplicatorException
    {
        assertTaskIdWithinRange(taskId);
        return lastHeaders[taskId];
    }

    /**
     * Returns the minimum last header processed in order to handle restart
     * correctly.
     */
    public ReplDBMSHeader getMinLastHeader() throws ReplicatorException
    {
        long minSeqno = Long.MAX_VALUE;
        ReplDBMSHeader minHeader = null;
        for (int i = 0; i < getPartitions(); i++)
        {
            ReplDBMSHeader nextHeader = getLastHeader(i);
            // Accept only a value header with a real sequence number as
            // the lowest starting sequence number.
            if (nextHeader == null)
                continue;
            else if (nextHeader.getSeqno() < 0)
                continue;
            else if (nextHeader.getSeqno() < minSeqno)
            {
                minHeader = nextHeader;
                minSeqno = minHeader.getSeqno();
            }
        }
        return minHeader;
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
     * Puts an event in the queue, blocking if it is full.
     */
    public synchronized void put(int taskId, ReplDBMSEvent event)
            throws InterruptedException, ReplicatorException
    {
        boolean needsSync = false;
        if (logger.isDebugEnabled())
        {
            logger.debug("Received event: seqno=" + event.getSeqno()
                    + " fragno=" + event.getFragno() + " lastFrag="
                    + event.getLastFrag() + " shardId=" + event.getShardId());
        }

        // Update transaction count at end.
        if (event.getLastFrag())
            transactionCount++;

        // Discard empty events.
        DBMSEvent dbmsEvent = event.getDBMSEvent();
        if (dbmsEvent == null
                | dbmsEvent instanceof DBMSEmptyEvent
                || (event.getFragno() == 0 && event.getLastFrag() && dbmsEvent
                        .getData().size() == 0))
        {
            discardCount++;
            return;
        }

        // Partition the event. Handle critical sections by "blocking to zero"
        // under the following circumstances:
        //
        // 1.) Event is critical and we are not in a critical section.
        // 2.) We are in a critical section but the shard ID has changed.
        // 3.) Event is not critical and we are in a critical section.
        //
        // At section boundaries all threads must commit fully to avoid
        // deadlocks.
        PartitionerResponse response = partitioner.partition(event, taskId);
        if (logger.isDebugEnabled())
        {
            logger.debug("Assigning event to partition: seqno="
                    + event.getSeqno() + " partition="
                    + response.getPartition() + " critical="
                    + response.isCritical());
        }
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
        else if (!response.isCritical() && criticalPartition >= 0)
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

        // Check the thread read interval. This check ropes in threads that
        // have exceeded the maximum online interval between highest and lowest
        // threads. The check must occur on the event that begins a transaction
        // or a deadlock can occur on fragmented transactions that block
        // waiting for additional fragments to be permitted through.
        if (event.getFragno() == 0)
        {
            // If we have a previously recorded event timestamp, it is
            // now time to see how our threads are doing and ensure nobody
            // is too far behind.
            long lastTimestampMillis = event.getExtractedTstamp().getTime();
            if (logger.isDebugEnabled())
            {
                logger.debug("Ensuring threads meet min offline wait: event timestamp="
                        + lastTimestampMillis
                        + " low seqno="
                        + intervalGuard.getLowSeqno()
                        + " low time="
                        + intervalGuard.getLowTime()
                        + " high time="
                        + intervalGuard.getHiTime());
            }

            // Initiate a loop to wait until our current timestamp is within
            // maxOfflineMillis of the oldest seqno currently being
            // processed. To avoid stalls we will release the event anyway
            // after waiting for maxDelayInterval seconds.
            long waitStartMillis = System.currentTimeMillis();
            while (true)
            {
                // Get the timestamp of the lowest sequence number currently
                // being processed.
                long lowSeqnoMillis = intervalGuard.getLowTime();

                // If the time is -1, the array is empty, which means
                // all channels are idle. We can proceed.
                if (lowSeqnoMillis == -1)
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Proceeding as interval array is empty");
                    break;
                }

                // Compute the difference between the lagging timestamp and
                // our current timestamp.
                long interval = lastTimestampMillis - lowSeqnoMillis;
                if (interval <= maxOfflineMillis)
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Proceeding after interval reaches 0");
                    break;
                }

                // Finally, we go to sleep for 100 milliseconds
                Thread.sleep(100);

                // After sleeping for a while, we now check the current time
                // to see how long we have been delaying. Note that we have
                // a condition to break if time moves backwards. This
                // protects our algorithm in case the system clock is reset
                // for any reason.
                long currentTimeMillis = System.currentTimeMillis();
                long delayMillis = currentTimeMillis - waitStartMillis;
                if (delayMillis > maxDelayMillis
                        || currentTimeMillis < waitStartMillis)
                {
                    logger.info("Releasing event to parallel queue after delay interval expired; if this message appears commonly you should consider increasing maxOfflineInterval: seqno="
                            + event.getSeqno()
                            + " timestamp="
                            + event.getExtractedTstamp()
                            + " maxDelayInterval="
                            + maxDelayInterval
                            + " maxOfflineInterval="
                            + maxOfflineInterval);
                    logger.info("Diagnostic information on interval guard: ["
                            + intervalGuard.toString() + "]");
                    break;
                }
            }
        }

        // Advance the head seqno counter. This allows all eligible threads
        // to move forward. Update the active size to show how many events are
        // logically in the queue.
        activeSize.incrAndGetSeqno();
        headSeqnoCounter.setSeqno(event.getSeqno());
        if (logger.isDebugEnabled())
        {
            logger.debug("Updating position: headSeqnoCounter="
                    + headSeqnoCounter.getSeqno() + " activeSize="
                    + activeSize.getSeqno());
        }

        // Record last event handled.
        lastInsertedEvent = event;

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

        // Even if we are not waiting for a heartbeat, these should always
        // generate a sync control event to ensure all tasks receive it.
        if (event.getDBMSEvent().getMetadataOptionValue(
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
            needsSync = false;
        }
    }

    // Block until all queues have committed their current transactions.
    private void blockToZero() throws InterruptedException, ReplicatorException
    {
        // Add a sync event so we can block on it committing.
        if (lastInsertedEvent != null)
        {
            long requiredSeqno = lastInsertedEvent.getSeqno();
            if (logger.isDebugEnabled())
            {
                logger.debug("Blocking to zero: seqno=" + requiredSeqno);
            }

            putControlEvent(ReplControlEvent.SYNC, lastInsertedEvent);
            if (logger.isDebugEnabled())
            {
                logger.debug("Scheduling wait for committed event: seqno="
                        + requiredSeqno);
            }
            Future<ReplDBMSHeader> future = context
                    .waitForCommitted(requiredSeqno);
            try
            {
                ReplDBMSHeader satisfyingEvent = future.get();
                if (logger.isDebugEnabled())
                {
                    if (satisfyingEvent == null)
                        logger.debug("Finished wait as seqno is committed: requiredSeqno="
                                + requiredSeqno + " awaitedSeqno=null");
                    else
                        logger.debug("Finished wait as seqno is committed: requiredSeqno="
                                + requiredSeqno
                                + " awaitedSeqno="
                                + satisfyingEvent.getSeqno());
                }
            }
            catch (ExecutionException e)
            {
                throw new ReplicatorException(
                        "Failure while waiting for pending event to commit: seqno="
                                + requiredSeqno, e);
            }
        }
    }

    // Inserts a control event in all queues.
    private void putControlEvent(int type, ReplDBMSEvent event)
            throws InterruptedException
    {
        long ctrlSeqno;
        if (event == null)
            ctrlSeqno = this.headSeqnoCounter.getSeqno();
        else
            ctrlSeqno = event.getSeqno();
        ReplControlEvent ctrl = new ReplControlEvent(type, ctrlSeqno, event);

        if (logger.isDebugEnabled())
        {
            logger.debug("Inserting control event: type=" + type + " seqno="
                    + ctrlSeqno);
        }

        for (THLParallelReadTask readTask : this.readTasks)
        {
            readTask.putControlEvent(ctrl);
        }
    }

    /**
     * Removes and returns next event from the queue, blocking if empty.
     */
    public ReplEvent get(int taskId) throws InterruptedException,
            ReplicatorException
    {
        // Fetch the event.
        assertTaskIdWithinRange(taskId);
        ReplEvent event = readTasks.get(taskId).get();
        if (logger.isDebugEnabled())
        {
            logger.debug("Returning event: taskId=" + taskId + " seqno="
                    + event.getSeqno() + " type="
                    + event.getClass().getSimpleName() + " activeSize="
                    + activeSize.getSeqno());
        }

        // Only decrement for a proper event belonging to a transaction.
        if (event instanceof ReplDBMSEvent)
            activeSize.decrAndGetSeqno();
        return event;
    }

    /**
     * Returns next event from the queue without removing it, returning null if
     * queue is empty.
     */
    public ReplEvent peek(int taskId) throws InterruptedException,
            ReplicatorException
    {
        assertTaskIdWithinRange(taskId);
        return readTasks.get(taskId).peek();
    }

    /**
     * Returns the current queue size.
     */
    public int size(int taskId)
    {
        return readTasks.get(taskId).size();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public synchronized void configure(PluginContext context)
            throws ReplicatorException
    {
        // Store the context.
        this.context = context;

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

        // Stateful partitioners are not accepted.
        if (partitioner instanceof StatefulPartitioner)
        {
            throw new ReplicatorException(
                    "This store does not support StatefulPartitioner implementations: class="
                            + partitionerClass);
        }

        // Set the sync interval only if sync'ing is enabled.
        if (syncInterval <= 0)
            throw new ReplicatorException(
                    "Sync interval must be greater than 0");

        // Allocate the thread interval checker now that we know the number of
        // partitions.
        intervalGuard = new AtomicIntervalGuard<Object>(partitions);
        maxOfflineMillis = maxOfflineInterval * 1000;
        maxDelayMillis = maxDelayInterval * 1000;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public synchronized void prepare(PluginContext context)
            throws ReplicatorException, InterruptedException
    {
        // Find the THL from which we expect to feed.
        try
        {
            thl = (THL) context.getStore(thlStoreName);
        }
        catch (ClassCastException e)
        {
            throw new ReplicatorException(
                    "Invalid THL storage class; thlStoreName parameter may be in error: "
                            + context.getStore(thlStoreName).getClass()
                                    .getName());
        }
        if (thl == null)
            throw new ReplicatorException(
                    "Unknown storage name; thlStoreName may be in error: "
                            + thlStoreName);

        // Instantiate reader tasks, followed by array of last sequence numbers
        // to permit propagation of restart points from each output task.
        readTasks = new ArrayList<THLParallelReadTask>(partitions);
        for (int i = 0; i < partitions; i++)
        {
            THLParallelReadTask readTask = new THLParallelReadTask(i, thl,
                    partitioner, headSeqnoCounter, intervalGuard, maxSize,
                    maxControlEvents, syncInterval,
                    context.getEventDispatcher());
            readTasks.add(readTask);
            readTask.prepare(context);
        }
        lastHeaders = new ReplDBMSHeader[partitions];
    }

    /**
     * Release queue. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public synchronized void release(PluginContext context)
            throws ReplicatorException
    {
        if (readTasks != null)
        {
            // Dump statistics.
            TungstenProperties status = status();
            logger.info("Releasing THL parallel queue store: "
                    + status.toString());

            // Stop processing.
            for (THLParallelReadTask readTask : readTasks)
            {
                // Stop the task thread again for good measure.
                readTask.stop();
                readTask.release();
            }
            readTasks = null;
            lastHeaders = null;
        }
    }

    /**
     * Start the reader for a particular task.
     */
    public void start(int taskId)
    {
        this.readTasks.get(taskId).start();
    }

    /**
     * Stop the reader for a particular task.
     */
    public void stop(int taskId)
    {
        this.readTasks.get(taskId).stop();
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
        for (THLParallelReadTask readTask : readTasks)
        {
            readTask.addWatchSyncPredicate(predicate);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.storage.Store#status()
     */
    public TungstenProperties status()
    {
        TungstenProperties props = new TungstenProperties();
        props.setLong("headSeqno", headSeqnoCounter.getSeqno());
        props.setLong("maxSize", maxSize);
        props.setLong("eventCount", transactionCount);
        props.setLong("discardCount", discardCount);
        props.setInt("queues", partitions);
        props.setInt("syncInterval", syncInterval);
        props.setInt("maxOfflineInterval", maxOfflineInterval);
        props.setInt("maxDelayInterval", maxDelayInterval);
        props.setDouble("estimatedOfflineInterval",
                ((double) intervalGuard.getInterval()) / 1000.0);
        props.setBoolean("serialized", this.criticalPartition >= 0);
        props.setLong("serializationCount", serializationCount);
        props.setBoolean("stopRequested", stopRequested);
        props.setInt("criticalPartition", criticalPartition);
        props.setString("intervalGuard", intervalGuard.toString());
        for (int i = 0; i < readTasks.size(); i++)
        {
            props.setString("store." + i, readTasks.get(i).toString());
        }
        return props;
    }
}