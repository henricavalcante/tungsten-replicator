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

import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.continuent.tungsten.fsm.event.EventDispatcher;
import com.continuent.tungsten.replicator.ErrorNotification;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.ReplControlEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplDBMSHeaderData;
import com.continuent.tungsten.replicator.event.ReplEvent;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.storage.parallel.Partitioner;
import com.continuent.tungsten.replicator.storage.parallel.PartitionerResponse;
import com.continuent.tungsten.replicator.thl.log.LogConnection;
import com.continuent.tungsten.replicator.thl.log.LogEventReadFilter;
import com.continuent.tungsten.replicator.thl.log.LogEventReplReader;
import com.continuent.tungsten.replicator.util.AtomicCounter;
import com.continuent.tungsten.replicator.util.AtomicIntervalGuard;
import com.continuent.tungsten.replicator.util.WatchPredicate;

/**
 * Performs coordinated reads on the THL on behalf of a particular client (a
 * task thread) and buffers log records up to a local limit.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class THLParallelReadTask implements Runnable
{
    private static Logger          logger               = Logger.getLogger(THLParallelReadTask.class);

    // Task number on whose behalf we are reading.
    private final int              taskId;
    private final int              maxSize;
    private final int              syncInterval;

    // Partitioner instance.
    private final Partitioner      partitioner;

    // Counters to coordinate queue operation.
    private AtomicCounter          headSeqnoCounter;
    private AtomicIntervalGuard<?> intervalGuard;
    private AtomicLong             lowWaterMark         = new AtomicLong(0);
    private AtomicLong             readCount            = new AtomicLong(0);

    // Dispatcher to report errors.
    EventDispatcher                dispatcher;

    // Queue parameters.
    private final int              maxControlEvents;
    private long                   restartSeqno         = 0;
    private long                   restartExtractMillis = Long.MAX_VALUE;
    private ReplDBMSHeader         lastHeader;

    // Pending control events to be integrated into the event queue and seqno
    // of next event if known.
    private THLParallelReadQueue   readQueue;

    // Connection to the log.
    private THL                    thl;
    private LogConnection          connection;

    // Throwable trapped from run loop.
    private volatile Throwable     throwable;

    // Thread ID for this read task.
    private volatile Thread        taskThread;

    // Flag indicating task is cancelled.
    private volatile boolean       cancelled            = false;

    /**
     * Instantiate a read task.
     */
    public THLParallelReadTask(int taskId, THL thl, Partitioner partitioner,
            AtomicCounter headSeqnoCounter,
            AtomicIntervalGuard<?> intervalGuard, int maxSize,
            int maxControlEvents, int syncInterval, EventDispatcher dispatcher)
    {
        this.taskId = taskId;
        this.thl = thl;
        this.partitioner = partitioner;
        this.headSeqnoCounter = headSeqnoCounter;
        this.intervalGuard = intervalGuard;
        this.maxSize = maxSize;
        this.maxControlEvents = maxControlEvents;
        this.syncInterval = syncInterval;
        this.dispatcher = dispatcher;
    }

    /**
     * Set the starting header. This must be called before prepare().
     */
    public synchronized void setRestartHeader(ReplDBMSHeader header)
    {
        this.restartSeqno = header.getSeqno() + 1;
        this.restartExtractMillis = header.getExtractedTstamp().getTime();
        this.lastHeader = header;
    }

    /**
     * Connect to THL and seek start sequence number. Must be called before
     * run().
     */
    public synchronized void prepare(PluginContext context)
            throws ReplicatorException, InterruptedException
    {
        // Set up the read queue.
        this.readQueue = new THLParallelReadQueue(taskId, maxSize,
                maxControlEvents, restartSeqno, syncInterval, lastHeader,
                intervalGuard);

        // Connect to the log.
        connection = thl.connect(true);

        // Add a read filter that will accept only events that are in this
        // partition. We use an inner class so we can access the partitioner
        // and task id easily.
        LogEventReadFilter filter = new LogEventReadFilter()
        {
            public boolean accept(LogEventReplReader reader)
                    throws ReplicatorException
            {
                ReplDBMSHeaderData header = new ReplDBMSHeaderData(
                        reader.getSeqno(), reader.getFragno(),
                        reader.isLastFrag(), reader.getSourceId(),
                        reader.getEpochNumber(), reader.getEventId(),
                        reader.getShardId(), new Timestamp(
                                reader.getSourceTStamp()), 0);
                PartitionerResponse response;
                try
                {
                    response = partitioner.partition(header, taskId);
                }
                catch (THLException e)
                {
                    throw e;
                }
                catch (ReplicatorException e)
                {
                    throw new THLException(e.getMessage(), e);
                }
                return (taskId == response.getPartition());
            }
        };
        connection.setReadFilter(filter);

        // Report our starting position to the interval guard.
        intervalGuard.report(taskId, restartSeqno, restartExtractMillis);
    }

    /**
     * Start the task thread. This must be called after prepare.
     */
    public synchronized void start()
    {
        if (this.taskThread == null)
        {
            taskThread = new Thread(this);
            taskThread.setName("store-" + thl.getName() + "-" + taskId);
            taskThread.start();
        }
    }

    /**
     * Cancel the thread. This must be called prior to release.
     */
    public synchronized void stop()
    {
        cancelled = true;
        if (this.taskThread != null)
        {
            taskThread.interrupt();
            try
            {
                taskThread.join(2000);
            }
            catch (InterruptedException e)
            {
            }
        }
    }

    /**
     * Terminate reader task and free all resources. Must be called following
     * run().
     */
    public synchronized void release()
    {
        if (connection != null)
        {
            connection.release();
            connection = null;
            readQueue.release();
            readQueue = null;
        }
    }

    /**
     * Implements read loop on the log to feed event queue.
     */
    @Override
    public void run()
    {
        // Get the starting sequence number.
        long readSeqno = restartSeqno;

        try
        {
            // Seek to initial position to start reading.
            if (!connection.seek(restartSeqno))
            {
                throw new THLException(
                        "Unable to locate starting seqno in log: seqno="
                                + restartSeqno + " store=" + thl.getName()
                                + " taskId=" + taskId);
            }

            // Read records until we are cancelled.
            while (!cancelled)
            {
                // Read next event from the log.
                THLEvent thlEvent = connection.next();
                readSeqno = thlEvent.getSeqno();
                if (lowWaterMark.get() == 0)
                    lowWaterMark.set(readSeqno);
                readCount.incrementAndGet();
                if (logger.isDebugEnabled())
                {
                    logger.debug("Read event from THL: seqno="
                            + thlEvent.getSeqno() + " fragno="
                            + thlEvent.getFragno() + " lastFrag="
                            + thlEvent.getLastFrag() + " deserialized="
                            + (thlEvent.getReplEvent() != null));
                }

                // Ensure it is safe to process this value. This lock prevents
                // our thread from jumping too far ahead of others and
                // coordinates serialization.
                headSeqnoCounter.waitSeqnoGreaterEqual(thlEvent.getSeqno());

                // Post to the queue.
                if (logger.isDebugEnabled())
                {
                    logger.debug("Adding event to parallel queue:  taskId="
                            + taskId + " seqno=" + thlEvent.getSeqno()
                            + " fragno=" + thlEvent.getFragno());
                }
                readQueue.post(thlEvent);
            }
        }
        catch (InterruptedException e)
        {
            if (!cancelled)
                logger.warn("Unexpected interrupt before reader thread was cancelled");
        }
        catch (Throwable e)
        {
            // Store the error and try to log it at the point of failure.
            // This mitigates liveness problems if we fail due to lack of memory
            // as errors may not be logged. However, callers will pick up
            // the throwable and signal an error.
            throwable = e;
            try
            {
                String msg = "Read failed on transaction log: seqno="
                        + readSeqno + " taskId=" + taskId;
                logger.error(msg, e);
                dispatcher.put(new ErrorNotification(msg, e));
            }
            catch (InterruptedException e1)
            {
                logger.warn("Task cancelled while posting error notification",
                        null);
            }
            catch (Throwable t1)
            {
                logger.warn("Failure while attempting to log an error: " + e,
                        t1);
            }
        }

        // Close up shop.
        logger.info("Terminating parallel reader thread: seqno=" + readSeqno
                + " store=" + thl.getName() + " taskId=" + taskId);
    }

    // QUEUE INTERFACE STARTS HERE.

    /**
     * Returns the current queue size.
     */
    public int size()
    {
        return readQueue.size();
    }

    /**
     * Removes and returns next event from the queue, blocking if empty. This
     * call blocks if no event is available. Internally it polls so that we
     * correctly detect if the thread has failed.
     * 
     * @return The next event in the queue
     * @throws InterruptedException Thrown if method is interrupted
     * @throws ReplicatorException Thrown if the reader thread has failed
     */
    public ReplEvent get() throws InterruptedException, ReplicatorException
    {
        // Use a polling loop so that if there is a problem with the THL thread
        // we will see the throwable. This prevents us from blocking while the
        // thread is actually dead.
        ReplEvent event = null;
        while (event == null)
        {
            // Check for read thread liveness.
            if (throwable != null)
            {
                // If this happens the thread has died.
                throw new ReplicatorException("THL reader thread failed",
                        throwable);
            }
            else if (cancelled)
            {
                // If this is true the thread has been cancelled. This should
                // not occur before the caller thread has exited.
                throw new ReplicatorException("THL reader thread is cancelled");
            }

            // Get the next event and return it.
            event = readQueue.take(1000, TimeUnit.MILLISECONDS);
        }

        if (logger.isDebugEnabled())
        {
            logger.debug("Returning event from queue: seqno="
                    + event.getSeqno() + " type="
                    + event.getClass().getSimpleName() + " taskId=" + taskId
                    + " activeSize=" + size());
        }
        return event;
    }

    /**
     * Returns but does not remove next event from the queue if it exists or
     * returns null if queue is empty.
     */
    public ReplEvent peek() throws InterruptedException
    {
        return readQueue.peek();
    }

    /**
     * Inserts a control event.
     */
    public void putControlEvent(ReplControlEvent controlEvent)
            throws InterruptedException
    {
        readQueue.postOutOfBand(controlEvent);
    }

    /**
     * Adds a watch predicate.
     */
    public void addWatchSyncPredicate(WatchPredicate<ReplDBMSHeader> predicate)
            throws InterruptedException
    {
        readQueue.addWatchSyncPredicate(predicate);
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        sb.append(this.getClass().getSimpleName());
        sb.append(" task_id=").append(taskId);
        sb.append(" thread_name=");
        if (taskThread == null)
            sb.append("null");
        else
            sb.append(taskThread.getName());
        sb.append(" hi_seqno=").append(this.readQueue.getReadSeqno());
        sb.append(" lo_seqno=").append(lowWaterMark.get());
        sb.append(" read=").append(readCount);
        sb.append(" accepted=").append(readQueue.getAcceptCount());
        sb.append(" discarded=").append(readQueue.getDiscardCount());
        sb.append(" events=").append(readQueue.size());
        return sb.toString();
    }
}