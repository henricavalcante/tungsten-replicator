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

package com.continuent.tungsten.replicator.pipeline;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplDBMSHeaderData;
import com.continuent.tungsten.replicator.storage.ParallelStore;
import com.continuent.tungsten.replicator.util.AtomicIntervalGuard;
import com.continuent.tungsten.replicator.util.EventIdWatchPredicate;
import com.continuent.tungsten.replicator.util.HeartbeatWatchPredicate;
import com.continuent.tungsten.replicator.util.SeqnoWatchPredicate;
import com.continuent.tungsten.replicator.util.SourceTimestampWatchPredicate;
import com.continuent.tungsten.replicator.util.Watch;
import com.continuent.tungsten.replicator.util.WatchAction;
import com.continuent.tungsten.replicator.util.WatchManager;
import com.continuent.tungsten.replicator.util.WatchPredicate;

/**
 * Tracks the current status of replication and implements event watches. This
 * class maintains a clear distinction between the latest event processed and
 * the latest event committed. The methods to get these values are designated
 * "dirty" and "committed" respectively to make this distinction as clear as
 * possible.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class StageProgressTracker
{
    private static Logger                        logger              = Logger.getLogger(StageProgressTracker.class);
    String                                       name;

    // Record of last processed event on each task.
    private final int                            threadCount;
    private final TaskProgress[]                 taskInfo;

    // Record of last processed info on each shard.
    private final TreeMap<String, ShardProgress> shardInfo           = new TreeMap<String, ShardProgress>();

    // Watch lists.
    private final WatchManager<ReplDBMSHeader>   processingWatches   = new WatchManager<ReplDBMSHeader>();
    private final WatchManager<ReplDBMSHeader>   commitWatches       = new WatchManager<ReplDBMSHeader>();

    // Upstream parallel store for inserting watch events.
    ParallelStore                                upstreamStore       = null;

    // If this is set, the task should be interrupted.
    private boolean                              shouldInterruptTask = false;

    // Watch action to terminate this task.
    WatchAction<ReplDBMSHeader>                  cancelAction        = new WatchAction<ReplDBMSHeader>()
                                                                     {
                                                                         public void matched(
                                                                                 ReplDBMSHeader event,
                                                                                 int taskId)
                                                                         {
                                                                             taskInfo[taskId]
                                                                                     .setCancelled(true);
                                                                         }

                                                                         public String toString()
                                                                         {
                                                                             return "cancel tasks";
                                                                         }
                                                                     };

    // Global reporting counters. We also report on individual tasks.
    private long                                 eventCount          = 0;
    private long                                 loggingInterval     = 0;
    private long                                 applyLatencyMillis  = 0;

    // Variables used to skip events.
    private long                                 applySkipCount      = 0;
    private SortedSet<Long>                      seqnosToBeSkipped   = null;

    // Task tracking for committed IDs. This is used to maintain the minimum and
    // maximum committed sequence number.
    AtomicIntervalGuard<ReplDBMSHeader>          committedSeqno;

    /**
     * Creates a new stage process tracker.
     * 
     * @param name
     */
    public StageProgressTracker(String name, int threadCount)
    {
        // Set instance variables.
        this.name = name;
        this.threadCount = threadCount;
        this.taskInfo = new TaskProgress[threadCount];
        this.committedSeqno = new AtomicIntervalGuard<ReplDBMSHeader>(
                threadCount);

        // Initialize task processing data.
        for (int i = 0; i < taskInfo.length; i++)
            taskInfo[i] = new TaskProgress(name, i);

        if (logger.isDebugEnabled())
        {
            logger.info("Initiating stage process tracker for stage: name="
                    + name + " threadCount=" + threadCount);
        }
    }

    /** Sets the upstream parallel store, if such a thing exists. */
    public void setUpstreamStore(ParallelStore upstreamStore)
    {
        this.upstreamStore = upstreamStore;
    }

    /** Print a log message every time we process this many events. */
    public void setLoggingInterval(long loggingInterval)
    {
        this.loggingInterval = loggingInterval;
    }

    /** Set the number of events to skip after going online. */
    public void setApplySkipCount(long applySkipCount)
    {
        this.applySkipCount = applySkipCount;
    }

    /** Set a list of one or more events to skip. */
    public void setSeqnosToBeSkipped(SortedSet<Long> seqnosToBeSkipped)
    {
        this.seqnosToBeSkipped = seqnosToBeSkipped;
    }

    /**
     * Return last event that we have seen.
     */
    public synchronized ReplDBMSHeader getDirtyLastProcessedEvent(int taskId)
    {
        return taskInfo[taskId].getLastProcessedEvent();
    }

    /**
     * Return the last processed event or null if none such exists. This event
     * may not be committed.
     */
    public synchronized ReplDBMSHeader getDirtyMinLastEvent()
    {
        ReplDBMSHeader minEvent = null;
        for (TaskProgress progress : taskInfo)
        {
            ReplDBMSHeader event = progress.getLastProcessedEvent();
            if (event == null)
            {
                minEvent = null;
                break;
            }
            else if (minEvent == null || minEvent.getSeqno() > event.getSeqno())
            {
                minEvent = event;
            }
        }
        return minEvent;
    }

    /**
     * Return the last processed sequence number or -1 if no event exists. This
     * event is the minimum value that has been reached.
     */
    public synchronized long getDirtyMinLastSeqno()
    {
        long minSeqno = Long.MAX_VALUE;
        for (TaskProgress progress : taskInfo)
        {
            ReplDBMSHeader event = progress.getLastProcessedEvent();
            if (event == null)
                minSeqno = -1;
            else
                minSeqno = Math.min(minSeqno, event.getSeqno());
        }
        return minSeqno;
    }

    /**
     * Return the last safely committed sequence number. This value represents
     * the minimum value across tasks. It is very fast and minimizes lock
     * contention.
     */
    public synchronized long getCommittedMinSeqno()
    {
        return committedSeqno.getLowSeqno();
    }

    /**
     * Return the latency of the last committed event. This is the maximum
     * latency as it fetches the minimum committed event.
     */
    public synchronized long getCommittedApplyLatency()
    {
        return committedSeqno.getLowLatency();
    }

    /**
     * Return the last committed event. This is the minimum committed event
     * across tasks.
     */
    public synchronized ReplDBMSHeader getCommittedMinEvent()
    {
        return committedSeqno.getLowDatum();
    }

    /**
     * Returns a list of cloned task progress instances ordered by task ID.
     */
    public synchronized List<TaskProgress> cloneTaskProgress()
    {
        List<TaskProgress> progressList = new ArrayList<TaskProgress>();
        for (int i = 0; i < threadCount; i++)
            progressList.add(taskInfo[i].clone());
        return progressList;
    }

    /**
     * Return underlying progress instance for a particular task.
     */
    public synchronized TaskProgress getTaskProgress(int taskId)
    {
        return taskInfo[taskId];
    }

    /**
     * Returns a list of shard progress instances ordered by shard ID.
     */
    public synchronized List<ShardProgress> getShardProgress()
    {
        // Get a sorted array of keys and then generate the list.
        List<ShardProgress> progressList = new ArrayList<ShardProgress>();
        for (ShardProgress progress : shardInfo.values())
        {
            progressList.add(progress);
        }
        return progressList;
    }

    /**
     * Set the last processed event, which triggers checks for watches.
     */
    public synchronized void setLastProcessedEvent(int taskId,
            ReplDBMSHeader replEvent) throws InterruptedException
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("[" + name + "] setLastProcessedEvent: "
                    + replEvent.getSeqno());
        }
        // Log global statistics.
        eventCount++;
        applyLatencyMillis = System.currentTimeMillis()
                - replEvent.getExtractedTstamp().getTime();

        // Log per-task statistics.
        taskInfo[taskId].incrementEventCount();
        taskInfo[taskId].setApplyLatencyMillis(applyLatencyMillis);

        // Log per-shard statistics.
        String shardId = replEvent.getShardId();
        ShardProgress shardProgress = shardInfo.get(shardId);
        if (shardProgress == null)
        {
            shardProgress = new ShardProgress(shardId,
                    taskInfo[taskId].getStageName());
            shardInfo.put(shardId, shardProgress);
        }
        shardProgress.setLastSeqno(replEvent.getSeqno());
        shardProgress.setLastEventId(replEvent.getEventId());
        shardProgress.setApplyLatencyMillis(applyLatencyMillis);
        shardProgress.incrementEventCount();

        // Log last processed event if greater than stored sequence number or if
        // the seqno is the same but the fragment number is different.
        ReplDBMSHeader storedEvent = taskInfo[taskId].getLastProcessedEvent();
        if (storedEvent == null
                || storedEvent.getSeqno() < replEvent.getSeqno()
                || (storedEvent.getSeqno() == replEvent.getSeqno() && storedEvent
                        .getFragno() < replEvent.getFragno()))
        {
            taskInfo[taskId].setLastProcessedEvent(replEvent);
        }

        // If we have a real event, update watches for processed events.
        if (replEvent instanceof ReplDBMSEvent)
        {
            processingWatches.process(replEvent, taskId);
        }
        if (loggingInterval > 0 && eventCount % loggingInterval == 0)
            logger.info("Stage processing counter: event count=" + eventCount);
    }

    public synchronized void setInitialLastProcessedEvent(int taskId,
            ReplDBMSHeader replEvent) throws InterruptedException
    {
        Timestamp extractedTstamp = replEvent.getExtractedTstamp();
        long timeInMs = 0;
        if (extractedTstamp == null)
            timeInMs = System.currentTimeMillis();
        else
            timeInMs = extractedTstamp.getTime();

        long latencyInMs = 1000 * replEvent.getAppliedLatency();
        committedSeqno.report(taskId, replEvent.getSeqno(), timeInMs, timeInMs
                + latencyInMs, replEvent);
        taskInfo[taskId].setLastCommittedEvent(replEvent);
    }

    /**
     * Records the last committed event.
     */
    public synchronized void commit(int taskId) throws InterruptedException
    {
        ReplDBMSHeader processed = taskInfo[taskId].getLastProcessedEvent();
        if (processed != null)
        {
            // Note that the event has been committed.
            ReplDBMSHeader committed = new ReplDBMSHeaderData(processed);
            taskInfo[taskId].setLastCommittedEvent(committed);
            committedSeqno.report(taskId, committed.getSeqno(), committed
                    .getExtractedTstamp().getTime(), committed);
            if (logger.isDebugEnabled())
            {
                logger.debug("[" + name + "] commit: taskId=" + taskId
                        + " seqno=" + committed.getSeqno());
            }

            // Process watches for committed events.
            commitWatches.process(committed, taskId);
        }
        else
        {
            logger.warn("Attempt to commit task before marking processed event: stage="
                    + this.name + " taskId=" + taskId);
        }
    }

    /**
     * Signal that task has been cancelled.
     */
    public synchronized void cancel(int taskId)
    {
        taskInfo[taskId].setCancelled(true);
    }

    /**
     * Return true if task has been cancelled.
     */
    public synchronized boolean isCancelled(int taskId)
    {
        return taskInfo[taskId].isCancelled();
    }

    /**
     * Signal that all tasks have been cancelled.
     */
    public synchronized void cancelAll()
    {
        for (TaskProgress progress : taskInfo)
            progress.setCancelled(true);
    }

    /**
     * Return true if all task are cancelled.
     */
    public synchronized boolean allCancelled()
    {
        for (TaskProgress progress : taskInfo)
        {
            if (progress.isCancelled())
                return false;
        }
        return true;
    }

    /**
     * Return true if we need to interrupt the task(s) after cancellation.
     */
    public synchronized boolean shouldInterruptTask()
    {
        return shouldInterruptTask;
    }

    /**
     * Release progress tracker resources.
     */
    public synchronized void release()
    {
        processingWatches.cancelAll();
        commitWatches.cancelAll();
    }

    /**
     * Sets a watch for a particular sequence number to be processed.
     * 
     * @param seqno Sequence number to watch for
     * @param cancel If true, terminate task when watch is successful
     * @return Returns a watch on the matching event
     * @throws InterruptedException
     */
    public synchronized Future<ReplDBMSHeader> watchForProcessedSequenceNumber(
            long seqno, boolean cancel) throws InterruptedException
    {
        SeqnoWatchPredicate seqnoPredicate = new SeqnoWatchPredicate(seqno);
        return waitForProcessedEvent(seqnoPredicate, cancel);
    }

    /**
     * Sets a watch for a particular sequence number to be safely committed on
     * all channels.
     * 
     * @param seqno Sequence number to watch for
     * @param cancel If true, terminate task when watch is successful
     * @return Returns a watch on the matching event
     * @throws InterruptedException
     */
    public synchronized Future<ReplDBMSHeader> watchForCommittedSequenceNumber(
            long seqno, boolean cancel) throws InterruptedException
    {
        SeqnoWatchPredicate seqnoPredicate = new SeqnoWatchPredicate(seqno);
        return waitForCommittedEvent(seqnoPredicate, cancel);
    }

    /**
     * Sets a watch for a particular event ID to be processed.
     * 
     * @param eventId Native event ID to watch for
     * @param cancel If true, terminate task when watch is successful
     * @return Returns a watch on the matching event
     * @throws InterruptedException
     */
    public synchronized Future<ReplDBMSHeader> watchForProcessedEventId(
            String eventId, boolean cancel) throws InterruptedException
    {
        EventIdWatchPredicate eventPredicate = new EventIdWatchPredicate(
                eventId);
        return waitForProcessedEvent(eventPredicate, cancel);
    }

    /**
     * Sets a watch for a heartbeat event to be extracted.
     * 
     * @param cancel If true, terminate task when watch is successful
     * @return Returns a watch on the matching event
     * @throws InterruptedException
     */
    public synchronized Future<ReplDBMSHeader> watchForProcessedHeartbeat(
            String name, boolean cancel) throws InterruptedException
    {
        HeartbeatWatchPredicate predicate = new HeartbeatWatchPredicate(name);
        // For heartbeats we always want the next one and don't care if
        // there was one before. This prevents confusion in the event that
        // the last event processed happened to be a heartbeat.
        if (cancel)
            return processingWatches
                    .watch(predicate, threadCount, cancelAction);
        else
            return processingWatches.watch(predicate, threadCount);
    }

    /**
     * Sets a watch for a particular source timestamp to be extracted.
     * 
     * @param timestamp Timestame to watch for
     * @param cancel If true, terminate task when watch is successful
     * @return Returns a watch on the matching event
     * @throws InterruptedException
     */
    public synchronized Future<ReplDBMSHeader> watchForProcessedTimestamp(
            Timestamp timestamp, boolean cancel) throws InterruptedException
    {
        SourceTimestampWatchPredicate predicate = new SourceTimestampWatchPredicate(
                timestamp);
        return waitForProcessedEvent(predicate, cancel);
    }

    /**
     * Set a watch on a processed (as opposed to a committed event). This is an
     * event that has been processed completely the task loop but which there
     * has not been a commit. This must be synchronized to compute the minimum
     * processed event.
     */
    private Future<ReplDBMSHeader> waitForProcessedEvent(
            WatchPredicate<ReplDBMSHeader> predicate, boolean cancel)
            throws InterruptedException
    {
        return waitForEvent(predicate, cancel, processingWatches, false);
    }

    /**
     * Set a watch on a committed event. This is an event that has been
     * processed completely the task loop *and* committed. This must be
     * synchronized to compute the minimum committed event.
     */
    private Future<ReplDBMSHeader> waitForCommittedEvent(
            WatchPredicate<ReplDBMSHeader> predicate, boolean cancel)
            throws InterruptedException
    {
        return waitForEvent(predicate, cancel, commitWatches, true);
    }

    /**
     * Private utility to set a watch of arbitrary type. This *must* be
     * synchronized to ensure we compute minimum events correctly. Note the
     * arguments and the distinction between whether an event is fully committed
     * or merely processed.
     * 
     * @param predicate A predicate that goes to true when watch is fulfilled
     * @param cancel If true, execute cancelAction on fulfillment
     * @param lastHeader The most recent event that can satisfy this watch
     * @param watchManager The correct watch manager for this type of watch
     * @param committed If true, we are watching for a committed event,
     *            otherwise a processed event
     * @return A watch on this condition
     * @throws InterruptedException
     */
    private Future<ReplDBMSHeader> waitForEvent(
            WatchPredicate<ReplDBMSHeader> predicate, boolean cancel,
            WatchManager<ReplDBMSHeader> watchManager, boolean committed)
            throws InterruptedException
    {
        // Generate the watch.
        if (logger.isDebugEnabled())
        {
            logger.debug("Enqueueing watch for event: predicate="
                    + predicate.toString() + " cancel=" + cancel
                    + " committed=" + committed);
        }
        Watch<ReplDBMSHeader> watch;
        if (cancel)
            watch = watchManager.watch(predicate, threadCount, cancelAction);
        else
            watch = watchManager.watch(predicate, threadCount);

        // Process watches on each channel. This will cause the watch to
        // become completed if it is already satisfied and execute any
        // associated cancel action.
        for (int i = 0; i < this.taskInfo.length; i++)
        {
            ReplDBMSHeader event;
            if (committed)
                event = taskInfo[i].getLastCommittedEvent();
            else
                event = taskInfo[i].getLastProcessedEvent();

            if (event != null)
                watchManager.process(event, i);
        }

        // See if we have reached this event.
        boolean alreadyReached = watch.isDone();

        // If event is done and we have a cancel action, do it now.
        if (alreadyReached && cancel)
        {
            cancelAll();
            shouldInterruptTask = true;
        }

        // Return the watch.
        if (logger.isDebugEnabled())
        {
            logger.debug("Returning watch to caller: watch=" + watch.toString()
                    + " committed=" + committed + " alreadyReached="
                    + alreadyReached);
        }
        return watch;
    }

    /**
     * Returns current watches.
     * 
     * @param committed If true returned watches for committed events
     */
    public synchronized List<Watch<?>> getWatches(boolean committed)
    {
        WatchManager<ReplDBMSHeader> manager;
        if (committed)
            manager = commitWatches;
        else
            manager = processingWatches;
        return new ArrayList<Watch<?>>(manager.getWatches());
    }

    /**
     * Returns false if the current event should be skipped.
     */
    public synchronized boolean skip(ReplDBMSEvent event)
    {
        // If we are skipping the first N transactions to be applied,
        // try again.
        if (this.applySkipCount > 0)
        {
            logger.info("Skipping event: seqno=" + event.getSeqno()
                    + " fragno=" + event.getFragno(), null);
            if (event.getLastFrag())
                applySkipCount--;
            return true;
        }
        else if (this.seqnosToBeSkipped != null)
        {
            // Purge skip numbers processing has already reached.
            long minSeqno = getDirtyMinLastSeqno();
            while (!this.seqnosToBeSkipped.isEmpty()
                    && this.seqnosToBeSkipped.first() < minSeqno)
                this.seqnosToBeSkipped.remove(this.seqnosToBeSkipped.first());

            if (!this.seqnosToBeSkipped.isEmpty())
            {
                // If we are in the skip list, then skip!
                if (seqnosToBeSkipped.contains(event.getSeqno()))
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Skipping event with seqno "
                                + event.getSeqno());
                    // Skip event and remove seqno after last fragment.
                    if (event.getLastFrag())
                        this.seqnosToBeSkipped.remove(event.getSeqno());
                    return true;
                }
                // else seqnosToBeSkipped.first() > event.getSeqno()
                // so let's process this event
            }
            else
            {
                // the list is now empty... just free the list
                this.seqnosToBeSkipped = null;
                if (logger.isDebugEnabled())
                    logger.debug("No more events to be skipped");
            }
        }

        // No match, so we will process the event.
        return false;
    }
}