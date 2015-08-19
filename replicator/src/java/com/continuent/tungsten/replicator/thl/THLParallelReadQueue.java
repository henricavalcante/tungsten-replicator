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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.event.DBMSEmptyEvent;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplControlEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplDBMSHeaderData;
import com.continuent.tungsten.replicator.event.ReplEvent;
import com.continuent.tungsten.replicator.util.AtomicIntervalGuard;
import com.continuent.tungsten.replicator.util.WatchPredicate;

/**
 * Implements a queue that returns in total order the most recently read events
 * from its parent read task and pending control events required to process
 * watches properly. Items in this queue come from the following sources:
 * <ol>
 * <li>Events read from the log. The THLParallelReadTask thread supplies these
 * as it reads them.</li>
 * <li>Out-of-band control events. The THLParallelQueue inserts these to
 * implement synchronization between channels, i.e., stage tasks. They must be
 * merged in sequence number order into the queue so that the stage task sees
 * them.</li>
 * <li>Control events generated from watch predicates. These ensure that stage
 * tasks commit their current position so that watches can be fulfilled.
 * </ol>
 * The main responsibility of this class is to merge these events so the get()
 * method returns them in total order. The APIs of this class match standard
 * queue interfaces.
 * <p>
 * Access to this class is highly concurrent. Methods that merge queue contents
 * are synchronized to ensure atomicity of changes across queue structures.
 * Failure to do this could result in ordering violations. The event queue is
 * protected by a concurrent collection. Methods that access this as well as
 * statistical information are not synchronized, as doing so would raise the
 * risk of deadlocks.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class THLParallelReadQueue
{
    private static final Logger                        logger       = Logger.getLogger(THLParallelReadQueue.class);

    // PARAMETERS
    private final int                                  taskId;

    // Number of transactions between automatic sync events.
    private final int                                  syncInterval;

    // PUBLICLY ACCESSED MEMBERS -- MUST PROVIDE INDEPENDENT SYCHRONIZATION
    // Totally ordered queue of merged read task events and control events.
    private BlockingQueue<ReplEvent>                   eventQueue;

    // Counters to track when to merge control events. These are declared
    // volatile to permit non-blocking reads.
    private volatile long                              readSeqno    = 0;
    private volatile boolean                           lastFrag     = true;
    private volatile ReplDBMSHeader                    lastHeader   = null;

    // Interval guard for reporting our position.
    private AtomicIntervalGuard<?>                     intervalGuard;

    // Statistical counters.
    private AtomicLong                                 acceptCount  = new AtomicLong(
                                                                            0);
    private AtomicLong                                 discardCount = new AtomicLong(
                                                                            0);

    // PRIVATE MEMBERS PROTECTED BY SYNCHRONIZED METHODS

    // Pending control events to be integrated into the event queue.
    private LinkedList<ReplControlEvent>               controlQueue;

    // Queue for predicates belonging to pending wait synchronization requests.
    private LinkedList<WatchPredicate<ReplDBMSHeader>> watchPredicates;

    /**
     * Instantiates a new read queue.
     * 
     * @param taskId Task to which this queue belongs
     * @param maxSize Maximum number of all events to buffer
     * @param maxControlEvents Maximum number of control events to buffer
     * @param startingSeqno Sequence number of next transaction
     * @param syncInterval Interval at which to generate synchronization events
     * @param lastHeader Header of last transaction processed before start
     * @param intervalGuard Interval guard to track read position
     */
    public THLParallelReadQueue(int taskId, int maxSize, int maxControlEvents,
            long startingSeqno, int syncInterval, ReplDBMSHeader lastHeader,
            AtomicIntervalGuard<?> intervalGuard)
    {
        // Set starting parameters.
        this.taskId = taskId;
        this.readSeqno = startingSeqno;
        this.syncInterval = syncInterval;
        this.intervalGuard = intervalGuard;

        // Instantiate queues.
        this.eventQueue = new LinkedBlockingQueue<ReplEvent>(maxSize);
        this.watchPredicates = new LinkedList<WatchPredicate<ReplDBMSHeader>>();
        this.controlQueue = new LinkedList<ReplControlEvent>();

        // If required create a "fake" header in case we need to fulfill
        // predicates or process control events before the first new transaction
        // arrives.
        if (lastHeader == null)
        {
            this.lastHeader = new ReplDBMSHeaderData(startingSeqno - 1,
                    (short) 0, true, "dummy", -1, "dummy", "dummy",
                    new Timestamp(System.currentTimeMillis()), 0);
        }
        else
        {
            this.lastHeader = lastHeader;
        }
    }

    /** Returns current sequence number we have read. */
    public long getReadSeqno()
    {
        return readSeqno;
    }

    /** Returns whether last event read was the end of a transaction. */
    public boolean isLastFrag()
    {
        return lastFrag;
    }

    /** Return count of accepted events. */
    public long getAcceptCount()
    {
        return acceptCount.get();
    }

    /** Return count of discarded events. */
    public long getDiscardCount()
    {
        return discardCount.get();
    }

    /**
     * Frees resources including all queues and lists.
     */
    public synchronized void release()
    {
        if (eventQueue != null)
        {
            eventQueue.clear();
            eventQueue = null;
        }
        if (controlQueue != null)
        {
            controlQueue.clear();
            controlQueue = null;
        }
        if (watchPredicates != null)
        {
            watchPredicates.clear();
            watchPredicates = null;
        }
    }

    // UPSTREAM QUEUE INTERFACE. These methods perform operations that
    // affect consistency between data structures and must be synchronized.
    // Callers must ensure they do not hold monitors that would lead to
    // deadlock if a call must wait for underlying collections to empty.

    /**
     * Add a new predicate to the list of predicates that should generate sync
     * events. If we are at the end of a transaction, see if the event should be
     * posted immediately.
     * 
     * @param predicate Watch predicate
     */
    public synchronized void addWatchSyncPredicate(
            WatchPredicate<ReplDBMSHeader> predicate)
            throws InterruptedException
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("Adding watch sync predicate: taskId=" + taskId
                    + " predicate=" + predicate.toString());
        }
        // Add to the predicate list and evaluate said list if we are
        // at the end of the list.
        watchPredicates.add(predicate);
        if (lastFrag)
        {
            processPredicates();
        }
    }

    /**
     * Post a control event, which will either be immediately added to the event
     * queue or buffered in the control event queue until it is time to merge
     * it. The control event is ordered by seqno then by order of insertion in
     * cases where there are multiple control events for a single seqno.
     * <p/>
     * Note that if you post a control event whose seqno is <em>prior</em> to
     * the current seqno in the queue, the control event seqno will be altered
     * to use the current seqno. This is required to prevent seqno values from
     * appearing to move backwards, which could provoke bugs in downstream tasks
     * that expect sequence numbers to increase monotonically.
     * 
     * @param controlEvent Control event to post or buffer
     */
    public synchronized void postOutOfBand(ReplControlEvent controlEvent)
            throws InterruptedException
    {
        // Add event to the control queue.
        if (logger.isDebugEnabled())
        {
            logger.debug("Inserting out-of-band control event: taskId="
                    + taskId + " seqno=" + controlEvent.getSeqno() + " type="
                    + controlEvent.getEventType() + " readSeqno=" + readSeqno
                    + " lastFrag=" + lastFrag);
        }

        // Add the control event into the queue in insert order.
        int insertIndex = 0;
        for (int i = 0; i < controlQueue.size(); i++)
        {
            if (controlQueue.get(i).getSeqno() <= controlEvent.getSeqno())
                insertIndex++;
            else
                break;
        }
        controlQueue.add(insertIndex, controlEvent);

        // If we are on the last fragment of a transaction, post the
        // control event now.
        if (lastFrag)
        {
            processControlEvents();
        }
    }

    /**
     * Post a normal event which will be enqueued immediately. Synchronize
     * position to update counters and merge any pending control events or
     * predicates as well.
     * 
     * @param thlEvent Event to post.
     */
    public synchronized void post(THLEvent thlEvent)
            throws InterruptedException
    {
        // See if this is an event we need to post. The ReplDBMSEvent
        // would be null if the read filter discarded the event due
        // to it being in another partition.
        ReplDBMSEvent replDBMSEvent = (ReplDBMSEvent) thlEvent.getReplEvent();
        if (replDBMSEvent == null)
        {
            discardCount.incrementAndGet();
            mergeSync(thlEvent);
            if (logger.isDebugEnabled())
            {
                logger.debug("Discarded null event: taskId=" + " seqno="
                        + thlEvent.getSeqno() + " fragno="
                        + thlEvent.getFragno());
            }
            return;
        }

        // Discard empty events. These should not be common.
        DBMSEvent dbmsEvent = replDBMSEvent.getDBMSEvent();
        if (dbmsEvent == null
                || dbmsEvent instanceof DBMSEmptyEvent
                || (dbmsEvent.getData().size() == 0
                        && replDBMSEvent.getFragno() == 0 && replDBMSEvent
                        .getLastFrag()))
        {
            discardCount.incrementAndGet();
            if (logger.isDebugEnabled())
            {
                logger.debug("Discarded empty event: taskId=" + taskId
                        + " seqno=" + thlEvent.getSeqno() + " fragno="
                        + thlEvent.getFragno());
            }
            mergeSync(thlEvent);
            return;
        }

        // Post into the event queue.
        if (logger.isDebugEnabled())
        {
            logger.debug("Adding event to parallel queue: taskId=" + taskId
                    + " seqno=" + replDBMSEvent.getSeqno());
        }
        eventQueue.put(replDBMSEvent);
        acceptCount.incrementAndGet();

        // Now check for required synchronization.
        mergeSync(thlEvent);
    }

    // Merge control events that are enqueued, generated from predicates, or
    // automatically generate synchronization events. This method also updates
    // the current queue position.
    private void mergeSync(THLEvent thlEvent) throws InterruptedException
    {
        // Update the read position including the last header.
        this.readSeqno = thlEvent.getSeqno();
        this.lastFrag = thlEvent.getLastFrag();
        this.lastHeader = new ReplDBMSHeaderData(thlEvent.getSeqno(),
                thlEvent.getFragno(), thlEvent.getLastFrag(),
                thlEvent.getSourceId(), thlEvent.getEpochNumber(),
                thlEvent.getEventId(), thlEvent.getShardId(),
                thlEvent.getSourceTstamp(), 0);

        // See if we need to synchronize.
        if (lastFrag)
        {
            // If we have hit the sync interval, we need a control
            // event in the queue at this point so that downstream
            // stages mark their position.
            if (readSeqno > 0 && (readSeqno + 1) % syncInterval == 0)
            {
                ReplControlEvent ctrl = new ReplControlEvent(
                        ReplControlEvent.SYNC, readSeqno, lastHeader);

                if (logger.isDebugEnabled())
                {
                    logger.debug("Inserting sync event: taskId=" + taskId
                            + " seqno=" + readSeqno);
                }
                eventQueue.put(ctrl);
            }

            // If we have pending predicate matches, this should result
            // in a control event.
            processPredicates();

            // If there is a pending out-of-band control event, submit it now.
            processControlEvents();
        }
    }

    // Process the control event queue and submit any pending control event.
    // This must be called only on a transaction boundary.
    private void processControlEvents() throws InterruptedException
    {
        // If there is a pending out-of-band control event, submit it now.
        ReplControlEvent controlEvent = null;
        while ((controlEvent = controlQueue.peek()) != null
                && controlEvent.getSeqno() <= readSeqno)
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Dequeueing buffered control event and enqueueing to parallel queue: taskId="
                        + taskId
                        + " seqno="
                        + controlEvent.getSeqno()
                        + " type=" + controlEvent.getEventType());
            }

            // Test for the seqno being less than the current read seqno. If so,
            // recreate it to point to the current read seqno position to
            // prevent seqno values in the queue from going backwards.
            controlEvent = controlQueue.remove();
            if (controlEvent.getSeqno() < readSeqno)
            {
                if (logger.isDebugEnabled())
                {
                    logger.debug("Rewriting control event to use current seqno: taskId="
                            + taskId + " seqno=" + readSeqno);
                }
                controlEvent = new ReplControlEvent(
                        controlEvent.getEventType(), readSeqno, lastHeader);
            }
            eventQueue.put(controlEvent);
        }
    }

    // Check to see if we have active predicates that should generate
    // sync events. This must be called only on a transaction boundary.
    private void processPredicates() throws InterruptedException
    {
        // Scan for matches and record them.
        boolean needsSync = false;
        List<WatchPredicate<ReplDBMSHeader>> removeList = new ArrayList<WatchPredicate<ReplDBMSHeader>>();
        for (WatchPredicate<ReplDBMSHeader> predicate : watchPredicates)
        {
            if (predicate.match(lastHeader))
            {
                needsSync = true;
                removeList.add(predicate);
                if (logger.isDebugEnabled())
                {
                    logger.debug("Found and matched watch predicate:  taskId="
                            + taskId + " predicate=" + predicate.toString());
                }
            }
        }

        // If we have at least one match, add a sync event and remove all
        // matching predicates.
        if (needsSync)
        {
            ReplControlEvent ctrl = new ReplControlEvent(ReplControlEvent.SYNC,
                    readSeqno, lastHeader);
            if (logger.isDebugEnabled())
            {
                logger.debug("Inserting sync event: taskId=" + taskId
                        + " seqno=" + readSeqno);
            }
            eventQueue.put(ctrl);
            watchPredicates.removeAll(removeList);
        }
    }

    // DOWNSTREAM QUEUE INTERFACE. Synchronization in these methods is
    // provided by the collection classes.

    /**
     * Returns the current queue size.
     */
    public int size()
    {
        return eventQueue.size();
    }

    /**
     * Removes the next event from the queue, waiting indefinitely for something
     * to arrive.
     * 
     * @return An event
     * @throws InterruptedException Thrown if thread is interrupted.
     */
    public ReplEvent take() throws InterruptedException
    {
        ReplEvent event = null;
        while (event == null)
        {
            event = take(1000, TimeUnit.MILLISECONDS);
        }
        return event;
    }

    /**
     * Removes the next element from the event queue, returning null if it
     * cannot be found within the timeout.
     * 
     * @param timeout Interval to wait
     * @param unit Unit of time
     * @return An event or null if we time out
     * @throws InterruptedException Thrown if thread is interrupted.
     */
    public ReplEvent take(long timeout, TimeUnit unit)
            throws InterruptedException
    {
        // If there is nothing in the queue yet, remove this task
        // from interval tracking.
        if (eventQueue.size() == 0)
            intervalGuard.unreport(this.taskId);

        // Grab the next event. We poll to ensure a timeout.
        ReplEvent event = eventQueue.poll(timeout, unit);

        // Report the event we are processing to interval tracking if we got
        // something.
        if (event != null)
        {
            if (event instanceof ReplDBMSEvent)
            {
                ReplDBMSEvent rde = (ReplDBMSEvent) event;
                intervalGuard.report(taskId, rde.getSeqno(), rde
                        .getExtractedTstamp().getTime());
            }
            else if (event instanceof ReplControlEvent)
            {
                ReplControlEvent rce = (ReplControlEvent) event;
                if (rce.getHeader() != null)
                {
                    intervalGuard.report(taskId, rce.getHeader().getSeqno(),
                            rce.getHeader().getExtractedTstamp().getTime());
                }
            }
        }

        // Finally, return the event we just found.
        return event;
    }

    /**
     * Returns but does not remove next event from the queue if it exists or
     * returns null if queue is empty.
     */
    public ReplEvent peek() throws InterruptedException
    {
        return eventQueue.peek();
    }
}