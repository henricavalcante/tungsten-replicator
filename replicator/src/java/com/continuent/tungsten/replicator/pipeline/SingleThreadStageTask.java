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
 * Contributor(s): Stephane Giron
 */

package com.continuent.tungsten.replicator.pipeline;

import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import com.continuent.tungsten.fsm.event.EventDispatcher;
import com.continuent.tungsten.replicator.ErrorNotification;
import com.continuent.tungsten.replicator.InSequenceNotification;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.applier.Applier;
import com.continuent.tungsten.replicator.applier.ApplierException;
import com.continuent.tungsten.replicator.conf.FailurePolicy;
import com.continuent.tungsten.replicator.consistency.ConsistencyException;
import com.continuent.tungsten.replicator.event.ReplControlEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSFilteredEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplEvent;
import com.continuent.tungsten.replicator.event.ReplOptionParams;
import com.continuent.tungsten.replicator.extractor.Extractor;
import com.continuent.tungsten.replicator.extractor.ExtractorException;
import com.continuent.tungsten.replicator.filter.Filter;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.plugin.ReplicatorPlugin;
import com.continuent.tungsten.replicator.plugin.ShutdownHook;

/**
 * Implements thread logic for single-threaded stage execution. If your name is
 * not one of the two people listed below you probably should not change this
 * code without deep reflection and a lot of regression tests. *Every* line in
 * the task run loop is here for a reason.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 */
public class SingleThreadStageTask implements Runnable
{
    private static Logger      logger            = Logger.getLogger(SingleThreadStageTask.class);
    private Stage              stage;
    private int                taskId;
    private Extractor          extractor;
    private List<Filter>       filters;
    private Applier            applier;
    private List<ShutdownHook> shutdownHooks     = new LinkedList<ShutdownHook>();
    private boolean            usingBlockCommit;
    private int                blockCommitRowsCount;
    private EventDispatcher    eventDispatcher;
    private Schedule           schedule;
    private String             name;

    private long               blockEventCount   = 0;
    private TaskProgress       taskProgress;
    private PluginContext      context;
    private long               lastCommitMillis;
    private long               blockCommitIntervalMillis;
    private boolean            strictBlockCommit = true;

    private volatile boolean   cancelled         = false;

    public SingleThreadStageTask(Stage stage, int taskId)
    {
        this.taskId = taskId;
        this.name = stage.getName() + "-" + taskId;
        this.stage = stage;
        this.blockCommitRowsCount = stage.getBlockCommitRowCount();
        if (stage.getBlockCommitInterval() == null)
            this.blockCommitIntervalMillis = 0;
        else
            this.blockCommitIntervalMillis = stage.getBlockCommitInterval()
                    .longValue();
        if (stage.getCommitPolicy() == BlockCommitPolicy.lax)
            this.strictBlockCommit = false;
        this.usingBlockCommit = (blockCommitRowsCount > 1);
        this.taskProgress = stage.getProgressTracker().getTaskProgress(taskId);
    }

    /** Returns the id of this task. */
    public int getTaskId()
    {
        return taskId;
    }

    /**
     * Sets the event dispatcher.
     */
    public void setEventDispatcher(EventDispatcher eventDispatcher)
    {
        this.eventDispatcher = eventDispatcher;
    }

    /** Sets the schedule instance used to control loop continuation. */
    public void setSchedule(Schedule schedule)
    {
        this.schedule = schedule;
    }

    public void setExtractor(Extractor extractor)
    {
        this.extractor = extractor;
        addShutdownHook(extractor);
    }

    public void setFilters(List<Filter> filters)
    {
        this.filters = filters;
        for (Filter f : filters)
            addShutdownHook(f);
    }

    public void setApplier(Applier applier)
    {
        this.applier = applier;
        addShutdownHook(applier);
    }

    public Extractor getExtractor()
    {
        return extractor;
    }

    public List<Filter> getFilters()
    {
        return filters;
    }

    public Applier getApplier()
    {
        return applier;
    }

    public String getName()
    {
        return name;
    }

    /**
     * Cancel a currently running task.
     */
    public void cancel()
    {
        cancelled = true;
    }

    /**
     * Perform thread processing logic.
     */
    public void run()
    {
        logInfo("Starting stage task thread", null);
        taskProgress.begin();
        context = stage.getPluginContext();

        try
        {
            runTask();
        }
        catch (Throwable t)
        {
            // This catch block should not be normally reachable except if
            // failure recovery generates an exception. We need to try to
            // log the exception for later fault diagnosis and report an error.
            String msg = "Stage task error recovery failed: stage="
                    + stage.getName() + " message=" + t.getMessage();
            logger.error(msg, t);
            dispatchErrorNotification(msg, null, t);
        }

        logInfo("Terminating processing for stage task thread", null);
        ReplDBMSHeader lastEvent = stage.getProgressTracker()
                .getDirtyLastProcessedEvent(taskId);
        if (lastEvent != null)
        {
            String msg = "Last successfully processed event prior to termination: seqno="
                    + lastEvent.getSeqno()
                    + " eventid="
                    + lastEvent.getEventId();
            logInfo(msg, null);
        }
        logInfo("Task event count: " + taskProgress.getEventCount(), null);
        schedule.taskEnd();
    }

    /**
     * Perform single-threaded stage processing.
     * 
     * @throws ReplicatorException
     */
    public void runTask()
    {
        ReplDBMSEvent currentEvent = null;
        ReplDBMSEvent firstFilteredEvent = null;
        ReplDBMSEvent lastFilteredEvent = null;
        long filteredEventCount = 0;

        ReplEvent genericEvent = null;
        ReplDBMSEvent event = null;

        String currentService = null;

        try
        {
            // If we are supposed to auto-synchronize, do it now.
            if (stage.isAutoSync())
            {
                // Indicate that we are ready to go.
                eventDispatcher.put(new InSequenceNotification());
            }
            boolean syncTHLWithExtractor = stage.getPipeline()
                    .syncTHLWithExtractor();

            // Initialize the clock for checking block commit interval.
            lastCommitMillis = System.currentTimeMillis();

            while (!cancelled)
            {
                // Check for cancellation and exit loop if it has occurred.
                if (schedule.isCancelled())
                {
                    logInfo("Task has been cancelled", null);
                    break;
                }

                // Fetch the next event.
                event = null;
                try
                {
                    taskProgress.beginExtractInterval();
                    genericEvent = extractor.extract();
                }
                catch (ExtractorException e)
                {
                    String message = "Event extraction failed";
                    if (context.getExtractorFailurePolicy() == FailurePolicy.STOP)
                    {
                        if (logger.isDebugEnabled())
                            logger.debug(message, e);
                        dispatchErrorNotification(message, null, e);
                        break;
                    }
                    else
                    {
                        logError(message, e);
                        continue;
                    }
                }
                finally
                {
                    taskProgress.endExtractInterval();
                }

                // Retry if no event returned; debug logging goes here.
                if (genericEvent == null)
                {
                    if (logger.isDebugEnabled())
                        logger.debug("No event extracted, retrying...");
                    continue;
                }

                // Issue #15. If we detect a change in the service name, we
                // should commit now to prevent merging of transactions from
                // different services in block commit. However, we need to
                // ignore this rule for filtered events, as they are gaps
                // rather than real transactions.
                if (usingBlockCommit && strictBlockCommit
                        && genericEvent instanceof ReplDBMSEvent
                        && !(genericEvent instanceof ReplDBMSFilteredEvent))
                {
                    ReplDBMSEvent re = (ReplDBMSEvent) genericEvent;
                    String newService = re.getDBMSEvent()
                            .getMetadataOptionValue(ReplOptionParams.SERVICE);
                    if (currentService == null)
                        currentService = newService;
                    else if (!currentService.equals(newService))
                    {
                        // We assume changes in service only happen on the first
                        // fragment. Warn if this assumption is violated.
                        if (re.getFragno() == 0)
                        {
                            if (logger.isDebugEnabled())
                            {
                                String msg = String
                                        .format("Committing due to service change: prev svc=%s seqno=%d new_svc=%s\n",
                                                currentService, re.getSeqno(),
                                                newService);
                                logger.debug(msg);
                            }
                            commit();
                        }
                        else
                        {
                            String msg = String
                                    .format("Service name change between fragments: prev svc=%s seqno=%d fragno=%d new_svc=%s\n",
                                            currentService, re.getSeqno(),
                                            re.getFragno(), newService);
                            logger.warn(msg);
                        }
                    }
                }

                // Submit the event to the schedule to see what we should do
                // with it.
                int disposition = schedule.advise(genericEvent);
                if (disposition == Schedule.PROCEED)
                {
                    // Go ahead and apply this event.
                }
                else if (disposition == Schedule.CONTINUE_NEXT)
                {
                    // Update processed event position but do not commit.
                    updatePosition(genericEvent, false);
                    continue;
                }
                else if (disposition == Schedule.CONTINUE_NEXT_COMMIT)
                {
                    // Update position and commit. We must currently tell
                    // the schedule explicitly about the commit so that
                    // progress tracking correctly marks it as committed.
                    updatePosition(genericEvent, true);
                    continue;
                }
                else if (disposition == Schedule.QUIT)
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Quitting task processing loop");
                    updatePosition(genericEvent, false);
                    break;
                }
                else
                {
                    // This is a serious bug.
                    throw new ReplicatorException(
                            "Unexpected schedule disposition on event: disposition="
                                    + disposition + " event="
                                    + genericEvent.toString());
                }

                // Convert to a proper log event and proceed.
                event = (ReplDBMSEvent) genericEvent;
                if (logger.isDebugEnabled())
                {
                    logger.debug("Extracted event: seqno=" + event.getSeqno()
                            + " fragno=" + event.getFragno());
                }
                currentEvent = event;

                // Run filters, unless the event we are looking at is already
                // filtered. Filtering twice does not really makes sense and
                // makes filters themselves harder to write.
                if (!(event instanceof ReplDBMSFilteredEvent))
                {
                    taskProgress.beginFilterInterval();

                    try
                    {
                        for (Filter f : filters)
                        {
                            if ((event = f.filter(event)) == null)
                            {
                                if (logger.isDebugEnabled())
                                {
                                    logger.debug("Event discarded by filter: name="
                                            + f.getClass().toString());
                                }
                                break;
                            }
                        }
                    }
                    finally
                    {
                        taskProgress.endFilterInterval();
                    }
                }

                // Event was filtered... Get next event.
                if (event == null)
                {
                    if (firstFilteredEvent == null)
                    {
                        firstFilteredEvent = currentEvent;
                        lastFilteredEvent = currentEvent;
                        filteredEventCount = 1;
                    }
                    else
                    {
                        lastFilteredEvent = currentEvent;
                        filteredEventCount++;
                    }
                    continue;
                }
                else
                {
                    // This event is not filtered. Check if there are pending
                    // filtered events that should be stored. Filtered
                    // events do not by themselves cause a commit but do
                    // increment the block commit count, which may trigger a
                    // commit.
                    if (firstFilteredEvent != null)
                    {
                        if (logger.isDebugEnabled())
                        {
                            logger.debug("Applying filtered event");
                        }
                        apply(new ReplDBMSFilteredEvent(firstFilteredEvent,
                                lastFilteredEvent), false, false,
                                syncTHLWithExtractor);
                        if (this.usingBlockCommit)
                        {
                            blockEventCount += filteredEventCount;
                        }
                        firstFilteredEvent = null;
                        lastFilteredEvent = null;
                        filteredEventCount = 0;
                    }
                }

                boolean doRollback = false;
                boolean unsafeForBlockCommit = event.getDBMSEvent()
                        .getMetadataOptionValue(
                                ReplOptionParams.UNSAFE_FOR_BLOCK_COMMIT) != null;
                boolean forceCommit = event.getDBMSEvent()
                        .getMetadataOptionValue(ReplOptionParams.FORCE_COMMIT) != null;

                // The following rules take effect if strict block commit is in
                // effect.
                if (strictBlockCommit)
                {
                    // Handle implicit commit, if next transaction is
                    // fragmented, if next transaction is a DDL or if next
                    // transaction is a rollback.
                    if (event.getFragno() == 0 && !event.getLastFrag())
                    {
                        // Starting a new fragmented transaction
                        commit();
                    }
                    else
                    {
                        boolean isRollback = event.getDBMSEvent()
                                .getMetadataOptionValue(
                                        ReplOptionParams.ROLLBACK) != null;
                        if (event.getFragno() == 0 && isRollback)
                        {
                            // This is a transaction that rollbacks at the end :
                            // commit previous work, but only if it is not a
                            // fragmented transaction, as if it is fragmented
                            // transaction, previous work was already committed
                            // and the whole current transaction should be
                            // rolled back.
                            commit();
                            doRollback = true;
                        }
                        else if (unsafeForBlockCommit)
                        {
                            // Commit previous work and force transaction to
                            // commit afterwards.
                            commit();
                        }
                    }
                }

                // Should commit when :
                // 1. block commit is not used AND this is the last
                // fragment of the transaction
                // 2. (When maximum number of events is reached
                // OR when queue is empty)
                // AND this is the last fragment of the transaction
                boolean doCommit = false;

                if (unsafeForBlockCommit && strictBlockCommit)
                {
                    doCommit = true;
                }
                else if (forceCommit)
                {
                    doCommit = true;
                }
                else if (usingBlockCommit)
                {
                    blockEventCount++;
                    if (event.getLastFrag())
                    {
                        if ((blockEventCount >= blockCommitRowsCount))
                        {
                            // Commit if we are at the end of the block.
                            doCommit = true;
                        }
                        else if (extractorQueueEmpty())
                        {
                            // Commit if there is no more work to be done.
                            doCommit = true;
                        }
                    }
                }
                else
                {
                    doCommit = event.getLastFrag();
                }

                // Apply the event with optional commit.
                if (logger.isDebugEnabled())
                {
                    logger.debug("Applying event: seqno=" + event.getSeqno()
                            + " fragno=" + event.getFragno() + " doCommit="
                            + doCommit);
                }
                // doCommit should be false if doRollback is true.
                apply(event, doCommit, doRollback, syncTHLWithExtractor);
            }

            // At the end of the loop, issue commit to ensure partial block
            // becomes persistent. This should *only* occur if we are at the
            // end of a transaction to prevent partial block commits. Otherwise
            // we must roll back.
            if (event != null && event.getLastFrag())
            {
                commit();
            }
            else
            {
                String message;
                if (event == null)
                {
                    message = "Performing rollback of possible partial transaction: seqno=(unavailable)";
                }
                else
                {
                    message = "Performing rollback of partial transaction: seqno="
                            + event.getSeqno()
                            + " fragno="
                            + event.getFragno()
                            + " last_frag=" + event.getLastFrag();
                }
                logger.info(message);
                applier.rollback();
            }
        }
        catch (InterruptedException e)
        {
            // Provide appropriate logging.
            if (!schedule.isCancelled())
                logger.warn("Received unexpected interrupt in stage task: "
                        + stage.getName());
            else if (logger.isDebugEnabled())
                logger.debug("Task loop interrupted", e);

            // Roll back to release locks and clear partial work.
            try
            {
                applier.rollback();
            }
            catch (InterruptedException e1)
            {
                logWarn("Task cancelled while trying to rollback following cancellation",
                        null);
            }
        }
        catch (ApplierException e)
        {
            // Something happened to our target. Construct an appropriate
            // message.
            String message;
            if (event == null)
            {
                message = "Event application failed: seqno=(unavailable) message="
                        + e.getMessage();
            }
            else
            {
                message = "Event application failed: seqno=" + event.getSeqno()
                        + " fragno=" + event.getFragno() + " message="
                        + e.getMessage();
            }

            // Now shut down cleanly.
            emergencyRollback(message, event, e);
        }
        catch (Throwable e)
        {
            // An unexpected error occurred.
            String message;
            if (event == null)
            {
                message = "Stage task failed: " + stage.getName();
            }
            else
            {
                message = "Stage task failed: stage=" + stage.getName()
                        + " seqno=" + +event.getSeqno() + " fragno="
                        + event.getFragno();
            }

            // Now shut down cleanly.
            emergencyRollback(message, event, e);
        }
    }

    /**
     * Determines whether the extractor queue is currently empty. If the queue
     * is empty we wait up until the block commit interval using a quick sleep
     * interval.
     * 
     * @throws InterruptedException
     */
    private boolean extractorQueueEmpty() throws InterruptedException
    {
        if (extractor.hasMoreEvents())
            return false;
        else if (blockCommitIntervalMillis <= 0)
            return true;
        else
        {
            // Compute the next time when we can commit based on commit
            // interval.
            long nextCommitMillis = lastCommitMillis
                    + blockCommitIntervalMillis;
            long sleepMillis = nextCommitMillis - System.currentTimeMillis();

            // If we are not past the commit time loop around short sleep
            // followed by checking the extractor. The loop describes the
            // sleep time so that this exits.
            while (sleepMillis > 0)
            {
                Thread.sleep(1);
                if (extractor.hasMoreEvents())
                    return false;
                sleepMillis = nextCommitMillis - System.currentTimeMillis();
            }

            // If we get here the queue is really empty and has been for a
            // while.
            return true;
        }
    }

    /**
     * Roll back following an unexpected failure. This takes care of error
     * logging, rollback, and dispatching error notification to shut down the
     * pipeline.
     * 
     * @param message Message to print in the log
     * @param event Current transaction fragment or null if not available
     * @param t The exception that prompted a rollback
     */
    private void emergencyRollback(String message, ReplDBMSEvent event,
            Throwable t)
    {
        // Print the error to get as much information into the log as possible.
        logError(message, t);

        // Now roll back suppressing exceptions. Interrupts can be ignored
        // as the thread will end shortly anyway. Other errors can be ignored
        // since rollback might not work due to the original error, e.g. a
        // server going away. In both cases we write messages to ensure there
        // is full diagnostic information for post-mortem analysis.
        logger.info("Performing emergency rollback of applied changes");
        try
        {
            applier.rollback();
        }
        catch (InterruptedException e1)
        {
            logWarn("Task cancelled while trying to rollback following cancellation",
                    null);
        }
        catch (Throwable t1)
        {
            logWarn("Emergency rollback failed", t1);
        }

        // Finally, dispatch a notification to stop the pipeline. We need to be
        // very sure preceding code suppresses exceptions so we can get to this
        // point.
        dispatchErrorNotification(message, event, t);
    }

    // Utility routine to update position. This routine knows about control
    // events and block commit.
    private void updatePosition(ReplEvent replEvent, boolean doCommit)
            throws ReplicatorException, InterruptedException
    {
        // Find an event we can use to update our position.
        ReplDBMSHeader header = null;
        if (replEvent instanceof ReplControlEvent)
        {
            ReplControlEvent controlEvent = (ReplControlEvent) replEvent;
            header = controlEvent.getHeader();
        }
        else if (replEvent instanceof ReplDBMSEvent)
        {
            header = (ReplDBMSEvent) replEvent;
        }

        if (header == null)
        {
            // Bail if the event we found is null.
            if (logger.isDebugEnabled())
                logger.debug("Unable to update position due to null event value");
            return;
        }
        else if (header.getSeqno() < 0)
        {
            // Or if the seqno is less than 0, which indicates an uninitialized
            // position.
            if (logger.isDebugEnabled())
                logger.debug("Skipping update as position is less than 0");
            return;
        }

        // Decide whether to commit. This recapitulates logic in the main loop.
        if (usingBlockCommit)
        {
            blockEventCount++;
            if (blockEventCount >= blockCommitRowsCount)
            {
                // Commit if we are at the end of the block.
                doCommit = true;
            }
            else if (extractorQueueEmpty())
            {
                // Commit if there is no more work to be done.
                doCommit = true;
            }
            else
            {
                // Don't commit unless client really wants it.
                doCommit |= false;
            }
        }
        else
        {
            doCommit = true;
        }

        // Finally, update!
        if (logger.isDebugEnabled())
        {
            logger.debug("Updating position: seqno=" + header.getSeqno()
                    + " doCommit=" + doCommit);
        }
        taskProgress.beginApplyInterval();
        applier.updatePosition(header, doCommit, false);
        taskProgress.endApplyInterval();
        if (doCommit)
        {
            schedule.commit();
            blockEventCount = 0;
            lastCommitMillis = System.currentTimeMillis();
        }
    }

    /**
     * Utility routine to wrap apply operation with standard exception handling
     * and event accounting.
     * 
     * @param event Event to be applied
     * @param doCommit Boolean flag indicating whether this is the last part of
     *            multipart event
     * @param doRollback Boolean flag indicating whether this transaction should
     *            rollback
     * @param syncTHL Should this applier synchronize the trep_commit_seqno
     *            table? This should be false for slave.
     * @throws ReplicatorException Thrown if applier processing fails
     * @throws ConsistencyException Thrown if the applier detects that a
     *             consistency check has failed
     * @throws InterruptedException Thrown if the applier is interrupted
     */
    private void apply(ReplDBMSEvent event, boolean doCommit,
            boolean doRollback, boolean syncTHL) throws ReplicatorException,
            ConsistencyException, InterruptedException
    {
        try
        {
            taskProgress.beginApplyInterval();
            applier.apply(event, doCommit, doRollback, syncTHL);
            if (doCommit)
            {
                schedule.commit();
                blockEventCount = 0;
                lastCommitMillis = System.currentTimeMillis();
            }
        }
        catch (ApplierException e)
        {
            if (context.getApplierFailurePolicy() == FailurePolicy.STOP)
            {
                throw e;
            }
            else
            {
                String message = "Event application failed: seqno="
                        + event.getSeqno() + " fragno=" + event.getFragno()
                        + " message=" + e.getMessage();
                logError(message, e);
            }
        }
        finally
        {
            taskProgress.endApplyInterval();
        }

    }

    /**
     * Utility routine to issue commit with appropriate transaction accounting.
     * 
     * @throws ReplicatorException Thrown if applier processing fails
     * @throws InterruptedException Thrown if the applier is interrupted
     */
    private void commit() throws InterruptedException, ReplicatorException
    {
        applier.commit();
        schedule.commit();
        blockEventCount = 0;
        lastCommitMillis = System.currentTimeMillis();
    }

    /**
     * Utility routine to generate an error notification while trapping
     * interrupts. This is a terminal call and the caller thread *MUST* exit
     * unconditionally after making it. Interrupt suppression allows the caller
     * to complete any logging that may be necessary to diagnose the failure or
     * provide user-visible information.
     * 
     * @param message Error message
     * @param event Event associated with the error or null if no such event
     *            exists
     * @param t Throwable that generated the error
     */
    private void dispatchErrorNotification(String message, ReplDBMSEvent event,
            Throwable t)
    {
        logInfo("Dispatching error event: " + message, null);
        try
        {
            if (event == null)
            {
                eventDispatcher.put(new ErrorNotification(message, t));
            }
            else
            {
                eventDispatcher.put(new ErrorNotification(message, event
                        .getSeqno(), event.getEventId(), t));
            }
        }
        catch (InterruptedException e)
        {
            logWarn("Task cancelled while posting error notification", null);
        }
    }

    // Utility routines to print log messages with stage names.
    private void logInfo(String message, Throwable e)
    {
        if (e == null)
            logger.info(message);
        else
            logger.info(message, e);
    }

    private void logWarn(String message, Throwable e)
    {
        if (e == null)
            logger.warn(message);
        else
            logger.warn(message, e);
    }

    private void logError(String message, Throwable e)
    {
        if (e == null)
            logger.error(message);
        else
            logger.error(message, e);
    }

    // Checks a plugin and if it implements ShutdownHook add to list of
    // shutdown hooks that must be invoked.
    private void addShutdownHook(ReplicatorPlugin plugin)
    {
        if (plugin instanceof ShutdownHook)
            this.shutdownHooks.add((ShutdownHook) plugin);
    }

    /**
     * Invoke shutdown hooks (if any) defined on components of this task.
     * 
     * @param context Plugin context
     * @throws InterruptedException
     */
    public void execShutdownHooks(PluginContext context)
            throws InterruptedException
    {
        for (ShutdownHook hook : shutdownHooks)
        {
            try
            {
                hook.shutdown(context);
            }
            catch (InterruptedException e)
            {
                throw e;
            }
            catch (ReplicatorException e)
            {
                logger.warn("Received exception on shutdown hook invocation", e);
            }
            catch (Exception e)
            {
                logger.error("Unexpected error on shutdown hook invocation", e);
            }
        }
    }

    public void reportInitialPosition(ReplDBMSHeader lastHeader)
            throws InterruptedException
    {
        stage.getProgressTracker().setInitialLastProcessedEvent(getTaskId(),
                lastHeader);
    }
}