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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import com.continuent.tungsten.fsm.event.EventDispatcher;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.applier.Applier;
import com.continuent.tungsten.replicator.applier.ApplierWrapper;
import com.continuent.tungsten.replicator.applier.ParallelApplier;
import com.continuent.tungsten.replicator.applier.RawApplier;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.extractor.Extractor;
import com.continuent.tungsten.replicator.extractor.ExtractorWrapper;
import com.continuent.tungsten.replicator.extractor.ParallelExtractor;
import com.continuent.tungsten.replicator.extractor.RawExtractor;
import com.continuent.tungsten.replicator.filter.Filter;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.plugin.PluginSpecification;
import com.continuent.tungsten.replicator.plugin.ReplicatorPlugin;
import com.continuent.tungsten.replicator.storage.ParallelStore;

/**
 * This class encapsulates a group of tasks that run together in a single stage.
 * It handles life-cycle operations ranging from instantiation to release.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class StageTaskGroup implements ReplicatorPlugin
{
    private static Logger           logger    = Logger.getLogger(StageTaskGroup.class);
    private Stage                   stage;
    private int                     taskCount;
    private StageProgressTracker    tracker;
    private ParallelStore           parallelStore;

    private SingleThreadStageTask[] tasks;

    private boolean                 shutdown  = false;

    private HashMap<String, Thread> threadMap = new HashMap<String, Thread>();

    /**
     * Instantiated a task group including underlying tasks and their respective
     * extractors, filters, and appliers.
     * 
     * @param taskCount Number of tasks in the group
     */
    public StageTaskGroup(Stage stage, int taskCount,
            StageProgressTracker tracker)
    {
        this.taskCount = taskCount;
        this.stage = stage;
        this.tracker = tracker;
    }

    public int getTaskCount()
    {
        return taskCount;
    }

    public SingleThreadStageTask[] getTasks()
    {
        return tasks;
    }

    public SingleThreadStageTask getTask(int id)
    {
        return tasks[id];
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        // Instantiate and configure each task.
        logger.info("Instantiating and configuring tasks for stage: "
                + stage.getName());
        tasks = new SingleThreadStageTask[taskCount];
        for (int i = 0; i < taskCount; i++)
        {
            // Instantiate the task.
            tasks[i] = new SingleThreadStageTask(stage, i);

            // Instantiate and configure the extractor. Parallel extractors
            // get the task ID.
            ReplicatorPlugin extractor = this.stage.getExtractorSpec()
                    .instantiate(i);
            if (extractor instanceof RawExtractor)
                extractor = new ExtractorWrapper((RawExtractor) extractor);

            if (extractor instanceof ParallelExtractor)
                ((ParallelExtractor) extractor).setTaskId(i);

            extractor.configure(context);
            tasks[i].setExtractor((Extractor) extractor);

            // Instantiate and configure filters.
            List<Filter> filterList = new ArrayList<Filter>(stage
                    .getFilterSpecs().size());
            for (PluginSpecification filter : stage.getFilterSpecs())
            {
                Filter f = (Filter) filter.instantiate(i);
                f.configure(context);
                filterList.add(f);
            }
            tasks[i].setFilters(filterList);

            // Instantiate and configure the applier.
            ReplicatorPlugin applier = this.stage.getApplierSpec().instantiate(
                    i);
            if (applier instanceof RawApplier)
                applier = new ApplierWrapper((RawApplier) applier);

            if (applier instanceof ParallelApplier)
                ((ParallelApplier) applier).setTaskId(i);

            applier.configure(context);
            tasks[i].setApplier((Applier) applier);
        }

        // Figure out if we have a parallel store up-stream from this stage.
        // If so store the reference and also pass it to the progress tracker,
        // which needs to access the store to coordinate watches.
        if (getTask(0).getExtractor() instanceof ParallelExtractor)
        {
            ParallelExtractor parallelExtractor = (ParallelExtractor) getTask(0)
                    .getExtractor();
            String storeName = parallelExtractor.getStoreName();
            parallelStore = (ParallelStore) stage.getPipeline().getStore(
                    storeName);
            tracker.setUpstreamStore(parallelStore);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        // Prepare components within each task.
        logger.info("Preparing tasks for stage: " + stage.getName());
        for (int i = 0; i < taskCount; i++)
        {
            // Prepare components.
            logger.debug("Preparing task: " + i);
            SingleThreadStageTask task = tasks[i];

            ReplicatorRuntime.preparePlugin(task.getExtractor(), context);

            for (Filter f : task.getFilters())
            {
                ReplicatorRuntime.preparePlugin(f, context);
            }

            ReplicatorRuntime.preparePlugin(task.getApplier(), context);

            // Get the starting event data and position extractor.
            logger.debug("Looking up last applied event to position extractor");
            ReplDBMSHeader lastHeader = task.getApplier().getLastEvent();
            if (lastHeader == null || lastHeader.getSeqno() < 0)
            {
                logger.warn("[" + task.getName() + "] "
                        + "Last event data not available; "
                        + "Setting extractor to current position");
                task.getExtractor().setLastEvent(null);
            }
            else
            {
                task.reportInitialPosition(lastHeader);

                logger.info("[" + task.getName() + "] "
                        + "Setting extractor position: seqno="
                        + lastHeader.getSeqno() + " event="
                        + lastHeader.getEventId());
                task.getExtractor().setLastEvent(lastHeader);
            }
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context)
    {
        // Release components within each task.
        logger.info("Releasing tasks for stage: " + stage.getName());
        for (int i = 0; i < taskCount; i++)
        {
            logger.debug("Releasing task: " + i);
            ReplicatorRuntime.releasePlugin(tasks[i].getExtractor(), context);

            for (Filter f : tasks[i].getFilters())
            {
                ReplicatorRuntime.releasePlugin(f, context);
            }

            ReplicatorRuntime.releasePlugin(tasks[i].getApplier(), context);
        }
    }

    /**
     * Start all tasks in the group.
     * 
     * @throws ReplicatorException
     */
    public void start(EventDispatcher dispatcher) throws ReplicatorException
    {
        for (SingleThreadStageTask task : tasks)
        {
            if (threadMap.get(task.getName()) != null)
            {
                logger.warn("Task has already been started: " + task.getName());
                return;
            }

            // If we have an initial event ID, set that now to override
            // any default value.
            if (stage.getInitialEventId() != null)
                task.getExtractor().setLastEventId(stage.getInitialEventId());

            // Create and start the processing thread.
            try
            {
                task.setEventDispatcher(dispatcher);
                task.setSchedule(new SimpleSchedule(stage, task));
                Thread stageThread = new Thread(task);
                stageThread.setName(task.getName());
                threadMap.put(task.getName(), stageThread);
                stageThread.start();
            }
            catch (Throwable t)
            {
                String message = "Failed to start stage task";
                logger.error(message, t);
                throw new ReplicatorException(message, t);
            }
        }
    }

    /**
     * Stop all tasks in the group.
     * 
     * @param immediate If true, interrupt and exit immediately
     */
    public synchronized void stop(boolean immediate)
            throws InterruptedException
    {
        // Do not shut down twice.
        if (shutdown)
            return;

        // If we are doing an orderly shutdown of processing from a parallel
        // store, we need to insert a stop control event to ensure
        // all queues stop at the same time.
        if (!immediate && parallelStore != null)
            parallelStore.insertStopEvent();

        // Wait for stages to shut down.
        for (SingleThreadStageTask task : tasks)
        {
            Thread stageThread = threadMap.remove(task.getName());
            if (stageThread != null)
            {
                try
                {
                    // We have to interrupt for non-parallel store or if
                    // this an immediate shutdown.
                    if (immediate || parallelStore == null)
                    {
                        task.cancel();
                        stageThread.interrupt();
                        task.execShutdownHooks(stage.getPluginContext());
                        stageThread.join();
                    }
                    else
                        stageThread.join();
                }
                catch (InterruptedException e)
                {
                    logger.warn("Interrupted while waiting for stage thread to exit");
                }
            }
        }

        if (threadMap.size() == 0)
        {
            shutdown = true;
        }
    }

    /**
     * Reports that a task has shut down. If no threads remain, we report that
     * the stage has shut down.
     */
    public void reportTaskShutdown(Thread taskThread, SingleThreadStageTask task)
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("Recording task shutdown: thread=" + taskThread
                    + " task=" + task);
        }
        if (threadMap.remove(task.getName()) != null)
        {
            if (logger.isDebugEnabled())
                logger.debug("Task shutdown: " + task.getName());
        }
        if (threadMap.size() == 0)
        {
            this.shutdown = true;
            if (logger.isDebugEnabled())
                logger.debug("Task group has shut down following last task end");
        }
    }

    /**
     * Interrupts currently running tasks.
     */
    public void notifyTasks()
    {
        if (tracker.shouldInterruptTask())
        {
            for (Thread t : threadMap.values())
            {
                t.interrupt();
            }
        }
    }

    /**
     * Returns true if the stage has stopped.
     */
    public synchronized boolean isShutdown()
    {
        return shutdown;
    }
}