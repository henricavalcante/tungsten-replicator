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
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.Interval;
import com.continuent.tungsten.fsm.event.EventDispatcher;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.applier.Applier;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.extractor.Extractor;
import com.continuent.tungsten.replicator.filter.Filter;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.plugin.PluginSpecification;
import com.continuent.tungsten.replicator.plugin.ReplicatorPlugin;

/**
 * Stores the implementation of a single replicator processing stage, which
 * consists of extract, filtering, and apply operations.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class Stage implements ReplicatorPlugin
{
    private static Logger             logger              = Logger.getLogger(Stage.class);

    // Stage elements.
    private String                    name;
    private PluginSpecification       extractorSpec;
    private List<PluginSpecification> filterSpecs;
    private PluginSpecification       applierSpec;
    private PluginContext             pluginContext;
    private int                       blockCommitRowCount = 1;
    private Interval                  blockCommitInterval = new Interval(0);
    private String                    blockCommitPolicy   = "strict";
    private BlockCommitPolicy         commitPolicy;
    private boolean                   autoSync            = false;

    // Read-only parameters.
    private StageProgressTracker      progressTracker;

    // Task processing variables.
    StageTaskGroup                    taskGroup;
    int                               taskCount           = 1;

    // Start-up parameters.
    String                            initialEventId;
    long                              applySkipCount      = 0;
    private SortedSet<Long>           seqnosToBeSkipped;

    private final Pipeline            pipeline;

    /**
     * Creates a new stage instance.
     * 
     * @param pipeline Pipeline to which this stage belongs.
     */
    public Stage(Pipeline pipeline)
    {
        this.pipeline = pipeline;
    }

    public String getName()
    {
        return name;
    }

    public int getTaskCount()
    {
        return taskCount;
    }

    public PluginSpecification getExtractorSpec()
    {
        return extractorSpec;
    }

    public List<PluginSpecification> getFilterSpecs()
    {
        return filterSpecs;
    }

    public PluginSpecification getApplierSpec()
    {
        return applierSpec;
    }

    public StageProgressTracker getProgressTracker()
    {
        return progressTracker;
    }

    public StageTaskGroup getTaskGroup()
    {
        return taskGroup;
    }

    public PluginContext getPluginContext()
    {
        return pluginContext;
    }

    /** Returns the maximum number of transactions to apply before committing. */
    public int getBlockCommitRowCount()
    {
        return blockCommitRowCount;
    }

    /**
     * Return the minimum time interval to wait before committing when using
     * block commit. If set to zero has no effect.
     */
    public Interval getBlockCommitInterval()
    {
        return blockCommitInterval;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public void setTaskCount(int taskCount)
    {
        this.taskCount = taskCount;
    }

    public void setExtractorSpec(PluginSpecification extractor)
    {
        this.extractorSpec = extractor;
    }

    public void setFilterSpecs(List<PluginSpecification> filters)
    {
        this.filterSpecs = filters;
    }

    public void setApplierSpec(PluginSpecification applier)
    {
        this.applierSpec = applier;
    }

    public void setBlockCommitRowCount(int blockCommitRowCount)
    {
        this.blockCommitRowCount = blockCommitRowCount;
    }

    public void setBlockCommitInterval(Interval blockCommitInterval)
    {
        this.blockCommitInterval = blockCommitInterval;
    }

    /** Returns block commit policy as a string. */
    public String getBlockCommitPolicy()
    {
        return blockCommitPolicy;
    }

    public void setBlockCommitPolicy(String blockCommitPolicy)
    {
        this.blockCommitPolicy = blockCommitPolicy;
    }

    /** Returns the block commit policy as an enum for typed operation. */
    public BlockCommitPolicy getCommitPolicy()
    {
        return this.commitPolicy;
    }

    public void setLoggingInterval(long loggingInterval)
    {
        this.progressTracker.setLoggingInterval(loggingInterval);
    }

    public String getInitialEventId()
    {
        return initialEventId;
    }

    public void setInitialEventId(String initialEventId)
    {
        this.initialEventId = initialEventId;
    }

    public long getApplySkipCount()
    {
        return applySkipCount;
    }

    public void setApplySkipCount(long applySkipCount)
    {
        this.applySkipCount = applySkipCount;
    }

    public boolean isAutoSync()
    {
        return autoSync;
    }

    public void setAutoSync(boolean autoSync)
    {
        this.autoSync = autoSync;
    }

    /**
     * Returns task progress instances ordered by task ID.
     */
    public synchronized List<TaskProgress> getTaskProgress()
    {
        return progressTracker.cloneTaskProgress();
    }

    /**
     * Returns shard progress instances ordered by shard ID.
     */
    public synchronized List<ShardProgress> getShardProgress()
    {
        return progressTracker.getShardProgress();
    }

    // Convenience methods for unit testing.
    public Applier getApplier0()
    {
        return taskGroup.getTask(0).getApplier();
    }

    public List<Filter> getFilters0()
    {
        return taskGroup.getTask(0).getFilters();
    }

    public Extractor getExtractor0()
    {
        return taskGroup.getTask(0).getExtractor();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public synchronized void configure(PluginContext context)
            throws ReplicatorException, InterruptedException
    {
        // Check the task count.
        this.pluginContext = context;
        if (taskCount < 1)
            throw new ReplicatorException(
                    "Stage task count may not be less than 1: stage=" + name
                            + " taskCount=" + taskCount);

        // Store the block commit policy.
        if ("lax".equals(blockCommitPolicy))
            commitPolicy = BlockCommitPolicy.lax;
        else if ("strict".equals(blockCommitPolicy))
            commitPolicy = BlockCommitPolicy.strict;
        else
        {
            throw new ReplicatorException("Unrecognized block commit policy;"
                    + " allowed values are lax or strict: stage=" + name
                    + " policy=" + blockCommitPolicy);
        }

        // Set up control structures for managing stage tasks.
        progressTracker = new StageProgressTracker(name, taskCount);
        taskGroup = new StageTaskGroup(this, taskCount, progressTracker);
        taskGroup.configure(context);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public synchronized void prepare(PluginContext context)
            throws ReplicatorException, InterruptedException
    {
        taskGroup.prepare(context);
    }

    /**
     * {@inheritDoc}
     * 
     * @throws ReplicatorException
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public synchronized void release(PluginContext context)
            throws ReplicatorException
    {
        // Need to ensure we are properly shut down.
        shutdown(true);

        taskGroup.release(context);
        progressTracker.release();
    }

    /**
     * Start task thread(s) that implement this stage.
     */
    public synchronized void start(EventDispatcher dispatcher)
            throws ReplicatorException
    {
        // Set values for sequence numbers to be skipped.
        progressTracker.setApplySkipCount(applySkipCount);
        progressTracker.setSeqnosToBeSkipped(seqnosToBeSkipped);

        // Start tasks.
        taskGroup.start(dispatcher);
    }

    /**
     * Shut down tasks threads that implement the stage.
     */
    public synchronized void shutdown(boolean immediate)
    {
        try
        {
            taskGroup.stop(immediate);
        }
        catch (InterruptedException e)
        {
            logger.warn("Stage shutdown was interrupted");
        }
    }

    /**
     * Returns true if the stage has stopped.
     */
    public synchronized boolean isShutdown()
    {
        return taskGroup.isShutdown();
    }

    /**
     * Sets a watch for a particular sequence number to be safely committed on
     * all channels.
     * 
     * @param seqno Sequence number to watch for
     * @param terminate If true, terminate task when watch is successful
     * @return Returns a watch on matching event
     * @throws InterruptedException
     */
    public Future<ReplDBMSHeader> watchForCommittedSequenceNumber(long seqno,
            boolean terminate) throws InterruptedException
    {
        Future<ReplDBMSHeader> watch = progressTracker
                .watchForCommittedSequenceNumber(seqno, terminate);
        notifyThreads();
        return watch;
    }

    /**
     * Sets a watch for a particular sequence number to be processed.
     * 
     * @param seqno Sequence number to watch for
     * @param terminate If true, terminate task when watch is successful
     * @return Returns a watch on matching event
     * @throws InterruptedException
     */
    public Future<ReplDBMSHeader> watchForProcessedSequenceNumber(long seqno,
            boolean terminate) throws InterruptedException
    {
        Future<ReplDBMSHeader> watch = progressTracker
                .watchForProcessedSequenceNumber(seqno, terminate);
        notifyThreads();
        return watch;
    }

    /**
     * Sets a watch for a particular event ID to be extracted.
     * 
     * @param eventId Native event ID to watch for
     * @return Returns a watch on matching event
     * @param terminate If true, terminate task when watch is successful
     * @throws InterruptedException
     */
    public Future<ReplDBMSHeader> watchForProcessedEventId(String eventId,
            boolean terminate) throws InterruptedException
    {
        Future<ReplDBMSHeader> watch = progressTracker
                .watchForProcessedEventId(eventId, terminate);
        notifyThreads();
        return watch;
    }

    /**
     * Sets a watch for a heartbeat event to be extracted.
     * 
     * @return Returns a watch on matching event
     * @param terminate If true, terminate task when watch is successful
     * @throws InterruptedException
     */
    public Future<ReplDBMSHeader> watchForProcessedHeartbeat(String name,
            boolean terminate) throws InterruptedException
    {
        Future<ReplDBMSHeader> watch = progressTracker
                .watchForProcessedHeartbeat(name, terminate);
        notifyThreads();
        return watch;
    }

    /**
     * Sets a watch for a source timestamp to be extracted.
     * 
     * @param timestamp Timestamp to watch for
     * @param terminate If true, terminate task when watch is successful
     * @return Returns a watch on matching event
     * @throws InterruptedException
     */
    public Future<ReplDBMSHeader> watchForProcessedTimestamp(
            Timestamp timestamp, boolean terminate) throws InterruptedException
    {
        Future<ReplDBMSHeader> watch = progressTracker
                .watchForProcessedTimestamp(timestamp, terminate);
        notifyThreads();
        return watch;
    }

    // Utility routines to notify task thread(s) if necessary after a wait.
    private void notifyThreads()
    {
        taskGroup.notifyTasks();
    }

    /** Call configure method on a plugin class. */
    protected void configurePlugin(ReplicatorPlugin plugin,
            PluginContext context) throws ReplicatorException
    {
        ReplicatorRuntime.configurePlugin(plugin, context);
    }

    /** Call prepare method on a plugin class. */
    protected void preparePlugin(ReplicatorPlugin plugin, PluginContext context)
            throws ReplicatorException
    {
        ReplicatorRuntime.preparePlugin(plugin, context);
    }

    /** Call release method on a plugin class, warning on errors. */
    protected void releasePlugin(ReplicatorPlugin plugin, PluginContext context)
    {
        ReplicatorRuntime.releasePlugin(plugin, context);
    }

    /**
     * Returns the pipeline value.
     * 
     * @return Returns the pipeline.
     */
    public Pipeline getPipeline()
    {
        return pipeline;
    }

    public void setApplySkipEvents(SortedSet<Long> seqnos)
    {
        this.seqnosToBeSkipped = seqnos;
    }
}