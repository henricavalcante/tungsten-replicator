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

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.conf.ReplicatorMonitor;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.management.MockEventDispatcher;
import com.continuent.tungsten.replicator.management.MockOpenReplicatorContext;

/**
 * This class implements a test of pipeline monitoring functions.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class PipelineProgressTest extends TestCase
{
    private PipelineHelper helper = new PipelineHelper();

    /**
     * Setup.
     * 
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
    }

    /**
     * Teardown.
     * 
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

    /**
     * Verify that pipelines with no events return default values.
     */
    public void testPipelineWithNoEvents() throws Exception
    {
        // Create pipeline.
        TungstenProperties config = helper.createSimpleRuntimeWithXacts(0);
        ReplicatorRuntime runtime = new ReplicatorRuntime(config,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        Pipeline pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Check a selection of default values.
        assertEquals("default latency", 0.0, pipeline.getApplyLatency());
        assertNull("default applied event", pipeline.getLastAppliedEvent());
        assertEquals("default applied seqno", -1,
                pipeline.getLastAppliedSeqno());
        assertEquals("default extracted seqno", -1,
                pipeline.getLastExtractedSeqno());

        // Shard list should be empty.
        List<ShardProgress> shards = pipeline.getShardProgress();
        assertEquals("empty shard list", 0, shards.size());

        // Pipeline should have one task.
        List<TaskProgress> tasks = pipeline.getTaskProgress();
        assertEquals("single task in list", 1, tasks.size());
        TaskProgress task = tasks.get(0);
        assertNull("default processed event", task.getLastProcessedEvent());
        assertNull("default committed event", task.getLastCommittedEvent());
        assertEquals("no events processed on task", 0, task.getEventCount());

        // Shut down.
        pipeline.shutdown(false);
        pipeline.release(runtime);
    }

    /**
     * Verify that the pipeline tracks processed sequence numbers.
     */
    public void testPipelineWithEvents() throws Exception
    {
        TungstenProperties config = helper.createRuntimeWithStore(1);
        ReplicatorRuntime runtime = new ReplicatorRuntime(config,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        Pipeline pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Wait for and verify events.
        Future<ReplDBMSHeader> wait = pipeline.watchForCommittedSequenceNumber(
                9, false);
        ReplDBMSHeader lastEvent = wait.get(10, TimeUnit.SECONDS);
        assertEquals("Expected 10 sequence numbers", 9, lastEvent.getSeqno());

        // Check a selection of default values.
        assertEquals("committed event seqno", 9, pipeline.getLastAppliedEvent()
                .getSeqno());
        assertEquals("applied seqno", 9, pipeline.getLastAppliedSeqno());
        assertEquals("extracted seqno", 9, pipeline.getLastExtractedSeqno());

        // Shard list should be empty.
        List<ShardProgress> shards = pipeline.getShardProgress();
        assertEquals("empty shard list", 1, shards.size());

        // Tasks should have a single task.
        List<TaskProgress> tasks = pipeline.getTaskProgress();
        assertEquals("two tasks in list", 2, tasks.size());
        TaskProgress task = tasks.get(1);
        assertEquals("applied event", lastEvent, task.getLastCommittedEvent());
        assertEquals("committed event seqno", lastEvent.getSeqno(), task
                .getLastCommittedEvent().getSeqno());
        assertEquals("events processed on task", 10, task.getEventCount());

        // Shut down.
        pipeline.shutdown(false);
        pipeline.release(runtime);
    }
}