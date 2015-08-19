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
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.applier.ApplierWrapper;
import com.continuent.tungsten.replicator.applier.DummyApplier;
import com.continuent.tungsten.replicator.applier.RawApplier;
import com.continuent.tungsten.replicator.conf.ReplicatorMonitor;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplOptionParams;
import com.continuent.tungsten.replicator.extractor.DummyExtractor;
import com.continuent.tungsten.replicator.extractor.ExtractorWrapper;
import com.continuent.tungsten.replicator.management.MockEventDispatcher;
import com.continuent.tungsten.replicator.management.MockOpenReplicatorContext;
import com.continuent.tungsten.replicator.service.PipelineService;
import com.continuent.tungsten.replicator.storage.InMemoryQueueStore;

/**
 * This class implements a test of the Pipeline class.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class PipelineTest extends TestCase
{
    private static Logger  logger = Logger.getLogger(PipelineTest.class);
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
     * Verify that we can build and immediately release a pipeline without
     * starting.
     */
    public void testSimplePipeline() throws Exception
    {
        TungstenProperties config = helper.createSimpleRuntime();
        ReplicatorRuntime runtime = new ReplicatorRuntime(config,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();

        Pipeline pipeline = runtime.getPipeline();
        pipeline.release(runtime);
    }

    /**
     * Verify that we can build, start, stop, and release a pipeline without
     * failures.
     */
    public void testStartStopPipeline() throws Exception
    {
        TungstenProperties config = helper.createSimpleRuntime();
        ReplicatorRuntime runtime = new ReplicatorRuntime(config,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        Pipeline pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());
        pipeline.shutdown(false);
        pipeline.release(runtime);
    }

    /**
     * Verify that pipelines load and configure declared service classes.
     */
    public void testServiceConfiguration() throws Exception
    {
        TungstenProperties config = helper.createSimpleRuntimeWith2Services();
        ReplicatorRuntime runtime = new ReplicatorRuntime(config,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();

        Pipeline pipeline = runtime.getPipeline();

        // Confirm we have two services and that they have been
        // configured.
        List<PipelineService> services = runtime.getServices();
        assertEquals("checking for service number", 2, services.size());

        for (PipelineService service : services)
        {
            String name = service.getName();
            SampleService sSvc = (SampleService) runtime.getService(name);
            assertEquals("listed vs. look-up", service, sSvc);
            assertTrue("configured", sSvc.configured);
            assertFalse("prepared", sSvc.prepared);
            assertFalse("released", sSvc.released);
        }

        // Prepare pipeline and ensure services are now prepared.
        pipeline.prepare(runtime);
        for (PipelineService service : services)
        {
            SampleService sSvc = (SampleService) service;
            assertTrue("prepared", sSvc.prepared);
        }

        // Release pipeline and confirm services are released.
        pipeline.release(runtime);
        for (PipelineService service : services)
        {
            SampleService sSvc = (SampleService) service;
            assertTrue("released", sSvc.released);
        }
    }

    /**
     * Verify that the pipeline tracks processed sequence numbers.
     */
    public void testSeqnoTracking() throws Exception
    {
        TungstenProperties config = helper.createSimpleRuntime();
        ReplicatorRuntime runtime = new ReplicatorRuntime(config,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        Pipeline pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());
        Thread.sleep(1000);
        long seqno = pipeline.getLastExtractedSeqno();
        assertEquals("Expect seqno to be 9 after 10 Xacts", 9, seqno);
        pipeline.shutdown(false);
        pipeline.release(runtime);
    }

    /**
     * Verify that we can skip transactions on start up using method
     * Stage.applySkipCount().
     */
    public void testSeqnoSkip() throws Exception
    {
        TungstenProperties config = helper.createSimpleRuntime();
        ReplicatorRuntime runtime = new ReplicatorRuntime(config,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        Pipeline pipeline = runtime.getPipeline();

        // Store events in the applier.
        DummyApplier dummy = (DummyApplier) ((ApplierWrapper) pipeline
                .getTailApplier()).getApplier();
        dummy.setStoreAppliedEvents(true);

        // Set the skip count.
        Stage stage = pipeline.getStages().get(0);
        stage.applySkipCount = 5;
        pipeline.start(new MockEventDispatcher());
        Thread.sleep(1000);

        // Ensure that we have gotten the right count of extract transactions.
        long seqno = pipeline.getLastExtractedSeqno();
        assertEquals("Expect seqno to be 9 after 10 Xacts", 9, seqno);

        // Ensure we have only a few applied transactions.
        long stmtCount = dummy.getTrx().size();
        assertEquals(
                "Expect statement count to be 10 after skipping 5 of 10 transactions",
                10, stmtCount);

        // Close up and go home.
        pipeline.shutdown(false);
        pipeline.release(runtime);
    }

    /**
     * Verify that we can set up the pipeline to go offline at a particular
     * sequence ID.
     */
    public void testRunToSeqno() throws Exception
    {
        TungstenProperties config = helper.createSimpleRuntime();
        ReplicatorRuntime runtime = new ReplicatorRuntime(config,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();

        // Shut down pipeline at seqno 5.
        Pipeline pipeline = runtime.getPipeline();
        Future<Pipeline> future = pipeline.shutdownAfterSequenceNumber(5);
        startAndAssertEventsApplied(pipeline, future);
        pipeline.release(runtime);
    }

    /**
     * Verify that we can set up the pipeline to go offline at a particular
     * native event ID. (Identical to previous case, but using the event ID.)
     */
    public void testRunToEventId() throws Exception
    {
        TungstenProperties config = helper.createSimpleRuntime();
        ReplicatorRuntime runtime = new ReplicatorRuntime(config,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();

        // Shut down pipeline at seqno 5.
        Pipeline pipeline = runtime.getPipeline();
        Future<Pipeline> future = pipeline.shutdownAfterEventId("5");
        startAndAssertEventsApplied(pipeline, future);
        pipeline.release(runtime);
    }

    // Start pipeline and assert number of events completed.
    private void startAndAssertEventsApplied(Pipeline pipeline,
            Future<Pipeline> wait) throws Exception
    {
        pipeline.start(new MockEventDispatcher());
        Thread.sleep(1000);

        // Ensure future completes.
        Pipeline p = wait.get(5, TimeUnit.SECONDS);
        assertEquals("Future should return pipeline", pipeline, p);

        // Ensure that we have gotten the right count of extract transactions.
        long seqno = pipeline.getLastExtractedSeqno();
        assertEquals("Expect seqno to be 5 after shutdown", 5, seqno);

        // Ensure we have only a few extracted transactions.
        RawApplier ra = ((ApplierWrapper) pipeline.getTailApplier())
                .getApplier();
        long eventCount = ((DummyApplier) ra).getEventCount();
        assertEquals("Expect count to be 6 after exiting @ seqno=5", 6,
                eventCount);

        // Close up and go home.
        pipeline.shutdown(false);
    }

    /**
     * Verify that we can set up the pipeline to go offline at a particular
     * sequence ID.
     */
    public void testPipelineShutdownAfterEvent() throws Exception
    {
        TungstenProperties config = helper.createSimpleRuntime();
        ReplicatorRuntime runtime = new ReplicatorRuntime(config,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();

        // Start pipeline and let it run.
        Pipeline pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());
        Thread.sleep(1000);

        // Shut down at an event we have already reached and assert that
        // it is shut down.
        Future<Pipeline> future = pipeline.shutdownAfterEventId("9");
        Pipeline p = future.get(60, TimeUnit.SECONDS);
        assertEquals("Future should return pipeline", pipeline, p);
        assertTrue("Pipeline should have shut down", pipeline.isShutdown());

        // Close up and go home.
        pipeline.shutdown(false);
        pipeline.release(runtime);
    }

    /**
     * Verify that we can shut down a running pipeline at a particular sequence
     * number. This test is approximate, since it depends on thread timing.
     * Outcomes are unambiguous.
     */
    public void testActivePipelineShutdownAtSeqno() throws Exception
    {
        TungstenProperties config = helper.createSimpleRuntime();
        ReplicatorRuntime runtime = new ReplicatorRuntime(config,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        Pipeline pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Set dummy extractor to do 5M transactions, but don't store anything.
        Stage stage0 = pipeline.getStages().get(0);
        stage0.setLoggingInterval(100000);
        ExtractorWrapper ew = (ExtractorWrapper) stage0.getExtractor0();
        DummyExtractor de = (DummyExtractor) ew.getExtractor();
        de.setNTrx(5000000);
        ApplierWrapper aw = (ApplierWrapper) stage0.getApplier0();
        DummyApplier da = (DummyApplier) aw.getApplier();
        da.setStoreAppliedEvents(false);

        // Tell the pipeline to stop after 1m events.
        Future<Pipeline> future = pipeline.shutdownAfterSequenceNumber(999999);
        Pipeline p = future.get(90, TimeUnit.SECONDS);
        assertEquals("Future should return pipeline", pipeline, p);

        // Ensure that we got not less than 1m events.
        long seqno = pipeline.getLastExtractedSeqno();
        assertTrue("Expect seqno to be >= 1m", seqno >= 999999);
        if (seqno == 999999)
            logger.info("Got exactly 1m events!");

        // Ensure we have at least 1m events stored.
        RawApplier ra = ((ApplierWrapper) pipeline.getTailApplier())
                .getApplier();
        long eventCount = ((DummyApplier) ra).getEventCount();
        assertTrue("Expect count >= 1000000", eventCount >= 1000000);

        // Shut it down.
        pipeline.shutdown(false);
        pipeline.release(runtime);
    }

    /**
     * Verify that waiting for an existing sequence number returns immediately
     * whereas waiting for a higher number times out.
     */
    public void testWaitSeqno() throws Exception
    {
        TungstenProperties config = helper.createSimpleRuntime();
        ReplicatorRuntime runtime = new ReplicatorRuntime(config,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        Pipeline pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Test for successfully applied and extracted sequence numbers.
        Future<ReplDBMSHeader> future = pipeline
                .watchForCommittedSequenceNumber(9, false);
        ReplDBMSHeader matchingEvent = future.get(3, TimeUnit.SECONDS);
        assertTrue("Applied sequence number matches",
                matchingEvent.getSeqno() >= 9);
        assertTrue("Applied seqnence number not higher",
                matchingEvent.getSeqno() < 10);

        future = pipeline.watchForExtractedSequenceNumber(9);
        matchingEvent = future.get(3, TimeUnit.SECONDS);
        assertTrue("Extracted sequence number matches",
                matchingEvent.getSeqno() >= 9);
        assertTrue("Extracted seqnence number not higher",
                matchingEvent.getSeqno() < 10);

        // Check for successfully applied and extracted event IDs.
        String eventId = matchingEvent.getEventId();
        future = pipeline.watchForExtractedEventId(eventId);
        matchingEvent = future.get(3, TimeUnit.SECONDS);
        assertTrue("Extracted event ID matches",
                eventId.equals(matchingEvent.getEventId()));

        future = pipeline.watchForProcessedEventId(eventId);
        matchingEvent = future.get(3, TimeUnit.SECONDS);
        assertTrue("Applied event ID matches",
                eventId.equals(matchingEvent.getEventId()));

        // Test for higher numbers, which should time out.
        future = pipeline.watchForExtractedSequenceNumber(99);
        try
        {
            matchingEvent = future.get(1, TimeUnit.SECONDS);
            throw new Exception(
                    "Wait for extracted event did not time out! seqno="
                            + matchingEvent.getSeqno());
        }
        catch (TimeoutException e)
        {
        }
        future = pipeline.watchForProcessedSequenceNumber(99);
        try
        {
            matchingEvent = future.get(1, TimeUnit.SECONDS);
            throw new Exception(
                    "Wait for applied event did not time out! seqno="
                            + matchingEvent.getSeqno());
        }
        catch (TimeoutException e)
        {
        }

        // Shut it down.
        pipeline.shutdown(false);
        pipeline.release(runtime);
    }

    /**
     * Verify that we can handle a two stage pipeline with an intervening store.
     */
    public void testTwoStages() throws Exception
    {
        // Create config with pipeline that has no fragmentation.
        TungstenProperties config = helper.createRuntimeWithStore(0);
        ReplicatorRuntime runtime = new ReplicatorRuntime(config,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        Pipeline pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Test for successfully applied and extracted sequence numbers.
        Future<ReplDBMSHeader> future = pipeline
                .watchForCommittedSequenceNumber(9, false);
        ReplDBMSHeader matchingEvent = future.get(2, TimeUnit.SECONDS);
        assertEquals("Applied sequence number matches", 9,
                matchingEvent.getSeqno());

        // Shut it down.
        pipeline.shutdown(false);
        pipeline.release(runtime);
    }

    /**
     * Verify that we can handle fragmented events where each event consists of
     * N fragments.
     */
    public void testFragmentHandling() throws Exception
    {
        // Create configuration; ask dummy extractor to generate 3 fragments
        // per transaction.
        TungstenProperties conf = helper.createRuntimeWithStore(3);
        ReplicatorRuntime runtime = new ReplicatorRuntime(conf,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());

        // Configure pipeline. Set dummy applier to store events so we an fetch
        // them later.
        runtime.configure();
        Pipeline pipeline = runtime.getPipeline();
        ApplierWrapper wrapper = (ApplierWrapper) pipeline.getStage("apply")
                .getApplier0();
        DummyApplier applier = (DummyApplier) wrapper.getApplier();
        applier.setStoreAppliedEvents(true);

        // Prepare and start the pipeline.
        runtime.prepare();
        pipeline.start(new MockEventDispatcher());

        // Wait for and verify events.
        Future<ReplDBMSHeader> wait = pipeline.watchForCommittedSequenceNumber(
                9, false);
        ReplDBMSHeader lastEvent = wait.get(10, TimeUnit.SECONDS);
        assertEquals("Expected 10 sequence numbers", 9, lastEvent.getSeqno());

        // Confirm we have 30x2 statements, i.e., two statements for each
        // sequence number.
        ArrayList<StatementData> sql = ((DummyApplier) applier).getTrx();
        assertEquals("Expected 30x2 statements", 60, sql.size());

        // Close down pipeline.
        pipeline.shutdown(false);
        runtime.release();
    }

    /**
     * Verify that we properly support differing levels of block commit. We put
     * a queue on both ends but have the input queue full at the start. We then
     * test different levels of group commit and confirm the number of blocks
     * and group commit size recorded.
     */
    public void testBlockCommit() throws Exception
    {
        // Define transaction count and different block sizes to use.
        int xacts = 40;
        int[] blockSizes = {1, 2, 4};

        // Try test for each block size.
        for (int blockSize : blockSizes)
        {
            logger.info("Testing block commit: transactions=" + xacts
                    + " blockSize=" + blockSize);

            // Create config with pipeline with input and output queues.
            TungstenProperties config = helper.createDoubleQueueRuntime(40,
                    blockSize, 0);
            ReplicatorRuntime runtime = new ReplicatorRuntime(config,
                    new MockOpenReplicatorContext(),
                    ReplicatorMonitor.getInstance());
            runtime.configure();
            runtime.prepare();
            Pipeline pipeline = runtime.getPipeline();

            // Load data into the queue and start the pipeline.
            InMemoryQueueStore input = (InMemoryQueueStore) pipeline
                    .getStore("q1");
            for (int i = 0; i < 40; i++)
            {
                ReplDBMSEvent event = helper.createEvent(i, "db0");
                input.put(event);
            }
            pipeline.start(new MockEventDispatcher());

            // Test for successfully applied and extracted sequence numbers.
            // We must look for committed seqnos as we'll be checking commit
            // stats from the pipeline. 
            Future<ReplDBMSHeader> future = pipeline
                    .watchForCommittedSequenceNumber(39, false);
            ReplDBMSHeader matchingEvent = future.get(2, TimeUnit.SECONDS);
            assertEquals("Applied sequence number matches", xacts - 1,
                    matchingEvent.getSeqno());

            // Check the number of commits and block size from the single task
            // in our single stage.
            Stage stage = pipeline.getStages().get(0);
            TaskProgress progress = stage.getProgressTracker().getTaskProgress(
                    0);
            assertEquals("Number of block commits", xacts / blockSize,
                    progress.getBlockCount());
            double blockDifference = Math.abs(progress.getAverageBlockSize()
                    - blockSize);
            assertTrue("Average block size", blockDifference < 0.1);

            // Shut it down.
            pipeline.shutdown(false);
            pipeline.release(runtime);
        }
    }

    /**
     * Verify that if an event has the force_commit flag in the header metadata
     * it will always commit the current block regardless of the block commit
     * policy.
     */
    public void testForcedBlockCommit() throws Exception
    {
        // Create events that are marked unsafe for block commit.
        ArrayList<ReplDBMSEvent> events = new ArrayList<ReplDBMSEvent>();
        for (int seqno = 0; seqno < 10; seqno++)
        {
            ReplDBMSEvent event = helper.createEvent(seqno, "db01");
            event.getDBMSEvent().setMetaDataOption(
                    ReplOptionParams.FORCE_COMMIT, "true");
            events.add(event);
        }

        // Confirm the transactions are processed in multiple blocks for
        // both policy types.
        checkBlockCommitSemantics(events, BlockCommitPolicy.lax, false, null);
        checkBlockCommitSemantics(events, BlockCommitPolicy.strict, false, null);
    }

    /**
     * Verify that filtering an event within a task does not cause an automatic
     * commit but does cause the block commit row count to be incremented for
     * the number of filtered events. This case works by setting up a stream of
     * events, some of which are tagged to force a commit. We then filter those
     * transactions out and expect everything to commit as a single block.
     */
    public void testNoCommitOnFilteredEvent() throws Exception
    {
        // Create some vanilla events but put tags to force commit
        // on events 5 to 7, which we will later filter.
        ArrayList<ReplDBMSEvent> events = new ArrayList<ReplDBMSEvent>();
        for (int seqno = 0; seqno < 10; seqno++)
        {
            ReplDBMSEvent event = helper.createEvent(seqno, "db01");
            if (seqno >= 5 && seqno <= 7)
            {
                event.getDBMSEvent().setMetaDataOption(
                        ReplOptionParams.FORCE_COMMIT, "true");
            }
            events.add(event);
        }

        // Add properties to configure a filter to drop events 5 to 7.
        TungstenProperties filterProps = new TungstenProperties();
        filterProps.setString("replicator.stage.stage.filters", "myfilter");
        filterProps.setString("replicator.filter.myfilter",
                SampleFilter.class.getName());
        filterProps.setString("replicator.filter.myfilter.skipSeqnoStart", "5");
        filterProps.setString("replicator.filter.myfilter.skipSeqnoRange", "3");

        // Confirm the transactions so filtered are in a single block. If we
        // fail to filter or commit improperly, there will be extra commits. If
        // the task loop counts are off for filtered events we'll get a timeout.
        List<ReplDBMSEvent> outputs = checkBlockCommitSemantics(events,
                BlockCommitPolicy.lax, true, filterProps);
        Assert.assertEquals("Events are filtered, lax policy", 8,
                outputs.size());

        checkBlockCommitSemantics(events, BlockCommitPolicy.strict, true,
                filterProps);
        Assert.assertEquals("Events are filtered, strict policy", 8,
                outputs.size());
    }

    /**
     * Verify that fragmented events cause blocks to commit if the commit policy
     * is strict and do not cause commits if the policy is lax.
     */
    public void testBlockCommitOnFragments() throws Exception
    {
        // Create a bunch of fragmented events.
        LinkedList<ReplDBMSEvent> events = new LinkedList<ReplDBMSEvent>();
        for (int seqno = 0; seqno < 10; seqno++)
        {
            for (short fragNo = 0; fragNo < 3; fragNo++)
            {
                ReplDBMSEvent event = helper.createEvent(seqno, "db01", fragNo,
                        (fragNo == 2));
                events.add(event);
            }
        }

        // Confirm the transactions are processed in a single block when
        // lax block commit is in effect.
        checkBlockCommitSemantics(events, BlockCommitPolicy.lax, true, null);

        // Confirm the transactions are processed in multiple blocks when
        // strict block commit is in effect. Note that since strict is
        // default if we don't supply it we also get the same result.
        checkBlockCommitSemantics(events, BlockCommitPolicy.strict, false, null);
        checkBlockCommitSemantics(events, null, false, null);
    }

    /**
     * Verify that events marked unsafe for block commit
     * (unsafe_for_block_commit tag in header) cause blocks to commit if the
     * commit policy is strict and do not cause commits if the policy is lax.
     */
    public void testBlockCommitOnUnsafeEvents() throws Exception
    {
        // Create events that are marked unsafe for block commit.
        LinkedList<ReplDBMSEvent> events = new LinkedList<ReplDBMSEvent>();
        for (int seqno = 0; seqno < 10; seqno++)
        {
            ReplDBMSEvent event = helper.createEvent(seqno, "db01");
            event.getDBMSEvent().setMetaDataOption(
                    ReplOptionParams.UNSAFE_FOR_BLOCK_COMMIT, "");
            events.add(event);
        }

        // Confirm the transactions are processed in a single block when
        // lax block commit is in effect.
        checkBlockCommitSemantics(events, BlockCommitPolicy.lax, true, null);

        // Confirm the transactions are processed in multiple blocks when
        // strict block commit is in effect.
        checkBlockCommitSemantics(events, BlockCommitPolicy.strict, false, null);
    }

    /**
     * Verify that events that change service names cause blocks to commit if
     * the commit policy is strict and do not cause commits if the policy is
     * lax.
     */
    public void testBlockCommitOnServiceChange() throws Exception
    {
        // Create events that change the service name with each new event.
        LinkedList<ReplDBMSEvent> events = new LinkedList<ReplDBMSEvent>();
        for (int seqno = 0; seqno < 10; seqno++)
        {
            ReplDBMSEvent event = helper.createEvent(seqno, "db01");
            event.getDBMSEvent().setMetaDataOption(ReplOptionParams.SERVICE,
                    "service_" + seqno);
            events.add(event);
        }

        // Confirm the transactions are processed in a single block when
        // lax block commit is in effect.
        checkBlockCommitSemantics(events, BlockCommitPolicy.lax, true, null);

        // Confirm the transactions are processed in multiple blocks when
        // strict block commit is in effect.
        checkBlockCommitSemantics(events, BlockCommitPolicy.strict, false, null);
    }

    /**
     * Confirm that a given queue of inputs either commits as a single block or
     * not. We do this by preloading the events to the pipeline then counting
     * the number of blocks that actually commit.
     * 
     * @param events Events to feed into the queue.
     * @param policy Block commit policy
     * @param singleBlock Whether we expect events under this policy to commit
     *            as a single block or not
     * @param extraProperties Additional properties that may be added
     * @return A list containing events from the output queue
     */
    private List<ReplDBMSEvent> checkBlockCommitSemantics(
            List<ReplDBMSEvent> events, BlockCommitPolicy policy,
            boolean singleBlock, TungstenProperties extraProperties)
            throws Exception
    {
        // Create config for pipeline with input and output queues
        // and add in properties from clients set set block commit
        // policy as well as add optional properties if provided.
        int queueSize = events.size();
        TungstenProperties config = helper.createDoubleQueueRuntime(queueSize,
                queueSize, 60000);
        if (policy != null)
        {
            config.setProperty("replicator.stage.stage.blockCommitPolicy",
                    policy.toString());
        }
        if (extraProperties != null)
        {
            for (String key : extraProperties.keyNames())
            {
                config.set(key, extraProperties.getObject(key));
            }
        }

        // Prepare and retrieve the said pipeline.
        ReplicatorRuntime runtime = new ReplicatorRuntime(config,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        Pipeline pipeline = runtime.getPipeline();

        // Load data into the pipeline.
        InMemoryQueueStore q1 = (InMemoryQueueStore) pipeline.getStore("q1");
        for (ReplDBMSEvent event : events)
        {
            q1.put(event);
        }

        // Find the last event and add a tag to force commit. This ensures a
        // commit on the final event even if the block is not full.
        ReplDBMSEvent lastEvent = events.get(queueSize - 1);
        if (lastEvent.getDBMSEvent().getMetadataOption(
                ReplOptionParams.FORCE_COMMIT) == null)
            lastEvent.getDBMSEvent().setMetaDataOption(
                    ReplOptionParams.FORCE_COMMIT, "true");
        long lastSeqno = lastEvent.getSeqno();
        logger.info("Added events; last seqno=" + lastSeqno);

        // Start the pipeline and wait for the last seqno to be commited.
        pipeline.start(new MockEventDispatcher());
        Future<ReplDBMSHeader> future = pipeline
                .watchForCommittedSequenceNumber(lastSeqno, false);
        ReplDBMSHeader matchingEvent = future.get(5, TimeUnit.SECONDS);
        assertEquals("Applied sequence number matches", lastSeqno,
                matchingEvent.getSeqno());

        // Check the number of blocks committed.
        Stage stage = pipeline.getStages().get(0);
        TaskProgress progress = stage.getProgressTracker().getTaskProgress(0);
        long actualBlocks = progress.getBlockCount();
        logger.info("Processed events, block count=" + actualBlocks);
        if (singleBlock)
        {
            Assert.assertEquals("Expect just one block", 1, actualBlocks);
        }
        else
        {
            Assert.assertTrue("Expect blocks to be more than 1: blocks="
                    + actualBlocks, actualBlocks > 1);
        }

        // Pull out the output values so we can return them to the caller.
        InMemoryQueueStore q2 = (InMemoryQueueStore) pipeline.getStore("q2");
        List<ReplDBMSEvent> outputs = new ArrayList<ReplDBMSEvent>(q2.size());
        while (q2.peek() != null)
        {
            outputs.add(q2.get());
        }

        // Shut it down.
        pipeline.shutdown(false);
        pipeline.release(runtime);

        // Return the output values.
        return outputs;
    }

    /**
     * Verify that if a block commit interval is defined transactions will not
     * commit at shorter intervals than specified. We can do this by setting
     * block commit to a high number (100) but set the interval to be 1 seconds.
     * We then insert X events per second over 10 seconds. If intervals are
     * working correctly there should be no more commits than the number of
     * elapsed seconds.
     * <p/>
     * Note: this case assumes that pipeline threads are scheduled fairly! If
     * you have a tiny VM it might not work as some threads might not get enough
     * CPU time, which would cause blocks not to commit in a timely fashion.
     */
    public void testBlockCommitIntervals() throws Exception
    {
        // Define transaction count and different block sizes to use.
        int xacts = 50;
        int millisPerXact = 100;
        int[] intervals = {200, 1000};

        // Try test for each block size.
        for (int i = 0; i < intervals.length; i++)
        {
            int interval = intervals[i];
            logger.info("Testing block commit: transactions=" + xacts
                    + " block interval=" + interval);

            // Create config with pipeline with input and output queues
            // and start resulting pipeline.
            TungstenProperties config = helper.createDoubleQueueRuntime(100,
                    100, interval);
            ReplicatorRuntime runtime = new ReplicatorRuntime(config,
                    new MockOpenReplicatorContext(),
                    ReplicatorMonitor.getInstance());
            runtime.configure();
            runtime.prepare();
            Pipeline pipeline = runtime.getPipeline();
            pipeline.start(new MockEventDispatcher());

            // Load data into the pipeline with sufficient spacing to
            // trigger block interval.
            InMemoryQueueStore input = (InMemoryQueueStore) pipeline
                    .getStore("q1");
            long seqno = -1;
            logger.info("Starting to add events; first seqno=0");
            for (int xactCount = 0; xactCount < xacts; xactCount++)
            {
                // Sleep between each event so that we get block commit
                // effects to kick in.
                seqno = xactCount;
                ReplDBMSEvent event = helper.createEvent(seqno, "db0");
                input.put(event);
                Thread.sleep(millisPerXact);
            }
            logger.info("Added events; last seqno=" + seqno);

            // Test for successfully applied and extracted sequence numbers.
            Future<ReplDBMSHeader> future = pipeline
                    .watchForCommittedSequenceNumber(seqno, false);
            ReplDBMSHeader matchingEvent = future.get(5, TimeUnit.SECONDS);
            assertEquals("Applied sequence number matches", xacts - 1,
                    matchingEvent.getSeqno());

            // Check the number of blocks committed. It must be greater than
            // or equal to the expected number, which we derive by dividing the
            // interval into the time over which transactions were added.
            long minimumBlocks = xacts * millisPerXact / interval;
            Stage stage = pipeline.getStages().get(0);
            TaskProgress progress = stage.getProgressTracker().getTaskProgress(
                    0);
            long actualBlocks = progress.getBlockCount();
            String message = String.format(
                    "actualBlocks (%d) >= minimumBlocks (%d)", actualBlocks,
                    minimumBlocks);
            logger.info("Checking commits: " + message);
            assertTrue(message, actualBlocks >= minimumBlocks);

            // Shut it down.
            pipeline.shutdown(false);
            pipeline.release(runtime);
        }
    }

    /**
     * Verify that we can handle 10M events without problems.
     */
    public void testManyEvents() throws Exception
    {
        int maxEvents = 5000000;
        TungstenProperties config = helper.createSimpleRuntime();
        ReplicatorRuntime runtime = new ReplicatorRuntime(config,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        Pipeline pipeline = runtime.getPipeline();

        // Set dummy extractor to do 1M transactions, but don't store anything.
        Stage stage0 = pipeline.getStages().get(0);
        stage0.setLoggingInterval(1000000);
        ExtractorWrapper ew = (ExtractorWrapper) stage0.getExtractor0();
        DummyExtractor de = (DummyExtractor) ew.getExtractor();
        de.setNTrx(maxEvents);
        ApplierWrapper aw = (ApplierWrapper) stage0.getApplier0();
        DummyApplier da = (DummyApplier) aw.getApplier();
        da.setStoreAppliedEvents(false);

        // Start the pipeline.
        pipeline.start(new MockEventDispatcher());

        // Test for successfully applied and extracted sequence numbers.
        // NOTE:  This call can use Pipeline.watchForProcessedSequenceNumber() 
        // as there is no race condition with the event actually getting into
        // the end of the pipeline. 
        Future<ReplDBMSHeader> future = pipeline
                .watchForProcessedSequenceNumber(maxEvents - 1);
        ReplDBMSHeader matchingEvent = future.get(600, TimeUnit.SECONDS);
        assertEquals("Applied sequence number matches", maxEvents - 1,
                matchingEvent.getSeqno());

        // Shut it down.
        pipeline.shutdown(false);
        pipeline.release(runtime);
    }
}