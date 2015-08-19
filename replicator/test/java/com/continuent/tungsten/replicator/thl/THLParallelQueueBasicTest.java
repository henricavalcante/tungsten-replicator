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
 * Initial developer(s): Teemu Ollakka, Robert Hodges
 * Contributor(s): 
 */

package com.continuent.tungsten.replicator.thl;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorMonitor;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSFilteredEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplOptionParams;
import com.continuent.tungsten.replicator.management.MockEventDispatcher;
import com.continuent.tungsten.replicator.management.MockOpenReplicatorContext;
import com.continuent.tungsten.replicator.pipeline.Pipeline;
import com.continuent.tungsten.replicator.pipeline.Stage;
import com.continuent.tungsten.replicator.storage.CommitAction;
import com.continuent.tungsten.replicator.storage.InMemoryMultiQueue;
import com.continuent.tungsten.replicator.storage.InMemoryTransactionalQueue;
import com.continuent.tungsten.replicator.storage.Store;
import com.continuent.tungsten.replicator.thl.log.LogConnection;

/**
 * Implements a basic test of parallel THL operations. Parallel THL operation
 * requires a pipeline consisting of a THL coupled with a THLParallelQueue. This
 * test focuses on basic operations.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class THLParallelQueueBasicTest
{
    private static Logger             logger = Logger.getLogger(THLParallelQueueBasicTest.class);
    private static TungstenProperties testProperties;

    // Each test uses this pipeline and runtime.
    private Pipeline                  pipeline;
    private ReplicatorRuntime         runtime;

    // Test helper instance.
    private THLParallelQueueHelper    helper = new THLParallelQueueHelper();

    /** Define a commit action to introduce delay into commit operations. */
    class RandomCommitAction implements CommitAction
    {
        long maxWait = -1;

        public void execute(int taskId) throws ReplicatorException
        {
            // Randomly wait up
            // to 99ms
            long waitMillis = (long) (maxWait * Math.random());
            try
            {
                Thread.sleep(waitMillis);
            }
            catch (InterruptedException e)
            {
                logger.info("Unexpected interruption on commit", e);
            }
        }
    };

    /**
     * Make sure we have expected test properties.
     * 
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        // Set test.properties file name.
        String testPropertiesName = System.getProperty("test.properties");
        if (testPropertiesName == null)
        {
            testPropertiesName = "test.properties";
            logger.info("Setting test.properties file name to default: test.properties");
        }

        // Load properties file.
        testProperties = new TungstenProperties();
        File f = new File(testPropertiesName);
        if (f.canRead())
        {
            logger.info("Reading test.properties file: " + testPropertiesName);
            FileInputStream fis = new FileInputStream(f);
            testProperties.load(fis);
            fis.close();
        }
    }

    /**
     * Shut down pipeline at end of test.
     */
    @After
    public void teardown()
    {
        if (pipeline != null)
        {
            logger.info("Shutting down pipeline...");
            pipeline.shutdown(false);
        }
        if (runtime != null)
        {
            logger.info("Releasing runtime...");
            runtime.release();
        }
    }

    /*
     * Verify that we can start and stop a pipeline containing a THL with a
     * THLParallelQueue.
     */
    @Test
    public void testPipelineStartStop() throws Exception
    {
        logger.info("##### testPipelineStartStop #####");

        // Set up and start pipelines.
        TungstenProperties conf = helper.generateTHLParallelQueueProps(
                "testPipelineStartStop", 1);
        runtime = new ReplicatorRuntime(conf, new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Close down pipeline.
        pipeline.shutdown(false);
        runtime.release();
    }

    /*
     * Verify that a pipeline with a single channel successfully transmits
     * events from end to end.
     */
    @Test
    public void testSingleChannel() throws Exception
    {
        logger.info("##### testSingleChannel #####");

        // Set up and start pipelines.
        TungstenProperties conf = helper.generateTHLParallelQueueProps(
                "testSingleChannel", 1);
        runtime = new ReplicatorRuntime(conf, new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Wait for and verify events.
        Store thl = pipeline.getStore("thl");
        verifyStoredEvents(pipeline, thl, 0, 9);
    }

    /*
     * Verify that a pipeline with multiple channels successfully transmits
     * events from end to end.
     */
    @Test
    public void testMultipleChannels() throws Exception
    {
        logger.info("##### testMultipleChannels #####");

        // Set up and start pipelines.
        TungstenProperties conf = helper.generateTHLParallelQueueProps(
                "testMultipleChannels", 1);
        runtime = new ReplicatorRuntime(conf, new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Wait for and verify events.
        Store thl = pipeline.getStore("thl");
        verifyStoredEvents(pipeline, thl, 0, 9);
    }

    /**
     * Verify that a parallel THL queue with more than one partition assigns
     * each event to the correct channel. This test uses 3 channels with
     * partitioning on shard name. We write and read directly to/from the linked
     * THL and THLParallelQueue to confirm behavior.
     */
    @Test
    public void testMultiChannelBasic() throws Exception
    {
        logger.info("##### testMultiChannelBasic #####");

        // Set up and prepare pipeline.
        TungstenProperties conf = helper.generateTHLParallelPipeline(
                "testMultiChannelBasic", 3, 50, 100, true);
        runtime = new ReplicatorRuntime(conf, new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Fetch references to stores.
        THL thl = (THL) pipeline.getStore("thl");
        InMemoryMultiQueue mq = (InMemoryMultiQueue) pipeline
                .getStore("multi-queue");

        // Write events to the THL with three different shard IDs.
        LogConnection conn = thl.connect(false);
        for (int i = 0; i < 90; i++)
        {
            ReplDBMSEvent rde = helper.createEvent(i, "db" + (i % 3));
            THLEvent thlEvent = new THLEvent(rde.getSourceId(), rde);
            conn.store(thlEvent, false);
        }
        conn.commit();
        thl.disconnect(conn);

        // Confirm that each parallel queue on the other side gets 30 events and
        // that said events are partially ordered within each queue.
        for (int q = 0; q < 3; q++)
        {
            long seqno = -1;
            String shardId = "db" + q;
            for (int i = 0; i < 30; i++)
            {
                ReplDBMSEvent rde2 = (ReplDBMSEvent) mq.get(q);
                Assert.assertTrue("Seqno increases due to partial order",
                        rde2.getSeqno() > seqno);
                Assert.assertEquals("Shard ID matches queue", shardId,
                        rde2.getShardId());
            }
        }
    }

    /**
     * Verify that a parallel THL with multiple channels properly do not commit
     * additional transactions after a failure. This includes not committing any
     * partial transactions if only a fragment has arrived before the error.
     */
    @Test
    public void testMultiChannelErrorHandling() throws Exception
    {
        logger.info("##### testMultiChannelErrorHandling #####");

        // Set up and prepare pipeline.
        TungstenProperties conf = helper.generateTHLParallelPipeline(
                "testMultiChannelErrorHandling", 3, 50, 100, false);
        runtime = new ReplicatorRuntime(conf, new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Fetch references to stores.
        THL thl = (THL) pipeline.getStore("thl");
        InMemoryTransactionalQueue mq = (InMemoryTransactionalQueue) pipeline
                .getStore("multi-queue");

        // Write events to the THL with three different shard IDs.
        LogConnection conn = thl.connect(false);
        for (int i = 0; i < 90; i++)
        {
            if (i < 50)
            {
                // Write simple events for first 50 transactions.
                ReplDBMSEvent rde = helper.createEvent(i, "db" + (i % 3));
                THLEvent thlEvent = new THLEvent(rde.getSourceId(), rde);
                conn.store(thlEvent, false);
            }
            else if (i == 50)
            {
                // On the 50th transaction write a fragmented transaction
                // that triggers a failure on the second fragment.
                ReplDBMSEvent rde1 = helper.createEvent(i, (short) 0, false,
                        "db" + (i % 3));
                THLEvent thlEvent1 = new THLEvent(rde1.getSourceId(), rde1);
                conn.store(thlEvent1, false);

                ReplDBMSEvent rde2 = helper.createEvent(i, (short) 1, true,
                        "db" + (i % 3));
                rde2.getDBMSEvent().setMetaDataOption("fail", "true");
                THLEvent thlEvent2 = new THLEvent(rde2.getSourceId(), rde2);
                conn.store(thlEvent2, false);
            }
            else if (i > 50)
            {
                // Fail on any transaction whose seqno is > 50.
                ReplDBMSEvent rde = helper.createEvent(i, "db" + (i % 3));
                rde.getDBMSEvent().setMetaDataOption("fail", "true");
                THLEvent thlEvent = new THLEvent(rde.getSourceId(), rde);
                conn.store(thlEvent, false);
            }
        }
        conn.commit();
        thl.disconnect(conn);

        // Confirm that apply tasks shut down within 10 seconds. Note that the
        // pipeline itself does not "fail" because we don't have a state machine
        // available to prompt a shutdown after errors arise.
        long startTime = System.currentTimeMillis();
        Stage qToMq = pipeline.getStage("q-to-mq");
        while (10000 > (System.currentTimeMillis() - startTime))
        {
            if (qToMq.isShutdown())
            {
                logger.info("Stage q-to-mq shut down in "
                        + (System.currentTimeMillis() - startTime)
                        + " milliseconds");
                break;
            }
            else
                Thread.sleep(1000);
        }
        Assert.assertTrue("Final stage should be shut down", qToMq.isShutdown());

        // Confirm that no event greater than 49 reaches the multi-queue
        // even if we wait for it.
        long maxSeqno = -1;
        ReplDBMSEvent maxEvent = null;
        while (mq.peek() != null)
        {
            ReplDBMSEvent rde2 = (ReplDBMSEvent) mq.get();
            if (rde2.getSeqno() > maxSeqno)
            {
                maxSeqno = rde2.getSeqno();
                maxEvent = rde2;
            }
        }
        logger.info("Maximum event logged: seqno=" + maxEvent.getSeqno()
                + " fragno=" + maxEvent.getFragno());
        Assert.assertTrue("Checking max seqno more than 40", maxSeqno >= 40);
        Assert.assertTrue("Checking max seqno less than 50", maxSeqno < 50);
    }

    /**
     * Verify that a parallel THL with multiple channels properly does not
     * commit partial transactions on shutdown.
     */
    @Test
    public void testMultiChannelPartialCommit() throws Exception
    {
        logger.info("##### testMultiChannelPartialCommit #####");

        // Set up and prepare pipeline.
        TungstenProperties conf = helper.generateTHLParallelPipeline(
                "testMultiChannelPartialCommit", 3, 50, 100, false);
        runtime = new ReplicatorRuntime(conf, new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Fetch references to stores.
        THL thl = (THL) pipeline.getStore("thl");
        InMemoryTransactionalQueue mq = (InMemoryTransactionalQueue) pipeline
                .getStore("multi-queue");

        // Write normal transactions on each shard and allow them to commit.
        LogConnection conn = thl.connect(false);
        for (int i = 0; i < 10; i++)
        {
            ReplDBMSEvent rde = helper.createEvent(i, "db" + (i % 3));
            THLEvent thlEvent = new THLEvent(rde.getSourceId(), rde);
            conn.store(thlEvent, false);
        }
        conn.commit();
        Future<ReplDBMSHeader> wait9 = pipeline
                .watchForCommittedSequenceNumber(9, false);
        wait9.get(5, TimeUnit.SECONDS);

        // Write partial transactions with three different shard IDs.
        for (int i = 10; i < 13; i++)
        {
            for (int j = 0; j < 4; j++)
            {
                ReplDBMSEvent rde = helper.createEvent(i, (short) j, false,
                        "db" + (i % 3));
                THLEvent thlEvent = new THLEvent(rde.getSourceId(), rde);
                conn.store(thlEvent, false);
            }
        }
        conn.commit();
        thl.disconnect(conn);

        // Wait for 2 seconds to let the transactions get into the apply task.
        Thread.sleep(2000);

        // Shut down the pipeline without waiting for current transaction to
        // complete.
        pipeline.shutdown(true);

        // Confirm that the maximum seqno that reaches the multi-queue is 9.
        // The later transactions are fragmented and must roll back on shutdown.
        long maxSeqno = -1;
        ReplDBMSEvent maxEvent = null;
        while (mq.peek() != null)
        {
            ReplDBMSEvent rde2 = (ReplDBMSEvent) mq.get();
            if (rde2.getSeqno() > maxSeqno)
            {
                maxSeqno = rde2.getSeqno();
                maxEvent = rde2;
            }
        }
        logger.info("Maximum event logged: seqno=" + maxEvent.getSeqno()
                + " fragno=" + maxEvent.getFragno());
        Assert.assertEquals("Checking max seqno is exactly 9", 9, maxSeqno);
    }

    /**
     * Verify that a parallel queue with a single partition does not fail or
     * stall when it receives events having commit times that are separated by
     * an amount greater than the maxOfflineInterval value.
     */
    @Test
    public void testSingleChannelPerniciousLag() throws Exception
    {
        logger.info("##### testSingleChannelPerniciousLag #####");

        // Set up and prepare pipeline. Add an extra property setting for
        // maxOfflineInterval.
        TungstenProperties conf = helper.generateTHLParallelPipeline(
                "testSingleChannelPerniciousLag", 1, 50, 100, true);
        conf.setLong("replicator.store.parallel-queue.maxOfflineInterval", 5);

        runtime = new ReplicatorRuntime(conf, new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Fetch references to stores.
        THL thl = (THL) pipeline.getStore("thl");
        InMemoryMultiQueue mq = (InMemoryMultiQueue) pipeline
                .getStore("multi-queue");

        // Write 10 events to the log with timestamps greater than the
        // maxOfflineInterval.
        long startTimestamp = System.currentTimeMillis() - 10000000;
        LogConnection conn = thl.connect(false);
        for (int i = 0; i < 10; i++)
        {
            Timestamp ts = new Timestamp(startTimestamp + (i * 100000));
            ReplDBMSEvent rde = helper.createEvent(i, (short) 0, true, "1", ts);
            THLEvent thlEvent = new THLEvent(rde.getSourceId(), rde);
            conn.store(thlEvent, false);
        }
        conn.commit();
        thl.disconnect(conn);

        // Ensure THL received expected events.
        Assert.assertEquals("Expected as first event: " + 0, 0,
                thl.getMinStoredSeqno());
        Assert.assertEquals("Expected as last event: " + 9, 9,
                thl.getMaxStoredSeqno());

        // Wait for and verify events.
        verifyStoredEvents(pipeline, mq, 0, 9);
    }

    /**
     * Verify that a parallel queue with a single partition does not fail or
     * stall when it receives events having commit times that are separated by
     * an amount greater than the maxOfflineInterval value. Those events should
     * include a mix of normal events, events with fragments, and skipped
     * events.
     */
    @Test
    public void testMultiChannelPerniciousLag() throws Exception
    {
        logger.info("##### testMultiChannelPerniciousLag #####");
        int events = 10;

        // Set up and prepare pipeline. Add an extra property setting for
        // maxOfflineInterval.
        TungstenProperties conf = helper.generateTHLParallelPipeline(
                "testMultiChannelPerniciousLag", 5, 50, 200, true);
        conf.setLong("replicator.store.parallel-queue.maxOfflineInterval", 5);

        runtime = new ReplicatorRuntime(conf, new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Fetch references to stores.
        THL thl = (THL) pipeline.getStore("thl");
        InMemoryMultiQueue mq = (InMemoryMultiQueue) pipeline
                .getStore("multi-queue");

        // Write 100 events to the log with timestamps greater than the
        // maxOfflineInterval.
        long startTimestamp = System.currentTimeMillis() - 100000000;
        LogConnection conn = thl.connect(false);
        long seqno = -1;
        for (int i = 0; i < events; i++)
        {
            // Generate the timestamp and shard ID for our event(s).
            seqno++;
            Timestamp ts = new Timestamp(startTimestamp + (i * 100000));
            String shard = "shard_" + (i % 5);

            // Decide what sort of event to create. Since there are three
            // channels we create a nice mix.
            switch (i % 3)
            {
                case 0 :
                {
                    // Unfragmented event.
                    ReplDBMSEvent rde = helper.createEvent(seqno, (short) 0,
                            true, shard, ts);
                    THLEvent thlEvent = new THLEvent(rde.getSourceId(), rde);
                    conn.store(thlEvent, false);
                    break;
                }
                case 1 :
                {
                    // Filtered event.
                    ReplDBMSEvent rde1 = helper.createEvent(seqno, (short) 0,
                            true, shard, ts);
                    ReplDBMSEvent rde2 = helper.createEvent(seqno + 4,
                            (short) 0, true, shard, ts);
                    ReplDBMSFilteredEvent fe = new ReplDBMSFilteredEvent(rde1,
                            rde2);
                    THLEvent thlEvent = new THLEvent(fe.getSourceId(), fe);
                    conn.store(thlEvent, false);
                    seqno += 4;
                    break;
                }
                case 2 :
                {
                    // Fragmented event.
                    for (short fragno = 0; fragno < 3; fragno++)
                    {
                        ReplDBMSEvent rde = helper.createEvent(seqno,
                                (short) fragno, (fragno >= 2), shard, ts);
                        THLEvent thlEvent = new THLEvent(rde.getSourceId(), rde);
                        conn.store(thlEvent, false);
                    }
                    break;
                }
                default :
                {
                    throw new Exception("Unexpected switch value!");
                }
            }
        }

        // Send along a heartbeat event to flush through transactions.
        seqno++;
        ReplDBMSEvent rde = helper.createEvent(seqno, (short) 0, true, "end",
                new Timestamp(startTimestamp));
        rde.getDBMSEvent().addMetadataOption(ReplOptionParams.HEARTBEAT,
                "heartbeat");
        THLEvent thlEvent = new THLEvent(rde.getSourceId(), rde);
        conn.store(thlEvent, false);

        // Commit and close the THL store.
        conn.commit();
        thl.disconnect(conn);

        // Ensure THL received expected events.
        Assert.assertEquals("Expected as first event: " + 0, 0,
                thl.getMinStoredSeqno());
        Assert.assertEquals("Expected as last event: " + seqno, seqno,
                thl.getMaxStoredSeqno());

        // Wait for and verify events.
        verifyStoredEvents(pipeline, mq, 0, seqno);
    }

    // Verify that events are committed to a particular store.
    private void verifyStoredEvents(Pipeline pipeline, Store store,
            long firstSeqno, long lastSeqno) throws InterruptedException,
            ExecutionException, TimeoutException
    {
        Future<ReplDBMSHeader> wait = pipeline.watchForCommittedSequenceNumber(
                lastSeqno, false);
        ReplDBMSHeader lastEvent = wait.get(5, TimeUnit.SECONDS);
        Assert.assertEquals("Expected " + (lastSeqno + 1) + " server events",
                lastSeqno, lastEvent.getSeqno());

        Assert.assertEquals("Expected as first event: " + firstSeqno,
                firstSeqno, store.getMinStoredSeqno());
        Assert.assertEquals("Expected as last event: " + lastSeqno, lastSeqno,
                store.getMaxStoredSeqno());
    }

    /**
     * Confirm that watch synchronization control events go to a single
     * partition and appear in total order compared to all other events. We
     * implement this test by inserting watch events on even sequence numbers
     * then picking them out from queues.
     */
    @Test
    public void testSinglePartitionWatch() throws Exception
    {
        logger.info("##### testSinglePartitionWatch #####");

        // Set up and prepare pipeline. We set the channel count to
        // 1 as we just want to confirm that watches are working.
        TungstenProperties conf = helper.generateTHLParallelPipeline(
                "testSinglePartitionWatch", 1, 50, 200, true);
        runtime = new ReplicatorRuntime(conf, new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Fetch references to stores.
        THL thl = (THL) pipeline.getStore("thl");
        InMemoryMultiQueue mq = (InMemoryMultiQueue) pipeline
                .getStore("multi-queue");

        // Define arrays of futures to hold watch results. We want to check
        // processing and commit watches, hence have two arrays.
        List<Future<ReplDBMSHeader>> processing = new ArrayList<Future<ReplDBMSHeader>>(
                10);
        List<Future<ReplDBMSHeader>> commits = new ArrayList<Future<ReplDBMSHeader>>(
                10);
        for (int i = 0; i < 10; i++)
        {
            // Seqno is every 10 positions.
            long seqno = i * 10;
            processing.add(pipeline.watchForProcessedSequenceNumber(seqno));
            commits.add(pipeline.watchForCommittedSequenceNumber(seqno, false));
        }

        // Write 100 events, which should trigger the watches. Commit
        // at intervals so things show up in batches.
        LogConnection conn = thl.connect(false);
        for (int i = 0; i < 100; i++)
        {
            // Insert and read back an event from the end of the pipeline.
            ReplDBMSEvent rde = helper.createEvent(i, "db0");
            THLEvent thlEvent = new THLEvent(rde.getSourceId(), rde);
            conn.store(thlEvent, false);

            // Commit every third event.
            if (i % 3 == 0)
                conn.commit();
        }
        conn.commit();
        thl.disconnect(conn);

        // Read 100 events. Ensure the events follow the expected seqno
        // sequence.
        for (int i = 0; i < 100; i++)
        {
            ReplDBMSEvent rde2 = mq.get(0);
            Assert.assertEquals("Checking sequence number", i, rde2.getSeqno());
        }

        // Now check that all watches fired. Processing watches should fire
        // on the exact sequence number. Commit watches may commit afterwards.
        logger.info("Checking watches to ensure they fired at expected points");
        for (int i = 0; i < 10; i++)
        {
            // Seqno is every 10 positions.
            long seqno = i * 10;
            ReplDBMSHeader processed = processing.get(i).get(10,
                    TimeUnit.SECONDS);
            logger.info("Processing watch [" + i + "]: watch seqno=" + seqno
                    + " trigger seqno=" + processed.getSeqno());
            Assert.assertEquals("Processing watches must match exact seqno",
                    seqno, processed.getSeqno());
            ReplDBMSHeader committed = commits.get(i).get(10, TimeUnit.SECONDS);
            logger.info("Committed watch [" + i + "]: watch seqno=" + seqno
                    + " trigger seqno=" + committed.getSeqno());
            Assert.assertTrue(
                    "Commit watches may fire on equal or greater seqno",
                    seqno >= committed.getSeqno());
        }
    }

    /**
     * Verify that a parallel THL queue with more than one partition allows
     * reads from one partition even when another partition is filled to
     * capacity. This proves that the parallel THL queue can handle a very large
     * gap between the positions of different partitions.
     */
    @Test
    public void testLaggingChannels() throws Exception
    {
        logger.info("##### testLaggingChannels #####");

        // Set up and prepare pipeline.
        TungstenProperties conf = helper.generateTHLParallelPipeline(
                "testLaggingChannels", 3, 50, 100, true);
        runtime = new ReplicatorRuntime(conf, new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Fetch references to stores.
        THL thl = (THL) pipeline.getStore("thl");
        InMemoryMultiQueue mq = (InMemoryMultiQueue) pipeline
                .getStore("multi-queue");

        // Write a large number of events on an initial shard ID. This should be
        // far greater than the maxSize parameter of the queue.
        LogConnection conn = thl.connect(false);
        logger.info("Writing db0 events");
        for (int i = 0; i < 100000; i++)
        {
            ReplDBMSEvent rde = helper.createEvent(i, "db0");
            THLEvent thlEvent = new THLEvent(rde.getSourceId(), rde);
            conn.store(thlEvent, false);
        }
        conn.commit();

        // Write 100 events on a second shard ID.
        logger.info("Writing db1 events");
        for (int i = 100000; i < 100100; i++)
        {
            ReplDBMSEvent rde = helper.createEvent(i, "db1");
            THLEvent thlEvent = new THLEvent(rde.getSourceId(), rde);
            conn.store(thlEvent, false);
        }
        conn.commit();
        thl.disconnect(conn);

        // Read the 100 events on queue 1 first and confirm seqno as well as
        // shard ID.
        logger.info("Reading db1 events");
        for (int i = 100000; i < 100100; i++)
        {
            ReplDBMSEvent rde2 = (ReplDBMSEvent) mq.get(1);
            Assert.assertEquals("Seqno matches expected for this queue", i,
                    rde2.getSeqno());
            Assert.assertEquals("Shard ID matches queue", "db1",
                    rde2.getShardId());
        }

        // Now read the remaining events on queue 0.
        logger.info("Reading db0 events");
        for (int i = 0; i < 100000; i++)
        {
            ReplDBMSEvent rde3 = (ReplDBMSEvent) mq.get(0);
            Assert.assertEquals("Seqno matches expected for this queue", i,
                    rde3.getSeqno());
            Assert.assertEquals("Shard ID matches queue", "db0",
                    rde3.getShardId());
            if (i % 10000 == 0)
                logger.info("Current seqno: " + rde3.getSeqno());
        }
    }

    /**
     * Verify that the parallel queue correctly transfers data in pipeline where
     * only a few of many channels are actually used.
     */
    @Test
    public void testMultiChannelLag() throws Exception
    {
        logger.info("##### testMultiChannelLag #####");

        // Set up and prepare pipeline.
        TungstenProperties conf = helper.generateTHLParallelPipeline(
                "testMultiChannelLag", 30, 50, 100, true);
        runtime = new ReplicatorRuntime(conf, new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Fetch references to stores.
        THL thl = (THL) pipeline.getStore("thl");
        InMemoryMultiQueue mq = (InMemoryMultiQueue) pipeline
                .getStore("multi-queue");

        // Write a large number of events into the THL using only 3 shards.
        String[] shardNames = {"db01", "db07", "db09"};
        LogConnection conn = thl.connect(false);
        long seqno = 0;
        for (int i = 0; i < 50000; i++)
        {
            for (int shard = 0; shard < 3; shard++)
            {
                ReplDBMSEvent rde = helper.createEvent(seqno++,
                        shardNames[shard]);
                THLEvent thlEvent = new THLEvent(rde.getSourceId(), rde);
                conn.store(thlEvent, false);
            }
        }
        conn.commit();
        thl.disconnect(conn);

        // Read across each queue until we reach 100K events for the main
        // shards (i.e., 150K total). Time out after 60 seconds to avoid hangs.
        int shardTotal = 0;
        while (shardTotal < 150000)
        {
            // Iterate across all queues.
            for (int q = 0; q < 30; q++)
            {
                // If the current queue has something in it...
                while (mq.peek(q) != null)
                {
                    // Read next event from this queue.
                    ReplDBMSEvent event = mq.get(q);
                    String shard = event.getShardId();

                    // If it's from a shard we are tracking, count it.
                    for (String shardName : shardNames)
                    {
                        if (shardName.equals(shard))
                            shardTotal++;

                        if (shardTotal % 30000 == 0)
                            logger.info("Tracked shard entries read:"
                                    + shardTotal);
                    }
                }
            }
        }
    }
}