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

import java.io.File;
import java.io.FileInputStream;
import java.sql.Timestamp;
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
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.management.MockEventDispatcher;
import com.continuent.tungsten.replicator.management.MockOpenReplicatorContext;
import com.continuent.tungsten.replicator.pipeline.Pipeline;
import com.continuent.tungsten.replicator.storage.CommitAction;
import com.continuent.tungsten.replicator.storage.InMemoryMultiQueue;
import com.continuent.tungsten.replicator.storage.InMemoryTransactionalQueue;
import com.continuent.tungsten.replicator.thl.log.LogConnection;
import com.continuent.tungsten.replicator.util.AtomicIntervalGuard;

/**
 * Implements a test of the max offline interval in parallel replication.
 * Interval management ensures that apply threads do not get too far apart to go
 * offline easily. These cases check to ensure that we handle the default cases
 * as well as corner cases where timestamps walk backward or there are large
 * numbers of fragments.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class THLParallelQueueIntervalTest
{
    private static Logger             logger = Logger.getLogger(THLParallelQueueIntervalTest.class);
    private static TungstenProperties testProperties;

    // Each test uses this pipeline and runtime.
    private Pipeline                  pipeline;
    private ReplicatorRuntime         runtime;

    // Test helper instance.
    private THLParallelQueueHelper    helper = new THLParallelQueueHelper();

    /** Define a commit action to introduce delay into commit operations. */
    class RandomCommitAction implements CommitAction
    {
        private final long maxWait;

        public RandomCommitAction(long maxWait)
        {
            this.maxWait = maxWait;
        }

        public void execute(int taskId) throws ReplicatorException
        {
            // Randomly wait.
            long waitMillis = (long) (maxWait * Math.random());
            try
            {
                // logger.info("Sleeping: " + waitMillis);
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
            pipeline.shutdown(true);
        }
        if (runtime != null)
        {
            logger.info("Releasing runtime...");
            runtime.release();
        }
    }

    /**
     * Verify that the parallel queue blocks if we put two transactions into a
     * single channel. This is necessary for later tests, where we need to load
     * very specific transactions into the parallel queue and check interval
     * behavior.
     */
    @Test
    public void testQueueBlockingAssumptions() throws Exception
    {
        logger.info("##### testQueueBlockingAssumptions #####");
        long baseTimeMillis = System.currentTimeMillis();

        // Set up and prepare pipeline with 1 channel going into a multi-queue.
        setupPipeline("testQueueBlockingAssumptions", 1, 1, true, 1, -1);

        // Fetch references to stores.
        THL thl = (THL) pipeline.getStore("thl");
        InMemoryMultiQueue mq = (InMemoryMultiQueue) pipeline
                .getStore("multi-queue");

        // Write a stream of transactions with longer timestamp gaps
        // than maxOfflineInterval.
        LogConnection conn = thl.connect(false);
        int offset = 0;
        produceEvent(conn, 0, "db0", new Timestamp(baseTimeMillis
                + (offset++ * 5000)));
        produceEvent(conn, 1, "db1", new Timestamp(baseTimeMillis
                + (offset++ * 5000)));

        // Confirm we are committed through transaction 0 but not through 1.
        confirmCommitted(0, 1000);
        confirmNotCommitted(1, 500);

        // Read from the multi-queue and confirm that the second transaction
        // commits into it.
        ReplDBMSEvent rde = mq.get(0);
        Assert.assertEquals("Checking first seqno", 0, rde.getSeqno());
        confirmCommitted(1, 1000);
    }

    /**
     * Verify that if two transactions are blocked on separate channels with an
     * interval that exceeds maxOfflineInterval that a transaction with a later
     * timestamp on a third channel will block until the interval becomes less
     * than maxOfflineInterval.
     */
    @Test
    public void testBlockOnLongInterval() throws Exception
    {
        logger.info("##### testBlockOnLongInterval #####");
        long baseTimeMillis = System.currentTimeMillis();

        // Set up and prepare pipeline with 3 channels going into a multi-queue.
        // The maxOfflineInterval is 4 seconds. This means that events
        // separated by 3 seconds block the queue if two of them get in
        // at the same time.
        setupPipeline("testBlockOnLongInterval", 3, 4, true, 1, -1);

        // Fetch references to stores.
        THL thl = (THL) pipeline.getStore("thl");
        THLParallelQueue tpq = (THLParallelQueue) pipeline
                .getStore("thl-queue");
        InMemoryMultiQueue mq = (InMemoryMultiQueue) pipeline
                .getStore("multi-queue");

        // Load first transactions to block in the multi-queue and confirm they
        // commit.
        LogConnection conn = thl.connect(false);
        int offset = 1;
        produceEvent(conn, 0, "db0", new Timestamp(baseTimeMillis));
        produceEvent(conn, 1, "db1", new Timestamp(baseTimeMillis));
        confirmCommitted(1, 1000);

        // Load second set of transactions to set up a long interval between
        // them.
        produceEvent(conn, 2, "db0", new Timestamp(baseTimeMillis));
        produceEvent(conn, 3, "db1", new Timestamp(baseTimeMillis
                + (offset++ * 3000)));

        // Double-check that transaction 2 is the oldest transaction and that
        // 3 is the newest transaction.
        confirmHiSeqno(tpq, 3, 2000);
        confirmLowSeqno(tpq, 2, 2000);

        // Load a third, later transaction on a different shard that should
        // go through the last channel. This should block.
        produceEvent(conn, 4, "db2", new Timestamp(baseTimeMillis
                + (offset++ * 3000)));
        ReplDBMSEvent rde2 = mq.get(2, 1000);
        assertNullTransaction(rde2, tpq);

        // Allow the oldest pending transaction (seqno=2) to commit into the
        // multi-queue. Confirm this allows the blocked db2 transaction to
        // commit.
        ReplDBMSEvent rde0 = mq.get(0, 1000);
        Assert.assertNotNull("Should get the 1st transaction from queue 0",
                rde0);
        Assert.assertEquals("Checking seqno of xact", 0, rde0.getSeqno());

        rde2 = mq.get(2, 1000);
        Assert.assertNotNull("db2 should commit after interval shortened", rde2);
        Assert.assertEquals("Checking seqno", 4, rde2.getSeqno());
    }

    /**
     * Verify that if a first transaction t1 is followed by a second transaction
     * t2 and the timestamp of t2 is before t1 that we do not block. This
     * handles cases where timestamps do not increase monotonically with
     * sequence numbers. (Sometimes the DBMS logging mechanism may cause
     * timestamps to appear to move backwards in later transactions.)
     */
    @Test
    public void testExtremelyNegativeInterval() throws Exception
    {
        logger.info("##### testExtremelyNegativeInterval #####");
        long baseTimeMillis = System.currentTimeMillis();

        // Set up and prepare pipeline with 3 channels going into a multi-queue.
        // The maxOfflineInterval is 10 seconds.
        setupPipeline("testExtremelyNegativeInterval", 3, 10, true, 1, -1);

        // Fetch references to stores.
        THL thl = (THL) pipeline.getStore("thl");
        THLParallelQueue tpq = (THLParallelQueue) pipeline
                .getStore("thl-queue");
        InMemoryMultiQueue mq = (InMemoryMultiQueue) pipeline
                .getStore("multi-queue");

        // Load first transactions to block in the multi-queue and confirm they
        // commit.
        LogConnection conn = thl.connect(false);
        produceEvent(conn, 0, "db0", new Timestamp(baseTimeMillis));
        produceEvent(conn, 1, "db1", new Timestamp(baseTimeMillis));
        confirmCommitted(1, 1000);

        // Load second set of 3 transactions where the middle transaction
        // has a timestamp 120 seconds before the first transaction.
        produceEvent(conn, 2, "db0", new Timestamp(baseTimeMillis));
        produceEvent(conn, 3, "db1", new Timestamp(baseTimeMillis
                - (120 * 1000)));
        confirmHiSeqno(tpq, 3, 2000);
        confirmLowSeqno(tpq, 2, 2000);
        produceEvent(conn, 4, "db2", new Timestamp(baseTimeMillis + 1));

        // Confirm that the shard 2 transaction goes through immediately.
        ReplDBMSEvent rde2 = mq.get(2, 1000);
        Assert.assertNotNull("db2 should commit immediately", rde2);
        Assert.assertEquals("Checking seqno", 4, rde2.getSeqno());

        // Read the shard 0 transaction.
        ReplDBMSEvent rde = mq.get(0);
        Assert.assertEquals("Checking first seqno", 0, rde.getSeqno());
        confirmCommitted(1, 1000);

        // Load up another transaction on shard 2. This should not commit
        // because the transaction on shard 1 has a very old timestamp.
        produceEvent(conn, 5, "db2", new Timestamp(baseTimeMillis + 2));
        rde2 = mq.get(2, 1000);
        assertNullTransaction(rde2, tpq);

        // Allow the transaction on shard 1 to commit into the multi-queue.
        // At that point all transactions should clear.
        ReplDBMSEvent rde1 = mq.get(1, 1000);
        Assert.assertNotNull("db1 should not be null", rde1);
        Assert.assertEquals("Checking seqno", 1, rde1.getSeqno());

        rde2 = mq.get(2, 10000);
        Assert.assertNotNull("db2 should commit after shard 1 commits", rde2);
        Assert.assertEquals("Checking seqno", 5, rde2.getSeqno());

        // Confirm the pipeline has committed through to seqno 5.
        confirmCommitted(5, 1000);
    }

    /**
     * Verify that the THLParallelQueue will unblock after a configurable period
     * of time defined by the maxDelayInterval parameter. NOTE: This test
     * depends on timings so if you are debugging it you will need to reset the
     * delay interval to keep the test from failing.
     */
    @Test
    public void testMaxOfflineIntervalUnBlocking() throws Exception
    {
        logger.info("##### testMaxOfflineIntervalUnBlocking #####");
        long baseTimeMillis = System.currentTimeMillis();

        // Set up and prepare pipeline with 3 channels going into a multi-queue.
        // The maxOfflineInterval is 10 seconds, and the delay interval to
        // unblock
        // is 2 seconds.
        setupPipeline("testMaxOfflineIntervalUnBlocking", 3, 10, true, 1, 2);

        // Fetch references to stores.
        THL thl = (THL) pipeline.getStore("thl");
        THLParallelQueue tpq = (THLParallelQueue) pipeline
                .getStore("thl-queue");
        InMemoryMultiQueue mq = (InMemoryMultiQueue) pipeline
                .getStore("multi-queue");

        // Load first transactions to block in the multi-queue and confirm they
        // commit.
        LogConnection conn = thl.connect(false);
        int offset = 1;
        produceEvent(conn, 0, "db0", new Timestamp(baseTimeMillis));
        produceEvent(conn, 1, "db1", new Timestamp(baseTimeMillis));
        confirmCommitted(1, 1000);

        // Load second set of transactions to set up a long interval between
        // them. This will prevent any further transactions from coming in.
        produceEvent(conn, 2, "db0", new Timestamp(baseTimeMillis));
        produceEvent(conn, 3, "db1", new Timestamp(baseTimeMillis
                + (offset++ * 6000)));

        // Double-check that transaction 2 is the oldest transaction and that
        // 3 is the newest transaction. The interval guard updates
        // asynchronously so a slow processor can cause scheduling problems.
        confirmHiSeqno(tpq, 3, 2000);
        confirmLowSeqno(tpq, 2, 2000);

        // Load a third, later transaction on a different shard that should
        // go through the last channel. This should block, because it is 12
        // seconds behind the last transaction.
        produceEvent(conn, 4, "db2", new Timestamp(baseTimeMillis
                + (offset++ * 6000)));

        // Try to get the transaction using a timeout that is shorter than
        // the delay timeout for the parallel queue. We use 1 second. This
        // should fail because the parallel queue is blocked by the previous
        // two transactions.
        // logger.info("Confirming that most recent transaction is blocked...");
        ReplDBMSEvent rde2 = mq.get(2, 1000);
        assertNullTransaction(rde2, tpq);

        // Try again with a longer timeout. This should work because the
        // parallel queue will allow the transaction to move forward despite
        // being blocked.
        rde2 = mq.get(2, 10000);
        Assert.assertNotNull("db2 should commit after delay timeout", rde2);
        Assert.assertEquals("Checking seqno", 4, rde2.getSeqno());
        logger.info("Queue unblocked transaction after a wait period...seqno="
                + rde2.getSeqno());
    }

    /**
     * Verify that the parallel queue never blocks if the difference between
     * least and greatest timestamps on each channel is less than
     * maxOfflineInterval.
     */
    @Test
    public void testMaxOfflineIntervalNoBlocks() throws Exception
    {
        logger.info("##### testMaxOfflineIntervalNoBlocks #####");
        long baseTimeMillis = System.currentTimeMillis();

        // Set up and prepare pipeline with a multi-queue size of 1 with three
        // channels. This allows us to control commits easily.
        setupPipeline("testMaxOfflineIntervalNoBlocks", 3, 10, true, 1, -1);

        // Fetch references to stores.
        THL thl = (THL) pipeline.getStore("thl");
        InMemoryMultiQueue mq = (InMemoryMultiQueue) pipeline
                .getStore("multi-queue");

        // Write transactions on all shards with 1 ms difference between
        // successive timestamps. Assert that all three have committed in
        // the pipeline and are distributed across the queues of the
        // multi-queue.
        LogConnection conn = thl.connect(false);
        long seqno = -1;
        produceEvent(conn, ++seqno, "db0", new Timestamp(baseTimeMillis++));
        produceEvent(conn, ++seqno, "db1", new Timestamp(baseTimeMillis++));
        produceEvent(conn, ++seqno, "db2", new Timestamp(baseTimeMillis++));
        confirmCommitted(seqno, 5000);

        for (int i = 0; i <= 2; i++)
        {
            Assert.assertEquals("Checking queue: " + i, 1, mq.size(i));
        }

        // Load up with a large number of transactions.
        logger.info("Loading transactions...");
        for (int i = 0; i < 1000; i++)
        {
            for (int j = 0; j < 3; j++)
            {
                String shardId = "db" + j;
                produceEvent(conn, ++seqno, shardId, new Timestamp(
                        baseTimeMillis++));
            }
        }
        logger.info("Final transaction seqno written: " + seqno);

        // Cycle through and confirm that reading in any order from multi-queue
        // always gets the next transaction on the shard and that reads do not
        // block. Note that we must read 1001 times, as we added 3 * 1
        // transaction at the beginning whilst setting up the input.
        long maxSeqno = -1;
        logger.info("Reading back events");
        for (int i = 0; i < 1001; i++)
        {
            int[] queues;
            int order = i % 6;
            if (order == 0)
                queues = new int[]{0, 1, 2};
            else if (order == 1)
                queues = new int[]{0, 2, 1};
            else if (order == 2)
                queues = new int[]{1, 0, 2};
            else if (order == 3)
                queues = new int[]{1, 2, 0};
            else if (order == 4)
                queues = new int[]{2, 0, 1};
            else
                queues = new int[]{2, 1, 0};

            for (int queue : queues)
            {
                // Get the event and ensure it looks ok, waiting a maximum of 3
                // seconds.
                ReplDBMSEvent rde = mq.get(queue, 3000);
                Assert.assertNotNull("Expect to read an event", rde);
                Assert.assertEquals("shard ID", "db" + (queue % 3),
                        rde.getShardId());
                Assert.assertTrue("Must be greater than expected seqno",
                        (i * 3) <= rde.getSeqno());

                // Record the max seqno reached.
                if (maxSeqno < rde.getSeqno())
                    maxSeqno = rde.getSeqno();
            }
        }
        logger.info("Read back to maximum seqno: " + maxSeqno);

        // Verify we read expected max seqno.
        Assert.assertEquals("max seqno", seqno, maxSeqno);
    }

    /**
     * Verify that we maintain the gaps between transactions when
     * maxOfflineInterval is smaller than the jump between successive sequence
     * numbers running in a single channel with a single shard.
     */
    @Test
    public void testIntervalLargeGap() throws Exception
    {
        // Create a generator for events on a single shard.
        EventProducer generator = new EventProducer(100, 1, 1, 10000, 0);
        execShardIntervalTest("testMaxOfflineLargeGapSingle", 1, generator, 2);
    }

    /**
     * Verify the same as the previous case but use many shards on multiple
     * channels with a large gap between successive transactions.
     */
    @Test
    public void testIntervalLargeGapMulti() throws Exception
    {
        // Create a generator for events on a single shard.
        EventProducer generator = new EventProducer(100, 1, 23, 20000, 0);
        execShardIntervalTest("testIntervalLargeGapMulti", 5, generator, 2);
    }

    /**
     * Verify that we maintain the gaps between transactions when timestamp
     * values go backwards instead of forwards. This should catch all kinds of
     * weirdness when we don't handle timestamps properly.
     */
    @Test
    public void testIntervalBackwardsTime() throws Exception
    {
        // Create a generator for events on a single shard.
        EventProducer generator = new EventProducer(100, 1, 1, -10000, 0);
        execShardIntervalTest("testIntervalBackwardsTime", 1, generator, 2);
    }

    /**
     * Same case as before but with many channels and many shards.
     */
    @Test
    public void testIntervalBackwardsTimeMulti() throws Exception
    {
        // Create a generator for events on a single shard.
        EventProducer generator = new EventProducer(100, 1, 23, -10000, 0);
        execShardIntervalTest("testIntervalBackwardsTimeMulti", 5, generator, 2);
    }

    /**
     * Verify that we maintain the gaps between transactions when timestamp
     * values walk mostly forward but also occasionally go backwards.
     */
    @Test
    public void testIntervalVariableTime() throws Exception
    {
        // Create a generator for events on a single shard.
        EventProducer generator = new EventProducer(5000, 1, 1, 2500, 10000);
        execShardIntervalTest("testIntervalVariableTime", 1, generator, 2);
    }

    /**
     * Generate and write to the log a new event.
     */
    public void produceEvent(LogConnection conn, long seqno, short fragno,
            boolean lastFrag, String shardId, Timestamp ts)
            throws ReplicatorException, InterruptedException
    {
        ReplDBMSEvent rde = helper.createEvent(seqno, fragno, lastFrag,
                shardId, ts);
        if (logger.isDebugEnabled())
        {
            logger.debug("Writing event: seqno=" + seqno + " timestamp="
                    + ts.toString());
        }
        THLEvent thlEvent = new THLEvent(rde.getSourceId(), rde);
        conn.store(thlEvent, false);
        conn.commit();
    }

    /**
     * Helper method to write unfragmented transactions to log.
     */
    public void produceEvent(LogConnection conn, long seqno, String shardId,
            Timestamp ts) throws ReplicatorException, InterruptedException
    {
        produceEvent(conn, seqno, (short) 0, true, shardId, ts);
    }

    /**
     * Runs a shard interval test using an event generator that writes events to
     * a fixed queue. The queue will fill up if there are too many events.
     * 
     * @throws Throwable
     */
    public void execShardIntervalTest(String name, int channelCount,
            EventProducer producer, int maxOfflineInterval) throws Exception
    {
        logger.info("##### " + name + " #####");
        logger.info("Producer properties: " + producer.toString());

        // Set up the pipeline.
        setupPipeline(name, channelCount, maxOfflineInterval, false, 10000, -1);

        // Fetch references to stores.
        THL thl = (THL) pipeline.getStore("thl");
        // THLParallelQueue tpq = (THLParallelQueue) pipeline
        // .getStore("thl-queue");
        InMemoryTransactionalQueue mq = (InMemoryTransactionalQueue) pipeline
                .getStore("multi-queue");

        // Add a commit action to randomize the time to commit. This
        // will stimulate race conditions if there is bad coordination
        // between serialized and non-serialized events.
        RandomCommitAction ca = new RandomCommitAction(50);
        mq.setCommitAction(ca);

        // Write events using separately spawned producer task.
        EventProducerTask producerTask = new EventProducerTask(producer, thl);
        Thread producerThread = new Thread(producerTask);
        producerThread.start();

        // Read events using a separately spawned consumer task.
        EventConsumerTask consumerTask = new EventConsumerTask(mq,
                producer.getExpectedEvents());
        Thread consumerThread = new Thread(consumerTask);
        consumerThread.start();

        // Wait for the producer task to finish and check the results.
        producerThread.join(120000);
        int eventsProduced = producerTask.getEvents();
        long maxSeqnoProduced = producerTask.getMaxSeqno();
        if (producerTask.getException() != null)
            throw producerTask.getException();
        else if (!producerTask.isDone())
        {
            throw new Exception("Producer task did not complete: maxSeqno="
                    + maxSeqnoProduced + " events=" + eventsProduced);
        }
        logger.info("Finished writing events to log: events=" + eventsProduced
                + " maxSeqno=" + maxSeqnoProduced);

        // Wait for the last event to commit.
        this.confirmCommitted(maxSeqnoProduced, 120000);

        // Wait for the consumer to finish up.
        consumerThread.join(60000);
        int eventsConsumed = consumerTask.getEvents();
        long maxSeqnoConsumed = consumerTask.getMaxSeqno();
        if (consumerTask.getException() != null)
            throw consumerTask.getException();
        else if (!consumerTask.isDone())
        {
            throw new Exception("Consumer task did not complete: maxSeqno="
                    + maxSeqnoProduced + " events=" + eventsProduced);
        }
        logger.info("Finished reading events from log: events="
                + eventsConsumed + " maxSeqno=" + maxSeqnoConsumed);

        // Ensure we received the max seqno and number of events we expected.
        Assert.assertEquals("Checking event count consumed", eventsProduced,
                eventsConsumed);
        Assert.assertEquals("Checking max seqno consumed", maxSeqnoProduced,
                maxSeqnoConsumed);
    }

    /**
     * Generates a parallel pipeline.
     * 
     * @param name Name of the log
     * @param channelCount number of channels
     * @param maxOfflineInterval Value of maxOfflineInterval property
     * @param multiQueue If true this is a multi-queue
     * @param mqSize Size of each queue structure
     * @param maxDelayInterval Value of maxDelayInterval or -1 to take the
     *            default
     */
    public void setupPipeline(String name, int channelCount,
            int maxOfflineInterval, boolean multiQueue, int mqSize,
            int maxDelayInterval) throws Exception
    {
        // Set up and prepare pipeline. We add an extra property to set the
        // maxOfflineInterval.
        TungstenProperties conf = helper.generateTHLParallelPipeline(name,
                channelCount, 10, mqSize, multiQueue);
        conf.setLong("replicator.store.thl-queue.maxOfflineInterval",
                maxOfflineInterval);
        if (maxDelayInterval != -1)
        {
            conf.setLong("replicator.store.thl-queue.maxDelayInterval",
                    maxDelayInterval);
        }
        runtime = new ReplicatorRuntime(conf, new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());
    }

    /**
     * Confirm the pipeline is committed through the given seqno.
     */
    private void confirmCommitted(long seqno, int waitMillis)
            throws InterruptedException, ExecutionException
    {
        Future<ReplDBMSHeader> committed = pipeline
                .watchForCommittedSequenceNumber(seqno, false);
        boolean reached = false;
        try
        {
            committed.get(waitMillis, TimeUnit.MILLISECONDS);
            reached = true;
            logger.info("Pipeline has committed base events through seqno="
                    + seqno);
        }
        catch (TimeoutException e)
        {
            logger.error("Timed out waiting for seqno: " + seqno);
        }
        catch (ExecutionException e)
        {
            // This should not occur.
            throw e;
        }
        Assert.assertTrue("Expected to commit through seqno=" + seqno, reached);
    }

    /**
     * Confirm the pipeline is *not* committed through the given seqno.
     */
    private void confirmNotCommitted(long seqno, int waitMillis)
            throws InterruptedException, ExecutionException
    {
        Future<ReplDBMSHeader> committed = pipeline
                .watchForCommittedSequenceNumber(seqno, false);
        boolean reached = false;
        try
        {
            committed.get(waitMillis, TimeUnit.MILLISECONDS);
            reached = true;
            logger.error("Pipeline has unexpectedly committed base events through seqno="
                    + seqno);
        }
        catch (TimeoutException e)
        {
            logger.info("As expected, timed out waiting for seqno: " + seqno);
        }
        catch (ExecutionException e)
        {
            // This should not occur.
            throw e;
        }
        Assert.assertFalse("Did not expect to commit through seqno=" + seqno,
                reached);
    }

    /**
     * Waits for a particular high seqno in the THL interval guard structure.
     */
    private void confirmHiSeqno(THLParallelQueue tpq, long seqno,
            long waitMillis) throws Exception
    {
        AtomicIntervalGuard<?> intervalGuard = tpq.getIntervalGuard();
        long currentSeqno;
        long startMillis = System.currentTimeMillis();
        while ((currentSeqno = intervalGuard.getHiSeqno()) != seqno)
        {
            if (System.currentTimeMillis() - startMillis > waitMillis)
                throw new Exception(
                        "Timed out waiting for interval guard to reach expected high seqno: expected seqno="
                                + seqno + " actual value=" + currentSeqno);
            Thread.sleep(100);
        }
        logger.info("Confirmed hi sequence number in interval guard structure: seqno="
                + seqno);
    }

    /**
     * Waits for a particular high seqno in the THL interval guard structure.
     */
    private void confirmLowSeqno(THLParallelQueue tpq, long seqno,
            long waitMillis) throws Exception
    {
        AtomicIntervalGuard<?> intervalGuard = tpq.getIntervalGuard();
        long currentSeqno;
        long startMillis = System.currentTimeMillis();
        while ((currentSeqno = intervalGuard.getLowSeqno()) != seqno)
        {
            if (System.currentTimeMillis() - startMillis > waitMillis)
                throw new Exception(
                        "Timed out waiting for interval guard to reach expected low seqno: expected seqno="
                                + seqno + " actual value=" + currentSeqno);
            Thread.sleep(100);
        }
        logger.info("Confirmed low sequence number in interval guard structure: seqno="
                + seqno);
    }

    /**
     * Ensures that an event is null and prints a meaningful transaction
     */
    private void assertNullTransaction(ReplDBMSEvent rde, THLParallelQueue tpq)
            throws Exception
    {
        if (rde != null)
        {
            logger.error("Transaction is not blocked! Parallel queue status: "
                    + tpq.status().toString());
            throw new Exception(
                    "Parallel queue unexpectedly released a transaction that should be blocked: seqno="
                            + rde.getSeqno()
                            + " timestamp="
                            + rde.getExtractedTstamp());
        }

    }
}