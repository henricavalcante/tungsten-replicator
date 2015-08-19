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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
import com.continuent.tungsten.replicator.storage.InMemoryTransactionalQueue;
import com.continuent.tungsten.replicator.thl.log.LogConnection;

/**
 * Implements a test of parallel THL operations, focusing on serializing, which
 * occurs when a parallel queue has an operation that cannot be run in parallel.
 * This requires specialized logic to wait for channels to commit, run the
 * serialized transaction(s), commit, and restart parallel apply. Most of the
 * hard parallel replication bugs have been in this part of the code, hence a
 * separate unit test.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class THLParallelQueueSerializationTest
{
    private static Logger             logger = Logger.getLogger(THLParallelQueueSerializationTest.class);
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

    /**
     * Verify that #UNKNOWN is serialized without blocking.
     */
    @Test
    public void testSerializeUnknown() throws Exception
    {
        String[] shards = {"#UNKNOWN"};
        execSerializationTest("testSerializeUnknown", 5, shards);
    }

    /**
     * Verify that empty string shard ID is serialized without blocking.
     */
    @Test
    public void testSerializeEmptyString() throws Exception
    {
        String[] shards = {""};
        execSerializationTest("testSerializeEmptyString", 5, shards);
    }

    /**
     * Verify that a null shardId is serialized.
     */
    @Test
    public void testSerializeNullShard() throws Exception
    {
        String[] shards = {null};
        execSerializationTest("testSerializeUnknown", 5, shards);
    }

    /**
     * Verify that #UNKNOWN at beginning and end is serialized without blocking.
     */
    @Test
    public void testSerialize2Unknowns() throws Exception
    {
        String[] shards = {"#UNKNOWN", "db0", "db2", "#UNKNOWN"};
        execSerializationTest("testSerialize2Unknowns", 5, shards);
    }

    /**
     * Verify that #UNKNOWN bracketed by outside transactions is correctly
     * serialized.
     */
    @Test
    public void testSerializeBracketedUnknown() throws Exception
    {
        String[] shards = {"db1", "#UNKNOWN", "db1"};
        execSerializationTest("testSerializeBracketedUnknown", 5, shards);
    }

    /**
     * Verify multiple #UNKNOWN transactions are correctly serialized.
     */
    @Test
    public void testSerializeMultipleUnknown() throws Exception
    {
        String[] shards = {"db1", "#UNKNOWN", "#UNKNOWN", "#UNKNOWN", "db1"};
        execSerializationTest("testSerializeMultipleUnknown", 5, shards);
    }

    /**
     * Verify that multiple channels correctly "stratify" serialized and
     * non-serialized transactions without blocking when there are long
     * sequences of parallel events followed by occasional #UNKNOWN sequences.
     * <p/>
     * This case uses a large number of channels with most of them idle. The
     * idea is to try to trigger a deadlock as the empty channels read far ahead
     * of the active channels.
     */
    @Test
    public void testSparseMultiChannelStratification() throws Exception
    {
        String[] shards = new String[500];
        int unknownInterval = 100;

        // Populate the shard sequence which #UNKNOWN every 100 events.
        for (int i = 0; i < shards.length; i++)
        {
            // Generate the event.
            if (i % unknownInterval == 0)
            {
                shards[i] = "#UNKNOWN";
            }
            else
            {
                int id = (i + 1) % 2;
                shards[i] = "db" + id;
            }
        }

        // Execute test with many more channels than shard IDs.
        execSerializationTest("testSparseMultiChannelStratification", 10,
                shards);
    }

    /**
     * Verify that multiple channels correctly "stratify" serialized and
     * non-serialized transactions into ordered groups. We do so by adding some
     * randomization to the commit times on a transactional in-memory queue,
     * which helps make serialization errors easier to see. This simulates a
     * DBMS with slow commits.
     */
    @Test
    public void testMultiChannelStratification() throws Exception
    {
        logger.info("##### testMultiChannelStratification #####");
        int maxEvents = 50;
        int channelCount = 5;

        // Set up and prepare pipeline. We set the channel count to
        // 1 as we just want to confirm that serialization counts are
        // increasing.
        TungstenProperties conf = helper.generateTHLParallelPipeline(
                "testMultiChannelSerialization", channelCount, 50, 1000, false);
        runtime = new ReplicatorRuntime(conf, new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Fetch references to stores.
        THL thl = (THL) pipeline.getStore("thl");
        THLParallelQueue tpq = (THLParallelQueue) pipeline
                .getStore("thl-queue");
        InMemoryTransactionalQueue mq = (InMemoryTransactionalQueue) pipeline
                .getStore("multi-queue");

        // Add a commit action to randomize the time to commit. This
        // will stimulate race conditions if there is bad coordination
        // between serialized and non-serialized events.
        RandomCommitAction ca = new RandomCommitAction(100);
        mq.setCommitAction(ca);

        // Write events where every channelCount events is #UNKNOWN, thereby
        // forcing serialization.
        int serialized = 0;
        LogConnection conn = thl.connect(false);
        for (int i = 0; i < maxEvents; i++)
        {
            // Generate the event.
            String shardId;
            int id = (i + 1) % channelCount;
            if (id == 0)
            {
                shardId = "#UNKNOWN";
                serialized++;
            }
            else
                shardId = "db" + id;

            // Write same to the log.
            ReplDBMSEvent rde = helper.createEvent(i, shardId);
            THLEvent thlEvent = new THLEvent(rde.getSourceId(), rde);
            conn.store(thlEvent, false);
            conn.commit();
        }
        thl.disconnect(conn);

        // Wait for the last event to commit and then ensure we
        // serialized the expected number of times.
        Future<ReplDBMSHeader> committed = pipeline
                .watchForCommittedSequenceNumber(maxEvents - 1, false);
        committed.get(30, TimeUnit.SECONDS);

        int actualSerialized = getSerializationCount(tpq);
        Assert.assertEquals("Checking expected serialization count",
                serialized, actualSerialized);

        // Read through the events in the serialized queue and ensure they
        // are properly stratified. Basically the serialized #UNKNOWN
        // shards must be in total order, between them the db0 and db1 shards
        // can come in either order.
        Map<String, Long> dbHash = new HashMap<String, Long>();
        long lastSerializedSeqno = -1;
        for (int i = 0; i < maxEvents; i++)
        {
            ReplDBMSEvent rde2 = mq.get();
            long seqno = rde2.getSeqno();
            String shardId = rde2.getShardId();
            logger.info("Read event: seqno=" + seqno + " shardId=" + shardId);
            if (shardId.equals("#UNKNOWN"))
            {
                // If we are on a serialized shard there must be a transaction
                // for every other channel in the hash map unless we are on the
                // first iteration.
                if (i > 0)
                {
                    Assert.assertEquals(
                            "Checking preceding events for serialized seqno="
                                    + seqno, channelCount - 1, dbHash.size());
                }

                // Prepare for the next round of unordered updates on shards
                lastSerializedSeqno = seqno;
                dbHash.clear();
            }
            else
            {
                // Must be an unserialized shard. Ensure it is within
                // channelCount -1 positions of the last serialized seqno.
                if (seqno <= lastSerializedSeqno
                        || seqno > lastSerializedSeqno + channelCount - 1)
                {
                    throw new Exception(
                            "Serialization violation; non-serialized event "
                                    + "not within range of serialized event: lastSerializedSeqno="
                                    + lastSerializedSeqno
                                    + " non-serialized event seqno=" + seqno);
                }
                else
                {
                    dbHash.put(rde2.getShardId(), seqno);
                }
            }
        }
    }

    /**
     * Verify that on-disk queues increment the serialization count each time a
     * serialized event is processed.
     */
    @Test
    public void testSimpleSerializationCount() throws Exception
    {
        logger.info("##### testSimpleSerializationCount #####");

        // Set up and prepare pipeline. We set the channel count to
        // 1 as we just want to confirm that serialization counts are
        // increasing.
        TungstenProperties conf = helper.generateTHLParallelPipeline(
                "testSerialization", 1, 50, 100, false);
        runtime = new ReplicatorRuntime(conf, new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Fetch references to stores.
        THL thl = (THL) pipeline.getStore("thl");
        THLParallelQueue tpq = (THLParallelQueue) pipeline
                .getStore("thl-queue");
        InMemoryTransactionalQueue mq = (InMemoryTransactionalQueue) pipeline
                .getStore("multi-queue");

        // Write and read back 33 events where every third event is #UNKNOWN,
        // hence should be serialized by the HashSerializer class.
        int serialized = 0;
        LogConnection conn = thl.connect(false);
        for (int i = 0; i < 33; i++)
        {
            // Get the serialization count from the store.
            int serializationCount = getSerializationCount(tpq);

            // Insert and read back an event from the end of the pipeline.
            String shardId = (i % 3 == 0 ? "#UNKNOWN" : "db0");
            ReplDBMSEvent rde = helper.createEvent(i, shardId);
            THLEvent thlEvent = new THLEvent(rde.getSourceId(), rde);
            conn.store(thlEvent, false);
            conn.commit();
            ReplDBMSEvent rde2 = mq.get();
            logger.info("Read event: seqno=" + rde2.getSeqno() + " shardId="
                    + rde2.getShardId());

            // Ensure that we got the event back and that the serialization
            // count incremented by one *only* for #UNKNOWN events.
            Assert.assertEquals("Read back same event", rde.getSeqno(),
                    rde2.getSeqno());
            int serializationCount2 = getSerializationCount(tpq);
            if ("#UNKNOWN".equals(rde.getShardId()))
            {
                serialized++;
                Assert.assertEquals("Expect serialization to increment",
                        serializationCount + 1, serializationCount2);
            }
            else
            {
                Assert.assertEquals("Expect serialization to remain the same",
                        serializationCount, serializationCount2);
            }
        }
        thl.disconnect(conn);

        // Ensure we serialized 11 (= 33 / 3) events in total.
        Assert.assertEquals("Serialization total", 11, serialized);
    }

    /**
     * Verify that we can schedule any number of waits prior to starting
     * parallel processing, then correctly wait for each one even when we
     * serialize at regular intervals.
     */
    @Test
    public void testSerializationAndWaits() throws Exception
    {
        logger.info("##### testSerializationAndWaits #####");

        // Set up and prepare pipeline. We set the channel count to
        // 5 so we can see some serialization.
        TungstenProperties conf = helper.generateTHLParallelPipeline(
                "testSerializationAndWaits", 5, 50, 100, false);
        runtime = new ReplicatorRuntime(conf, new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Fetch references to stores.
        THL thl = (THL) pipeline.getStore("thl");
        THLParallelQueue tpq = (THLParallelQueue) pipeline
                .getStore("thl-queue");

        // Schedule a set of waits for future seqno positions.
        Future<?>[] waits = new Future<?>[10];
        for (int i = 0; i < waits.length; i++)
        {
            waits[i] = pipeline.watchForCommittedSequenceNumber(i * 10, false);
        }

        // Write and read back 100 events where every fourth event is #UNKNOWN,
        // hence should be serialized by the HashSerializer class.
        LogConnection conn = thl.connect(false);
        int serialized = 0;
        for (int i = 0; i < 100; i++)
        {
            // Insert and read back an event from the end of the pipeline.
            String shardId = (i % 4 == 0 ? "#UNKNOWN" : "db0");
            if ("#UNKNOWN".equals(shardId))
                serialized++;
            ReplDBMSEvent rde = helper.createEvent(i, shardId);
            THLEvent thlEvent = new THLEvent(rde.getSourceId(), rde);
            conn.store(thlEvent, false);
            conn.commit();
        }
        thl.disconnect(conn);

        // Wait for each of the sequence numbers in turn.
        for (int i = 0; i < waits.length; i++)
        {
            logger.info("Waiting for seqno: " + waits[i].toString());
            waits[i].get(5, TimeUnit.SECONDS);
        }

        // Wait for the last event to commit.
        Future<ReplDBMSHeader> committed = pipeline
                .watchForCommittedSequenceNumber(99, false);
        committed.get(10, TimeUnit.SECONDS);

        // Check the serialization count.
        int actualSerialized = getSerializationCount(tpq);
        Assert.assertEquals("Checking expected serialization count",
                serialized, actualSerialized);
    }

    /**
     * Execute a generate serialization test.
     * 
     * @param name Name of the test
     * @param channelCount Number of channels
     * @param shards Array of shard names
     */
    private void execSerializationTest(String name, int channelCount,
            String[] shards) throws Exception
    {
        logger.info("##### " + name + " #####");

        // Set up and prepare pipeline. We set the channel count to
        // 1 as we just want to confirm that serialization counts are
        // increasing.
        TungstenProperties conf = helper.generateTHLParallelPipeline(name,
                channelCount, 50, shards.length, false);
        runtime = new ReplicatorRuntime(conf, new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Fetch references to stores and add commit randomization to the
        // transaction queue. In this case the max wait is 5ms.
        THL thl = (THL) pipeline.getStore("thl");
        THLParallelQueue tpq = (THLParallelQueue) pipeline
                .getStore("thl-queue");
        InMemoryTransactionalQueue mq = (InMemoryTransactionalQueue) pipeline
                .getStore("multi-queue");
        RandomCommitAction ca = new RandomCommitAction(100);
        mq.setCommitAction(ca);

        // Distribute transactions as follows: write transactions,
        // then insert an #UNKOWN transaction to trigger serialization.
        int serialized = 0;
        String lastShardId = null;
        LogConnection conn = thl.connect(false);
        for (int i = 0; i < shards.length; i++)
        {
            String shardId = shards[i];

            ReplDBMSEvent rde = helper.createEvent(i, shardId);
            THLEvent thlEvent = new THLEvent(rde.getSourceId(), rde);
            conn.store(thlEvent, false);
            conn.commit();

            // Count number of times we expect to serialize. Multiple
            // #UNKNOWN events in a row do not count.
            if ("#UNKNOWN".equals(shardId)
                    && (lastShardId == null || !lastShardId.equals(shardId)))
            {
                // #UNKNOWN events should serialize
                serialized++;
            }
            else if ("".equals(shardId)
                    && (lastShardId == null || !lastShardId.equals(shardId)))
            {
                // Empty strings should always serialize.
                serialized++;
            }
            else if (shardId == null && (lastShardId != null || i == 0))
            {
                // Likewise null shardId should serialize.
                serialized++;
            }

            // Remember the last shardId.
            lastShardId = shardId;
        }
        thl.disconnect(conn);

        // Wait for the last event to commit and then ensure we
        // serialized the expected number of times.
        Future<ReplDBMSHeader> committed = pipeline
                .watchForCommittedSequenceNumber(shards.length - 1, false);
        committed.get(10, TimeUnit.SECONDS);

        // Read events and get serialization.
        int actualSerialized = getSerializationCount(tpq);
        Assert.assertEquals("Checking expected serialization count",
                serialized, actualSerialized);

        // Read through the events and ensure all are present.
        int count = 0;
        for (int i = 0; i < shards.length; i++)
        {
            // Get the event.
            ReplDBMSEvent rde2 = mq.get();
            long seqno = rde2.getSeqno();
            String shardId = rde2.getShardId();
            if (logger.isDebugEnabled())
            {
                logger.debug("Read event: seqno=" + seqno + " shardId="
                        + shardId);
            }
            count++;

            // If it's unknown it should match.
            if ("#UNKNOWN".equals(shardId))
            {
                // Ensure we have an unknown event...
                Assert.assertEquals("Expected an unknown event: seqno=" + seqno
                        + " shardId=" + shardId, "#UNKNOWN", shardId);
            }
            else
            {
                // This should not be unknown.
                Assert.assertFalse("Did not expect an unknown event: seqno="
                        + seqno + " shardId=" + shardId,
                        "#UNKNOWN".equals(shardId));
            }
        }

        // Confirm we got the right number of events.
        Assert.assertEquals("Checking number of events", shards.length, count);
    }

    // Returns the current serialization count from a parallel queue.
    private int getSerializationCount(THLParallelQueue tpq)
    {
        TungstenProperties props = tpq.status();
        return props.getInt("serializationCount");
    }
}