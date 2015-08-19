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

package com.continuent.tungsten.replicator.store.parallel;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Timestamp;
import java.util.ArrayList;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.conf.ReplicatorMonitor;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplControlEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplEvent;
import com.continuent.tungsten.replicator.event.ReplOptionParams;
import com.continuent.tungsten.replicator.management.MockOpenReplicatorContext;
import com.continuent.tungsten.replicator.pipeline.PipelineConfigBuilder;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.storage.parallel.LoadBalancingPartitioner;
import com.continuent.tungsten.replicator.storage.parallel.ParallelQueueStore;
import com.continuent.tungsten.replicator.storage.parallel.ShardListPartitioner;
import com.continuent.tungsten.replicator.util.SeqnoWatchPredicate;

/**
 * Tests behavior of parallel queue store.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ParallelQueueStoreTest extends TestCase
{
    private static Logger logger = Logger.getLogger(ParallelQueueStoreTest.class);

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
     * Confirm that we can set up a parallel queue with a default partition size
     * of 1.
     */
    public void testSinglePartition() throws Exception
    {
        TungstenProperties conf = generateConfig();
        PluginContext context = new ReplicatorRuntime(conf,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        ParallelQueueStore pqs = new ParallelQueueStore();
        pqs.configure(context);
        pqs.prepare(context);
        assertEquals("1 partition by default", 1, pqs.getPartitions());
        pqs.release(context);
    }

    /**
     * Confirm that we can setup, write to, and read from all partitions of a
     * multi-partition parallel queue.
     */
    public void testMultiPartitions() throws Exception
    {
        // Configure and prepare store.
        TungstenProperties conf = generateConfig();
        PluginContext context = new ReplicatorRuntime(conf,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        ParallelQueueStore pqs = new ParallelQueueStore();
        pqs.setPartitions(12);
        pqs.setMaxSize(2);
        pqs.setSyncEnabled(false);
        pqs.configure(context);
        pqs.prepare(context);
        assertEquals("12 partitions defined", 12, pqs.getPartitions());

        // Spread events evenly across partitions.
        for (int i = 0; i < 24; i++)
        {
            ReplDBMSEvent event = createEvent(i);
            pqs.put(i % 12, event);
        }

        // Check size of partitions and total number of events.
        int total = 0;
        for (int i = 0; i < pqs.getPartitions(); i++)
        {
            assertEquals("Each partition has 2 events", 2, pqs.size(i));
            total += pqs.size(i);
        }
        assertEquals("24 total events across partitions", 24, total);

        // Peek and get each individual event.
        for (int i = 0; i < 24; i++)
        {
            int partId = i % 12;
            ReplDBMSEvent event1 = (ReplDBMSEvent) pqs.peek(partId);
            assertEquals("First event has same seqno as partition", i,
                    event1.getSeqno());
            ReplDBMSEvent event2 = (ReplDBMSEvent) pqs.get(partId);
            assertEquals("First event has same seqno as partition", i,
                    event2.getSeqno());
        }

        // Ensure queue is empty.
        for (int i = 0; i < 12; i++)
        {
            assertEquals("Each partition is empty", 0, pqs.size(i));
        }

        pqs.release(context);
    }

    /**
     * Confirm that stop events go to all partitions and appear in total order
     * compared to all other events. We implement the test by sending a stop
     * after every log event and ensuring that we see the stop event on all
     * queues.
     */
    public void testMultStop() throws Exception
    {
        // Configure and prepare store.
        TungstenProperties conf = generateConfig();
        PluginContext context = new ReplicatorRuntime(conf,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        ParallelQueueStore pqs = new ParallelQueueStore();
        pqs.setPartitions(3);
        pqs.setMaxSize(10);
        pqs.setSyncEnabled(false);
        pqs.configure(context);
        pqs.prepare(context);

        // Spread events evenly across partitions.
        for (int i = 0; i < 3; i++)
        {
            // Put in a real event. This goes to one queue.
            ReplDBMSEvent event = createEvent(i);
            pqs.put(i % 3, event);

            // Put in a stop event after it--this goes to all queues.
            pqs.insertStopEvent();
        }

        // Each queue should now have 4 events, where one is a real event
        // and the other 3 are stop events.
        for (int i = 0; i < pqs.getPartitions(); i++)
        {
            // Assert total size of the queue.
            assertEquals("Each partition has 4 events", 4, pqs.size(i));

            // Check ordering--sequence numbers must be greater than/equal.
            long lastSeqno = 0;
            for (int j = 0; j < 4; j++)
            {
                ReplEvent next = pqs.get(i);
                long curSeqno;
                if (next instanceof ReplDBMSEvent)
                    curSeqno = ((ReplDBMSEvent) next).getSeqno();
                else if (next instanceof ReplControlEvent)
                    curSeqno = ((ReplControlEvent) next).getHeader().getSeqno();
                else
                    throw new Exception("Unexpected event type: "
                            + next.getClass().toString());

                assertTrue("Sequence number greater/equal on partition",
                        curSeqno >= lastSeqno);
                lastSeqno = curSeqno;
            }

            // The last sequence number must be 2 on all queues, as we either
            // get it from the
            // control event or the last real event.
            assertEquals("Last seqno on partition is 2", 2, lastSeqno);
        }

        pqs.release(context);
    }

    /**
     * Confirm that watch synchronization control events go to all partitions
     * and appear in total order compared to all other events. We implement this
     * test by inserting watch events on even sequence numbers then picking them
     * out from queues.
     */
    public void testMultWatchSync() throws Exception
    {
        // Configure and prepare store.
        TungstenProperties conf = generateConfig();
        PluginContext context = new ReplicatorRuntime(conf,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        ParallelQueueStore pqs = new ParallelQueueStore();
        pqs.setPartitions(3);
        pqs.setMaxSize(10);
        pqs.setSyncEnabled(false);
        pqs.configure(context);
        pqs.prepare(context);
        assertEquals("3 partitions defined", 3, pqs.getPartitions());

        // Pre-load watch events for event numbered sequence numbers.
        for (int i = 1; i <= 6; i++)
        {
            if (i % 2 == 0)
                pqs.insertWatchSyncEvent(new SeqnoWatchPredicate(i));
        }

        // Add events, spreading them across queues.
        for (int i = 1; i <= 6; i++)
        {
            // Put in a real event. This goes to one queue.
            ReplDBMSEvent event = createEvent(i);
            pqs.put(i % 3, event);
        }

        // Each queue should now have 5 events, where two are real events and
        // 3 are watch control events from even numbered events.
        for (int p = 0; p < pqs.getPartitions(); p++)
        {
            // Assert total size of the queue.
            assertEquals("Each partition has 5 events", 5, pqs.size(p));

            // Check ordering--sequence numbers must be greater than/equal.
            long lastSeqno = 0;
            for (int j = 0; j < 5; j++)
            {
                ReplEvent next = pqs.get(p);
                long curSeqno;
                if (next instanceof ReplDBMSEvent)
                    curSeqno = ((ReplDBMSEvent) next).getSeqno();
                else if (next instanceof ReplControlEvent)
                {
                    curSeqno = ((ReplControlEvent) next).getHeader().getSeqno();
                    assertTrue("Control events must be on even seqnos only",
                            curSeqno % 2 == 0);
                }
                else
                    throw new Exception("Unexpected event type: "
                            + next.getClass().toString());

                assertTrue("Sequence number greater/equal on partition",
                        curSeqno >= lastSeqno);
                lastSeqno = curSeqno;
            }

            // The last sequence number must be 2 on all queues, as we either
            // get it from the control event or the last real event.
            assertEquals("Last seqno on partition " + p + " is 6", 6, lastSeqno);
        }

        pqs.release(context);
    }

    /**
     * Confirm that we can perform basic partitioning using a list partitioner
     * that divides shards into fixed partitions with a default partition.
     */
    public void testNonCriticalPartitioning() throws Exception
    {
        // Configure and prepare store.
        TungstenProperties conf = generateConfig();
        PluginContext context = new ReplicatorRuntime(conf,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        ParallelQueueStore pqs = new ParallelQueueStore();
        pqs.setPartitions(4);
        pqs.setMaxSize(10);
        pqs.setSyncEnabled(false);
        pqs.setPartitionerClass(ShardListPartitioner.class.getName());

        // Configure and prepare our humble store.
        pqs.configure(context);
        pqs.prepare(context);

        // Set up a list partitioner that will send db0-2 to specific
        // partitions and all others to last partition.
        ShardListPartitioner partitioner = (ShardListPartitioner) pqs
                .getPartitioner();
        TungstenProperties partCfg = new TungstenProperties();
        partCfg.setString("db0", "0");
        partCfg.setString("db1", "1");
        partCfg.setString("db2", "2");
        partCfg.setString("(*)", "3");
        File partCfgFile = File.createTempFile("part1", "properties");
        FileOutputStream cfgFos = new FileOutputStream(partCfgFile);
        partCfg.store(cfgFos);
        cfgFos.close();
        partitioner.setShardMap(partCfgFile);

        // Load 6 events with assigned shard IDs.
        // Pre-load watch events for event numbered sequence numbers.
        for (int i = 0; i <= 5; i++)
        {
            ReplDBMSEvent event = createEvent(i, "db" + i);
            pqs.put(0, event);
        }

        // Confirm that we have 6 total events.
        long storeSize = pqs.getStoreSize();
        assertEquals("Total store size after loading", 6, storeSize--);

        // First three partitions should have one event each from db0-2
        // respectively.
        for (int i = 0; i < 3; i++)
        {
            ReplDBMSEvent event = (ReplDBMSEvent) pqs.get(i);
            assertEquals("Sequence number must match partition", i,
                    event.getSeqno());
            assertEquals("Total store size after removing 1", storeSize--,
                    pqs.getStoreSize());
        }

        // Last partition should have 3 events from db3-5.
        for (int i = 3; i < 6; i++)
        {
            ReplDBMSEvent event = (ReplDBMSEvent) pqs.get(3);
            assertEquals("Sequence number must match partition", i,
                    event.getSeqno());
            assertEquals("Total store size after removing 1", storeSize--,
                    pqs.getStoreSize());
        }

        pqs.release(context);
        partCfgFile.delete();
    }

    /**
     * Confirm that we can handle writes of events to single critical
     * partitions. This case does not check blocking behavior, merely that we
     * can correctly assign events in the presence of critical partition
     * information.
     */
    public void testSimpleCriticalPartitioning() throws Exception
    {
        // Configure and prepare store.
        TungstenProperties conf = generateConfig();
        PluginContext context = new ReplicatorRuntime(conf,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        ParallelQueueStore pqs = new ParallelQueueStore();
        pqs.setPartitions(4);
        pqs.setMaxSize(10);
        pqs.setSyncEnabled(false);
        pqs.setPartitionerClass(ShardListPartitioner.class.getName());

        // Configure and prepare our humble store.
        pqs.configure(context);
        pqs.prepare(context);

        // Set up a list partitioner that will hash all shards but will treat
        // db0 as a critical partition. We'll also confirm the hashing
        // algorithm.
        ShardListPartitioner partitioner = (ShardListPartitioner) pqs
                .getPartitioner();
        TungstenProperties partCfg = new TungstenProperties();
        partCfg.setString("(critical)", "db0");
        File partCfgFile = File.createTempFile("part2", "properties");
        FileOutputStream cfgFos = new FileOutputStream(partCfgFile);
        partCfg.store(cfgFos);
        cfgFos.close();
        partitioner.setShardMap(partCfgFile);
        pqs.setPartitioner(partitioner);

        // Load 3 events with shard IDs and read them back out. We use this to
        // find out how the events get hashed by the partitioner.
        int[] eventPartitions = new int[3];
        for (int i = 0; i < 3; i++)
        {
            // Put in the event.
            ReplDBMSEvent event = createEvent(i, "db" + i);
            pqs.put(0, event);

            // Now find out where it went.
            ReplDBMSEvent event2 = null;
            for (int p = 0; p < 4; p++)
            {
                if (pqs.peek(p) != null)
                {
                    eventPartitions[i] = p;
                    event2 = (ReplDBMSEvent) pqs.get(p);
                    break;
                }
            }
            assertNotNull("Must have found event again", event2);
            assertEquals("Sequence number must match", i, event2.getSeqno());
        }

        // Now load 100 events and read them out, confirming that the sharding
        // is the same for each supported db.
        for (int i = 0; i <= 100; i++)
        {
            // Add the event.
            int p = i % 3;
            ReplDBMSEvent event = createEvent(i, "db" + p);
            pqs.put(0, event);

            // Read it out and confirm identity.
            assertNotNull("Must see event in queue",
                    pqs.peek(eventPartitions[p]));
            ReplDBMSEvent event2 = (ReplDBMSEvent) pqs.get(eventPartitions[p]);
            assertNotNull("Must have found event again", event2);
            assertEquals("Sequence number must match", i, event2.getSeqno());

        }

        // Confirm store is empty after test.
        assertEquals("Total store size after test", 0, pqs.getStoreSize());

        pqs.release(context);
        partCfgFile.delete();
    }

    /**
     * Confirm that sync events are generated if sync is enabled and that such
     * events are generated at expected intervals.
     */
    public void testSyncIntervals() throws Exception
    {
        int loopSize = 100;

        // Configure and prepare store.
        TungstenProperties conf = generateConfig();
        PluginContext context = new ReplicatorRuntime(conf,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());

        // Confirm sync events are generated at expected intervals.
        for (int syncInterval = 1; syncInterval < 10; syncInterval++)
        {
            // Create a queue with specified sync interval.
            ParallelQueueStore pqs = new ParallelQueueStore();
            pqs.setPartitions(1);
            pqs.setMaxSize(200);
            pqs.setSyncEnabled(true);
            pqs.setSyncInterval(syncInterval);
            pqs.configure(context);
            pqs.prepare(context);
            assertEquals("1 partition defined", 1, pqs.getPartitions());
            assertEquals("Sync interval set", syncInterval,
                    pqs.getSyncInterval());
            logger.info("Sync interval: " + syncInterval);

            // Write events to partitions.
            for (int i = 0; i < loopSize; i++)
            {
                ReplDBMSEvent event = createEvent(i);
                pqs.put(0, event);
            }

            // Compute the expected number of events including sync events
            // and confirm queue contains this many.
            int expectedSyncEvents = loopSize / syncInterval;
            int expectedEvents = loopSize + expectedSyncEvents;

            // Check size of partition.
            assertEquals("Partition has generated plus events sync events",
                    expectedEvents, pqs.size(0));

            // Read generated events.
            for (int i = 0; i < loopSize; i++)
            {
                ReplDBMSEvent event1 = (ReplDBMSEvent) pqs.get(0);
                assertEquals("First event has same seqno as loop counter", i,
                        event1.getSeqno());
                if ((i + 1) % syncInterval == 0)
                {
                    ReplEvent ctl = pqs.get(0);
                    assertTrue("Control event", ctl instanceof ReplControlEvent);
                    ReplDBMSHeader event2 = ((ReplControlEvent) ctl)
                            .getHeader();
                    assertEquals("Control event contains previous event",
                            event1.getSeqno(), event2.getSeqno());
                }
            }

            // Release queue.
            pqs.release(context);
        }
    }

    /**
     * Confirm that sync events are properly distributed across queues at
     * expected intervals.
     */
    public void testSyncDistribution() throws Exception
    {
        int loopSize = 100;
        int syncInterval = 10;

        // Configure and prepare store.
        TungstenProperties conf = generateConfig();
        PluginContext context = new ReplicatorRuntime(conf,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());

        // Confirm sync events are generated at expected intervals.
        for (int queueCount = 1; queueCount < 10; queueCount++)
        {
            // Create a queue with specified sync interval.
            ParallelQueueStore pqs = new ParallelQueueStore();
            pqs.setPartitions(queueCount);
            pqs.setMaxSize(200);
            pqs.setSyncEnabled(true);
            pqs.setSyncInterval(syncInterval);
            pqs.configure(context);
            pqs.prepare(context);
            assertEquals("N partitions defined", queueCount,
                    pqs.getPartitions());
            logger.info("# of partitions: " + queueCount);

            // Write events to partitions.
            for (int i = 0; i < loopSize; i++)
            {
                ReplDBMSEvent event = createEvent(i);
                pqs.put(0, event);
            }

            // Compute the expected number of events including sync events
            // and confirm queue contains this many.
            int expectedSyncEvents = loopSize / syncInterval;
            int expectedEvents = loopSize + expectedSyncEvents;

            // Check size of partition.
            assertEquals("Partition has generated plus events sync events",
                    expectedEvents, pqs.size(0));

            // Read generated events.
            for (int i = 0; i < loopSize; i++)
            {
                ReplDBMSEvent event1 = (ReplDBMSEvent) pqs.get(0);
                assertEquals("First event has same seqno as loop counter", i,
                        event1.getSeqno());
                if ((i + 1) % syncInterval == 0)
                {
                    ReplEvent ctl = pqs.get(0);
                    assertTrue("Control event", ctl instanceof ReplControlEvent);
                    ReplDBMSHeader event2 = ((ReplControlEvent) ctl)
                            .getHeader();
                    assertEquals("Control event contains previous event",
                            event1.getSeqno(), event2.getSeqno());
                }
            }

            // Release queue.
            pqs.release(context);
        }
    }

    /**
     * Confirm that a load balancing partitioner spreads events evenly across
     * all queues.
     */
    public void testLoadBalancingPartitioner() throws Exception
    {
        // Configure and prepare store.
        TungstenProperties conf = generateConfig();
        PluginContext context = new ReplicatorRuntime(conf,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        ParallelQueueStore pqs = new ParallelQueueStore();
        pqs.setPartitions(4);
        pqs.setMaxSize(10);
        pqs.setSyncEnabled(false);
        pqs.setPartitionerClass(LoadBalancingPartitioner.class.getName());

        // Configure and prepare our humble store.
        pqs.configure(context);
        pqs.prepare(context);

        // Load 32 events, which amounts to 8 events per partition.
        for (int i = 0; i < 32; i++)
        {
            ReplDBMSEvent event = createEvent(i, "db" + i);
            pqs.put(0, event);
        }

        // Confirm that we have 32 total events.
        long storeSize = pqs.getStoreSize();
        assertEquals("Total store size after loading", 32, storeSize);

        // Confirm each partition has exactly 8 events.
        for (int i = 0; i < 4; i++)
        {
            // Read and check 8 events.
            for (int j = 0; j < 8; j++)
            {
                ReplDBMSEvent event = (ReplDBMSEvent) pqs.peek(i);
                assertNotNull("Expected event: queue=" + i + " number=" + j,
                        event);
                pqs.get(i);
            }

            // Confirm queue is empty.
            ReplDBMSEvent event = (ReplDBMSEvent) pqs.peek(i);
            if (event != null)
            {
                throw new Exception(
                        "Expected queue to be empty but found event: queue="
                                + i + " seqno=" + event.getSeqno());
            }
        }

        // Confirm the queue is now empty.
        assertEquals("Queue should be empty", 0, pqs.getStoreSize());

        // Release the queue.
        pqs.release(context);
    }

    // Generate a stripped-down runtime properties.
    private TungstenProperties generateConfig() throws Exception
    {
        PipelineConfigBuilder builder = new PipelineConfigBuilder();
        builder.setProperty(ReplicatorConf.SERVICE_NAME, "pqsService");
        builder.setProperty(ReplicatorConf.METADATA_SCHEMA, "tungsten");
        return builder.getConfig();
    }

    // Returns a well-formed event with a default shard ID.
    private ReplDBMSEvent createEvent(long seqno)
    {
        return createEvent(seqno, ReplOptionParams.SHARD_ID_UNKNOWN);
    }

    // Returns a well-formed ReplDBMSEvent with a specified shard ID.
    private ReplDBMSEvent createEvent(long seqno, String shardId)
    {
        ArrayList<DBMSData> t = new ArrayList<DBMSData>();
        t.add(new StatementData("SELECT 1"));
        DBMSEvent dbmsEvent = new DBMSEvent(new Long(seqno).toString(), null,
                t, true, new Timestamp(System.currentTimeMillis()));
        ReplDBMSEvent replDbmsEvent = new ReplDBMSEvent(seqno, dbmsEvent);
        replDbmsEvent.getDBMSEvent().addMetadataOption(
                ReplOptionParams.SHARD_ID, shardId);
        return replDbmsEvent;
    }
}