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
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.applier.ApplierWrapper;
import com.continuent.tungsten.replicator.applier.DummyApplier;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.conf.ReplicatorMonitor;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSFilteredEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplOptionParams;
import com.continuent.tungsten.replicator.extractor.DummyExtractor;
import com.continuent.tungsten.replicator.management.MockEventDispatcher;
import com.continuent.tungsten.replicator.management.MockOpenReplicatorContext;
import com.continuent.tungsten.replicator.pipeline.Pipeline;
import com.continuent.tungsten.replicator.pipeline.PipelineConfigBuilder;
import com.continuent.tungsten.replicator.storage.InMemoryQueueAdapter;
import com.continuent.tungsten.replicator.storage.InMemoryQueueStore;
import com.continuent.tungsten.replicator.storage.Store;

/**
 * Implements a test of THL. This test implements a practical test of the
 * pipeline architecture under various transaction use cases, which are
 * documented below.
 * 
 * @author <a href="mailto:jussi-pekka.kurikka@continuent.com">Jussi-Pekka
 *         Kurikka</a>
 * @version 1.0
 */
public class TestTHL2
{
    private static Logger     logger = Logger.getLogger(TestTHL2.class);

    // Many tests use this pipeline and runtime, which shut down automatically.
    private Pipeline          pipeline;
    private ReplicatorRuntime runtime;

    /**
     * Shut down default pipeline and runtime at end of test.
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
     * Verify that we can start a THL as a store in a pipeline.
     */
    @Test
    public void testBasicService() throws Exception
    {
        logger.info("##### testBasicService #####");

        // Set up and start pipelines.
        TungstenProperties conf = this.generateTwoStageProps(
                "testBasicServices", 1);
        runtime = new ReplicatorRuntime(conf, new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Wait for and verify events.
        Future<ReplDBMSHeader> wait = pipeline
                .watchForProcessedSequenceNumber(9);
        ReplDBMSHeader lastEvent = wait.get(5, TimeUnit.SECONDS);
        Assert.assertEquals("Expected 10 server events", 9,
                lastEvent.getSeqno());

        Store thl = pipeline.getStore("thl");
        Assert.assertEquals("Expected 0 as first event", 0,
                thl.getMinStoredSeqno());
        Assert.assertEquals("Expected 9 as last event", 9,
                thl.getMaxStoredSeqno());
    }

    /**
     * Verify that filtered events are correctly replicated and stored. This
     * includes checking that the latency is correctly reported when the
     * filtered event commits and that the epoch number is visible after
     * storage.
     */
    @Test
    public void testFilteredEvents() throws Exception
    {
        String schema = "testFilteredEvents";
        logger.info("##### " + schema + " #####");

        // Prepare log directory and pipeline configuration.
        this.prepareLogDir(schema);
        TungstenProperties conf = this.generateQueueTHLQueuePipeline(schema);
        runtime = new ReplicatorRuntime(conf, new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());

        // Configure and start pipeline
        runtime.configure();
        runtime.prepare();
        pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Find the input and output queues.
        InMemoryQueueStore input = (InMemoryQueueStore) pipeline
                .getStore("queue");
        InMemoryQueueStore output = (InMemoryQueueStore) pipeline
                .getStore("queue2");

        // Put 2 initial events into the log so that it starts nicely.
        THLParallelQueueHelper helper = new THLParallelQueueHelper();
        long commitMillis = System.currentTimeMillis() - 60000;
        Timestamp commitTime = new Timestamp(commitMillis);
        logger.info("Commit time for all events: " + commitTime);
        ReplDBMSEvent e0 = helper.createEvent(0, (short) 0, true, "NONE",
                commitTime, 0);
        input.put(e0);
        ReplDBMSEvent e1 = helper.createEvent(1, (short) 0, true, "NONE",
                commitTime, 1);
        input.put(e1);

        // Wait for these to clear.
        Future<ReplDBMSHeader> wait1 = pipeline
                .watchForCommittedSequenceNumber(1, false);
        ReplDBMSHeader lastEvent = wait1.get(5, TimeUnit.SECONDS);
        Assert.assertEquals("Expected to reach 1", 1, lastEvent.getSeqno());

        // Generate a filtered event and confirm that it commits. Pick commit
        // time in past to ensure that the latency is correctly reported as this
        // was a problem in earlier code versions.
        ReplDBMSEvent e2 = helper.createEvent(2, (short) 0, true, "NONE",
                commitTime, 1);
        ReplDBMSEvent e4 = helper.createEvent(4, (short) 0, true, "NONE",
                commitTime, 1);
        ReplDBMSFilteredEvent fe24 = new ReplDBMSFilteredEvent(e2, e4);
        logger.info("Pipeline latency before filtered event: "
                + pipeline.getApplyLatency());
        logger.info("Filtered event commit time on input: "
                + fe24.getExtractedTstamp());
        input.put(fe24);

        // Wait for and verify events.
        Future<ReplDBMSHeader> wait2 = pipeline
                .watchForCommittedSequenceNumber(2, false);
        ReplDBMSHeader lastEvent2 = wait2.get(5, TimeUnit.SECONDS);
        Assert.assertEquals("Expected to reach 2", 2, lastEvent2.getSeqno());

        // Fetch events 0 and 1 to clear the output queue.
        ReplDBMSEvent e0out = output.get();
        Assert.assertEquals("e0 event seqno", 0, e0out.getSeqno());
        ReplDBMSEvent e1out = output.get();
        Assert.assertEquals("e1 event seqno", 1, e1out.getSeqno());

        // Confirm that the filtered event has reached the output queue.
        long currentMillis = System.currentTimeMillis();
        ReplDBMSFilteredEvent fe24out = (ReplDBMSFilteredEvent) output.get();
        Assert.assertEquals("First filtered seqno", 2, fe24out.getSeqno());
        Assert.assertEquals("Last filtered seqno", 4, fe24out.getSeqnoEnd());
        Assert.assertEquals("Epoch number", 1, fe24out.getEpochNumber());

        // Test the latency and ensure it is at least the interval between
        // now and the commit time less a buffer of 10 seconds *or* 0.
        double pipelineLatency = pipeline.getApplyLatency();
        logger.info("Current pipeline latency: " + pipelineLatency);
        logger.info("Filtered event commit time on output: "
                + fe24out.getExtractedTstamp());
        double expectedLatency = ((currentMillis - commitMillis) / 1000.0) - 10.0;
        Assert.assertTrue("Testing pipeline latency: expect " + pipelineLatency
                + " >= " + expectedLatency, pipelineLatency >= expectedLatency);
    }

    /**
     * Verify that two THLs may be chained together using separate pipelines and
     * that following replication they contain the same number of events.
     */
    @Test
    public void testTHL2Chaining() throws Exception
    {
        logger.info("##### testTHL2Chaining #####");

        // Prepare the log directories.
        prepareLogDir("testTHL2Chaining1");
        prepareLogDir("testTHL2Chaining2");

        // Generate server pipeline from dummy extractor to THL.
        PipelineConfigBuilder builder = new PipelineConfigBuilder();
        builder.setProperty(ReplicatorConf.SERVICE_NAME, "test");
        builder.setRole("master");
        builder.setProperty(ReplicatorConf.METADATA_SCHEMA, "testTHL2Chaining1");
        builder.addPipeline("master", "extract-s", "thl");
        builder.addStage("extract-s", "dummy", "thl-apply", null);

        builder.addComponent("extractor", "dummy", DummyExtractor.class);
        builder.addComponent("applier", "thl-apply", THLStoreApplier.class);
        builder.addProperty("applier", "thl-apply", "storeName", "thl");

        builder.addComponent("store", "thl", THL.class);
        builder.addProperty("store", "thl", "logDir", "testTHL2Chaining1");
        builder.addProperty("store", "thl", "storageListenerUri",
                "thl://localhost:2112/");
        TungstenProperties serverConf = builder.getConfig();

        // Generate slave pipeline from remote extractor to THL to dummy
        // applier.
        PipelineConfigBuilder builder2 = new PipelineConfigBuilder();
        builder2.setProperty(ReplicatorConf.SERVICE_NAME, "test");
        builder2.setRole("master");
        builder2.setProperty(ReplicatorConf.METADATA_SCHEMA,
                "testTHL2Chaining2");
        builder2.addPipeline("master", "extract-c,apply-c", "thl");
        builder2.addStage("extract-c", "thl-remote-extractor", "thl-apply",
                null);
        builder2.addStage("apply-c", "thl-extract", "dummy", null);

        builder2.addComponent("extractor", "thl-remote-extractor",
                RemoteTHLExtractor.class);
        builder2.addProperty("extractor", "thl-remote-extractor", "connectUri",
                "thl://localhost:2112/");
        builder2.addComponent("applier", "thl-apply", THLStoreApplier.class);
        builder2.addProperty("applier", "thl-apply", "storeName", "thl");

        builder2.addComponent("extractor", "thl-extract",
                THLStoreExtractor.class);
        builder2.addProperty("extractor", "thl-extract", "storeName", "thl");
        builder2.addComponent("applier", "dummy", DummyApplier.class);

        builder2.addComponent("store", "thl", THL.class);
        builder2.addProperty("store", "thl", "logDir", "testTHL2Chaining2");
        builder2.addProperty("store", "thl", "storageListenerUri",
                "thl://localhost:2113/");

        TungstenProperties clientConf = builder2.getConfig();

        // Configure and get pipelines.
        ReplicatorRuntime serverRuntime = new ReplicatorRuntime(serverConf,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        serverRuntime.configure();
        serverRuntime.prepare();
        Pipeline serverPipeline = serverRuntime.getPipeline();

        ReplicatorRuntime clientRuntime = new ReplicatorRuntime(clientConf,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        clientRuntime.configure();
        clientRuntime.prepare();
        Pipeline clientPipeline = clientRuntime.getPipeline();

        // Start both pipelines.
        serverPipeline.start(new MockEventDispatcher());
        clientPipeline.start(new MockEventDispatcher());

        // Wait for both pipelines to finish.
        Future<ReplDBMSHeader> waitServer = serverPipeline
                .watchForProcessedSequenceNumber(9);
        Future<ReplDBMSHeader> waitClient = clientPipeline
                .watchForProcessedSequenceNumber(9);

        logger.info("Waiting for server pipeline to clear");
        ReplDBMSHeader lastMasterEvent = waitServer.get(5, TimeUnit.SECONDS);
        Assert.assertEquals("Expected 10 server events", 9,
                lastMasterEvent.getSeqno());

        logger.info("Waiting for client pipeline to clear");
        ReplDBMSHeader lastClientEvent = waitClient.get(5, TimeUnit.SECONDS);
        Assert.assertEquals("Expected 10 client events", 9,
                lastClientEvent.getSeqno());

        // Ensure each THL contains expected number of events.
        Store serverThl = serverPipeline.getStore("thl");
        Assert.assertEquals("Expected 0 as first event", 0,
                serverThl.getMinStoredSeqno());
        Assert.assertEquals("Expected 9 as last event", 9,
                serverThl.getMaxStoredSeqno());

        Store thlClient = clientPipeline.getStore("thl");
        Assert.assertEquals("Expected 0 as first event", 0,
                thlClient.getMinStoredSeqno());
        Assert.assertEquals("Expected 9 as last event", 9,
                thlClient.getMaxStoredSeqno());

        // Shut down both pipelines.
        clientPipeline.shutdown(true);
        serverPipeline.shutdown(true);
        clientRuntime.release();
        serverRuntime.release();
    }

    /**
     * Verify that multiple pipelines work slave pipeline extracts from the
     * master pipeline.
     */
    @Test
    public void testInstanceConnections() throws Exception
    {
        logger.info("##### testInstanceConnections #####");

        // Prepare log directory.
        prepareLogDir("testInstanceConnections");

        // Generate server pipeline from dummy extractor to THL.
        PipelineConfigBuilder builder = new PipelineConfigBuilder();
        builder.setProperty(ReplicatorConf.SERVICE_NAME, "test");
        builder.setRole("master");
        builder.addPipeline("master", "extract", "thl");
        builder.addStage("extract", "dummy", "thl-apply", null);
        builder.addComponent("extractor", "dummy", DummyExtractor.class);
        builder.addComponent("applier", "thl-apply", THLStoreApplier.class);
        builder.addProperty("applier", "thl-apply", "storeName", "thl");
        builder.addComponent("store", "thl", THL.class);
        builder.addProperty("store", "thl", "logDir", "testInstanceConnections");
        TungstenProperties serverConf = builder.getConfig();

        // Generate slave pipeline from remote THL extractor to dummy applier.
        PipelineConfigBuilder builder2 = new PipelineConfigBuilder();
        builder2.setProperty(ReplicatorConf.SERVICE_NAME, "test");
        builder2.setRole("slave");
        builder2.addPipeline("slave", "extract", null);
        builder2.addStage("extract", "thl-remote-extractor", "dummy", null);
        builder2.addComponent("extractor", "thl-remote-extractor",
                RemoteTHLExtractor.class);
        builder2.addComponent("applier", "dummy", DummyApplier.class);
        TungstenProperties clientConf = builder2.getConfig();

        // Configure and get pipelines.
        ReplicatorRuntime serverRuntime = new ReplicatorRuntime(serverConf,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        serverRuntime.configure();
        serverRuntime.prepare();
        Pipeline serverPipeline = serverRuntime.getPipeline();

        ReplicatorRuntime clientRuntime = new ReplicatorRuntime(clientConf,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        clientRuntime.configure();
        clientRuntime.prepare();
        Pipeline clientPipeline = clientRuntime.getPipeline();

        // Start both pipelines.
        serverPipeline.start(new MockEventDispatcher());
        clientPipeline.start(new MockEventDispatcher());

        // Wait for both pipelines to finish.
        Future<ReplDBMSHeader> waitServer = serverPipeline
                .watchForProcessedSequenceNumber(9);
        Future<ReplDBMSHeader> waitClient = clientPipeline
                .watchForProcessedSequenceNumber(9);

        logger.info("Waiting for server pipeline to clear");
        ReplDBMSHeader lastMasterEvent = waitServer.get(5, TimeUnit.SECONDS);
        Assert.assertEquals("Expected 10 server events", 9,
                lastMasterEvent.getSeqno());

        logger.info("Waiting for client pipeline to clear");
        ReplDBMSHeader lastClientEvent = waitClient.get(5, TimeUnit.SECONDS);
        Assert.assertEquals("Expected 10 client events", 9,
                lastClientEvent.getSeqno());

        // Ensure THL contains expected number of events.
        Store thl = serverPipeline.getStore("thl");
        Assert.assertEquals("Expected 0 as first event", 0,
                thl.getMinStoredSeqno());
        Assert.assertEquals("Expected 9 as last event", 9,
                thl.getMaxStoredSeqno());

        // Shut down both pipelines.
        clientPipeline.shutdown(true);
        serverPipeline.shutdown(true);
        clientRuntime.release();
        serverRuntime.release();
    }

    /**
     * Verify that a slave can choose selectively to connect to a slave, a
     * master, or either one. This test works by defining master/slave
     * pipelines, then constructs a slave that connects to each in turn.
     */
    @Test
    public void testMultiThlServerConnect() throws Exception
    {
        logger.info("##### testMultiThlServerConnect #####");

        // Configure and start server pipeline from queue extractor to THL.
        TungstenProperties masterConf = generateQueueFedMasterProps("testMultiThlMaster");
        ReplicatorRuntime masterRuntime = new ReplicatorRuntime(masterConf,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        masterRuntime.configure();
        masterRuntime.prepare();
        Pipeline master = masterRuntime.getPipeline();
        master.start(new MockEventDispatcher());

        // Configure and start slave pipeline.
        Pipeline slave1 = this.createMultiThlSlave("testMultiThlSlave1",
                "thl://localhost:2112/", 2113, null);

        // Confirm the master/slave connection works by writing a transaction
        // and ensuring it reaches the slave.
        logger.info("Testing master/slave connection with seqno 0");
        InMemoryQueueStore masterQueue = (InMemoryQueueStore) master
                .getStore("queue");
        masterQueue.put(createEvent(0));
        Future<ReplDBMSHeader> wait = slave1.watchForProcessedSequenceNumber(0);
        ReplDBMSHeader lastEvent = wait.get(5, TimeUnit.SECONDS);
        Assert.assertEquals("Expected event we put in", 0, lastEvent.getSeqno());

        // Create a slave that prefers to read from a slave rather than a
        // master.
        Pipeline slave2 = this.createMultiThlSlave("testMultiThlSlave2",
                "thl://localhost:2112,thl://localhost:2113", 2114, "slave");

        // Confirm that we can connect and receive preferentially
        // from the slave. Shut down the slave once this is accomplished.
        logger.info("Testing read from slave #2 with seqno 1");
        masterQueue.put(createEvent(1));
        Future<ReplDBMSHeader> wait2 = slave2
                .watchForProcessedSequenceNumber(1);
        ReplDBMSHeader lastEvent2 = wait2.get(5, TimeUnit.SECONDS);
        Assert.assertEquals("Expected event we put in", 1,
                lastEvent2.getSeqno());
        Assert.assertEquals("Slave should read from slave",
                "thl://localhost:2113", slave2.getPipelineSource());

        // Shut down the slave #1 and ensure slave2 reads switch to the
        // master.
        slave1.shutdown(true);
        ((ReplicatorRuntime) slave1.getContext()).release();
        slave1 = null;

        logger.info("Testing read from master with seqno 2");
        masterQueue.put(createEvent(2));
        Future<ReplDBMSHeader> wait3 = slave2
                .watchForProcessedSequenceNumber(2);
        ReplDBMSHeader lastEvent3 = wait3.get(5, TimeUnit.SECONDS);
        Assert.assertEquals("Expected event we put in", 2,
                lastEvent3.getSeqno());
        Assert.assertEquals("Slave should read from master",
                "thl://localhost:2112", slave2.getPipelineSource());

        // Shut down the test slave.
        slave2.shutdown(true);
        ((ReplicatorRuntime) slave2.getContext()).release();
        slave2 = null;

        // Shut down original master and slave pipelines.
        master.shutdown(true);
        masterRuntime.release();
    }

    // Helper function to create and start a slave pipeline while avoiding test
    // errors due to silly typos in intermediate variables.
    private Pipeline createMultiThlSlave(String svc, String connectUris,
            int localThlPort, String preferredRole) throws Exception
    {
        TungstenProperties conf = generateSlaveProps(svc, connectUris,
                localThlPort, preferredRole);
        ReplicatorRuntime runtime = new ReplicatorRuntime(conf,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        Pipeline slave = runtime.getPipeline();
        slave.start(new MockEventDispatcher());
        return slave;
    }

    /**
     * Verify that if we store events in a THL, shutdown, and then restart a new
     * pipeline referring to the same in-memory storage, the starting sequence
     * number is correctly propagated back to the extractor so that new events
     * begin at the next sequence number.
     */
    @Test
    public void testSeqnoPropagation() throws Exception
    {
        logger.info("##### testSeqnoPropagation #####");
        this.prepareLogDir("testSeqnoPropagation");

        // Generate config.
        PipelineConfigBuilder builder = new PipelineConfigBuilder();
        builder.setProperty(ReplicatorConf.SERVICE_NAME, "test");
        builder.setRole("master");
        builder.setProperty(ReplicatorConf.METADATA_SCHEMA,
                "testSeqnoPropagation");
        builder.addPipeline("master", "extract-s", "thl");
        builder.addStage("extract-s", "dummy", "thl-apply", null);

        builder.addComponent("extractor", "dummy", DummyExtractor.class);
        builder.addComponent("applier", "thl-apply", THLStoreApplier.class);
        builder.addProperty("applier", "thl-apply", "storeName", "thl");

        builder.addComponent("store", "thl", THL.class);
        builder.addProperty("store", "thl", "logDir", "testSeqnoPropagation");
        builder.addProperty("store", "thl", "storageListenerUri",
                "thl://localhost:2112/");
        TungstenProperties conf = builder.getConfig();

        // Run pipeline through the first time.
        ReplicatorRuntime runtime1 = new ReplicatorRuntime(conf,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime1.configure();
        runtime1.prepare();
        Pipeline pipeline1 = runtime1.getPipeline();
        pipeline1.start(new MockEventDispatcher());

        // Wait for pipeline to finish.
        Future<ReplDBMSHeader> wait1 = pipeline1
                .watchForProcessedSequenceNumber(9);
        logger.info("Waiting for pipeline #1 to clear");
        ReplDBMSHeader lastEvent1 = wait1.get(5, TimeUnit.SECONDS);
        Assert.assertEquals("Expected 10 events", 9, lastEvent1.getSeqno());

        // Shut down first pipeline.
        pipeline1.shutdown(true);
        runtime1.release();

        // Run pipeline through the second time.
        ReplicatorRuntime runtime2 = new ReplicatorRuntime(conf,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime2.configure();
        runtime2.prepare();
        Pipeline pipeline2 = runtime2.getPipeline();
        pipeline2.start(new MockEventDispatcher());

        // Wait for pipeline to finish. It should get to event #19.
        Future<ReplDBMSHeader> wait2 = pipeline2
                .watchForProcessedSequenceNumber(19);
        logger.info("Waiting for pipeline #2 to clear");
        ReplDBMSHeader lastEvent2 = wait2.get(5, TimeUnit.SECONDS);
        Assert.assertEquals("Expected 20 events", 19, lastEvent2.getSeqno());

        // Ensure THL contains expected number of events. We must sleep
        // very briefly to allow the THL to commit.
        Thread.sleep(50);
        Store thl = pipeline2.getStore("thl");
        Assert.assertEquals("Expected 0 as first event", 0,
                thl.getMinStoredSeqno());
        Assert.assertEquals("Expected 19 as last event", 19,
                thl.getMaxStoredSeqno());

        // Shut down second pipeline.
        pipeline2.shutdown(true);
        runtime2.release();
    }

    /**
     * Verify that fragmented events are correctly replicated and stored.
     */
    @Test
    public void testFragmentedEvents() throws Exception
    {
        logger.info("##### testFragmentedEvents #####");

        // Prepare log directory.
        this.prepareLogDir("testFragmentedEvents");

        // Create configuration; ask dummy extractor to generate 3 fragments
        // per transaction.
        PipelineConfigBuilder builder = new PipelineConfigBuilder();
        builder.setProperty(ReplicatorConf.SERVICE_NAME, "test");
        builder.setRole("master");
        builder.setProperty(ReplicatorConf.METADATA_SCHEMA,
                "testFragmentedEvents");
        builder.addPipeline("master", "extract, apply", "thl");
        builder.addStage("extract", "dummy", "thl-apply", null);
        builder.addStage("apply", "thl-extract", "dummy", null);

        // Extract stage components.
        builder.addComponent("extractor", "dummy", DummyExtractor.class);
        builder.addProperty("extractor", "dummy", "nFrags",
                new Integer(3).toString());
        builder.addComponent("applier", "thl-apply", THLStoreApplier.class);
        builder.addProperty("applier", "thl-apply", "storeName", "thl");
        builder.addComponent("store", "thl", THL.class);
        builder.addProperty("store", "thl", "logDir", "testFragmentedEvents");

        // Apply stage components.
        builder.addComponent("extractor", "thl-extract",
                THLStoreExtractor.class);
        builder.addProperty("extractor", "thl-extract", "storeName", "thl");
        builder.addComponent("applier", "dummy", DummyApplier.class);
        builder.addProperty("applier", "dummy", "storeAppliedEvents", "true");

        TungstenProperties conf = builder.getConfig();
        runtime = new ReplicatorRuntime(conf, new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());

        // Configure and start pipeline
        runtime.configure();
        runtime.prepare();
        pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Wait for and verify events.
        Future<ReplDBMSHeader> wait = pipeline.watchForCommittedSequenceNumber(
                9, false);
        ReplDBMSHeader lastEvent = wait.get(5, TimeUnit.SECONDS);
        Assert.assertEquals("Expected 10 server events", 9,
                lastEvent.getSeqno());

        Store thl = pipeline.getStore("thl");
        Assert.assertEquals("Expected 0 as first event", 0,
                thl.getMinStoredSeqno());
        Assert.assertEquals("Expected 9 as last event", 9,
                thl.getMaxStoredSeqno());

        // Confirm we have 10x2 statements.
        ApplierWrapper wrapper = (ApplierWrapper) pipeline.getStage("apply")
                .getApplier0();
        DummyApplier applier = (DummyApplier) wrapper.getApplier();
        ArrayList<StatementData> sql = ((DummyApplier) applier).getTrx();
        Assert.assertEquals("Expected 10x2 statements", 60, sql.size());
    }

    /*
     * Verify that the THLExtractor extract() method waits for events to arrive
     * in the THL and then supplies them to the waiting THL. This simulates the
     * case where an applier stage has applied everything from the stage and is
     * now waiting for new events to arrive.
     */
    @Test
    public void testTHLExtractWaiting() throws Exception
    {
        logger.info("##### testTHLExtractWaiting #####");

        // Set up a pipeline with a queue at the beginning. We will feed
        // transactions into the queue.
        TungstenProperties conf = generateQueueFedMasterProps("testTHLExtractWaiting");
        runtime = new ReplicatorRuntime(conf, new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Fetch out the queue store so we can write events thereunto.
        InMemoryQueueStore queue = (InMemoryQueueStore) pipeline
                .getStore("queue");

        // Feed events into the pipeline and confirm they reach the other side.
        for (int i = 0; i < 10; i++)
        {
            Assert.assertFalse("Pipeline must be OK", pipeline.isShutdown());

            // Create and insert an event.
            ReplDBMSEvent e = createEvent(i);
            queue.put(e);

            // Now wait for it.
            Future<ReplDBMSHeader> wait = pipeline
                    .watchForProcessedSequenceNumber(i);
            ReplDBMSHeader lastEvent = wait.get(5, TimeUnit.SECONDS);
            Assert.assertEquals("Expected event we put in", i,
                    lastEvent.getSeqno());
        }

        // Close down pipeline.
        pipeline.shutdown(false);
        runtime.release();

        // Restart the pipeline and do the same test again, starting with event
        // 10.
        runtime = new ReplicatorRuntime(conf, new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Feed events into the pipeline and confirm they reach the other side.
        // We have a sleep to simulate not getting events into the THL for a
        // while.
        Thread.sleep(1000);
        queue = (InMemoryQueueStore) pipeline.getStore("queue");
        for (int i = 10; i < 20; i++)
        {
            Assert.assertFalse("Pipeline must be OK", pipeline.isShutdown());

            // Create and insert an event.
            ReplDBMSEvent e = createEvent(i);
            queue.put(e);

            // Now wait for it.
            Future<ReplDBMSHeader> wait = pipeline
                    .watchForProcessedSequenceNumber(i);
            ReplDBMSHeader lastEvent = wait.get(5, TimeUnit.SECONDS);
            Assert.assertEquals("Expected event we put in", i,
                    lastEvent.getSeqno());
        }
    }

    // Generate configuration properties for a double stage-pipeline
    // going through THL.
    public TungstenProperties generateTwoStageProps(String schemaName,
            int nFrags) throws Exception
    {
        // Clear the THL log directory.
        prepareLogDir(schemaName);

        // Create pipeline.
        PipelineConfigBuilder builder = new PipelineConfigBuilder();
        builder.setProperty(ReplicatorConf.SERVICE_NAME, "test");
        builder.setRole("master");
        builder.setProperty(ReplicatorConf.METADATA_SCHEMA, schemaName);
        builder.addPipeline("master", "extract, apply", "thl");
        builder.addStage("extract", "dummy", "thl-apply", null);
        builder.addStage("apply", "thl-extract", "dummy", null);

        // Extract stage components.
        builder.addComponent("extractor", "dummy", DummyExtractor.class);
        builder.addProperty("extractor", "dummy", "nFrags",
                new Integer(nFrags).toString());
        builder.addComponent("applier", "thl-apply", THLStoreApplier.class);
        builder.addProperty("applier", "thl-apply", "storeName", "thl");
        builder.addComponent("store", "thl", THL.class);
        builder.addProperty("store", "thl", "logDir", schemaName);

        // Apply stage components.
        builder.addComponent("extractor", "thl-extract",
                THLStoreExtractor.class);
        builder.addProperty("extractor", "thl-extract", "storeName", "thl");
        builder.addComponent("applier", "dummy", DummyApplier.class);

        return builder.getConfig();
    }

    // Generate configuration properties for a double stage master pipeline
    // going through THL. This pipeline uses a queue as the initial head
    // of the queue.
    public TungstenProperties generateQueueFedMasterProps(String schemaName)
            throws Exception
    {
        // Clear the THL log directory.
        prepareLogDir(schemaName);

        // Create pipeline.
        PipelineConfigBuilder builder = new PipelineConfigBuilder();
        builder.setProperty(ReplicatorConf.SERVICE_NAME, "test");
        builder.setRole("master");
        builder.setProperty(ReplicatorConf.METADATA_SCHEMA, schemaName);
        builder.addPipeline("master", "extract, apply", "queue,thl");
        builder.addStage("extract", "queue", "thl-apply", null);
        builder.addStage("apply", "thl-extract", "dummy", null);

        // Define stores.
        builder.addComponent("store", "thl", THL.class);
        builder.addProperty("store", "thl", "logDir", schemaName);
        builder.addComponent("store", "queue", InMemoryQueueStore.class);
        builder.addProperty("store", "queue", "maxSize", "5");

        // Extract stage components.
        builder.addComponent("extractor", "queue", InMemoryQueueAdapter.class);
        builder.addProperty("extractor", "queue", "storeName", "queue");
        builder.addComponent("applier", "thl-apply", THLStoreApplier.class);
        builder.addProperty("applier", "thl-apply", "storeName", "thl");

        // Apply stage components.
        builder.addComponent("extractor", "thl-extract",
                THLStoreExtractor.class);
        builder.addProperty("extractor", "thl-extract", "storeName", "thl");
        builder.addComponent("applier", "dummy", DummyApplier.class);

        return builder.getConfig();
    }

    // Generate a pipeline with a queue to feed and a queue to receive
    // transactions and a THL in the middle. This will be marked as a
    // slave.
    public TungstenProperties generateQueueTHLQueuePipeline(String schemaName)
            throws Exception
    {
        // Clear the THL log directory.
        prepareLogDir(schemaName);

        // Create pipeline.
        PipelineConfigBuilder builder = new PipelineConfigBuilder();
        builder.setProperty(ReplicatorConf.SERVICE_NAME, "test");
        builder.setRole("slave");
        builder.setProperty(ReplicatorConf.METADATA_SCHEMA, schemaName);
        builder.addPipeline("slave", "extract, apply", "queue,thl,queue2");
        builder.addStage("extract", "queue", "thl-apply", null);
        builder.addStage("apply", "thl-extract", "queue2", null);

        // Define stores.
        builder.addComponent("store", "thl", THL.class);
        builder.addProperty("store", "thl", "logDir", schemaName);
        builder.addComponent("store", "queue", InMemoryQueueStore.class);
        builder.addProperty("store", "queue", "maxSize", "5");
        builder.addComponent("store", "queue2", InMemoryQueueStore.class);
        builder.addProperty("store", "queue2", "maxSize", "5");

        // Extract stage components.
        builder.addComponent("extractor", "queue", InMemoryQueueAdapter.class);
        builder.addProperty("extractor", "queue", "storeName", "queue");
        builder.addComponent("applier", "thl-apply", THLStoreApplier.class);
        builder.addProperty("applier", "thl-apply", "storeName", "thl");

        // Apply stage components.
        builder.addComponent("extractor", "thl-extract",
                THLStoreExtractor.class);
        builder.addProperty("extractor", "thl-extract", "storeName", "thl");
        builder.addComponent("applier", "queue2", InMemoryQueueAdapter.class);
        builder.addProperty("applier", "queue2", "storeName", "queue2");

        return builder.getConfig();
    }

    // Generate pipeline properties for a slave with ability to listen
    // optionally on multiple THL URIs.
    public TungstenProperties generateSlaveProps(String svc,
            String connectUris, int localThlPort, String preferredRole)
            throws Exception
    {
        // Clear the THL log directory.
        prepareLogDir(svc);

        // Generate pipeline properties.
        PipelineConfigBuilder builder2 = new PipelineConfigBuilder();
        builder2.setProperty(ReplicatorConf.SERVICE_NAME, svc);
        builder2.setRole("slave");
        builder2.setProperty(ReplicatorConf.METADATA_SCHEMA, svc);
        builder2.addPipeline("slave", "extract-c,apply-c", "thl");
        builder2.addStage("extract-c", "thl-remote-extractor", "thl-apply",
                null);
        builder2.addStage("apply-c", "thl-extract", "dummy", null);

        builder2.addComponent("extractor", "thl-remote-extractor",
                RemoteTHLExtractor.class);
        builder2.addProperty("extractor", "thl-remote-extractor", "connectUri",
                connectUris);
        if (preferredRole != null)
        {
            builder2.addProperty("extractor", "thl-remote-extractor",
                    "preferredRole", preferredRole);
            builder2.addProperty("extractor", "thl-remote-extractor",
                    "preferredRoleTimeout", "3");
        }
        builder2.addComponent("applier", "thl-apply", THLStoreApplier.class);
        builder2.addProperty("applier", "thl-apply", "storeName", "thl");

        builder2.addComponent("extractor", "thl-extract",
                THLStoreExtractor.class);
        builder2.addProperty("extractor", "thl-extract", "storeName", "thl");
        builder2.addComponent("applier", "dummy", DummyApplier.class);

        builder2.addComponent("store", "thl", THL.class);
        builder2.addProperty("store", "thl", "logDir", svc);
        builder2.addProperty("store", "thl", "storageListenerUri",
                "thl://localhost:" + localThlPort + "/");

        return builder2.getConfig();
    }

    // Create an empty log directory or if the directory exists remove
    // any files within it.
    private File prepareLogDir(String logDirName)
    {
        File logDir = new File(logDirName);
        // Delete old log if present.
        if (logDir.exists())
        {
            for (File f : logDir.listFiles())
            {
                f.delete();
            }
            logDir.delete();
        }

        // Create new log directory.
        logDir.mkdirs();
        return logDir;
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