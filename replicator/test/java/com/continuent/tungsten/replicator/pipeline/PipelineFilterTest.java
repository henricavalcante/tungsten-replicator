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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorMonitor;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSFilteredEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.management.MockEventDispatcher;
import com.continuent.tungsten.replicator.management.MockOpenReplicatorContext;
import com.continuent.tungsten.replicator.storage.InMemoryQueueStore;

/**
 * This class tests filter operations on pipeline tasks. It includes checks of
 * filter life-cycles, filtering of single and multiple events, fragmented event
 * handling, and filter consistency with block commit.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class PipelineFilterTest extends TestCase
{
    private static Logger  logger = Logger.getLogger(PipelineFilterTest.class);
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
     * Verify that if we can start up a pipeline containing filters and release
     * it the filters are correctly configured, prepared, and released.
     */
    public void testStartStopFilterPipeline() throws Exception
    {
        // Confirm that the filter counts are uninitialized.
        SampleFilter.clearCounters();

        // Start pipeline.
        TungstenProperties config = helper.createDoubleQueueWithFilter(1, 1,
                -1, 0, false);
        ReplicatorRuntime runtime = new ReplicatorRuntime(config,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        Pipeline pipeline = prepareAndStartPipeline(config, runtime);

        // Confirm life-cycle counts.
        assertEquals("configured", 1, SampleFilter.configured);
        assertEquals("prepared", 1, SampleFilter.prepared);
        assertEquals("released", 0, SampleFilter.released);

        // Shutdown and confirm final counts.
        pipeline.shutdown(false);
        pipeline.release(runtime);

        assertEquals("configured", 1, SampleFilter.configured);
        assertEquals("prepared", 1, SampleFilter.prepared);
        assertEquals("released", 1, SampleFilter.released);
    }

    /**
     * Verify that if we filter a single event then all other events pass
     * through unharmed and the filtered event is replaced with a single
     * filtered event whose start and end seqno is the one seqno that was
     * skipped.
     */
    public void testSingleFilteredEvent() throws Exception
    {
        // Configure double queue pipeline with filter set to skip event #2.
        TungstenProperties config = helper.createDoubleQueueWithFilter(10, 10,
                2, 1, false);
        ReplicatorRuntime runtime = new ReplicatorRuntime(config,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        Pipeline pipeline = prepareAndStartPipeline(config, runtime);

        // Write 5 events to pipeline.
        InMemoryQueueStore input = (InMemoryQueueStore) pipeline.getStore("q1");
        this.insertEvents(input, 0, 5);

        // Ensure we reached seqno 4.
        verifyProcessedSeqno(pipeline, 4);

        // Read 5 events and look for filtered event on #2.
        InMemoryQueueStore output = (InMemoryQueueStore) pipeline
                .getStore("q2");
        for (int i = 0; i < 5; i++)
        {
            ReplDBMSEvent event = output.poll();
            assertNotNull("Expected to read non-null event: " + i, event);
            if (event.getSeqno() == 2)
            {
                assertTrue("Filtered event: " + i,
                        event instanceof ReplDBMSFilteredEvent);
                ReplDBMSFilteredEvent filteredEvent = (ReplDBMSFilteredEvent) event;
                assertEquals("Filter start", 2, filteredEvent.getSeqno());
                assertEquals("Filter end", 2, filteredEvent.getSeqnoEnd());
            }
            else
            {
                assertFalse("Non-filtered event: " + i,
                        event instanceof ReplDBMSFilteredEvent);
            }
        }

        // Shut down.
        pipeline.shutdown(false);
        pipeline.release(runtime);
    }

    /**
     * Verify that if we filter more than one event in succession, a filtered
     * event is generated with the start sequence number set to the first event
     * filtered and the end sequence number set to the last event filtered.
     */
    public void testMultipleFilteredEvents() throws Exception
    {
        // Configure double queue pipeline with filter set to skip events #0 to
        // #2.
        TungstenProperties config = helper.createDoubleQueueWithFilter(10, 10,
                0, 3, false);
        ReplicatorRuntime runtime = new ReplicatorRuntime(config,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        Pipeline pipeline = prepareAndStartPipeline(config, runtime);

        // Write 5 events to pipeline.
        InMemoryQueueStore input = (InMemoryQueueStore) pipeline.getStore("q1");
        this.insertEvents(input, 0, 5);

        // Ensure we reached seqno 4.
        verifyProcessedSeqno(pipeline, 4);

        // Read 5 events looking for filtered event.
        InMemoryQueueStore output = (InMemoryQueueStore) pipeline
                .getStore("q2");
        long seqno = -1;
        ReplDBMSFilteredEvent filteredEvent = null;

        while (seqno < 4)
        {
            ReplDBMSEvent event = output.poll();
            seqno = event.getSeqno();
            assertNotNull("Expected to read non-null event (last seqno="
                    + seqno + ")", event);
            if (event instanceof ReplDBMSFilteredEvent)
                filteredEvent = (ReplDBMSFilteredEvent) event;
        }

        // Validate that we received a filtered event with the expected range.
        assertNotNull("Expect to find a filtered event", filteredEvent);
        assertEquals("Filter start", 0, filteredEvent.getSeqno());
        assertEquals("Filter end", 2, filteredEvent.getSeqnoEnd());

        // Shut down.
        pipeline.shutdown(false);
        pipeline.release(runtime);
    }

    /**
     * Verify that if we filter multiple fragmented events in succession, a
     * filtered event is generated with the start sequence number set to the
     * first event filtered and the end sequence number set to the last event
     * filtered. Also, all fragments are removed.
     */
    public void testFilteredFragmentHandling() throws Exception
    {
        // Create configuration; ask dummy extractor to generate 3 fragments
        // per transaction.
        TungstenProperties config = helper.createDoubleQueueWithFilter(100, 10,
                3, 1, true);
        ReplicatorRuntime runtime = new ReplicatorRuntime(config,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        Pipeline pipeline = prepareAndStartPipeline(config, runtime);

        // Insert fragmented events.
        InMemoryQueueStore input = (InMemoryQueueStore) pipeline.getStore("q1");
        for (int i = 0; i < 30; i++)
        {
            long seqno = i;
            for (short fragNo = 0; fragNo < 3; fragNo++)
            {
                boolean lastFrag = (fragNo == 2);
                ReplDBMSEvent event = helper.createEvent(seqno, "db0", fragNo,
                        lastFrag);
                input.put(event);
                logger.info("Added event: seqno=" + seqno + " fragNo=" + fragNo
                        + " lastFrag=" + lastFrag);
            }
        }

        // Wait for seqno 29 (last one).
        verifyProcessedSeqno(pipeline, 29);

        // Confirm that any event with a seqno that is a multiple of 3 is a
        // filter event. Keep count to ensure we have a total of 10 filtered
        // events.
        InMemoryQueueStore output = (InMemoryQueueStore) pipeline
                .getStore("q2");
        long seqno = -1;
        int filteredEvents = 0;
        int nonFilteredEvents = 0;

        // Loop until we find the last fragment of sequence number 29.
        boolean done = false;
        while (!done)
        {
            ReplDBMSEvent event = output.poll();
            seqno = event.getSeqno();
            done = (seqno >= 29 && event.getLastFrag());
            assertNotNull("Expected to read non-null event (last seqno="
                    + seqno + ")", event);
            if (seqno % 3 == 0)
            {
                // Must be a filtered event.
                assertTrue("Filtered event: " + seqno,
                        event instanceof ReplDBMSFilteredEvent);
                ReplDBMSFilteredEvent filteredEvent = (ReplDBMSFilteredEvent) event;
                assertEquals("Filter start", seqno, filteredEvent.getSeqno());
                assertEquals("Filter end", seqno, filteredEvent.getSeqnoEnd());
                filteredEvents++;
            }
            else
            {
                // Must a non-filtered event.
                assertFalse("Non-filtered event: " + seqno,
                        event instanceof ReplDBMSFilteredEvent);
                nonFilteredEvents++;
            }
        }

        // Check counts to ensure we did not drop or corrupt anything.
        assertEquals("Expected number of filtered events", 10, filteredEvents);
        assertEquals(
                "Expected number of non-filtered events (3 frags per seqno)",
                60, nonFilteredEvents);

        // Close down pipeline.
        pipeline.shutdown(false);
        runtime.release();
    }

    /**
     * Verify that filtering does not conflict with block commit. We put a queue
     * on both ends but have the input queue full at the start. We then test
     * different levels of group commit and confirm the number of blocks and
     * group commit size recorded.
     */
    public void testFilteringAndBlockCommit() throws Exception
    {
        // Define transaction count and different block sizes to use.
        int xacts = 40;
        int[] blockSizes = {2, 7};

        // Try test for each block size.
        for (int blockSize : blockSizes)
        {
            logger.info("Testing block commit: transactions=" + xacts
                    + " blockSize=" + blockSize);

            // Create config with pipeline with input and output queues.
            TungstenProperties config = helper.createDoubleQueueWithFilter(40,
                    blockSize, 5, 2, true);
            ReplicatorRuntime runtime = new ReplicatorRuntime(config,
                    new MockOpenReplicatorContext(),
                    ReplicatorMonitor.getInstance());
            runtime.configure();
            runtime.prepare();
            Pipeline pipeline = runtime.getPipeline();

            // Load data into the queue and start the pipeline.
            InMemoryQueueStore input = (InMemoryQueueStore) pipeline
                    .getStore("q1");
            this.insertEvents(input, 0, 40);
            pipeline.start(new MockEventDispatcher());

            // Test for successfully applied and extracted sequence numbers.
            this.verifyProcessedSeqno(pipeline, xacts - 1);

            // Loop through and count filtered vs. non-filtered events.
            InMemoryQueueStore output = (InMemoryQueueStore) pipeline
                    .getStore("q2");
            long seqno = -1;
            int filteredEvents = 0;
            int nonFilteredEvents = 0;
            while (seqno < (xacts - 1))
            {
                ReplDBMSEvent event = output.poll();
                seqno = event.getSeqno();
                assertNotNull("Expected to read non-null event (last seqno="
                        + seqno + ")", event);
                if (event instanceof ReplDBMSFilteredEvent)
                {
                    ReplDBMSFilteredEvent filteredEvent = (ReplDBMSFilteredEvent) event;
                    long numberOfEventsFiltered = filteredEvent.getSeqnoEnd()
                            - seqno + 1;
                    filteredEvents += numberOfEventsFiltered;
                }
                else
                {
                    nonFilteredEvents++;
                }
            }

            // Confirm the number of filtered vs. non-filtered events.
            assertEquals("Expected filtered events", 16, filteredEvents);
            assertEquals("Expected non-filtered events", 24, nonFilteredEvents);

            // Shut it down.
            pipeline.shutdown(false);
            pipeline.release(runtime);
        }
    }

    // Starting with configuration properties and a runtime as input, prepare
    // and start a pipeline.
    private Pipeline prepareAndStartPipeline(TungstenProperties config,
            ReplicatorRuntime runtime) throws ReplicatorException,
            InterruptedException
    {
        runtime.configure();
        runtime.prepare();
        Pipeline pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());
        return pipeline;
    }

    // Insert a series of events into an in-memory queue.
    private void insertEvents(InMemoryQueueStore queue, int startSeqno,
            int numberOfEvents) throws InterruptedException,
            ReplicatorException
    {
        for (int i = 0; i < numberOfEvents; i++)
        {
            long seqno = startSeqno + i;
            ReplDBMSEvent event = helper.createEvent(seqno, "db0");
            queue.put(event);
        }
    }

    // Verify we have reached a specific sequence number.
    private void verifyProcessedSeqno(Pipeline pipeline, long seqno)
            throws InterruptedException, ExecutionException, TimeoutException
    {
        // Ensure 5 events are applied.
        Future<ReplDBMSHeader> future = pipeline
                .watchForCommittedSequenceNumber(seqno, false);
        ReplDBMSHeader matchingEvent = future.get(120, TimeUnit.SECONDS);
        assertEquals("Applied sequence number matches", seqno,
                matchingEvent.getSeqno());
    }
}