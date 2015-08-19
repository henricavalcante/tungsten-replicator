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

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import com.continuent.tungsten.replicator.event.ReplControlEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplEvent;
import com.continuent.tungsten.replicator.util.AtomicIntervalGuard;
import com.continuent.tungsten.replicator.util.SeqnoWatchPredicate;

/**
 * Tests operations on the THLParallelReadQueue class, which is responsible for
 * merging control events used to manage parallel execution with normal events
 * read from the replicator log.
 */
public class THLParallelReadQueueTest
{
    private static Logger          logger = Logger.getLogger(THLParallelReadQueueTest.class);

    // Test helper instance.
    private THLParallelQueueHelper helper = new THLParallelQueueHelper();

    /*
     * Verify that we can instantiate and release the parallel read queue.
     */
    @Test
    public void testInit() throws Exception
    {
        logger.info("##### testInit #####");
        THLParallelReadQueue prq = new THLParallelReadQueue(0, 10, 10, 0, 100,
                null, new AtomicIntervalGuard<Object>(1));
        Assert.assertEquals(0, prq.getAcceptCount());
        Assert.assertEquals(0, prq.getDiscardCount());
        prq.release();
    }

    /**
     * Verify that a read queue accepts events and returns them in the same
     * order.
     */
    @Test
    public void testPost() throws Exception
    {
        logger.info("##### testPost #####");
        THLParallelReadQueue prq = new THLParallelReadQueue(0, 100, 100, -1,
                100, null, new AtomicIntervalGuard<Object>(1));

        // Enqueue 50 events.
        this.genEvents(prq, 50, -1, -1);

        // Ensure we got 50 events back in the correct order.
        this.checkEvents(prq, 50, 0);
    }

    /**
     * Verify that a read queue correctly adds control events at the right
     * points in the event flow.
     */
    @Test
    public void testControlEvents() throws Exception
    {
        logger.info("##### testControlEvents #####");
        THLParallelReadQueue prq = new THLParallelReadQueue(0, 100, 100, -1,
                100, null, new AtomicIntervalGuard<Object>(1));

        // Enqueue 5 control events in advance.
        for (int i = 0; i < 5; i++)
        {
            long seqno = i * 10;
            ReplDBMSEvent rde = helper.createEvent(seqno, "db" + (i % 3));
            ReplControlEvent ce = new ReplControlEvent(ReplControlEvent.SYNC,
                    seqno, rde);
            prq.postOutOfBand(ce);
        }

        // Enqueue 50 events with additional embedded control events.
        this.genEvents(prq, 50, 5, 10);

        // Ensure we got 50 events and 10 control events in the correct order.
        this.checkEvents(prq, 50, 10);
    }

    /**
     * Verify that automatic synchronization events are generated at the
     * expected synchronization intervals.
     */
    @Test
    public void testAutoSyncEvents() throws Exception
    {
        logger.info("##### testAutoSyncEvents #####");

        // Initialize queue with synchronization set for every 10 events.
        THLParallelReadQueue prq = new THLParallelReadQueue(0, 100, 100, -1,
                10, null, new AtomicIntervalGuard<Object>(1));

        // Enqueue 50 events with no additional embedded control events. With
        // sync every 10 events, we should get 5 control events.
        this.genEvents(prq, 50, -1, -1);

        // Ensure we got 50 events and 5 automatically generated control events
        // in the correct order.
        this.checkEvents(prq, 50, 5);
    }

    /**
     * Verify that if we register a predicate to find a particular sequence
     * number a control event will be generated in sequence for each one.
     */
    @Test
    public void testPredicates() throws Exception
    {
        logger.info("##### testPredicates #####");
        THLParallelReadQueue prq = new THLParallelReadQueue(0, 100, 100, -1,
                100, null, new AtomicIntervalGuard<Object>(1));

        // Declare a predicate to find seqno's at beginning, middle, and end
        // of events in queue.
        prq.addWatchSyncPredicate(new SeqnoWatchPredicate(0));
        prq.addWatchSyncPredicate(new SeqnoWatchPredicate(2));
        prq.addWatchSyncPredicate(new SeqnoWatchPredicate(4));

        // Enqueue 5 events with no embedded control events.
        this.genEvents(prq, 5, -1, -1);

        // Ensure we got 5 events and 3 control events in the correct order.
        this.checkEvents(prq, 5, 3);
    }

    /**
     * Verify that if we generate a control event from before the current
     * position of the event queue, a control event is immediately generated for
     * the current seqno.
     */
    @Test
    public void testOutOfOrderControlEvent() throws Exception
    {
        logger.info("##### testPredicates #####");
        THLParallelReadQueue prq = new THLParallelReadQueue(0, 100, 100, -1,
                100, null, new AtomicIntervalGuard<Object>(1));

        // Enqueue 5 events with no embedded control events.
        this.genEvents(prq, 5, -1, -1);

        // Ensure we got 5 events in the correct order.
        this.checkEvents(prq, 5, 0);

        // Generate an out-of-order event.
        ReplDBMSEvent rde = helper.createEvent(2, "db2");
        ReplControlEvent ce = new ReplControlEvent(ReplControlEvent.SYNC,
                rde.getSeqno(), rde);
        prq.postOutOfBand(ce);

        // Ensure we got 1 control event starting with the current seqno.
        this.checkEvents(prq, 0, 1, 4);
    }

    /**
     * Verify that if we generate a predicate for a seqno before the current
     * position of the event queue, a control event is immediately generated for
     * the current seqno.
     */
    @Test
    public void testOutOfOrderPredicate() throws Exception
    {
        logger.info("##### testOutOfOrderPredicate #####");
        THLParallelReadQueue prq = new THLParallelReadQueue(0, 100, 100, -1,
                100, null, new AtomicIntervalGuard<Object>(1));

        // Enqueue 5 events with no embedded control events.
        this.genEvents(prq, 5, -1, -1);

        // Ensure we got 5 events in the correct order.
        this.checkEvents(prq, 5, 0);

        // Add an out-of-order predicate.
        prq.addWatchSyncPredicate(new SeqnoWatchPredicate(2));

        // Ensure we got 1 control event starting with the current seqno.
        this.checkEvents(prq, 0, 1, 4);
    }

    /**
     * Verify that if we register predicates and insert control events separate
     * control events will be generated for each.
     */
    @Test
    public void testPredicatesPlusCtrl() throws Exception
    {
        logger.info("##### testPredicatesPlusCtrl #####");
        THLParallelReadQueue prq = new THLParallelReadQueue(0, 100, 100, -1,
                100, null, new AtomicIntervalGuard<Object>(1));

        // Declare a predicate to find seqnos 4 and 49. This will
        // generate 2 extra control events, one of which should overlap
        // an embedded event.
        prq.addWatchSyncPredicate(new SeqnoWatchPredicate(5));
        prq.addWatchSyncPredicate(new SeqnoWatchPredicate(49));

        // Enqueue 50 events with 5 additional embedded control events.
        this.genEvents(prq, 50, 4, 10);

        // Check resulting flow for 50 events plus 7 control events.
        this.checkEvents(prq, 50, 7);
    }

    /**
     * Verify that control events are always generated after the last fragment
     * of the transaction which they follow.
     */
    @Test
    public void testFragmentsAndControlEvents() throws Exception
    {
        logger.info("##### testFragmentsAndControlEvents #####");
        THLParallelReadQueue prq = new THLParallelReadQueue(0, 100, 100, -1,
                100, null, new AtomicIntervalGuard<Object>(1));

        // Enqueue 5 fragmented transactions with a predicate and a control
        // event inserted after fragment.
        for (int i = 0; i < 5; i++)
        {
            for (short frag = 0; frag < 3; frag++)
            {
                // Create and add event.
                THLEvent thlEvent = this.genEvent(i, frag, frag >= 2, "db" + i);
                prq.post(thlEvent);

                // Add watches and control events depending on the fragment
                // number.
                if (frag == 0)
                {
                    // Add a control event after the first fragment.
                    ReplControlEvent ce = new ReplControlEvent(
                            ReplControlEvent.SYNC, i,
                            (ReplDBMSEvent) thlEvent.getReplEvent());
                    prq.postOutOfBand(ce);
                }
                else if (frag == 1)
                {
                    // Add a watch after the second fragment.
                    prq.addWatchSyncPredicate(new SeqnoWatchPredicate(i));
                }
            }
        }

        // Ensure we got 5 * 3 events in the correct order with 10 control
        // events.
        this.checkEvents(prq, 15, 10);
    }

    /**
     * Generate events with intermixed control events.
     * 
     * @param prq Read queue that is being tested
     * @param count Number of normal events
     * @param ctrlOffset Seqno of first control event (-1 to omit)
     * @param ctrlRepeat Gap between seqno's of control events
     */
    private void genEvents(THLParallelReadQueue prq, int count, int ctrlOffset,
            int ctrlRepeat) throws InterruptedException
    {
        // Enqueue events with additional embedded control events.
        for (int i = 0; i < count; i++)
        {
            // Add event.
            ReplDBMSEvent rde = helper.createEvent(i, "db" + (i % 3));
            THLEvent thlEvent = new THLEvent(rde.getSourceId(), rde);
            prq.post(thlEvent);

            // Starting with seqno ctrlOffset, generate a new control event
            if (ctrlOffset > -1 && ((i - ctrlOffset) % ctrlRepeat) == 0)
            {
                ReplControlEvent ce = new ReplControlEvent(
                        ReplControlEvent.SYNC, i, rde);
                prq.postOutOfBand(ce);
            }
        }

        // Ensure we accepted count events with 0 discards.
        Assert.assertEquals(count, prq.getAcceptCount());
        Assert.assertEquals(0, prq.getDiscardCount());
    }

    /**
     * Generate a single event fragment.
     * 
     * @return
     */
    private THLEvent genEvent(long seqno, short fragNo, boolean lastFrag,
            String shardId)
    {
        ReplDBMSEvent rde = helper
                .createEvent(seqno, fragNo, lastFrag, shardId);
        THLEvent thlEvent = new THLEvent(rde.getSourceId(), rde);
        return thlEvent;
    }

    /**
     * Generate events with intermixed control events.
     * 
     * @param prq Read queue that is being tested
     * @param expectedCount Expected number of normal events
     * @param expectedCtrlEvents Expected number of control events
     */
    private void checkEvents(THLParallelReadQueue prq, int expectedEvents,
            int expectedCtrlEvents, long lastSeqno) throws InterruptedException
    {
        int events = 0;
        int ctrlEvents = 0;
        boolean lastFrag = true;
        while (prq.peek() != null)
        {
            // Make sure seqno is increasing.
            ReplEvent event = prq.take();
            if (event instanceof ReplDBMSEvent)
            {
                lastFrag = ((ReplDBMSEvent) event).getLastFrag();
            }
            logger.info("Found event: seqno=" + event.getSeqno() + " lastFrag="
                    + lastFrag + " type=" + event.getClass().getSimpleName());
            Assert.assertTrue("Checking for increasing seqno: current seqno="
                    + event.getSeqno() + " last seqno=" + lastSeqno,
                    event.getSeqno() >= lastSeqno);
            lastSeqno = event.getSeqno();

            if (event instanceof ReplDBMSEvent)
                events++;
            else if (event instanceof ReplControlEvent)
            {
                ctrlEvents++;
                Assert.assertEquals(
                        "Ensure control event generated after last frag: last seqno="
                                + lastSeqno + " lastFrag=" + lastFrag, true,
                        lastFrag);
            }
        }
        Assert.assertEquals("Check count of returned events", expectedEvents,
                events);
        Assert.assertEquals("Check count of returned ctrl events",
                expectedCtrlEvents, ctrlEvents);
    }

    /**
     * Convenience method with zero start seqno.
     */
    private void checkEvents(THLParallelReadQueue prq, int expectedEvents,
            int expectedCtrlEvents) throws InterruptedException
    {
        checkEvents(prq, expectedEvents, expectedCtrlEvents, 0);
    }
}