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

package com.continuent.tungsten.replicator.util;

import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;

import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;

/**
 * This class tests the EventWatcher class and event watches.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class TestEventWatcher extends TestCase
{
    class StringWatchPredicate implements WatchPredicate<String>
    {
        private final String string;

        StringWatchPredicate(String s)
        {
            this.string = s;
        }

        public boolean match(String event)
        {
            return string.equals(event);
        }
    }

    class StringWatchAction implements WatchAction<String>
    {
        private String[] string;

        StringWatchAction(int taskCount)
        {
            string = new String[taskCount];
        }

        public void matched(String event, int taskId)
        {
            this.string[taskId] = event;
        }

        public String getString(int taskId)
        {
            return this.string[taskId];
        }
    }

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
     * Prove that we can implement a simple watch for String events.
     */
    public void testSimpleWatchGet() throws Exception
    {
        WatchManager<String> em = new WatchManager<String>();
        Watch<String> w = em.watch(new StringWatchPredicate("hello"), 1);

        // Submit a non-matching string.
        em.process("hello!", 0);
        assertFalse("Not found yet", w.isDone());

        // Submit a matching string.
        em.process("hello", 0);
        assertTrue("Should be done", w.isDone());
        assertFalse("Should not be cancelled", w.isCancelled());
        assertEquals("Should have string event", "hello", w.get());
    }

    /**
     * Prove that an action associated with a watch is executed when the watch
     * matches.
     */
    public void testSimpleWatchAction() throws Exception
    {
        WatchManager<String> em = new WatchManager<String>();
        StringWatchAction action = new StringWatchAction(1);
        Watch<String> w = em
                .watch(new StringWatchPredicate("hello"), 1, action);

        // Submit a non-matching string.
        em.process("hello!", 0);
        assertNull("String not matched yet", action.getString(0));

        // Submit a matching string.
        em.process("hello", 0);
        assertEquals("Should have string", "hello", action.getString(0));
        assertTrue("Should be done", w.isDone());
    }

    /**
     * Prove that get() with timeout returns when the watch is done and times
     * out when it is not done.
     */
    public void testTimedWatchGet() throws Exception
    {
        WatchManager<String> em = new WatchManager<String>();
        Watch<String> w = em.watch(new StringWatchPredicate("hello"), 1);

        // Should timeout now.
        assertFalse(w.isDone());
        try
        {
            w.get(1, TimeUnit.SECONDS);
            throw new Exception(
                    "get() did not timeout when there was nothing to return");
        }
        catch (TimeoutException e)
        {
            // OK
        }

        // Should work after submitting a matching event.
        em.process("hello", 0);
        assertTrue("Should be done", w.isDone());
        assertFalse("Should not be cancelled", w.isCancelled());
        assertEquals("Should have string event", "hello", w.get(1,
                TimeUnit.SECONDS));
    }

    /**
     * Prove that we can cancel a simple watch for String events.
     */
    public void testWatchCancellation() throws Exception
    {
        WatchManager<String> em = new WatchManager<String>();
        Watch<String> w = em.watch(new StringWatchPredicate("hello"), 1);

        // Cancel the watch.
        w.cancel(true);
        assertTrue("Should be done", w.isDone());
        assertTrue("Should be cancelled", w.isCancelled());
        try
        {
            w.get();
            throw new Exception(
                    "Did not get cancellation exception from cancelled watch");
        }
        catch (CancellationException e)
        {
            // OK.
        }
    }

    /**
     * Prove that we can cancel all watches by canceling the EventWatcher.
     */
    public void testEventWatcherCancellation() throws Exception
    {
        WatchManager<String> em = new WatchManager<String>();
        List<Watch<String>> watches = new LinkedList<Watch<String>>();
        watches.add(em.watch(new StringWatchPredicate("hello"), 1));
        watches.add(em.watch(new StringWatchPredicate("goodbye"), 1));
        watches.add(em.watch(new StringWatchPredicate("hello again"), 1));

        em.cancelAll();
        for (Watch<String> w : watches)
        {
            assertTrue("Should be done", w.isDone());
            assertTrue("Should be cancelled", w.isCancelled());
            try
            {
                w.get();
                throw new Exception(
                        "Did not get cancellation exception from cancelled watch");
            }
            catch (CancellationException e)
            {
                // OK.
            }
        }
    }

    /**
     * Verify that a timestamp watch is triggered by an event with a greater
     * equal source timestamp.
     */
    public void testTimestampWatch() throws Exception
    {
        WatchManager<ReplDBMSHeader> em = new WatchManager<ReplDBMSHeader>();
        long currentTimeMillis = System.currentTimeMillis();
        Watch<ReplDBMSHeader> w = em.watch(new SourceTimestampWatchPredicate(
                new Timestamp(currentTimeMillis + 1)), 1);

        // Should ignore an earlier event. Note the source event is on the
        // earlier time.
        DBMSEvent dbmsEvent1 = new DBMSEvent("1", null, null, new Timestamp(
                currentTimeMillis));
        ReplDBMSEvent replEvent1 = new ReplDBMSEvent(1, (short) 0, true,
                "source", 0, new Timestamp(currentTimeMillis), dbmsEvent1);
        em.process(replEvent1, 0);
        assertFalse("Should not be done", w.isDone());

        // Should accept an event at the same time.
        DBMSEvent dbmsEvent2 = new DBMSEvent("1", null, null, new Timestamp(
                currentTimeMillis + 1));
        ReplDBMSEvent event2 = new ReplDBMSEvent(1, (short) 0, true, "source",
                0, new Timestamp(currentTimeMillis + 1), dbmsEvent2);
        em.process(event2, 0);
        assertTrue("Should be done", w.isDone());
        assertEquals("Should return event", event2, w.get(1, TimeUnit.SECONDS));
    }

    /**
     * Verify that watches correctly handle multiple tasks, specifically: (a)
     * that we correctly handle match for a particular task and (b) only signal
     * we are done when all tasks match.
     */
    public void testSimpleWatchGetMulti() throws Exception
    {
        // Set up a watch for 3 tasks.
        WatchManager<String> em = new WatchManager<String>();
        Watch<String> w = em.watch(new StringWatchPredicate("hello"), 3);

        // Confirm that we get a match for each task but that we are not
        // done before all three matches succeed.
        for (int i = 0; i < 3; i++)
        {
            assertFalse("Match not done before iteration: " + i, w.isDone());
            em.process("hello", i);
        }

        // Ensure watch is now done after all three succeed.
        assertTrue("Should be done", w.isDone());
        assertFalse("Should not be cancelled", w.isCancelled());
        assertEquals("Should have string event", "hello", w.get());
    }

    /**
     * Prove that an action associated with a watch is executed on the task ID
     * for which the watch has just matched.
     */
    public void testSimpleWatchActionMulti() throws Exception
    {
        // Set up a watch for 3 tasks with corresponding action.
        WatchManager<String> em = new WatchManager<String>();
        StringWatchAction action = new StringWatchAction(3);
        Watch<String> w = em
                .watch(new StringWatchPredicate("hello"), 3, action);

        // Confirm no actions have been taken to start and that we take action
        // on each succeeding task ID as we match.
        for (int i = 0; i < 3; i++)
        {
            assertNull("Action not taken before match: " + i, action
                    .getString(i));
            em.process("hello", i);
            assertEquals("Should have string after match: " + i, "hello",
                    action.getString(i));
        }
        assertTrue("Should be done", w.isDone());
    }
}