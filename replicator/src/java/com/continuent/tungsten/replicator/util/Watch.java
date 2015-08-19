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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

/**
 * Implements a "watch" operation that waits for a predicate to be fulfilled on
 * a particular event in an event processing queue.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges/a>
 */
public class Watch<E> implements Future<E>
{
    private static final Logger              logger        = Logger.getLogger(Watch.class);
    private final WatchPredicate<E>          predicate;
    private final WatchAction<E>             action;
    private final BlockingQueue<EventHolder> responseQueue = new LinkedBlockingQueue<EventHolder>();
    private final boolean[]                  matched;
    private boolean                          cancelled     = false;
    private boolean                          done          = false;

    // Defines a wrapper class to hold events in the queue. The wrapper
    // allows us to insert a null event for cancellation.
    class EventHolder
    {
        private final E event;

        EventHolder(E event)
        {
            this.event = event;
        }

        E getEvent()
        {
            return event;
        }
    }

    /**
     * Create watch with predicate and task count.
     */
    public Watch(WatchPredicate<E> predicate, int taskCount)
    {
        this(predicate, taskCount, null);
    }

    /**
     * Create watch with all components.
     * 
     * @param predicate Predicate to match
     * @param action Action to execute
     * @param taskCount Number of tasks that must report for a match
     */
    public Watch(WatchPredicate<E> predicate, int taskCount,
            WatchAction<E> action)
    {
        this.predicate = predicate;
        this.action = action;
        this.matched = new boolean[taskCount];
    }

    /**
     * Returns the watch predicate.
     */
    public WatchPredicate<E> getPredicate()
    {
        return predicate;
    }

    /**
     * Returns the action or null if no action is defined.
     */
    public WatchAction<E> getAction()
    {
        return action;
    }

    /**
     * Returns the array of matched conditions.
     */
    public boolean[] getMatched()
    {
        return matched;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.util.concurrent.Future#cancel(boolean)
     */
    public boolean cancel(boolean mayInterruptIfRunning)
    {
        try
        {
            responseQueue.put(new EventHolder(null));
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
        cancelled = true;
        done = true;
        return true;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.util.concurrent.Future#get()
     */
    public E get() throws InterruptedException, ExecutionException
    {
        EventHolder holder = responseQueue.take();
        if (cancelled)
            throw new CancellationException();
        else
        {
            done = true;
            return holder.getEvent();
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.util.concurrent.Future#get(long, java.util.concurrent.TimeUnit)
     */
    public E get(long timeout, TimeUnit unit) throws InterruptedException,
            ExecutionException, TimeoutException
    {
        EventHolder holder = responseQueue.poll(timeout, unit);
        if (cancelled)
            throw new CancellationException("This watch was cancelled");
        else if (holder == null)
        {
            logger.info("Watch timed out: " + this.toString());
            throw new TimeoutException();
        }
        else
        {
            done = true;
            return holder.getEvent();
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.util.concurrent.Future#isCancelled()
     */
    public boolean isCancelled()
    {
        return cancelled;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.util.concurrent.Future#isDone()
     */
    public boolean isDone()
    {
        return done;
    }

    /**
     * Offer an event to this watch instance. If it accepts the event we note
     * the task ID and return true.
     */
    public boolean offer(E event, int taskId) throws InterruptedException
    {
        assertNotDone();

        if (predicate.match(event))
        {
            // Log the match for this task.
            this.matched[taskId] = true;

            // If we are not completed we can just return true.
            for (boolean match : matched)
            {
                if (!match)
                    return true;
            }

            // All expected tasks have matched, so we need to report
            // we are done.
            responseQueue.put(new EventHolder(event));
            done = true;

            return true;
        }
        else
            return false;
    }

    private void assertNotDone()
    {
        if (done)
            throw new IllegalStateException(
                    "Operation submitted after watch completion");

    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        // Turn the list of matches into a string.
        StringBuffer matchString = new StringBuffer("[");
        for (int i = 0; i < matched.length; i++)
        {
            if (i > 0)
                matchString.append(",");
            matchString.append("[").append(i).append(":").append(matched[i])
                    .append("]");
        }
        matchString.append("]");
        return this.getClass().getSimpleName() + " predicate="
                + predicate.toString() + " done=" + done + " cancelled="
                + cancelled + " matched=" + matchString.toString();
    }
}