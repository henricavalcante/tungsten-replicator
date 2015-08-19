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

import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;

/**
 * Manages a list of event watches and allows clients to submit events to the
 * list for processing to see if there is a predicate match. Methods are
 * synchronized to ensure the object is updated transactionally and to ensure
 * proper visibility across threads.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class WatchManager<E>
{
    private static Logger  logger    = Logger.getLogger(WatchManager.class);
    private List<Watch<E>> watchList = new Vector<Watch<E>>();
    boolean                cancelled = false;

    public WatchManager()
    {
    }

    /**
     * Adds a new watch predicate to the queue including an accompanying action.
     */
    public synchronized Watch<E> watch(WatchPredicate<E> predicate,
            int taskCount, WatchAction<E> action)
    {
        assertNotCancelled();
        Watch<E> watch = new Watch<E>(predicate, taskCount, action);
        watchList.add(watch);
        return watch;
    }

    /**
     * Adds a new watch predicate to the queue.
     */
    public synchronized Watch<E> watch(WatchPredicate<E> predicate,
            int taskCount)
    {
        return watch(predicate, taskCount, null);
    }

    /**
     * Returns the current list of watches.
     */
    public synchronized List<Watch<E>> getWatches()
    {
        return watchList;
    }

    /**
     * Submits an event for watch processing. This automatically dequeues any
     * matching watch instances and informs the watchers.
     * 
     * @param event An event for processing.
     * @param taskId Id of task for which we are checking the predicate
     * @throws InterruptedException
     */
    public synchronized void process(E event, int taskId)
            throws InterruptedException
    {
        assertNotCancelled();
        // Walk backwards down list to avoid ConcurrentModificationException
        // from using an Iterator. Note we also clean out anything that is
        // done; this is how cancelled watches are removed.
        for (int i = watchList.size() - 1; i >= 0; i--)
        {
            Watch<E> watch = watchList.get(i);
            if (watch.isDone())
                watchList.remove(watch);
            else if (watch.offer(event, taskId))
            {
                if (logger.isDebugEnabled())
                {
                    logger.debug("Watch succeeded: taskId=" + taskId
                            + " watch=" + watch.toString());
                }
                // Execute the watch action.
                WatchAction<E> action = watch.getAction();
                if (action != null)
                {
                    action.matched(event, taskId);
                }

                // Dequeue if watch is fulfilled.
                if (watch.isDone())
                {
                    if (logger.isDebugEnabled())
                    {
                        logger.debug("Watch completed: watch="
                                + watch.toString());
                    }
                    watchList.remove(watch);
                }
            }
        }
    }

    /**
     * Cancel all pending watches.
     */
    public synchronized void cancelAll()
    {
        assertNotCancelled();
        for (Watch<E> w : watchList)
        {
            logger.info("Cancelling pending watch: " + w.toString());
            w.cancel(true);
        }
        cancelled = true;
    }

    private void assertNotCancelled()
    {
        if (cancelled)
            throw new IllegalStateException(
                    "Operation submitted after cancellation");
    }
}