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

package com.continuent.tungsten.common.concurrent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * This class implements a wrapper around the Java ExecutorService interface to
 * present a simplified interface for spawning and managing tasks. Tasks
 * submitted to the service return a particular type, which is used to
 * parameterize this class.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class SimpleJobService<V>
{
    private ThreadPoolExecutor pool;

    /**
     * Creates the job service.
     * 
     * @param name Name of this service, which will be prefixed to threads
     * @param maxThreads Maximum number of threads in the pool; must be at least
     *            1
     * @param maxRequests Maximum number of pending requests; must be at least 1
     * @param keepAlive Number of seconds to retain unused threads
     */
    public SimpleJobService(String name, int maxThreads, int maxRequests,
            int keepAlive)
    {
        BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>(
                maxRequests);
        ThreadFactory factory = new SimpleThreadFactory(name);
        pool = new ThreadPoolExecutor(maxThreads, maxThreads, keepAlive,
                TimeUnit.SECONDS, queue, factory);
    }

    /**
     * Submits a task for execution.
     * 
     * @return A Future that allows clients to fetch result of running task.
     * @throws RejectedExecutionException Thrown if the task cannot be
     *             submitted, typically because the request queue is full
     */
    public synchronized Future<V> submit(Callable<V> task)
            throws RejectedExecutionException
    {
        return pool.submit(task);
    }

    /**
     * Returns the number of tasks that are submitted and awaiting execution.
     * This number is approximate.
     */
    public int getPendingTaskCount()
    {
        return pool.getQueue().size();
    }

    /**
     * Returns the number of tasks that are currently executing. This number is
     * approximate.
     */
    public int getActiveTaskCount()
    {
        return pool.getQueue().size();
    }

    /**
     * Shut down the job service by letting current tasks run to completion but
     * accepting no further tasks.
     */
    public void shutdown()
    {
        pool.shutdown();
    }

    /**
     * Shut down the job service immediately. This may leave tasks in odd states
     * depending how well they clean up.
     */
    public void shutdownNow()
    {
        pool.shutdownNow();
    }
}