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
 * Initial developer(s):  Robert Hodges
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.pipeline;

import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplEvent;

/**
 * Denotes a schedule, which monitors and directs task execution.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface Schedule
{
    /**
     * Task must call this method after extracting event but before processing
     * to decide disposition.
     * 
     * @return A disposition for the event.
     * @throws InterruptedException Thrown if thread is interrupted
     */
    public int advise(ReplEvent replEvent) throws InterruptedException;

    /**
     * Task must call this method before exit to tell the schedule that it has
     * completed.
     */
    public void taskEnd();

    /**
     * Set the last processed event, which triggers checks for watches. If a
     * fulfilled watch directs the task to terminate, the isCancelled call will
     * return true.
     * 
     * @throws InterruptedException Thrown if thread is interrupted.
     */
    public void setLastProcessedEvent(ReplDBMSHeader event)
            throws InterruptedException;

    /**
     * Marks the last processed exception as committed. This information is used
     * by upstream stages that implement synchronous pipeline processing, e.g.,
     * not dropping logs before they are safely committed downstream.
     * 
     * @throws InterruptedException Thrown if thread is interrupted.
     */
    public void commit() throws InterruptedException;

    /**
     * Returns true if the task is canceled. Tasks must check this each
     * iteration to decide whether to continue.
     */
    public boolean isCancelled();

    // Processing dispositions for events.

    /** Proceed with event processing. */
    public static int PROCEED              = 1;

    /** Commit current transaction and terminate task processing loop. */
    public static int QUIT                 = 2;

    /** Continue with the next event. */
    public static int CONTINUE_NEXT        = 3;

    /** Continue with next event but commit current position. */
    public static int CONTINUE_NEXT_COMMIT = 4;
}