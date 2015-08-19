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

package com.continuent.tungsten.replicator.storage.parallel;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.ReplControlEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplEvent;
import com.continuent.tungsten.replicator.extractor.ExtractorException;
import com.continuent.tungsten.replicator.extractor.ParallelExtractor;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Implements ParallelExtractor interface for a parallel queue.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */

public class ParallelQueueExtractor implements ParallelExtractor
{
    private static Logger      logger    = Logger.getLogger(ParallelQueueExtractor.class);

    private int                taskId    = -1;
    private String             storeName;
    private ParallelQueueStore parallelQueue;

    // Last extracted seqno set by task. Skip any event before or equal to this
    // sequence number.
    private long               lastSeqno = -1;

    /**
     * Instantiate the adapter.
     */
    public ParallelQueueExtractor()
    {
    }

    public String getStoreName()
    {
        return storeName;
    }

    public void setStoreName(String storeName)
    {
        this.storeName = storeName;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.Extractor#extract()
     */
    public ReplEvent extract() throws ReplicatorException, InterruptedException
    {
        // Get next event. We use a loop to ensure any previously seen events
        // are thrown away.
        for (;;)
        {
            try
            {
                ReplEvent replEvent = parallelQueue.get(taskId);
                if (replEvent != null)
                {
                    // Return all events that are past the restart point.
                    if (replEvent.getSeqno() > this.lastSeqno)
                    {
                        return replEvent;
                    }
                    // Always return a stop event.
                    else if (replEvent instanceof ReplControlEvent
                            && ((ReplControlEvent) replEvent).getEventType() == ReplControlEvent.STOP)
                    {
                        return replEvent;
                    }
                }
            }
            catch (ReplicatorException e)
            {
                throw new ExtractorException(
                        "Unable to extract event from parallel queue: name="
                                + storeName, e);
            }
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.Extractor#getCurrentResourceEventId()
     */
    public String getCurrentResourceEventId() throws ReplicatorException,
            InterruptedException
    {
        try
        {
            ReplEvent event = parallelQueue.peek(taskId);
            if (event == null)
                return null;
            else if (event instanceof ReplDBMSEvent)
                return ((ReplDBMSEvent) event).getEventId();
            else if (event instanceof ReplControlEvent)
            {
                ReplDBMSHeader event2 = ((ReplControlEvent) event).getHeader();
                if (event2 == null)
                    return null;
                else
                    return event2.getEventId();
            }
            else
            {
                // This should not happen.
                logger.warn("Returned unexpected event type from peek operation: "
                        + event.getClass().toString());
                return null;
            }
        }
        catch (ReplicatorException e)
        {
            throw new ExtractorException(
                    "Unable to extract event from parallel queue: name="
                            + storeName, e);
        }
    }

    /**
     * Returns true if the queue has more events. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.Extractor#hasMoreEvents()
     */
    public boolean hasMoreEvents()
    {
        return parallelQueue.size(taskId) > 0;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException
    {
    }

    /**
     * Connect to underlying queue. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException
    {
        try
        {
            parallelQueue = (ParallelQueueStore) context.getStore(storeName);
        }
        catch (ClassCastException e)
        {
            throw new ReplicatorException(
                    "Invalid storage class; configuration may be in error: "
                            + context.getStore(storeName).getClass().getName());
        }
        if (parallelQueue == null)
            throw new ReplicatorException(
                    "Unknown storage name; configuration may be in error: "
                            + storeName);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException
    {
        parallelQueue = null;
    }

    /**
     * Return the header, which should have been place here by an extractor
     * during restart.
     * 
     * @see com.continuent.tungsten.replicator.applier.Applier#getLastEvent()
     */
    public ReplDBMSHeader getLastEvent() throws ReplicatorException,
            InterruptedException
    {
        return parallelQueue.getLastHeader(taskId);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.ParallelApplier#setTaskId(int)
     */
    public void setTaskId(int id)
    {
        this.taskId = id;
    }

    /**
     * Store the header so that it can be propagated back through the pipeline
     * for restart. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.Extractor#setLastEvent(com.continuent.tungsten.replicator.event.ReplDBMSHeader)
     */
    public void setLastEvent(ReplDBMSHeader header) throws ReplicatorException
    {
        parallelQueue.setLastHeader(taskId, header);
        if (header != null)
            this.lastSeqno = header.getSeqno();
    }

    /**
     * Ignored for now as in-memory queues do not extract. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.Extractor#setLastEventId(java.lang.String)
     */
    public void setLastEventId(String eventId) throws ReplicatorException
    {
        logger.warn("Attempt to set last event ID on queue storage: " + eventId);
    }
}
