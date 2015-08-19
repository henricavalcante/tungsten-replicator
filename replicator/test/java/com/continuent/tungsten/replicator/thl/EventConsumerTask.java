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

import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.storage.InMemoryTransactionalQueue;

/**
 * Consumes transactions from a queue, keeping count of the transactions read as
 * well as the maximum sequence number.
 */
public class EventConsumerTask implements Runnable
{
    private static Logger                    logger   = Logger.getLogger(EventConsumerTask.class);

    // Parameters.
    private final InMemoryTransactionalQueue queue;
    private final long                       expectedEvents;

    // Number of transactions & events generated.
    private volatile int                     events   = 0;
    private volatile long                    maxSeqno = -1;
    private volatile Exception               exception;
    private volatile boolean                 done     = false;

    /**
     * Instantiates a consumer task.
     * 
     * @param queue Queue from which to read transactions
     */
    public EventConsumerTask(InMemoryTransactionalQueue queue,
            long expectedEvents)
    {
        this.queue = queue;
        this.expectedEvents = expectedEvents;
    }

    public int getEvents()
    {
        return events;
    }

    public long getMaxSeqno()
    {
        return maxSeqno;
    }

    public Exception getException()
    {
        return exception;
    }

    public boolean isDone()
    {
        return done;
    }

    /**
     * Run until there are no more events.
     * 
     * @see java.lang.Runnable#run()
     */
    public void run()
    {
        try
        {
            // Read events from queue.
            while (events < expectedEvents)
            {
                ReplDBMSEvent rde = queue.get();
                long seqno = rde.getSeqno();
                if (seqno > maxSeqno)
                    maxSeqno = seqno;
                events++;
                if ((events % 10000) == 0)
                {
                    logger.info("Consuming events: events=" + events
                            + " maxSeqno=" + maxSeqno);
                }
            }
            logger.info("Finished reading events from queue: events=" + events
                    + " maxSeqno=" + maxSeqno);
        }
        catch (InterruptedException e)
        {
            logger.info("Event consumer task loop interrupted");
        }
        catch (Exception e)
        {
            logger.error("Consumer loop failed!", e);
            exception = e;
        }
        catch (Throwable t)
        {
            logger.error("Consumer loop failed!", t);
        }
        finally
        {
            done = true;
        }
    }
}