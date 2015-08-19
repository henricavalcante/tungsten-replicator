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

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.thl.log.LogConnection;

/**
 * Runnable to create transactions for test and report the number thereof as
 * well as any exceptions.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class EventProducerTask implements Runnable
{
    private static Logger      logger   = Logger.getLogger(EventProducerTask.class);

    // Parameters.
    private EventProducer      producer;
    private THL                thl;

    // Number of transactions & events generated.
    private volatile int       events   = 0;
    private volatile long      maxSeqno = -1;
    private volatile Exception exception;
    private volatile boolean   done     = false;

    /**
     * Instantiates a generator task.
     * 
     * @param generator Generates transactions
     * @param thl Log
     */
    public EventProducerTask(EventProducer generator, THL thl)
    {
        this.producer = generator;
        this.thl = thl;
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
        LogConnection conn = null;
        try
        {
            // Connect to log.
            conn = thl.connect(false);

            // Write events from generator.
            ReplDBMSEvent rde;
            while ((rde = producer.nextEvent()) != null)
            {
                maxSeqno = rde.getSeqno();
                THLEvent thlEvent = new THLEvent(rde.getSourceId(), rde);
                conn.store(thlEvent, false);
                conn.commit();
                events++;
            }
            logger.info("Finished writing events to log: events=" + events
                    + " maxSeqno=" + maxSeqno);
        }
        catch (InterruptedException e)
        {
            logger.info("Event generator task loop interrupted");
        }
        catch (Exception e)
        {
            logger.error("Generation loop failed!", e);
            exception = e;
        }
        catch (Throwable t)
        {
            logger.error("Generation loop failed!", t);
        }
        finally
        {
            if (conn != null)
            {
                try
                {
                    thl.disconnect(conn);
                }
                catch (ReplicatorException e)
                {
                    logger.warn(
                            "Unable to disconnect from log after generating events",
                            e);
                }
            }
            done = true;
        }
    }
}