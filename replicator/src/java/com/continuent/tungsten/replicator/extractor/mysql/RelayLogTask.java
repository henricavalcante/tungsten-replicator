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

package com.continuent.tungsten.replicator.extractor.mysql;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.extractor.ExtractorException;

/**
 * Implements a task that runs in a separate thread to manage relay logs.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class RelayLogTask implements Runnable
{
    private static Logger    logger    = Logger.getLogger(RelayLogTask.class);
    private RelayLogClient   relayClient;
    private volatile boolean cancelled = false;
    private volatile boolean finished  = false;

    /**
     * Creates a new relay log task.
     */
    public RelayLogTask(RelayLogClient relayClient)
    {
        this.relayClient = relayClient;
    }

    /**
     * Extracts from the relay log until cancelled or we fail.
     */
    public void run()
    {
        logger.info("Relay log task starting: "
                + Thread.currentThread().getName());
        try
        {
            while (!cancelled && !Thread.currentThread().isInterrupted())
            {
                if (!relayClient.processEvent())
                {
                    if (cancelled)
                    {
                        logger.info("Event processing was cancelled. Returning without processing event.");
                        return;
                    }
                    else
                    {
                        throw new ExtractorException(
                                "Network download of binlog failed; may indicated that MySQL terminated the connection.  Check your serverID setting!");
                    }

                }
            }
        }
        catch (InterruptedException e)
        {
            logger.info("Relay log task cancelled by interrupt");
        }
        catch (Throwable t)
        {
            logger.error(
                    "Relay log task failed due to exception: " + t.getMessage(),
                    t);
        }
        finally
        {
            relayClient.disconnect();
        }

        logger.info("Relay log task ending: "
                + Thread.currentThread().getName());
        finished = true;
    }

    /**
     * Signal that the task should end.
     */
    public void cancel()
    {
        cancelled = true;
    }

    /**
     * Returns true if the task has completed.
     */
    public boolean isFinished()
    {
        return finished;
    }

    /**
     * Returns the current relay log position.
     */
    public RelayLogPosition getPosition()
    {
        return relayClient.getPosition();
    }
}