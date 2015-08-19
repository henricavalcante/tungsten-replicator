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
 * Contributor(s): Linas Virbalas
 */
package com.continuent.tungsten.replicator.filter;

import java.sql.Timestamp;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Filter to delay a transaction until a particular point in time has passed.
 * The time delay filter uses the originating timestamp from the replication
 * event to just time delays. We assume that clocks are synchronized to within
 * some reasonable precision between event producers and consumers.
 * <strong>Note</strong>: This filter may cause problems if used on a master. It
 * should only be used as an applier filter.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class TimeDelayFilter implements Filter
{
    private static Logger logger = Logger.getLogger(TimeDelayFilter.class);
    private long timeDelayMillis = 0;

    /**
     * Sets the time delay in seconds. 
     */
    public void setDelay(long timeDelaySeconds)
    {
        timeDelayMillis = timeDelaySeconds * 1000;
    }

    /**
     * Implements a delay in processing the event.  
     * {@inheritDoc}
     * @see com.continuent.tungsten.replicator.filter.Filter#filter(com.continuent.tungsten.replicator.event.ReplDBMSEvent)
     */
    public ReplDBMSEvent filter(ReplDBMSEvent event) throws ReplicatorException, InterruptedException
    {
        // Compute the interval that we should delay.  
        Timestamp sourceTstamp = event.getDBMSEvent().getSourceTstamp(); 
        long futureTime = sourceTstamp.getTime() + timeDelayMillis; 
        long intervalMillis = futureTime - System.currentTimeMillis(); 

        // Sleep until it is time to deliver this event.  We let
        // InterruptedException flow through or the replicator will not 
        // be able to shut down. 
        if (intervalMillis > 0)
            Thread.sleep(intervalMillis); 

        return event; 
    }

    /**
     * {@inheritDoc}
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException
    {
        logger.info("Time delay filtering: event delivery delay set to "
                + (timeDelayMillis / 1000) + " seconds");
    }

    /**
     * {@inheritDoc}
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException
    {
        // Nothing to be done. 
    }

    /**
     * {@inheritDoc}
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException
    {
        // Nothing to be done. 
    }
}
