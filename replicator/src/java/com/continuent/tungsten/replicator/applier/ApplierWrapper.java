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
 * Contributor(s): Stephane Giron
 */

package com.continuent.tungsten.replicator.applier;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.consistency.ConsistencyException;
import com.continuent.tungsten.replicator.event.DBMSEmptyEvent;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * This class wraps a basic Applier so that it handles ReplDBMSEvent values with
 * assigned sequence numbers.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ApplierWrapper implements ParallelApplier
{
    private static Logger logger = Logger.getLogger(ApplierWrapper.class);
    private RawApplier    applier;

    /**
     * Create a new instance to wrap a raw applier.
     * 
     * @param applier Extractor to be wrapped
     */
    public ApplierWrapper(RawApplier applier)
    {
        this.applier = applier;
    }

    /** Return wrapped applier. */
    public RawApplier getApplier()
    {
        return applier;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.ParallelApplier#setTaskId(int)
     */
    public void setTaskId(int id)
    {
        applier.setTaskId(id);
    }

    /**
     * Apply the DBMSEvent in the ReplDBMSEvent. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.Applier#apply(com.continuent.tungsten.replicator.event.ReplDBMSEvent,
     *      boolean, boolean, boolean)
     */
    public void apply(ReplDBMSEvent event, boolean doCommit,
            boolean doRollback, boolean syncTHL) throws ReplicatorException,
            ConsistencyException, InterruptedException
    {
        DBMSEvent myEvent = event.getDBMSEvent();
        if (myEvent instanceof DBMSEmptyEvent)
        {
            // Handling empty events :
            // - if it is the first fragment, this is an empty
            // commit, it can then be safely ignored
            // - if it is the last fragment, it should commit
            if (event.getFragno() > 0)
            {
                applier.apply(myEvent, event, true, false);
            }
            else
            {
                // Empty commit : just ignore
                applier.apply(myEvent, event, false, false);
            }
        }
        else
            applier.apply(myEvent, event, doCommit, doRollback);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.Applier#updatePosition(com.continuent.tungsten.replicator.event.ReplDBMSHeader,
     *      boolean, boolean)
     */
    public void updatePosition(ReplDBMSHeader header, boolean doCommit,
            boolean syncTHL) throws ReplicatorException, InterruptedException
    {
        DBMSEmptyEvent empty = new DBMSEmptyEvent(null);
        applier.apply(empty, header, doCommit, false);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.Applier#commit()
     */
    public void commit() throws ReplicatorException, InterruptedException
    {
        applier.commit();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.Applier#rollback()
     */
    public void rollback() throws InterruptedException
    {
        applier.rollback();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.Applier#getLastEvent()
     */
    public ReplDBMSHeader getLastEvent() throws ReplicatorException,
            InterruptedException
    {
        return applier.getLastEvent();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        logger.debug("Configuring raw applier");
        applier.configure(context);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        logger.debug("Preparing raw applier");
        applier.prepare(context);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        logger.debug("Releasing raw applier");
        applier.release(context);
    }
}
