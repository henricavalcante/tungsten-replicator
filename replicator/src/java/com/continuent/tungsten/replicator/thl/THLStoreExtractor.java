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
import com.continuent.tungsten.replicator.event.ReplControlEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplEvent;
import com.continuent.tungsten.replicator.extractor.Extractor;
import com.continuent.tungsten.replicator.extractor.ExtractorException;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.thl.log.LogConnection;

/**
 * Implements Extractor interface for a transaction history log (THL).
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class THLStoreExtractor implements Extractor
{
    private static Logger logger     = Logger.getLogger(THLStoreExtractor.class);
    private String        storeName;
    private THL           thl;
    private LogConnection client;
    private PluginContext context;

    // Pointers to track storage.
    private boolean       positioned = false;
    private long          seqno;
    private short         fragno;

    /**
     * Instantiate the adapter.
     */
    public THLStoreExtractor()
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
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException
    {
        // Do nothing.
    }

    /**
     * Connect to store. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException
    {
        this.context = context;
        try
        {
            thl = (THL) context.getStore(storeName);
            client = thl.connect(true);
        }
        catch (ClassCastException e)
        {
            throw new ReplicatorException(
                    "Invalid storage class; configuration may be in error: "
                            + context.getStore(storeName).getClass().getName());
        }
        if (thl == null)
            throw new ReplicatorException(
                    "Unknown storage name; configuration may be in error: "
                            + storeName);
        logger.info("Storage adapter is prepared: name=" + storeName);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException
    {
        if (thl != null)
        {
            thl.disconnect(client);
            thl = null;
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.Extractor#extract()
     */
    public ReplEvent extract() throws ReplicatorException, InterruptedException
    {
        // If we did not position for the first time yet, do so now.
        if (!positioned)
        {
            try
            {
                if (!client.seek(seqno, fragno))
                {
                    throw new ExtractorException(
                            "Unable to find event; may not exist in log: seqno="
                                    + seqno + " fragno=" + fragno);
                }

            }
            catch (THLException e)
            {
                throw new ExtractorException(
                        "Unable to position on initial event: seqno=" + seqno
                                + " fragno=" + fragno, e);
            }
            positioned = true;
        }

        // Fetch next event and update sequence numbers.
        ReplEvent replEvent = null;
        try
        {
            THLEvent thlEvent = client.next();

            if (thlEvent == null)
            {
                throw new THLException("Event missing from storage");
            }
            replEvent = thlEvent.getReplEvent();

            // Process event by type.
            if (replEvent instanceof ReplDBMSEvent)
            {
                ReplDBMSEvent replDbmsEvent = (ReplDBMSEvent) replEvent;
                seqno = replDbmsEvent.getSeqno();
                fragno = replDbmsEvent.getFragno();
            }
            else if (replEvent instanceof ReplControlEvent)
            {
                ReplDBMSHeader replDbmsHeader = ((ReplControlEvent) replEvent)
                        .getHeader();
                seqno = replDbmsHeader.getSeqno();
                fragno = replDbmsHeader.getFragno();
            }
            else
            {
                logger.warn("No repl event found for seqno="
                        + thlEvent.getSeqno());
            }

            // While we are at it check the end of the pipeline and update the
            // log active sequence number to wherever we have committed.
            long lastCommittedSeqno = context.getCommittedSeqno();
            thl.updateActiveSeqno(lastCommittedSeqno);
        }
        catch (ReplicatorException e)
        {
            throw new ExtractorException("Unable to fetch after event: seqno="
                    + seqno + " fragno=" + fragno, e);
        }

        // Return whatever we found.
        return replEvent;
    }

    /**
     * Return the event ID for a flush; does not make sense for a store.
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.Extractor#getCurrentResourceEventId()
     */
    public String getCurrentResourceEventId() throws ReplicatorException,
            InterruptedException
    {
        return null;
    }

    /**
     * Returns true if the queue has more events. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.Extractor#hasMoreEvents()
     */
    public boolean hasMoreEvents()
    {
        return (fragno > 0 || thl.pollSeqno(seqno + 1));
    }

    /**
     * Stores the last event we have processed. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.Extractor#setLastEvent(com.continuent.tungsten.replicator.event.ReplDBMSHeader)
     */
    public void setLastEvent(ReplDBMSHeader event) throws ReplicatorException
    {
        // Remember where we were.
        if (event == null)
        {
            // Start at beginning for next event.
            seqno = 0;
            fragno = 0;
        }
        else
        {
            if (event.getLastFrag())
            {
                // Start at next full event following this one.
                seqno = event.getSeqno() + 1;
                fragno = 0;
            }
            else
            {
                // Start at next fragment in current event.
                seqno = event.getSeqno();
                fragno = (short) (event.getFragno() + 1);
            }
        }

        // Tell the THL about this. It can then propagate the restart point
        // forward if the log is empty.
        thl.setRestartPosition(event);
    }

    /**
     * Ignored for now as stores do not extract. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.Extractor#setLastEventId(java.lang.String)
     */
    public void setLastEventId(String eventId) throws ReplicatorException
    {
        logger.warn("Attempt to set last event ID on THL storage: " + eventId);
    }
}