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

package com.continuent.tungsten.replicator.extractor;

/**
 * Denotes a Runnable task that implements stage processing. 
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.cluster.resource.OpenReplicatorParams;
import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.consistency.ConsistencyCheckFilter;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.EventMetadataFilter;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.filter.Filter;
import com.continuent.tungsten.replicator.heartbeat.HeartbeatFilter;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * This class wraps a basic Extractor so that it returns ReplDBMSEvent values
 * with assigned sequence numbers. It contains logic to recognize that we have
 * failed over; see {@link #setLastEvent(ReplDBMSHeader)} for more information.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ExtractorWrapper implements Extractor
{
    private static Logger logger                  = Logger.getLogger(ExtractorWrapper.class);
    private PluginContext pluginContext;
    private RawExtractor  extractor;
    private String        sourceId;
    private long          seqno                   = 0;
    private short         fragno                  = 0;
    private long          epochNumber             = 0;
    private List<Filter>  autoFilters             = new ArrayList<Filter>();
    private boolean       autoMasterRepositioning = true;

    /**
     * Create a new instance to wrap Creates a new <code>ExtractorWrapper</code>
     * object
     * 
     * @param extractor Extractor to be wrapped
     */
    public ExtractorWrapper(RawExtractor extractor)
    {
        this.extractor = extractor;
        this.autoFilters.add(new EventMetadataFilter());
        this.autoFilters.add(new HeartbeatFilter());
        this.autoFilters.add(new ConsistencyCheckFilter());
    }

    /** Return wrapped extractor. */
    public RawExtractor getExtractor()
    {
        return extractor;
    }

    /**
     * Extracts a raw event and wraps it in a ReplDBMS complete with sequence
     * number, which increments each time we process the last fragment.
     * 
     * @see com.continuent.tungsten.replicator.extractor.Extractor#extract()
     */
    public ReplDBMSEvent extract() throws ReplicatorException,
            InterruptedException
    {
        DBMSEvent dbmsEvent = extractor.extract();

        if (dbmsEvent == null)
            return null;

        // Generate the event.
        Timestamp extractTimestamp = dbmsEvent.getSourceTstamp();
        ReplDBMSEvent replEvent = new ReplDBMSEvent(seqno, fragno,
                dbmsEvent.isLastFrag(), sourceId, epochNumber,
                extractTimestamp, dbmsEvent);
        if (logger.isDebugEnabled())
            logger.debug("Source timestamp = " + dbmsEvent.getSourceTstamp()
                    + " - Extracted timestamp = " + extractTimestamp);

        for (Filter filter : autoFilters)
        {
            try
            {
                replEvent = filter.filter(replEvent);
                if (replEvent == null)
                    return null;
            }
            catch (ReplicatorException e)
            {
                throw new ExtractorException(
                        "Auto-filter operation failed unexpectedly: "
                                + e.getMessage(), e);
            }
        }

        // See if this is the last fragment.
        if (dbmsEvent.isLastFrag())
        {
            seqno++;
            fragno = 0;
        }
        else
            fragno++;

        return replEvent;
    }

    /**
     * Delegates to underlying extractor. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.Extractor#getCurrentResourceEventId()
     */
    public String getCurrentResourceEventId() throws ReplicatorException,
            InterruptedException
    {
        return extractor.getCurrentResourceEventId();
    }

    /**
     * Returns false until we implement caching.
     * 
     * @see com.continuent.tungsten.replicator.extractor.Extractor#hasMoreEvents()
     */
    public boolean hasMoreEvents()
    {
        return false;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.Extractor#setLastEvent(com.continuent.tungsten.replicator.event.ReplDBMSHeader)
     */
    public void setLastEvent(ReplDBMSHeader header) throws ReplicatorException
    {
        String eventId;

        // Figure out if this is a failover and we need to reset the
        // event numbering.
        if (header == null)
        {
            // No previously stored event. Start from scratch.
            seqno = 0;
            eventId = null;
        }
        else if (sourceId.equals(header.getSourceId())
                )
        {
            // Continuing local extraction. Ask for next event.
            if (logger.isDebugEnabled())
                logger.debug("Source ID of max event verified: "
                        + header.getSourceId());
            seqno = header.getSeqno() + 1;
            eventId = header.getEventId();
        }
        else
        {
            // Master source ID has shifted; remember the seqno. 
            logger.info("Local source ID differs from last stored source ID: local="
                    + sourceId + " stored=" + header.getSourceId());
            seqno = header.getSeqno() + 1;

            // If auto repositioning is enabled, reposition.  Otherwise, print a
            // warning and try to use the source ID. 
            if (autoMasterRepositioning)
            {
                logger.info("Repositioning replication to current log position on master due to source ID change");
                eventId = null;
            }
            else
            {
                logger.info("Auto-repositioning is not enabled; continuing from last master log position");
                eventId = header.getEventId();
            }
        }

        // See if we have an override on the seqno. That takes priority over
        // any previous value.
        if (pluginContext.getOnlineOptions().get(
                OpenReplicatorParams.BASE_SEQNO) != null)
        {
            overrideBaseSeqno();
        }

        // Tell the extractor.
        setLastEventId(eventId);
        epochNumber = seqno;
    }

    // Override base sequence number if different from current base.
    private void overrideBaseSeqno()
    {
        long newBaseSeqno = pluginContext.getOnlineOptions().getLong(
                OpenReplicatorParams.BASE_SEQNO) + 1;
        if (newBaseSeqno != seqno)
        {
            seqno = newBaseSeqno;
            logger.info("Overriding base sequence number; next seqno will be: "
                    + seqno);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.Extractor#setLastEventId(java.lang.String)
     */
    public void setLastEventId(String eventId) throws ReplicatorException
    {
        extractor.setLastEventId(eventId);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        logger.info("Configuring raw extractor and heartbeat filter");
        this.pluginContext = context;
        sourceId = context.getSourceId();
        extractor.configure(pluginContext);
        for (Filter filter : autoFilters)
            filter.configure(pluginContext);

        // Fetch the auto-reposition policy for masters and print
        // an appropriate message.
        TungstenProperties replicatorProps = pluginContext
                .getReplicatorProperties();
        autoMasterRepositioning = replicatorProps
                .getBoolean(ReplicatorConf.AUTO_MASTER_REPOSITIONING);
        if (autoMasterRepositioning)
            logger.info("Master auto-repositioning on source_id change is enabled; extractor will reposition current log position if last extracted source_id differs from current source_id");
        else
            logger.info("Master auto-repositioning on source_id change is disabled; extractor will not reposition automatically");
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        // Prepare sub-components.
        logger.info("Preparing raw extractor and heartbeat filter");
        extractor.prepare(context);
        for (Filter filter : autoFilters)
            filter.prepare(context);

        // See if we have an online option that overrides the initial seqno.
        if (pluginContext.getOnlineOptions().get(
                OpenReplicatorParams.BASE_SEQNO) != null)
        {
            overrideBaseSeqno();
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        logger.info("Releasing raw extractor and heartbeat filter");
        extractor.release(context);
        for (Filter filter : autoFilters)
            filter.release(context);
    }
}
