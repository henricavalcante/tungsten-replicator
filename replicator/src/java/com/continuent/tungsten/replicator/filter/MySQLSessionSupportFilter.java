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

import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.patterns.order.HighWaterResource;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Filter to support session specific temp tables and variables
 * 
 * @author <a href="mailto:edward.archibald@continuent.com">Edward Archibald</a>
 * @version 1.0
 */
public class MySQLSessionSupportFilter implements Filter
{
    private static Logger       logger                = Logger.getLogger(LoggingFilter.class);

    private String              lastSessionId         = "";
    private static final String SET_PTHREAD_STATEMENT = "set @@session.pseudo_thread_id=";
    private static final String BLANK_THREAD          = "0";
    private boolean             privileged;

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.filter.Filter#filter(com.continuent.tungsten.replicator.event.ReplDBMSEvent)
     */
    public ReplDBMSEvent filter(ReplDBMSEvent event) throws ReplicatorException
    {
        if (!privileged)
        {
            logger.debug("Database update is not privileged; skipping pseudo-thread support");
            return event;
        }

        String eventId = event.getEventId();
        String sessionId = HighWaterResource.getSessionId(eventId);

        if (sessionId == null && logger.isDebugEnabled())
            logger.debug(String.format("Found null sessionId for eventId=%s",
                    eventId));

        if (sessionId != null && sessionId.equals("-1"))
            sessionId = BLANK_THREAD;

        if (sessionId != null && !sessionId.equals(lastSessionId))
        {
            ArrayList<DBMSData> data = event.getData();
            if (data != null)
            {
                StatementData ins = new StatementData(SET_PTHREAD_STATEMENT
                        + sessionId);
                data.add(0, ins);
                lastSessionId = sessionId;
                if (logger.isDebugEnabled())
                    logger.debug(String.format("%s%s for eventId=%s",
                            SET_PTHREAD_STATEMENT, sessionId, eventId));
            }
        }
        return event;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException
    {
        privileged = context.isPrivilegedSlave();
        if (!privileged)
        {
            logger.warn("Database update is not privileged; MySQL temp table support using pseudo-threads is disabled");
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException
    {
        if (logger.isDebugEnabled())
            logger.debug(String.format("Filter %s loaded successfully",
                    getClass().getSimpleName()));
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException
    {
    }
}
