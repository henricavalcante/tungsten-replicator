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

package com.continuent.tungsten.replicator.heartbeat;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.RowChangeData.ActionType;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.DBMSEmptyEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplOptionParams;
import com.continuent.tungsten.replicator.filter.Filter;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Implements a filter to detect heartbeat table updates and add appropriate
 * metadata to the event. This filter can run on either master or slave.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class HeartbeatFilter implements Filter
{
    private static Logger logger               = Logger.getLogger(HeartbeatFilter.class);
    private PluginContext context;
    // Pattern to match UPDATE fragment: "name = '<name>'.
    private Pattern       heartbeatNamePattern = Pattern
                                                       .compile(
                                                               "name\\s*=\\s*('|\")([a-zA-Z0-9 _]*)('|\")",
                                                               Pattern.CASE_INSENSITIVE);

    public ReplDBMSEvent filter(ReplDBMSEvent event)
            throws ReplicatorException, InterruptedException
    {
        if (logger.isDebugEnabled())
            logger.debug("Scanning potential heartbeat event: seqno="
                    + event.getSeqno());
        String name = "NONE";

        // See if this looks like a consistency check from the SQL.
        // Anything with more than one update is not a consistency
        // check.
        if (event.getDBMSEvent() instanceof DBMSEmptyEvent)
            return event;

        ArrayList<DBMSData> dbmsDataValues = event.getData();
        if (dbmsDataValues.size() != 1)
            return event;

        // See if we have Tungsten metadata. If not, this is not a Tungsten
        // event and cannot be a heartbeat.
        String metadata = event.getDBMSEvent().getMetadataOptionValue(
                ReplOptionParams.TUNGSTEN_METADATA);
        if (metadata == null)
            return event;

        DBMSData dbmsData = dbmsDataValues.get(0);
        if (dbmsData instanceof StatementData)
        {
            StatementData sd = (StatementData) dbmsData;

            String query = null;
            boolean usingWholeQuery = true;

            if (sd.getQueryAsBytes() == null)
            {
                if (sd.getQuery().length() <= 500)
                    query = sd.getQuery();
                else
                {
                    usingWholeQuery = false;
                    query = sd.getQuery().substring(0, 500);
                }
            }
            else
            // Is the query stored as bytes ?
            {
                if (sd.getQueryAsBytes().length <= 500)
                {
                    query = new String(sd.getQueryAsBytes());
                }
                else
                {
                    usingWholeQuery = false;
                    query = new String(sd.getQueryAsBytes(), 0, 500);
                }
            }

            if (!query.toUpperCase().startsWith("UPDATE")
                    || !query.contains(HeartbeatTable.TABLE_NAME))
            {
                return event;
            }
            // usingWholeQuery is set to false when and only when the length of
            // the statement was greater than what was used for checking if it
            // started with UPDATE and contained the heartbeat table name.
            // In that case, we want to extract the whole statement as it is
            // likely to be a heartbeat statement.
            if (!(usingWholeQuery))
                query = sd.getQuery();

            // Get the heartbeat name.
            Matcher m = heartbeatNamePattern.matcher(query);
            if (m.find())
            {
                name = m.group(2);
            }
        }
        else if (dbmsData instanceof RowChangeData)
        {
            RowChangeData rd = (RowChangeData) dbmsData;
            if (rd.getRowChanges().size() != 1)
                return event;
            OneRowChange orc = rd.getRowChanges().get(0);
            if (orc.getSchemaName().equals(context.getReplicatorSchemaName())
                    && orc.getTableName().equals(HeartbeatTable.TABLE_NAME))
            {
                if (orc.getAction() == ActionType.INSERT
                        || orc.getAction() == ActionType.DELETE)
                    // Code below expects an UPDATE of table HEARTBEAT. Don't
                    // process INSERT or DELETE
                    return event;

                // MySQL Row replication uses before/after images which result
                // in all before-image values looking like keys. For heartbeat
                // events we need to allow only the first column as a key.
                // If there are extra columns, drop them.
                int keySpecLen = orc.getKeySpec().size();
                if (keySpecLen > 1)
                {
                    for (int i = keySpecLen - 1; i > 0; i--)
                        orc.getKeySpec().remove(i);
                }
                if (orc.getKeyValues().size() > 0)
                {
                    List<OneRowChange.ColumnVal> keyValues = orc.getKeyValues()
                            .get(0);
                    int keyValueLen = keyValues.size();
                    if (keyValueLen > 1)
                    {
                        for (int i = keyValueLen - 1; i > 0; i--)
                            keyValues.remove(i);
                    }
                }

                // Now we have to find the name of the heartbeat, which is in
                // the 7th column. Extraction is a little painful, as you can
                // see.
                ArrayList<ArrayList<OneRowChange.ColumnVal>> colValues = orc
                        .getColumnValues();
                ArrayList<OneRowChange.ColumnVal> colValue0 = colValues.get(0);
                if (colValue0 != null)
                {
                    // TUC-228. Have to translate bytes to String. Heartbeat
                    // must be ASCII-only.
                    if (colValue0.size() >= 7)
                    {
                        Object value = colValue0.get(7).getValue();
                        if (value instanceof String)
                            name = (String) colValue0.get(7).getValue();
                        else if (value instanceof byte[])
                            name = new String((byte[]) value);
                    }
                }
            }
            else
            {
                return event;
            }
        }
        else
            return event;

        // If we got to this point we have a heartbeat event. Add
        // heartbeat metadata and return.
        if (logger.isDebugEnabled())
        {
            logger.debug("Detected a heartbeat event: " + event.getSeqno());
        }
        event.getDBMSEvent()
                .addMetadataOption(ReplOptionParams.HEARTBEAT, name);
        return event;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException
    {
        // Record the plugin context.
        this.context = context;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException
    {
        // Nothing to do.
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException
    {
        // Nothing to do.
    }
}
