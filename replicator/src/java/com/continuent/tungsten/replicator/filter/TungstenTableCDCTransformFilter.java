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
 * Initial developer(s): Stephane Giron
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.filter;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.RowChangeData.ActionType;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Filter to transform a specific database name and table into a new value.
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 * @see java.util.regex.Pattern
 * @see java.util.regex.Matcher
 */
public class TungstenTableCDCTransformFilter implements Filter
{
    private static Logger logger         = Logger.getLogger(TungstenTableCDCTransformFilter.class);

    private String        fromDB;
    private String        toDB;
    private String        fromTable;
    private String        toTable;

    private boolean       suppressInsert = true;

    /**
     * Sets the regex used to match the database name.
     * 
     * @throws ReplicatorException
     */
    public void setFrom(String name) throws ReplicatorException
    {
        int index = name.indexOf(".");
        if (index > -1)
        {
            fromDB = name.substring(0, index);
            fromTable = name.substring(index + 1, name.length());
        }
        else
        {
            throw new ReplicatorException("Malformed FROM clause : " + name + " - Should be <SCHEMA>.<TABLE>");
        }
    }

    /**
     * Sets the corresponding regex to transform the name.
     * 
     * @throws ReplicatorException
     */
    public void setTo(String name) throws ReplicatorException
    {
        int index = name.indexOf(".");
        if (index > -1)
        {
            toDB = name.substring(0, index);
            toTable = name.substring(index + 1, name.length());
        }
        else
        {
            throw new ReplicatorException("Malformed TO clause : " + name + " - Should be <SCHEMA>.<TABLE>");
        }
    }

    /**
     * Sets the suppressInsert value.
     * 
     * @param suppressInsert The suppressInsert to set.
     */
    public void setSuppressInsert(boolean suppressInsert)
    {
        this.suppressInsert = suppressInsert;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.filter.Filter#filter(com.continuent.tungsten.replicator.event.ReplDBMSEvent)
     */
    public ReplDBMSEvent filter(ReplDBMSEvent event) throws ReplicatorException
    {
        ArrayList<DBMSData> data = event.getData();
        for (Iterator<DBMSData> iterator = data.iterator(); iterator.hasNext();)
        {
            DBMSData dataElem = iterator.next();
            if (dataElem instanceof RowChangeData)
            {
                RowChangeData rdata = (RowChangeData) dataElem;
                for (Iterator<OneRowChange> iterator2 = rdata.getRowChanges()
                        .iterator(); iterator2.hasNext();)
                {
                    OneRowChange orc = iterator2.next();
                    if (orc.getSchemaName().equals(fromDB)
                            && orc.getTableName().equals(fromTable))
                    {
                        if (suppressInsert
                                && orc.getAction().equals(ActionType.INSERT))
                            iterator2.remove();
                        else
                        {
                            orc.setSchemaName(toDB);
                            orc.setTableName(toTable);
                        }
                    }
                    else if (logger.isDebugEnabled())
                    {
                        logger.debug("Row does not match "
                                + orc.getSchemaName() + "."
                                + orc.getTableName() + " != " + fromDB + "."
                                + fromTable);
                    }
                }

                if (rdata.getRowChanges().isEmpty())
                    iterator.remove();
            }
            // else if (dataElem instanceof StatementData), do nothing
        }
        // Don't drop events when dealing with fragmented events (This could
        // drop the commit part)
        if (event.getFragno() == 0 && event.getLastFrag()
                && event.getData().isEmpty())
            return null;
        return event;
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
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException
    {
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
