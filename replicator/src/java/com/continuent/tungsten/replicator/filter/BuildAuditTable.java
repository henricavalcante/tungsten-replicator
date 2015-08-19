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
 * Initial developer(s): Jeff Mace
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
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Filter to transform ROW INSERT/UPDATE changes into an audit table:<br />
 * 1. All DELETE events are dropped<br />
 * 2. All UPDATE events are turned into an INSERT<br />
 * 3. The table name is changed to the targetTableName setting<br />
 * 4. All KEY columns and values are dropped from the event<br />
 * <br />
 * In order to use this filter the targetTableName must be created by the user 
 * with the table name used in targetTableName. The table must have the same
 * structure as the source but with a different primary key defined at the 
 * beginning of the table.
 * 
 * @author <a href="mailto:jeff.mace@continuent.com">Jeff Mace</a>
 * @version 1.0
 * @see java.util.regex.Pattern
 * @see java.util.regex.Matcher
 */
public class BuildAuditTable implements Filter
{    
    private static Logger            logger = Logger.getLogger(BuildAuditTable.class);
    
    private String        targetTableName = null;
    
    public void setTargetTableName(String tableName)
    {
        this.targetTableName = tableName;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.filter.Filter#filter(com.continuent.tungsten.replicator.event.ReplDBMSEvent)
     */
    public ReplDBMSEvent filter(ReplDBMSEvent event) throws ReplicatorException
    {
        logger.debug("Modify RowChangeData in " + event.getSeqno());
        ArrayList<DBMSData> data = event.getData();
        
        if (data == null)
            return event;
            
        for (Iterator<DBMSData> eventIterator = data.iterator(); eventIterator.hasNext();)
        {
            DBMSData dataElem = eventIterator.next();
            if (dataElem instanceof RowChangeData)
            {
                RowChangeData rdata = (RowChangeData) dataElem;
                for (Iterator<OneRowChange> oneRowIterator = rdata.getRowChanges()
                        .iterator(); oneRowIterator.hasNext();)
                {
                    OneRowChange orc = oneRowIterator.next();
                    logger.debug("Parsing found schema = " + orc.getSchemaName()
                            + " / table = '" + orc.getTableName() + "'");
                    
                    if (orc.getAction() == ActionType.DELETE) {
                        logger.debug("Drop delete change");
                        oneRowIterator.remove();
                        continue;
                    }
                    
                    orc.setTableName(this.targetTableName);
                    
                    if (orc.getAction() == ActionType.UPDATE) {
                        orc.setAction(ActionType.INSERT);
                    }
                    
                    orc.getKeySpec().clear();
                    orc.getKeyValues().clear();
                }
                if (rdata.getRowChanges().isEmpty())
                {
                    logger.debug("empty the eventIterator");
                    eventIterator.remove();
                }
            }
            else if (dataElem instanceof StatementData)
            {
                // Nothing
            }
        }
        
        // Don't drop events when dealing with fragmented events (This could
        // drop the commit part)
        if (event.getFragno() == 0 && event.getLastFrag() && data.isEmpty())
        {
            return null;
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
        if (this.targetTableName == null)
        {
            throw new ReplicatorException("Unable to configure filter because targetTableName is not given");
        }
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