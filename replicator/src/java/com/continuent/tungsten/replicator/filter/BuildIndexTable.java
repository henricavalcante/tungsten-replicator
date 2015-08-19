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
import java.util.ListIterator;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Filter to transform INSERT/UPDATE/DELETE from many schemas
 * to all apply to a central schema. All events that are run 
 * through this filter will have these actions taken:<br />
 * 1. Add a 'schema' column value to the beginning of the event<br />
 * 2. Add a 'schema' key value to the beginning of the key values<br />
 * 3. Set the value for each of these to the original schema<br />
 * 4. Change the schema for the event to targetSchemaName<br />
 * <br />
 * Caveats:<br />
 * - The filter ignores any changes in the tungsten_<svc> schema<br />
 * - Use the 'replicate' filter to limit the scope of this filter
 * 
 * @author <a href="mailto:jeff.mace@continuent.com">Jeff Mace</a>
 * @version 1.0
 */
public class BuildIndexTable implements Filter
{    
    private static Logger            logger = Logger.getLogger(BuildIndexTable.class);
    
    private String                   targetSchemaName;
    
    private String                   tungstenSchema;
    
    public void setTargetSchemaName(String schemaName)
    {
        this.targetSchemaName = schemaName;
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
        for (DBMSData dataElem : data)
        {
            if (dataElem instanceof RowChangeData)
            {
                RowChangeData rdata = (RowChangeData) dataElem;
                for (OneRowChange orc : rdata.getRowChanges())
                {
                    // Tungsten schema is always passed through as dropping this can
                    // confuse the replicator.
                    if (orc.getSchemaName().equals(tungstenSchema)) {
                        continue;
                    }
                        
                    ArrayList<ColumnSpec> keys = orc.getKeySpec();
                    ArrayList<ColumnSpec> columns = orc.getColumnSpec();
                    ArrayList<ArrayList<ColumnVal>> keyValues = orc.getKeyValues();
                    ArrayList<ArrayList<ColumnVal>> columnValues = orc.getColumnValues();
                    
                    OneRowChange.ColumnSpec c = orc.new ColumnSpec();
                    c.setType(12);
                    c.setName("schema");
                    
                    OneRowChange.ColumnVal v = orc.new ColumnVal();
                    v.setValue(orc.getSchemaName());
                    
                    columns.add(c);
                    keys.add(c);
                    
                    for (ArrayList<OneRowChange.ColumnVal> cValues : columnValues)
                    {
                        cValues.add(v);
                    }
                    
                    for (ArrayList<OneRowChange.ColumnVal> kValues : keyValues)
                    {
                        kValues.add(v);
                    }
                    
                    ListIterator<OneRowChange.ColumnSpec> litr = null;
                    litr = orc.getColumnSpec()
                        .listIterator();
                    for (; litr.hasNext();)
                    {
                        OneRowChange.ColumnSpec cv = litr.next();
                        cv.setIndex(0);
                    }
        
                    litr = orc.getKeySpec().listIterator();
                    for (; litr.hasNext();)
                    {
                        OneRowChange.ColumnSpec cv = litr.next();
                        cv.setIndex(0);
                    }
                    
                    orc.setSchemaName(this.targetSchemaName);
                }
            }
            else if (dataElem instanceof StatementData)
            {
                // Nothing
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
        tungstenSchema = context.getReplicatorProperties().getString(
                ReplicatorConf.METADATA_SCHEMA);
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