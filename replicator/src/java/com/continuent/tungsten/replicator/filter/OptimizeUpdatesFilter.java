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
 * Initial developer(s): Linas Virbalas
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.filter;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.RowChangeData.ActionType;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.filter.Filter;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Removes "SET key1=value1" parts of an UPDATE if the values updated are the
 * same as current values in the key part. Eg.:<br/>
 * <br/>
 * Original statement:
 * - SQL(0) =
 * - ACTION = UPDATE
 * - TABLE = c
 * - ROW# = 0
 *  - COL(1: null) = 1
 *  - COL(2: null) = 100
 *  - COL(3: null) = Ten
 *  - KEY(1: null) = 1
 *  - KEY(2: null) = 10
 *  - KEY(3: null) = Ten
 * <br/>
 * Transformed statement:
 * - SQL(0) =
 * - ACTION = UPDATE
 * - TABLE = c
 * - ROW# = 0
 *  - COL(2: null) = 100
 *  - KEY(1: null) = 1
 *  - KEY(2: null) = 10
 *  - KEY(3: null) = Ten
 * <br/>
 * <br/>
 * NOTE: if all columns are static (Issue 355), all are left intact (not
 * removed). In this corner case, we can't remove the event completely, because
 * if there are triggers (eg. which count UPDATEs), they would not fire. Hence,
 * instead, we leave the event unchanged and unoptimized.
 *  
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 * @version 1.0
 */
public class OptimizeUpdatesFilter implements Filter
{
    private static Logger logger = Logger.getLogger(OptimizeUpdatesFilter.class);

    /**
     * Name of current replication service's internal tungsten schema.
     */
    private String        tungstenSchema;

    /**
     * Sets the Tungsten schema, which we ignore to prevent problems with the
     * replicator. This is mostly used for filter testing, which runs without a
     * pipeline.
     */
    public void setTungstenSchema(String tungstenSchema)
    {
        this.tungstenSchema = tungstenSchema;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException
    {
        if (tungstenSchema == null)
        {
            tungstenSchema = context.getReplicatorProperties().getString(
                    ReplicatorConf.METADATA_SCHEMA);
        }

        logger.info("OptimizeUpdatesFilter configured");
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

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.filter.Filter#filter(com.continuent.tungsten.replicator.event.ReplDBMSEvent)
     */
    public ReplDBMSEvent filter(ReplDBMSEvent event)
            throws ReplicatorException, InterruptedException
    {
        ArrayList<DBMSData> data = event.getData();
        if (data == null)
            return event;
        for (DBMSData dataElem : data)
        {
            if (dataElem instanceof RowChangeData)
            {
                RowChangeData rdata = (RowChangeData) dataElem;
                for (OneRowChange orc : rdata.getRowChanges())
                {
                    if (filterStaticColumns(orc))
                        logger.debug("Event " + event.getEventId()
                                + " transformed");
                }
            }
        }
        return event;
    }

    /**
     * Find out which columns' values didn't change by comparing them to the key
     * values. Remove columns, that didn't change, from the UPDATE.<br/>
     * NOTE: we depend on keys containing the same items as columns for this to
     * work, which is generally true for MySQL binary log, if it is not filtered
     * before (eg. with a PrimaryKeyFilter.java).
     * 
     * @param orc OneRowChange event to filter and possibly transform.
     * @throws Exception
     */
    private boolean filterStaticColumns(OneRowChange orc) throws ReplicatorException
    {
        boolean transformed = false;

        if (orc.getAction() != ActionType.UPDATE)
            return transformed;

        // Don't analyze tables from Tungsten schema.
        if (tungstenSchema != null
                && orc.getSchemaName().compareToIgnoreCase(tungstenSchema) == 0)
        {
            if (logger.isDebugEnabled())
                logger.debug("Ignoring " + tungstenSchema + " schema");
            return transformed;
        }

        ArrayList<ColumnSpec> keys = orc.getKeySpec();
        ArrayList<ColumnSpec> columns = orc.getColumnSpec();
        ArrayList<ArrayList<ColumnVal>> keyValues = orc.getKeyValues();
        ArrayList<ArrayList<ColumnVal>> columnValues = orc.getColumnValues();

        // Holds the list of columns that didn't change their values.
        ArrayList<ColumnSpec> columnsToRemove = new ArrayList<ColumnSpec>();

        if (columns.size() != keys.size())
            throw new ReplicatorException(
                    "OptimizeUpdatesFilter cannot filter, because column and key count is different. "
                            + "Make sure that it is defined before filters which remove keys (eg. PrimaryKeyFilter).");

        // Iterate key values (column value count is the same or more).
        for (int k = 0; k < keys.size(); k++)
        {
            ColumnSpec keySpec = keys.get(k);
            ColumnSpec colSpec = columns.get(k); // The candidate.

            // Iterate through multiple rows being updated.
            boolean columnStatic = true;
            for (int row = 0; row < columnValues.size()
                    || row < keyValues.size(); row++)
            {
                ColumnVal keyValue = keyValues.get(row).get(k);

                // Is corresponding column value different from key's
                // (i.e. current)?
                ColumnVal colValue = columnValues.get(row).get(k);
                if (!(keySpec.getType() == colSpec.getType()
                        && keySpec.getIndex() == colSpec.getIndex() && ((keyValue
                        .getValue() == null && colValue.getValue() == null) || (keyValue
                        .getValue() != null && keyValue.getValue().equals(
                        colValue.getValue())))))
                {
                    // Value is different, keep this column for update
                    // statement.
                    columnStatic = false;
                }
                else
                {
                    logger.debug("Col " + colSpec.getIndex() + " @ Row " + row
                            + " is static: " + keyValue.getValue() + " = "
                            + colValue.getValue());
                }
            }

            // Remember this column for removal.
            if (columnStatic)
                columnsToRemove.add(colSpec);
        }
        
        // Issue 355 - Oracle UPDATEs that change no columns lead to
        // OptimizeUpdatesFilter removing too much.
        if (columnsToRemove.size() == columns.size())
        {
            if (logger.isDebugEnabled())
                logger.debug("All "
                        + columnsToRemove.size()
                        + " of "
                        + columns.size()
                        + " columns where static - leaving them as is to have a valid transaction");
            return transformed;
        }

        // Remove static columns now that we know which those are.
        for (Iterator<ColumnSpec> iterator = columnsToRemove.iterator(); iterator
                .hasNext();)
        {
            ColumnSpec columnToRemoveSpec = iterator.next();

            // Remove this static column, so we don't try to
            // update it later. First remove the values.
            int idx = columns.indexOf(columnToRemoveSpec);
            // Iterate through each row.
            for (Iterator<ArrayList<ColumnVal>> iterator2 = columnValues
                    .iterator(); iterator2.hasNext();)
            {
                ArrayList<ColumnVal> values = iterator2.next();
                values.remove(idx);
            }
            // Then remove the column specs.
            columns.remove(idx);

            // Now we actually changed the event.
            logger.debug("Col " + columnToRemoveSpec.getIndex() + " removed");
            transformed = true;
        }

        return transformed;
    }
}
