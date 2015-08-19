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

import com.continuent.tungsten.common.cache.IndexedLRUCache;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.RowChangeData.ActionType;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.database.TableMatcher;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Filter to remove columns from RowChangeData
 * 
 * @author <a href="mailto:jeff.mace@continuent.com">Jeff Mace</a>
 * @version 1.0
 * @see java.util.regex.Pattern
 * @see java.util.regex.Matcher
 */
public class ReplicateColumnsFilter implements Filter
{    
    private static Logger            logger = Logger.getLogger(ReplicateColumnsFilter.class);
    
    private TableMatcher             doMatcher;
    private TableMatcher             ignoreMatcher;

    private String                   doFilter;
    private String                   ignoreFilter;
    
    private String                   tungstenSchema;
    
    // Cache to look up filtered tables.
    private IndexedLRUCache<Boolean> filterCache;
    
    /**
     * Define a comma-separated list of tables with optional column names (e.g.,
     * table1,table2.column,etc.) to replicate. If set, only operations that
     * match the list will be forwarded.
     */
    public void setDoFilter(String doFilter)
    {
        this.setDo(doFilter);
    }

    public void setDo(String doFilter)
    {
        this.doFilter = doFilter;
    }

    /**
     * Define a comma-separated list of tables with optional column names (e.g.,
     * table1,table2.column,etc.) to ignore. If set, all operations that match
     * the list will be ignored.
     * 
     * @param ignoreFilter
     */
    public void setIgnoreFilter(String ignoreFilter)
    {
        setIgnore(ignoreFilter);
    }

    public void setIgnore(String ignore)
    {
        this.ignoreFilter = ignore;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.filter.Filter#filter(com.continuent.tungsten.replicator.event.ReplDBMSEvent)
     */
    public ReplDBMSEvent filter(ReplDBMSEvent event) throws ReplicatorException
    {
        ArrayList<DBMSData> data = event.getData();
        for (DBMSData dataElem : data)
        {
            if (dataElem instanceof RowChangeData)
            {
                RowChangeData rdata = (RowChangeData) dataElem;
                for (OneRowChange orc : rdata.getRowChanges())
                {
                    ArrayList<ColumnSpec> keys = orc.getKeySpec();
                    ArrayList<ColumnSpec> columns = orc.getColumnSpec();
                    ArrayList<ArrayList<ColumnVal>> keyValues = orc.getKeyValues();
                    ArrayList<ArrayList<ColumnVal>> columnValues = orc.getColumnValues();
                    
                    // Holds the list of columns that should not be in the index table
                    ArrayList<ColumnSpec> keysToRemove = new ArrayList<ColumnSpec>();
                    ArrayList<ColumnSpec> columnsToRemove = new ArrayList<ColumnSpec>();
                    
                    if (orc.getAction() == ActionType.UPDATE) {
                        if (columns.size() != keys.size() && keys.size() != 1)
                        {
                            throw new ReplicatorException(
                                "Column and key count is different in this event! Cannot filter");
                        }
                    }

                    // Iterate key values (column value count is the same or more).
                    for (int k = 0; k < keys.size(); k++)
                    {
                        ColumnSpec keySpec = keys.get(k);
                        
                        if (filterColumn(orc.getSchemaName(),
                                orc.getTableName(), keySpec.getName()))
                        {
                            logger.debug("Drop data for " + orc.getTableName()
                                    + "." + keySpec.getName());
                            keysToRemove.add(keySpec);
                        }
                        else
                        {
                            logger.debug(
                                    "Replicate data for " + orc.getTableName()
                                            + "." + keySpec.getName());
                        }
                    }
                    
                    // Remove unwanted columns now that we know which those are.
                    for (Iterator<ColumnSpec> iteratorRemoveKey = keysToRemove.iterator(); iteratorRemoveKey
                            .hasNext();)
                    {
                        ColumnSpec keyToRemoveSpec = iteratorRemoveKey.next();
                        int idx = keys.indexOf(keyToRemoveSpec);
                        
                        for (Iterator<ArrayList<ColumnVal>> iteratorKeyValue = keyValues
                                .iterator(); iteratorKeyValue.hasNext();)
                        {
                            ArrayList<ColumnVal> kValues = iteratorKeyValue.next();
                            kValues.remove(idx);
                        }
                        
                        // Then remove the column specs.
                        keys.remove(idx);

                        // Now we actually changed the event.
                        logger.info("Key " + keyToRemoveSpec.getIndex() + " removed at " + idx);
                    }
                    
                    // Iterate key values (column value count is the same or more).
                    for (int c = 0; c < columns.size(); c++)
                    {
                        ColumnSpec colSpec = columns.get(c);
                        
                        if (filterColumn(orc.getSchemaName(),
                                orc.getTableName(), colSpec.getName()))
                        {
                            logger.debug("Drop data for " + orc.getTableName()
                                    + "." + colSpec.getName());
                            columnsToRemove.add(colSpec);
                        }
                        else
                        {
                            logger.debug(
                                    "Replicate data for " + orc.getTableName()
                                            + "." + colSpec.getName());
                        }
                    }
                    
                    // Remove unwanted columns now that we know which those are.
                    for (Iterator<ColumnSpec> iteratorRemoveColumn = columnsToRemove.iterator(); iteratorRemoveColumn
                            .hasNext();)
                    {
                        ColumnSpec columnToRemoveSpec = iteratorRemoveColumn.next();
                        int idx = columns.indexOf(columnToRemoveSpec);

                        // Iterate through each row.
                        for (Iterator<ArrayList<ColumnVal>> iteratorValue = columnValues
                                .iterator(); iteratorValue.hasNext();)
                        {
                            ArrayList<ColumnVal> cValues = iteratorValue.next();
                            cValues.remove(idx);
                        }
                        
                        // Then remove the column specs.
                        columns.remove(idx);

                        // Now we actually changed the event.
                        logger.debug("Col " + columnToRemoveSpec.getIndex() + " removed at " + idx);
                    }
                }
            }
            else if (dataElem instanceof StatementData)
            {
                // Nothing
            }
        }
        return event;
    }
    
    // Returns true if the table and column should be filtered using either a
    // cache look-up or a full scan based on filtering rules.
    private boolean filterColumn(String schema, String table, String column)
    {
        // if schema not provided, cannot filter
        if (table.length() == 0)
            return false;

        // Find out if we need to filter.
        String key = fullyQualifiedName(schema, table, column);
        Boolean filter = filterCache.get(key);
        if (filter == null)
        {
            filter = filterColumnRaw(schema, table, column);
            filterCache.put(key, filter);
        }

        // Return a value.
        return filter;
    }
    
    // Performs a scan of all rules to see if we need to filter this event.
    private boolean filterColumnRaw(String schema, String table, String column)
    {
        // Tungsten schema is always passed through as dropping this can
        // confuse the replicator.
        if (schema.equals(tungstenSchema))
            return false;

        // Check to see if we explicitly ignore this schema/table.
        if (ignoreMatcher != null)
        {
            if (logger.isDebugEnabled())
                logger.debug("Checking if we should ignore: table=" + table
                        + " column=" + column);
            if (ignoreMatcher.match(table, column))
                return true;
        }

        // Now to see if we accept this schema/table...
        if (doMatcher == null)
        {
            // If there is no explicit 'do' matcher, we do not filter anything.
            return false;
        }
        else
        {
            // If there is an explicit filter we filter only if we *do not*
            // match.
            if (logger.isDebugEnabled())
                logger.debug("Checking if we should replicate: table="
                        + table + " column=" + column);
            return !doMatcher.match(table, column);
        }
    }
    
    // Returns the fully qualified schema and/or table name, which can be used
    // as a key.
    public String fullyQualifiedName(String schema, String table, String column)
    {
        StringBuffer fqn = new StringBuffer();
        fqn.append(schema);
        fqn.append(table);
        if (column != null)
            fqn.append(".").append(column);
        return fqn.toString();
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
        if (logger.isDebugEnabled())
            logger.debug("Preparing ReplicateColumnsFilter Filter");

        // Implement filter rules.
        this.doMatcher = extractFilter(doFilter);
        this.ignoreMatcher = extractFilter(ignoreFilter);

        // Initialize LRU cache.
        this.filterCache = new IndexedLRUCache<Boolean>(1000, null);
    }
    
    // Prepares table matcher.
    private TableMatcher extractFilter(String filter)
    {
        // If empty, we do nothing.
        if (filter == null || filter.length() == 0)
            return null;

        TableMatcher tableMatcher = new TableMatcher();
        tableMatcher.prepare(filter);
        return tableMatcher;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException
    {
        if (filterCache != null)
            this.filterCache.invalidateAll();
    }
}