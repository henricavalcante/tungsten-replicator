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
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.replicator.filter;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.database.MySQLOperationMatcher;
import com.continuent.tungsten.replicator.database.SqlOperation;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Implements a filter to either apply or ignore operations on particular
 * schemas and/or tables. Patterns are comma separated lists, where each entry
 * may have the following form:
 * <ul>
 * <li>A schema name, for example "test"</li>
 * <li>A fully qualified table name, for example "test.foo"</li>
 * </ul>
 * Schema and table names may contain * and ? characters, which substitute for a
 * series of characters or a single character, respectively. For example,
 * "test.*" matches all tables in database test, and "test?.foo" matches tables
 * "test1.foo" and "test2.foo" but not "test.foo".
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class ReplicateFilter implements Filter
{
    private static Logger               logger = Logger.getLogger(ReplicateFilter.class);

    private String                      doFilter;
    private String                      ignoreFilter;
    private String                      filePrefix;

    private String                      tungstenSchema;
    private final MySQLOperationMatcher parser = new MySQLOperationMatcher();

    private SchemaTableFilter           filter;

    /**
     * Define a comma-separated list of schemas with optional table names (e.g.,
     * schema1,schema2.table1,etc.) to replicate. If set, only operations that
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
     * Define a comma-separated list of schemas with optional table names (e.g.,
     * schema1,schema2.table1,etc.) to ignore. If set, all operations that match
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
     * Sets the Tungsten schema, which we ignore to prevent problems with the
     * replicator. This is mostly used for filter testing, which runs without a
     * pipeline.
     */
    public void setTungstenSchema(String tungstenSchema)
    {
        this.tungstenSchema = tungstenSchema;
    }

    public void setFilePrefix(String filePrefix)
    {
        this.filePrefix = filePrefix;
    }

    /**
     * Filters transactions using do and ignore rules. The logic is as follows.
     * <ol>
     * <li>If the operation matches a schema or table to ignore, drop it.</li>
     * <li>If the operation matches a schema or table to do, forward it.</li>
     * <li>If the do list is enabled and the operation does not match, drop it.</li>
     * </ol>
     * Individual operations that match the filtering rules are removed. If the
     * entire transaction becomes empty as a result, it will be removed.
     * 
     * @see com.continuent.tungsten.replicator.filter.Filter#filter(com.continuent.tungsten.replicator.event.ReplDBMSEvent)
     */
    public ReplDBMSEvent filter(ReplDBMSEvent event)
            throws ReplicatorException, InterruptedException
    {
        ArrayList<DBMSData> data = event.getData();

        if (data == null)
            return event;

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

                    if (filterEvent(orc.getSchemaName(), orc.getTableName()))
                    {
                        if (logger.isDebugEnabled())
                            logger.debug("Filtering event");

                        iterator2.remove();
                    }
                }
                if (rdata.getRowChanges().isEmpty())
                {
                    iterator.remove();
                }
            }
            else if (dataElem instanceof StatementData)
            {
                StatementData sdata = (StatementData) dataElem;
                String schema = null;
                String table = null;

                // Make a best effort to get parsing metadata on the statement.
                // Invoke parsing again if necessary.
                Object parsingMetadata = sdata.getParsingMetadata();
                if (parsingMetadata == null)
                {
                    String query = sdata.getQuery();
                    parsingMetadata = parser.match(query);
                    sdata.setParsingMetadata(parsingMetadata);
                }

                // Usually we have parsing metadata at this point.
                if (parsingMetadata != null
                        && parsingMetadata instanceof SqlOperation)
                {
                    SqlOperation parsed = (SqlOperation) parsingMetadata;
                    schema = parsed.getSchema();
                    table = parsed.getName();
                    if (logger.isDebugEnabled())
                        logger.debug("Parsing found schema = " + schema
                                + " / table = " + table);
                }

                if (schema == null)
                    schema = sdata.getDefaultSchema();

                if (schema == null)
                {
                    final String query = sdata.getQuery();
                    logger.warn("Ignoring query : No schema found for this query from event "
                            + event.getSeqno()
                            + (query != null ? " ("
                                    + query.substring(0,
                                            Math.min(query.length(), 200))
                                    + "...)" : ""));
                    continue;
                }

                if (filterEvent(schema, table))
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Filtering event");
                    iterator.remove();
                }
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

    // Returns true if the schema and table should be filtered using either a
    // cache look-up or a full scan based on filtering rules.
    private boolean filterEvent(String schema, String table)
    {
        // if schema not provided, cannot filter
        if (schema.length() == 0)
            return false;

        // Tungsten schema is always passed through as dropping this can
        // confuse the replicator.
        if (schema.equals(tungstenSchema))
            return false;

        return filter.filterEvent(schema, table);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        if (tungstenSchema == null)
        {
            tungstenSchema = context.getReplicatorProperties().getString(
                    ReplicatorConf.METADATA_SCHEMA);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        if (logger.isDebugEnabled())
            logger.debug("Preparing Replicate Filter");

        if (doFilter != null || ignoreFilter != null)
            filter = new SchemaTableFilter(doFilter, ignoreFilter);
        else if (filePrefix != null)
            filter = new SchemaTableFilter(filePrefix);

    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        if (filter != null)
            filter.release();
    }

}
