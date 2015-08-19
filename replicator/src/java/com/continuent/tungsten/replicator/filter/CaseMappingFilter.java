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
import java.io.*;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Transforms database, table and column names into upper or lower case. In case
 * of statement replication generally it transforms everything except quoted
 * string values.
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 * @version 1.0
 */
public class CaseMappingFilter implements Filter
{
    private static Logger       logger      = Logger
                                                    .getLogger(CaseMappingFilter.class);

    /**
     * Defines quotes that are used for quoting string values in SQL statements.
     * Anything inside these quotes will not be case transformed. Note: do not
     * define quotes that are used to define column names, because than those
     * column names will not be transformed too.
     */
    private static final char[] valueQuotes = {'\'', '\"'};

    private Boolean             toUpperCase = null;

    /**
     * Sets to which case to transform the SQL objects.
     * 
     * @param toUpperCase true - transform to upper case, false - to lower case.
     */
    public void setToUpperCase(boolean toUpperCase)
    {
        this.toUpperCase = toUpperCase;
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
                    transformOneRowChangeCase(orc);
            }
            else if (dataElem instanceof StatementData)
            {
                StatementData sdata = (StatementData) dataElem;

                // Transform schema name.
                String oldSchema = sdata.getDefaultSchema();
                sdata.setDefaultSchema(transformCase(oldSchema));
                if (logger.isDebugEnabled())
                    logger.debug("Schema case transformed from " + oldSchema
                            + " to " + sdata.getDefaultSchema());

                // Transform SQL statement.
                String origSQL = sdata.getQuery();
                sdata.setQuery(transformSQLCase(origSQL));
                if (logger.isDebugEnabled())
                    logger.debug("SQL case transformed from: " + origSQL
                            + "\nto: " + sdata.getQuery());
            }
        }
        return event;
    }

    /**
     * Transforms case of schema, table and column names of a given
     * OneRowChange. Logs into DEBUG stream if changes were made.
     * 
     * @param oneRowChange Row change to transform.
     */
    private void transformOneRowChangeCase(OneRowChange oneRowChange)
    {
        // Transform schema name's case.
        String oldSchema = oneRowChange.getSchemaName();
        oneRowChange.setSchemaName(transformCase(oldSchema));
        if (logger.isDebugEnabled())
            logger.debug("Schema case transformed from " + oldSchema + " to "
                    + oneRowChange.getSchemaName());

        // Transform table name's case.
        String oldTable = oneRowChange.getTableName();
        oneRowChange.setTableName(transformCase(oldTable));
        if (logger.isDebugEnabled())
            logger.debug("Table case transformed from: " + oldTable + " to "
                    + oneRowChange.getTableName());

        // Transform table column names if defined.
        ArrayList<OneRowChange.ColumnSpec> keys = oneRowChange.getKeySpec();
        ArrayList<OneRowChange.ColumnSpec> columns = oneRowChange
                .getColumnSpec();
        for (int c = 0; c < columns.size(); c++)
        {
            OneRowChange.ColumnSpec colSpec = columns.get(c);
            transformColumnCase(colSpec);
        }
        for (int k = 0; k < keys.size(); k++)
        {
            OneRowChange.ColumnSpec colSpec = keys.get(k);
            transformColumnCase(colSpec);
        }
    }

    /**
     * Transforms column names of a given ColumnSpec. Does nothing if column
     * name is not defined. Logs into DEBUG stream if change was made.
     * 
     * @param colSpec OneRowChange.ColumnSpec to transform.
     */
    private void transformColumnCase(OneRowChange.ColumnSpec colSpec)
    {
        String oldColumn = colSpec.getName();
        if (oldColumn != null)
        {
            colSpec.setName(transformCase(oldColumn));
            if (logger.isDebugEnabled())
                logger.debug("Column case transformed from " + oldColumn
                        + " to " + colSpec.getName());
        }
    }

    /**
     * Transforms case of a whole SQL statement except values in quotes.
     * 
     * @param sql SQL to transform.
     * @param upperCase true - transform to upper case, false - to lower case.
     * @return Transformed SQL.
     */
    private String transformSQLCase(String sql)
    {
        StringBuilder s = new StringBuilder(sql);
        try
        {
            PositionedStringReader r = new PositionedStringReader(sql);
            StreamTokenizer st = new StreamTokenizer(r);
            st.resetSyntax();
            for (char quote : valueQuotes)
                st.quoteChar(quote);
            int start = 0;
            int end = 0;
            while (st.nextToken() != StreamTokenizer.TT_EOF)
            {
                // If we had a "jump" over a string value or reached the end,
                if (end < (r.getCurrentPosition() - 1)
                        || r.getCurrentPosition() >= (sql.length()))
                {
                    // change case of the previous part.
                    String str = sql.substring(start, end + 1);
                    str = transformCase(str);
                    s.replace(start, end + 1, str);

                    // Start new interval from here.
                    start = r.getCurrentPosition();
                }
                end = r.getCurrentPosition();
            }
        }
        catch (IOException e)
        {
            logger
                    .warn("Unexpected exception while transforming SQL statement case: "
                            + e);
        }
        return s.toString();
    }

    /**
     * Transforms given string's case to upper or lower case depending on
     * {@link #toUpperCase}.
     * 
     * @param str String to transform.
     * @return Transformed string.
     */
    private String transformCase(String str)
    {
        if (str != null)
        {
            if (toUpperCase)
                return str.toUpperCase();
            else
                return str.toLowerCase();
        }
        else
            return str;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException
    {
        if (toUpperCase == null)
            throw new ReplicatorException(
                    "toUpperCase property must be set for case mapping filter to work");
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
     * A simple wrapper to StringReader which has a public method for querying
     * current stream position.<br/>
     * WARNING: markers are not supported. Use of them will lead to incorrect
     * getCurrentPosition() return value.
     * 
     * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
     * @version 1.0
     */
    class PositionedStringReader extends StringReader
    {
        private int next = 0;

        public PositionedStringReader(String s)
        {
            super(s);
        }

        public int read() throws IOException
        {
            int c = super.read();
            if (c != -1)
                next++;
            return c;
        }

        public int getCurrentPosition()
        {
            return next;
        }
    }
}
