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

package com.continuent.tungsten.replicator.extractor.parallel;

import java.util.Arrays;
import java.util.List;

import com.continuent.tungsten.replicator.database.Table;

/**
 * Defines a chunk based on single numeric column primary key.
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class NumericChunk extends AbstractChunk implements Chunk
{
    private Number       from;

    private Number       to;

    private Table        table;

    private List<String> columns;

    private long         nbBlocks;

    public NumericChunk(Table table, Number from, Number to, String[] columns)
    {
        this.table = table;
        this.from = from;
        this.to = to;
        if (columns == null)
            this.columns = null;
        else
            this.columns = Arrays.asList(columns);
    }

    public NumericChunk(Table table, Number from, Number to, String[] columns,
            long nbBlocks)
    {
        this(table, from, to, columns);
        this.nbBlocks = nbBlocks;
    }

    public NumericChunk(Table table, String[] columns)
    {
        this(table, null, null, columns);
        this.nbBlocks = 1;
    }

    public NumericChunk()
    {
        // Generate an empty chunk that will tell threads that work is complete
        this.table = null;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.Chunk#getColumns()
     */
    @Override
    public List<String> getColumns()
    {
        return columns;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.Chunk#getFrom()
     */
    @Override
    public Long getFrom()
    {
        if (from == null)
            return null;
        return from.longValue();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.Chunk#getNbBlocks()
     */
    @Override
    public long getNbBlocks()
    {
        return nbBlocks;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.Chunk#getTable()
     */
    @Override
    public Table getTable()
    {
        return table;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.Chunk#getTo()
     */
    @Override
    public Long getTo()
    {
        if (to == null)
            return null;
        return to.longValue();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.Chunk#toString()
     */
    @Override
    public String toString()
    {
        return "Chunk for table " + table.getSchema() + "." + table.getName()
                + " for " + table.getPrimaryKey().getColumns().get(0).getName()
                + " from " + from + " to " + to;
    }

    @Override
    public String getWhereClause()
    {
        if (getFrom() != null)
        {
            StringBuffer sql = new StringBuffer(" WHERE ");
            String pkName = getTable().getPrimaryKey().getColumns().get(0)
                    .getName();

            sql.append(pkName);
            sql.append(" > ");
            sql.append(getFrom());
            sql.append(" AND ");
            sql.append(pkName);
            sql.append(" <= ");
            sql.append(getTo());
            return sql.toString();
        }
        return null;
    }

    protected String getOrderByClause()
    {
        return " ORDER BY "
                + getTable().getPrimaryKey().getColumns().get(0).getName();
    }
}
