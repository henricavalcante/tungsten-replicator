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

import java.util.List;

import com.continuent.tungsten.replicator.database.Table;

/**
 * Defines a chunk based on a single string column primary key.
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class StringChunk extends AbstractChunk implements Chunk
{

    private Table  table;
    private String min;
    private String max;
    private long   nbBlocks = 0;

    public StringChunk(Table table, String min, String max)
    {
        this.table = table;
        this.min = min;
        this.max = max;
    }

    /**
     * Creates a new <code>StringChunk</code> object
     * 
     * @param table
     * @param min
     * @param max
     * @param nbBlocks
     */
    public StringChunk(Table table, String min, String max, long nbBlocks)
    {
        this(table, min, max);
        this.nbBlocks = nbBlocks;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.Chunk#getTable()
     */
    @Override
    public Table getTable()
    {
        // TODO Auto-generated method stub
        return table;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.Chunk#getColumns()
     */
    @Override
    public List<String> getColumns()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.Chunk#getFrom()
     */
    @Override
    public String getFrom()
    {
        return min;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.Chunk#getTo()
     */
    @Override
    public Object getTo()
    {
        return max;
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
     * @see com.continuent.tungsten.replicator.extractor.parallel.AbstractChunk#getWhereClause()
     */
    @Override
    protected String getWhereClause()
    {
        if (getFrom() != null)
        {
            StringBuffer sql = new StringBuffer(" WHERE ");
            String pkName = getTable().getPrimaryKey().getColumns().get(0)
                    .getName();

            sql.append(pkName);
            sql.append(" >= '");
            sql.append(getFrom());
            sql.append("' AND ");
            sql.append(pkName);
            sql.append(" <= '");
            sql.append(getTo());
            sql.append("'");
        }
        return null;
    }

    protected String getOrderByClause()
    {
        return " ORDER BY "
                + getTable().getPrimaryKey().getColumns().get(0).getName();
    }

}
