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
 * Defines a chunk that contains the whole table (no chunk for this table).
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class NoChunk extends AbstractChunk implements Chunk
{

    private Table        table;
    private List<String> columns = null;

    public NoChunk(Table table, String[] columns)
    {
        this.table = table;
        if (columns != null)
            this.columns = Arrays.asList(columns);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.AbstractChunk#getTable()
     */
    @Override
    public Table getTable()
    {
        return table;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.AbstractChunk#getColumns()
     */
    @Override
    public List<String> getColumns()
    {
        return columns;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.AbstractChunk#getFrom()
     */
    @Override
    public Object getFrom()
    {
        return null;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.AbstractChunk#getTo()
     */
    @Override
    public Object getTo()
    {
        return null;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.AbstractChunk#getNbBlocks()
     */
    @Override
    public long getNbBlocks()
    {
        return 0;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.AbstractChunk#getWhereClause()
     */
    @Override
    protected String getWhereClause()
    {
        return null;
    }

}
