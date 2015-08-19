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

import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.Table;

/**
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public interface Chunk
{

    /**
     * Returns the table which is concerned by this chunk.
     * 
     * @return Returns the Table this chunk is based on.
     */
    public Table getTable();

    /**
     * Returns the list of columns that are going to be extracted.
     * 
     * @return Returns the list of columns.
     */
    public List<String> getColumns();

    /**
     * Returns the from value.
     * 
     * @return Returns the from.
     */
    public Object getFrom();

    /**
     * Returns the to value.
     * 
     * @return Returns the to.
     */
    public Object getTo();

    /**
     * Returns the nbBlocks value. This is the total number of chunks that will
     * be used to extract the whole table content. It is used to know when a
     * table was fully processed.
     * 
     * @return Returns the nbBlocks.
     */
    public long getNbBlocks();

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString();

    /**
     * Returns the query used to get the data of this chunk.
     * 
     * @param connection Database object used to generate the query.
     * @param eventId Position used for flashback queries (when supported)
     * @return Returns the query to execute to get data of this chunk from the
     *         database, eventually at the given position
     */
    public String getQuery(Database connection, String eventId);

    /**
     * Returns the list of values to be used as starting point of the chunk.
     * 
     * @return Returns the list of values to be used as starting point of the
     *         chunk.
     */
    public Object[] getFromValues();

    /**
     * Returns the list of values to be used as ending point of the chunk.
     * 
     * @return Returns the list of values to be used as ending point of the
     *         chunk.
     */
    public Object[] getToValues();
}
