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

import java.util.Iterator;
import java.util.List;

import com.continuent.tungsten.replicator.database.Column;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.Table;

/**
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public abstract class AbstractChunk implements Chunk
{

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.Chunk#getTable()
     */
    @Override
    public abstract Table getTable();

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.Chunk#getColumns()
     */
    @Override
    public abstract List<String> getColumns();

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.Chunk#getFrom()
     */
    @Override
    public abstract Object getFrom();

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.Chunk#getTo()
     */
    @Override
    public abstract Object getTo();

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.Chunk#getNbBlocks()
     */
    @Override
    public abstract long getNbBlocks();

    protected abstract String getWhereClause();

    /**
     * {@inheritDoc}
     */
    @Override
    public String getQuery(Database connection, String eventId)
    {
        StringBuffer sql = new StringBuffer();

        List<String> columns = getColumns();
        if (columns == null)
            for (Column column : getTable().getAllColumns())
            {
                if (sql.length() == 0)
                {
                    sql.append("SELECT ");
                }
                else
                {
                    sql.append(", ");
                }
                sql.append(column.getName());
            }
        else
            for (Iterator<String> iterator = columns.iterator(); iterator
                    .hasNext();)
            {
                if (sql.length() == 0)
                {
                    sql.append("SELECT ");
                }
                else
                {
                    sql.append(", ");
                }
                sql.append(iterator.next());
            }

        sql.append(" FROM ");
        sql.append(connection.getDatabaseObjectName(getTable().getSchema()));
        sql.append('.');
        sql.append(connection.getDatabaseObjectName(getTable().getName()));

        sql.append(AbstractChunk.getFlashbackQueryClause(connection, eventId));

        String where = getWhereClause();
        if (where != null)
            sql.append(where);

        String orderby = getOrderByClause();
        if (orderby != null)
            sql.append(orderby);

        return sql.toString();
    }

    /**
     * Returns the order by clause, if any.
     */
    protected String getOrderByClause()
    {
        return null;
    }

    @Override
    public Object[] getFromValues()
    {
        return null;
    }

    @Override
    public Object[] getToValues()
    {
        return null;
    }

    protected static String getFlashbackQueryClause(Database conn,
            String eventId)
    {
        if (eventId != null && conn.supportsFlashbackQuery())
            return conn.getFlashbackQuery(eventId);

        return "";
    }

}
