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
 * Initial developer(s): Robert Hodges
 * Contributor(s): 
 */

package com.continuent.tungsten.replicator.database;

import java.sql.PreparedStatement;

/**
 * Holds a single prepared statement in a form suitable for caches. We keep the
 * original statement for future reference.
 */
public class PreparedStatementHolder
{
    private final String            key;
    private final PreparedStatement preparedStatement;
    private final String            query;

    /**
     * Create a new holder for a prepared statement.
     * 
     * @param key Key to look up this prepared statement
     * @param preparedStatement Prepared statement
     * @param query Text of the prepared statement query
     */
    public PreparedStatementHolder(String key,
            PreparedStatement preparedStatement, String query)
    {
        this.key = key;
        this.preparedStatement = preparedStatement;
        this.query = query;
    }

    public synchronized String getKey()
    {
        return key;
    }

    public synchronized PreparedStatement getPreparedStatement()
    {
        return preparedStatement;
    }

    public synchronized String getQuery()
    {
        return query;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        StringBuffer sb = new StringBuffer(this.getClass().getSimpleName());
        sb.append(" key=").append(key);
        sb.append(" query=").append(query);
        return sb.toString();
    }
}