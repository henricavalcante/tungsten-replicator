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
import java.sql.SQLException;

import com.continuent.tungsten.common.cache.CacheResourceManager;
import com.continuent.tungsten.common.cache.IndexedLRUCache;

/**
 * Implements a cache for prepared statements, which are identified by a single
 * key.
 */
public class PreparedStatementCache
        implements
            CacheResourceManager<PreparedStatementHolder>
{
    IndexedLRUCache<PreparedStatementHolder> cache;

    /**
     * Creates a new table metadata cache.
     */
    public PreparedStatementCache(int capacity)
    {
        cache = new IndexedLRUCache<PreparedStatementHolder>(capacity, this);
    }

    public void release(PreparedStatementHolder psh)
    {
        try
        {
            if (psh.getPreparedStatement() != null)
            {
                psh.getPreparedStatement().close();
            }
        }
        catch (SQLException e)
        {
        }
    }

    /**
     * Returns the number of entries in the metadata cache.
     */
    public int size()
    {
        return cache.size();
    }

    /**
     * Store prepared statement.
     */
    public void store(String key, PreparedStatement ps, String query)
    {
        PreparedStatementHolder psh = new PreparedStatementHolder(key, ps,
                query);
        cache.put(key, psh);
    }

    /**
     * Retrieves prepared statement or returns null if it is not in the cache.
     */
    public PreparedStatement retrieve(String key)
    {
        PreparedStatementHolder psh = retrieveExtended(key);
        if (psh == null)
            return null;
        else
            return psh.getPreparedStatement();
    }

    /**
     * Retrieves prepared statement plus query text or returns null if not in
     * the cache.
     */
    public PreparedStatementHolder retrieveExtended(String key)
    {
        return cache.get(key);
    }

    /**
     * Release one prepared statement.
     */
    public void invalidate(String key)
    {
        cache.invalidate(key);
    }

    /**
     * Release all metadata in the cache.
     */
    public void invalidateAll()
    {
        cache.invalidateAll();
    }
}