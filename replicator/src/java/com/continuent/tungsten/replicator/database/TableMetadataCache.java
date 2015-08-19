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
 * Contributor(s): Stephane Giron
 */

package com.continuent.tungsten.replicator.database;

import com.continuent.tungsten.common.cache.CacheResourceManager;
import com.continuent.tungsten.common.cache.IndexedLRUCache;

/**
 * Implements a cache for table metadata. The cache organizes Table metadata by
 * schema and table name. It supports invalidation at multiple levels.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class TableMetadataCache implements CacheResourceManager<Table>
{
    IndexedLRUCache<Table> cache;

    /**
     * Creates a new table metadata cache.
     */
    public TableMetadataCache(int capacity)
    {
        cache = new IndexedLRUCache<Table>(capacity, this);
    }

    /**
     * Call back to release a table metadata instance that is dropped from the
     * cache.
     * 
     * @see com.continuent.tungsten.common.cache.CacheResourceManager#release(java.lang.Object)
     */
    public void release(Table metadata)
    {
        // Do nothing.
    }

    /**
     * Returns the number of entries in the metadata cache.
     */
    public int size()
    {
        return cache.size();
    }

    /**
     * Store metadata for a table.
     */
    public void store(Table metadata)
    {
        String key = generateKey(metadata.getSchema(), metadata.getName());
        cache.put(key, metadata);
    }

    /**
     * Retrieves table metadata or returns null if it is not in the cache.
     */
    public Table retrieve(String schema, String tableName)
    {
        String key = generateKey(schema, tableName);
        return cache.get(key);
    }

    /**
     * Release all metadata in the cache.
     */
    public void invalidateAll()
    {
        cache.invalidateAll();
    }

    /**
     * Release all table metadata instances for a given schema.
     */
    public int invalidateSchema(String schema)
    {
        return cache.invalidateByPrefix(schema);
    }

    /**
     * Release all a single table metadata instance
     */
    public int invalidateTable(String schema, String tableName)
    {
        String key = generateKey(schema, tableName);
        return cache.invalidate(key);
    }

    /**
     * Invalidate appropriate range of metadata based on a particular SQL
     * operation that we see.
     * 
     * @param sqlOperation A SQLOperation from parsing
     * @param defaultSchema Default schema in case it is not supplied by
     *            sqlOperation
     */
    public int invalidate(SqlOperation sqlOperation, String defaultSchema)
    {
        if (sqlOperation.getOperation() == SqlOperation.DROP
                && sqlOperation.getObjectType() == SqlOperation.SCHEMA)
        {
            return cache.invalidateByPrefix(sqlOperation.getSchema());
        }
        else if (sqlOperation.getOperation() == SqlOperation.DROP
                && sqlOperation.getObjectType() == SqlOperation.TABLE)
        {
            return invalidateTable(sqlOperation.getSchema(), defaultSchema,
                    sqlOperation.getName());
        }
        else if (sqlOperation.getOperation() == SqlOperation.ALTER)
        {
            return invalidateTable(sqlOperation.getSchema(), defaultSchema,
                    sqlOperation.getName());
        }
        else if (sqlOperation.getOperation() == SqlOperation.RENAME)
        {
            int count = invalidateTable(sqlOperation.getSchema(),
                    defaultSchema, sqlOperation.getName());
            if (sqlOperation.hasMoreDatabaseObjects())
            {
                for (SqlObject sqlObject : sqlOperation
                        .getMoreDatabaseObjects())
                {
                    count += invalidateTable(sqlObject.getSchema(),
                            defaultSchema, sqlObject.getName());
                }
            }
            return count;
        }
        return 0;
    }

    // Generate a key for table.
    private String generateKey(String schema, String tableName)
    {
        StringBuffer key = new StringBuffer();
        key.append(schema);
        key.append(".");
        key.append(tableName);
        return key.toString();
    }

    // Utility method to drop table metadata.
    private int invalidateTable(String schema, String defaultSchema,
            String tableName)
    {
        String key;
        if (schema == null)
            key = generateKey(defaultSchema, tableName);
        else
            key = generateKey(schema, tableName);
        return cache.invalidate(key);
    }
}