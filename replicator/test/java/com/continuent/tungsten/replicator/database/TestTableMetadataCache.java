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
 * Initial developer(s): Robert Hodges and Scott Martin
 * Contributor(s): 
 */

package com.continuent.tungsten.replicator.database;

import java.sql.Types;

import junit.framework.Assert;

import org.junit.Test;

/**
 * This class tests the TableMetadataCache. There is a small number of cases as
 * the underlying IndexedLRUCache has its own unit tests.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class TestTableMetadataCache
{
    /**
     * Ensure we can cache and retrieve Table instances by their names.
     */
    @Test
    public void testCacheTables() throws Exception
    {
        String[] schemas = {"a", "b", "c"};
        String[] tableNames = {"x", "y", "z"};

        // Create 3 tables in 3 schemas.
        TableMetadataCache tmc = this.populateCache(schemas, tableNames);

        // Ensure we find the proper number of items in the cache.
        Assert.assertEquals("Expected cache size", 9, tmc.size());

        // Ensure we can fetch all tables back.
        int tab = 0;
        for (String schema : schemas)
        {
            for (String tableName : tableNames)
            {
                Table t = tmc.retrieve(schema, tableName);
                Assert.assertNotNull("Found table", t);
                Assert.assertEquals("Schema name", schema, t.getSchema());
                Assert.assertEquals("Schema name", tableName, t.getName());
                tab++;
            }
        }

        // Assert we found expected number of tables.
        Assert.assertEquals("Checking table total", 9, tab);

        // Clear the cache.
        tmc.invalidateAll();
    }

    /**
     * Ensure we can invalidate tables using explicit names or SQL operations.
     */
    @Test
    public void testInvalidation() throws Exception
    {
        // Create 3 tables in 3 schemas.
        String[] schemas = {"a", "b", "c"};
        String[] tableNames = {"x", "y", "z"};
        TableMetadataCache tmc = this.populateCache(schemas, tableNames);

        // Invalidate by table name.
        int invalidated = tmc.invalidateTable("a", "x");
        Assert.assertEquals("Specific table", 1, invalidated);

        invalidated = tmc.invalidateTable("a", "x");
        Assert.assertEquals("Specific table", 0, invalidated);

        // Invalidate by database.
        invalidated = tmc.invalidateSchema("a");
        Assert.assertEquals("Specific schema", 2, invalidated);

        invalidated = tmc.invalidateSchema("a");
        Assert.assertEquals("Specific schema", 0, invalidated);

        // Invalidate by DROP DATABASE.
        SqlOperation op = new SqlOperation(SqlOperation.SCHEMA,
                SqlOperation.DROP, "b", null);
        invalidated = tmc.invalidate(op, "a");
        Assert.assertEquals("drop database", 3, invalidated);

        invalidated = tmc.invalidate(op, "a");
        Assert.assertEquals("drop database", 0, invalidated);

        // Invalidate by DROP TABLE.
        op = new SqlOperation(SqlOperation.TABLE, SqlOperation.DROP, "c", "x");
        invalidated = tmc.invalidate(op, "d");
        Assert.assertEquals("drop table", 1, invalidated);

        invalidated = tmc.invalidate(op, "d");
        Assert.assertEquals("drop table", 0, invalidated);

        // Invalidate by ALTER TABLE.
        op = new SqlOperation(SqlOperation.TABLE, SqlOperation.ALTER, null, "y");
        invalidated = tmc.invalidate(op, "c");
        Assert.assertEquals("alter table", 1, invalidated);

        invalidated = tmc.invalidate(op, "c");
        Assert.assertEquals("alter table", 0, invalidated);

        // Not invalidated by an insert.
        op = new SqlOperation(SqlOperation.TABLE, SqlOperation.INSERT, "c", "z");
        invalidated = tmc.invalidate(op, "c");
        Assert.assertEquals("alter table", 0, invalidated);

        // Clear the cache.
        tmc.invalidateAll();
    }

    // Create tables.
    public TableMetadataCache populateCache(String[] schemas,
            String[] tableNames) throws Exception
    {
        Table[] tables = new Table[schemas.length * tableNames.length];
        TableMetadataCache tmc = new TableMetadataCache(100);
        int tab = 0;

        for (String schema : schemas)
        {
            for (String tableName : tableNames)
            {
                Column historySeqno = new Column("seqno", Types.BIGINT);
                tables[tab] = new Table(schema, tableName);
                tables[tab].AddColumn(historySeqno);
                tmc.store(tables[tab]);
                tab++;
            }
        }

        return tmc;
    }
}
