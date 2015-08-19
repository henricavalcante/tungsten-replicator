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

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests SQL parsing capabilities for different DBMS types. (Currently parsing
 * is only supported for MySQL.)
 */
public class TestSqlStatementParser
{
    /**
     * Verify that basic wiring works for MySQL.
     */
    @Test
    public void testBasicParsing() throws Exception
    {
        String cmd = "create database foo";
        verifyCreateDatabase(cmd, Database.MYSQL);
    }

    /**
     * Verify all known DBMS types as well as the infamous null type.
     */
    @Test
    public void testAllDbmsTypes() throws Exception
    {
        String[] dbmsTypes = {Database.MYSQL, Database.ORACLE,
                Database.POSTGRESQL, Database.UNKNOWN, null};
        String cmd = "create database foo";
        for (String dbmsType : dbmsTypes)
        {
            verifyCreateDatabase(cmd, dbmsType);
        }
    }

    /**
     * Verify that we can do a large number of parses in a short period of time.
     */
    @Test
    public void testManyParsingCalls() throws Exception
    {
        String cmd = "create database foo";
        long startMillis = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++)
        {
            verifyCreateDatabase(cmd, Database.MYSQL);
        }
        long duration = System.currentTimeMillis() - startMillis;
        if ((duration / 1000) > 5)
            throw new Exception("Parsing calls took way too long");
    }

    /**
     * Run a simple parsing test.
     */
    private void verifyCreateDatabase(String cmd, String dbmsType)
    {
        SqlStatementParser p = SqlStatementParser.getParser();
        SqlOperation op = p.parse(cmd, dbmsType);
        Assert.assertEquals("Found object type: " + dbmsType,
                SqlOperation.SCHEMA, op.getObjectType());
        Assert.assertEquals("Found operation: " + dbmsType,
                SqlOperation.CREATE, op.getOperation());
        Assert.assertEquals("Found database: " + dbmsType, "foo",
                op.getSchema());
        Assert.assertNull("No table: " + dbmsType, op.getName());
        Assert.assertTrue("Is autocommit: " + dbmsType, op.isAutoCommit());
    }
}