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

package com.continuent.tungsten.replicator.dbms;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests behavior of StatementData class to ensure accessors function properly
 * with both binary as well as string statements.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class TestStatementData
{
    /**
     * Confirm we can create statement data instance and get the query back.
     */
    @Test
    public void testStatementData() throws Exception
    {
        StatementData sd = new StatementData("foo");
        Assert.assertEquals("Checking value", "foo", sd.getQuery());
        sd.appendToQuery("bar");
        Assert.assertEquals("Checking appended value", "foobar", sd.getQuery());
    }

    /**
     * Confirm we can store query as binary data and get it back.
     */
    @Test
    public void testBinaryQuery() throws Exception
    {
        // Test values are Cafe' Be'be' where ' is an accent grave.
        String value1 = "Caf\u00E9";
        byte[] value1Bytes = value1.getBytes("ISO8859_1");
        String value2 = "B\u00E9b\u00E9";
        String both = value1 + value2;
        byte[] bothBytes = both.getBytes("ISO8859_1");

        // Confirm we can set and get the binary array value back.
        StatementData sd = new StatementData(null);
        sd.setCharset("ISO8859_1");
        sd.setQuery(value1Bytes);
        Assert.assertArrayEquals("Value matches in bytes", value1Bytes, sd
                .getQueryAsBytes());
        Assert.assertEquals("String value matches", value1, sd.getQuery());

        // Confirm that we can append a value and also get that back.
        sd.appendToQuery(value2);
        Assert.assertArrayEquals("Value matches in bytes", bothBytes, sd
                .getQueryAsBytes());
        Assert.assertEquals("String value matches", both, sd.getQuery());
    }
}