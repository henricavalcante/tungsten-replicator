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
 * Implements simple unit tests on SqlOperation class.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class TestSqlOperation
{
    /** Verify default constructor settings. */
    @Test
    public void testDefaultConstructor() throws Exception
    {
        SqlOperation op = new SqlOperation();
        Assert.assertNull(op.getName());
        Assert.assertNull(op.getSchema());
        Assert.assertEquals(op.getObjectType(), SqlOperation.UNRECOGNIZED);
        Assert.assertEquals(op.getOperation(), SqlOperation.UNRECOGNIZED);
    }

    /** Verify constructor with explicit metadata values. */
    @Test
    public void testExplicitConstructor() throws Exception
    {
        SqlOperation op = new SqlOperation(SqlOperation.TABLE,
                SqlOperation.CREATE, "foo", "bar");
        Assert.assertEquals("foo", op.getSchema());
        Assert.assertEquals("bar", op.getName());
        Assert.assertEquals(op.getObjectType(), SqlOperation.TABLE);
        Assert.assertEquals(op.getOperation(), SqlOperation.CREATE);
    }

    /** Verify assignment of qualified name with and without schema. */
    @Test
    public void testQname() throws Exception
    {
        SqlOperation op = new SqlOperation(SqlOperation.TABLE,
                SqlOperation.CREATE, "x", "y");

        op.setQualifiedName("foo.bar");
        Assert.assertEquals("foo", op.getSchema());
        Assert.assertEquals("bar", op.getName());
        
        op.setQualifiedName("foo");
        Assert.assertNull(op.getSchema());
        Assert.assertEquals("foo", op.getName());
    }
}