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
 * Tests table matching rules.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class TestTableMatcher
{
    /**
     * Verify that an empty matcher does not match anything.
     */
    @Test
    public void testEmpty() throws Exception
    {
        TableMatcher tm = new TableMatcher();

        tm.prepare(null);
        Assert.assertFalse(tm.match(null, null));
        Assert.assertFalse(tm.match("test", "foo"));

        tm.prepare("");
        Assert.assertFalse(tm.match("test", "foo"));

        tm.prepare(",,,");
        Assert.assertFalse(tm.match("test", "foo"));
        Assert.assertFalse(tm.match("", ""));
    }

    /**
     * Verify that we match schema names without wild cards.
     */
    @Test
    public void testBasicSchemas() throws Exception
    {
        TableMatcher tm = new TableMatcher();
        tm.prepare("test1,test2");

        Assert.assertTrue(tm.match("test1", null));
        Assert.assertTrue(tm.match("test2", "foo"));
        Assert.assertFalse(tm.match("test", ""));
        Assert.assertFalse(tm.match("test11", ""));
    }

    /**
     * Verify that we match schema names with wild cards.
     */
    @Test
    public void testWildSchemas() throws Exception
    {
        TableMatcher tm = new TableMatcher();
        tm.prepare("test?,special*");

        Assert.assertTrue(tm.match("test1", null));
        Assert.assertTrue(tm.match("test1", "foo"));
        Assert.assertFalse(tm.match("test12", ""));
        Assert.assertFalse(tm.match("xtest1", ""));
        Assert.assertFalse(tm.match("test", ""));

        Assert.assertTrue(tm.match("special", ""));
        Assert.assertTrue(tm.match("specialX", "foo"));
        Assert.assertFalse(tm.match("Sspecial", "foo"));
    }

    /**
     * Verify that we match table names without wild cards.
     */
    @Test
    public void testBasicTables() throws Exception
    {
        TableMatcher tm = new TableMatcher();
        tm.prepare("test1.foo,test2.bar");

        Assert.assertTrue(tm.match("test1", "foo"));
        Assert.assertTrue(tm.match("test2", "bar"));

        Assert.assertFalse(tm.match("test", "foo"));
        Assert.assertFalse(tm.match("test1", null));
        Assert.assertFalse(tm.match("test1", "bar"));
    }

    /**
     * Verify that we match table names with wild cards.
     */
    @Test
    public void testWildTables() throws Exception
    {
        TableMatcher tm = new TableMatcher();
        tm.prepare("test?.foo,test?.b*r,special.fo?");

        Assert.assertTrue(tm.match("test1", "foo"));
        Assert.assertFalse(tm.match("test12", "foo"));
        Assert.assertFalse(tm.match("test1", "foo1"));

        Assert.assertTrue(tm.match("test1", "br"));
        Assert.assertTrue(tm.match("test2", "baaar"));
        Assert.assertFalse(tm.match("test3", "bara"));
        Assert.assertFalse(tm.match("test3", "bara"));

        Assert.assertTrue(tm.match("special", "foo"));
        Assert.assertTrue(tm.match("special", "for"));
        Assert.assertFalse(tm.match("special", "fo"));
        Assert.assertFalse(tm.match("special", "fooo"));
    }

    /**
     * Verify that we match mixed schema and table names.
     */
    @Test
    public void testMixed() throws Exception
    {
        TableMatcher tm = new TableMatcher();
        tm.prepare("test,test1.bar,*.foo");

        // These should all match.
        Assert.assertTrue(tm.match("test", "fix"));
        Assert.assertTrue(tm.match("test", "bar"));
        Assert.assertTrue(tm.match("test1", "bar"));
        Assert.assertTrue(tm.match("test25", "foo"));

        // These should not match.
        Assert.assertFalse(tm.match("test25", "bar"));
        Assert.assertFalse(tm.match("test1", "barx"));
        Assert.assertFalse(tm.match("db25", "xfoo"));
    }

    /**
     * Verify that we match mixed schema and table names.
     */
    @Test
    public void testSpecialCharacters() throws Exception
    {
        TableMatcher tm = new TableMatcher();
        tm.prepare("test.m-t_*,test.mt_*");

        // These should all match.
        Assert.assertTrue(tm.match("test", "m-t_1"));
        Assert.assertTrue(tm.match("test", "mt_1"));
        Assert.assertTrue(tm.match("test", "m-t_$1"));
    }

}