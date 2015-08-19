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

import junit.framework.Assert;

import org.junit.Test;

/**
 * Tests the MySQLOperationStringBuilder used to strip comments and leading
 * white space from MySQL statements.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class TestMySQLStringBuilder
{
    /**
     * Ensure leading whitespace is removed.
     */
    @Test
    public void testLeadingWhitespace() throws Exception
    {
        MySQLOperationStringBuilder sb = new MySQLOperationStringBuilder(25);
        Assert.assertEquals("Empty", "", sb.build(" "));
        Assert.assertEquals("String", "abc", sb.build(" abc"));
        Assert.assertEquals("String with blanks", "abc  abc",
                sb.build(" abc  abc"));
        Assert.assertEquals("String with trailing", "abc   ",
                sb.build("   abc   "));
    }

    /**
     * Ensure normal comments are completely removed
     */
    @Test
    public void testNormalComments() throws Exception
    {
        MySQLOperationStringBuilder sb = new MySQLOperationStringBuilder(100);
        Assert.assertEquals("Empty", "", sb.build("/**/"));
        Assert.assertEquals("String", "abc", sb.build("/*abc*/abc"));
        Assert.assertEquals("String", "abc", sb.build("/*def*/abc/*def*/"));
        Assert.assertEquals("String", "abc", sb.build(" /*def*/ /*def*/ abc"));
        Assert.assertEquals("String", "abc", sb.build("abc/*def*/"));
        Assert.assertEquals("String", "abcghi", sb.build("abc/*def*/ghi"));
        Assert.assertEquals("String", "abc ghi", sb.build("abc/*def*/ ghi"));
        Assert.assertEquals(
                "String",
                "delete  from foo where id=1",
                sb.build("/* comment */ delete /* comment */ from foo where id=1"));

    }

    /**
     * Ensure bang comment delimiters are removed.
     */
    @Test
    public void testBangComments() throws Exception
    {
        MySQLOperationStringBuilder sb = new MySQLOperationStringBuilder(100);
        Assert.assertEquals("Empty", "", sb.build("/*!12345*/"));
        Assert.assertEquals("String", "abc", sb.build("/*!23456 abc*/"));
        Assert.assertEquals("String", "abc  def  ghi",
                sb.build("abc /*!23456 def */ ghi"));
        Assert.assertEquals("String", "CREATE DATABASE  IF NOT EXISTS `foo`  "
                + "DEFAULT CHARACTER SET latin1 ", sb
                .build("CREATE DATABASE /*!32312 IF NOT EXISTS*/ `foo` "
                        + "/*!40100 DEFAULT CHARACTER SET latin1 */"));
    }

    /**
     * Ensure we always truncate at the expected number of characters.
     */
    @Test
    public void testTruncation() throws Exception
    {
        MySQLOperationStringBuilder sb = new MySQLOperationStringBuilder(10);
        Assert.assertEquals("Simple truncation", "123456789*",
                sb.build("123456789*1"));
        Assert.assertEquals("Simple truncation", "123456789*",
                sb.build(" 1234/*comment*/56789*1"));
        Assert.assertEquals("Simple truncation", "123456789*",
                sb.build(" 1234/*!9999956*/789*1"));
    }

    /**
     * Ensure "--" comment lines are removed completely.
     */
    @Test
    public void testMinusMinusComments() throws Exception
    {
        MySQLOperationStringBuilder sb = new MySQLOperationStringBuilder(250);
        Assert.assertEquals("Empty", "", sb.build("--"));
        Assert.assertEquals("Empty", "", sb.build("-- "));
        Assert.assertEquals("Empty", "", sb.build("-- X"));
        Assert.assertEquals("String", "abc", sb.build("--\nabc"));
        Assert.assertEquals("String", "def", sb.build("-- abc\ndef"));
        Assert.assertEquals("String", "abc  abc", sb.build("abc\n-- def\nabc"));
        Assert.assertEquals("String", "abc abc", sb.build("abc-- def\nabc"));
        Assert.assertEquals("String", "abc   def  ", sb.build("abc\n  def\n-- abc"));
        Assert.assertEquals(
                "Real thing",
                "DELETE my_event.*  "
                        + "FROM my_event as segment_event INNER JOIN"
                        + "           my_segment as segment ON segment_event.segment_fk",
                sb.build("-- segment_event\nDELETE my_event.* \n"
                        + "FROM my_event as segment_event INNER JOIN"
                        + "           my_segment as segment ON segment_event.segment_fk"));

    }

}