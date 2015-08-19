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

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests SQL name matching using a variety of typical SQL statements.
 * <p/>
 * As currently written, this test includes checks on MySQL schema. It needs to
 * be expanded to work with other database types, such as Drizzle.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class TestSqlOperationMatcher
{
    private static Logger logger = Logger.getLogger(SqlOperationMatcher.class);

    /**
     * Test unrecognized / junk values.
     */
    @Test
    public void testUnrecognized() throws Exception
    {
        String[] cmds = {"create xxxx database foo", "",
                "   create   DATABASxe  \"foo\"",
                "create `TABLE` `foo` /* hello*/"};
        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertEquals("Found database: " + cmd,
                    SqlOperation.UNRECOGNIZED, sqlName.getObjectType());
            Assert.assertEquals("Found database: " + cmd,
                    SqlOperation.UNRECOGNIZED, sqlName.getOperation());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
            Assert.assertNull("Found table: " + cmd, sqlName.getName());
        }
    }

    /**
     * Test basic create database commands.
     */
    @Test
    public void testCreateDb() throws Exception
    {
        String[] cmds = {
                "create database foo",
                "CREATE DATABASE IF NOT EXISTS foo",
                "CREATE DATABASE /*!32312 IF NOT EXISTS*/ `foo` /*!40100 DEFAULT CHARACTER SET latin1 */",
                "   create   DATABASe  \"foo\"",
                "create database `foo` /* hello*/"};

        String[] cmdsDash = {
                "create database `foo-dash`",
                "CREATE DATABASE IF NOT EXISTS `foo-dash`",
                "CREATE DATABASE /*!32312 IF NOT EXISTS*/ `foo-dash` /*!40100 DEFAULT CHARACTER SET latin1 */",
                "   create   DATABASe  \"foo-dash\"",
                "create database `foo-dash` /* hello*/"};

        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertEquals("Found database: " + cmd, SqlOperation.SCHEMA,
                    sqlName.getObjectType());
            Assert.assertEquals("Found database: " + cmd, SqlOperation.CREATE,
                    sqlName.getOperation());
            Assert.assertEquals("Found database: " + cmd, "foo",
                    sqlName.getSchema());
            Assert.assertNull("Found database: " + cmd, sqlName.getName());
            Assert.assertTrue("Is autocommit: " + cmd, sqlName.isAutoCommit());
        }

        for (String cmd : cmdsDash)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertEquals("Found database: " + cmd, SqlOperation.SCHEMA,
                    sqlName.getObjectType());
            Assert.assertEquals("Found database: " + cmd, SqlOperation.CREATE,
                    sqlName.getOperation());
            Assert.assertEquals("Found database: " + cmd, "foo-dash",
                    sqlName.getSchema());
            Assert.assertNull("Found database: " + cmd, sqlName.getName());
            Assert.assertTrue("Is autocommit: " + cmd, sqlName.isAutoCommit());
        }

    }

    /**
     * Test basic drop database commands.
     */
    @Test
    public void testDropDb() throws Exception
    {
        String[] cmds = {"drop database foo", "DROP DATABASE IF EXISTS foo",
                "  droP   DATABASe  \"foo\"", "drop database `foo` /* hello*/"};

        String[] cmdsDash = {"drop database `foo-dash`",
                "DROP DATABASE IF EXISTS `foo-dash`",
                "  droP   DATABASe  \"foo-dash\"",
                "drop database `foo-dash` /* hello*/"};

        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertEquals("Found database: " + cmd, SqlOperation.SCHEMA,
                    sqlName.getObjectType());
            Assert.assertEquals("Found database: " + cmd, SqlOperation.DROP,
                    sqlName.getOperation());
            Assert.assertEquals("Found database: " + cmd, "foo",
                    sqlName.getSchema());
            Assert.assertNull("Found database: " + cmd, sqlName.getName());
            Assert.assertTrue("Is autocommit: " + cmd, sqlName.isAutoCommit());
        }

        for (String cmd : cmdsDash)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertEquals("Found database: " + cmd, SqlOperation.SCHEMA,
                    sqlName.getObjectType());
            Assert.assertEquals("Found database: " + cmd, SqlOperation.DROP,
                    sqlName.getOperation());
            Assert.assertEquals("Found database: " + cmd, "foo-dash",
                    sqlName.getSchema());
            Assert.assertNull("Found database: " + cmd, sqlName.getName());
            Assert.assertTrue("Is autocommit: " + cmd, sqlName.isAutoCommit());
        }

    }

    /**
     * Test create table with and without db name.
     */
    @Test
    public void testCreateTable() throws Exception
    {
        String[] cmds1 = {"create table foo", "CREATE TABLE IF NOT EXISTS foo",
                "create   table   `foo` /* hello*/"};
        String[] cmds2 = {"create table bar.foo", "CREATE TABLE bar.foo",
                "creAtE TabLE \"bar\".\"foo\""};
        String[] cmds3 = {"   creAtE TEMPORary TabLE \"foo\"",
                "create temporary  table   `bar`.`foo` /* hello*/"};

        String[] cmdsDash1 = {"create table `foo-dash`",
                "CREATE TABLE IF NOT EXISTS `foo-dash`",
                "create   table   `foo-dash` /* hello*/"};
        String[] cmdsDash2 = {"create table `bar-dash`.`foo-dash`",
                "CREATE TABLE `bar-dash`.`foo-dash`",
                "creAtE TabLE \"bar-dash\".\"foo-dash\""};

        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.CREATE,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
            Assert.assertTrue("Is autocommit: " + cmd, sqlName.isAutoCommit());
        }

        for (String cmd : cmds2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.CREATE,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar",
                    sqlName.getSchema());
        }
        for (String cmd : cmds3)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.CREATE,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            if (sqlName.getSchema() != null)
                Assert.assertEquals("Found database: " + cmd, "bar",
                        sqlName.getSchema());
            Assert.assertFalse("Is autocommit: " + cmd, sqlName.isAutoCommit());

        }

        for (String cmd : cmdsDash1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.CREATE,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo-dash",
                    sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
            Assert.assertTrue("Is autocommit: " + cmd, sqlName.isAutoCommit());
        }

        for (String cmd : cmdsDash2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.CREATE,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo-dash",
                    sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar-dash",
                    sqlName.getSchema());
        }

    }

    /**
     * Test drop table with and without db name.
     */
    @Test
    public void testDropTable() throws Exception
    {
        String[] cmds1 = {"drop table foo", "DROP TABLE foo",
                "DrOp TabLE \"foo\"", "drop    table  if   exists  foo"};
        String[] cmds2 = {"drop table bar.foo", "DROP TABLE bar.foo",
                "drop   table   `bar`.`foo` /* hello*/",
                "drop table  if  exists bar.foo",
                "DROP TABLE IF EXISTS TUNGSTEN_INFO.bravo, `bar`.`foo`"};
        String[] cmds3 = {"drop temporary  table   `bar`.`foo` /* hello*/",
                "DRop TemporarY TabLE \"bar\".\"foo\""};

        String[] cmdsDash1 = {"drop table `foo-dash`", "DROP TABLE `foo-dash`",
                "DrOp TabLE \"foo-dash\"",
                "drop    table  if   exists  `foo-dash`"};
        String[] cmdsDash2 = {"drop table `bar-dash`.`foo-dash`",
                "DROP TABLE `bar-dash`.`foo-dash`",
                "drop   table   `bar-dash`.`foo-dash` /* hello*/",
                "drop table  if  exists `bar-dash`.`foo-dash`",
                "DROP TABLE IF EXISTS TUNGSTEN_INFO.bravo, `bar-dash`.`foo-dash`"};

        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.DROP,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
            Assert.assertTrue("Is autocommit: " + cmd, sqlName.isAutoCommit());
        }

        for (String cmd : cmds2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.DROP,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar",
                    sqlName.getSchema());
        }

        for (String cmd : cmds3)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.DROP,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            if (sqlName.getSchema() != null)
                Assert.assertEquals("Found database: " + cmd, "bar",
                        sqlName.getSchema());
            Assert.assertFalse("Is autocommit: " + cmd, sqlName.isAutoCommit());
            Assert.assertEquals("Found database: " + cmd, "bar",
                    sqlName.getSchema());
        }

        for (String cmd : cmdsDash1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.DROP,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo-dash",
                    sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
            Assert.assertTrue("Is autocommit: " + cmd, sqlName.isAutoCommit());
        }

        for (String cmd : cmdsDash2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.DROP,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo-dash",
                    sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar-dash",
                    sqlName.getSchema());
        }
    }

    /**
     * Test alter table commands.
     */
    @Test
    public void testAlterTable() throws Exception
    {
        String[] cmds1 = {
                "alter ignore table foo drop column ts1",
                "AlTeR IGNORE   table `foo` drop primary key",
                "alter table /* hello */ \"foo\"   add constraint primary key (id)",
                "alter table foo add column ts1 timestamp default now()"};
        String[] cmds2 = {
                "alter ignore table bar.foo drop column ts1",
                "AlTeR IGNORE   table `bar`.`foo` drop primary key",
                "alter table /* hello */ \"bar\".\"foo\"   add constraint primary key (id)",
                "alter table bar.foo add column ts1 timestamp default now()"};
        String[] cmdsDash1 = {
                "alter ignore table `foo-dash` drop column ts1",
                "AlTeR IGNORE   table `foo-dash` drop primary key",
                "alter table /* hello */ \"foo-dash\"   add constraint primary key (id)",
                "alter table `foo-dash` add column ts1 timestamp default now()"};
        String[] cmdsDash2 = {
                "alter ignore table `bar-dash`.`foo-dash` drop column ts1",
                "AlTeR IGNORE   table `bar-dash`.`foo-dash` drop primary key",
                "alter table /* hello */ \"bar-dash\".\"foo-dash\"   add constraint primary key (id)",
                "alter table `bar-dash`.`foo-dash` add column ts1 timestamp default now()"};

        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.ALTER,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
            Assert.assertTrue("Is autocommit: " + cmd, sqlName.isAutoCommit());
        }

        for (String cmd : cmds2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.ALTER,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar",
                    sqlName.getSchema());
        }
        for (String cmd : cmdsDash1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.ALTER,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo-dash",
                    sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
            Assert.assertTrue("Is autocommit: " + cmd, sqlName.isAutoCommit());
        }

        for (String cmd : cmdsDash2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.ALTER,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo-dash",
                    sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar-dash",
                    sqlName.getSchema());
        }

    }

    /**
     * Test rename table with and without db name. Samples include standard
     * commands from pt-online-schema-change.
     */
    @Test
    public void testRenameTable() throws Exception
    {
        String[] cmds1 = {"rename table foo to test.bar",
                "rename table foo to bar",
                "RENAME TABLE `foo` TO `_foo_old`, `_foo_new` TO `foo`"};
        String[] cmds2 = {"rename table bar.foo to bar.bar",
                "rename table bar.foo to bar",
                "RENAME TABLE `bar`.`foo` TO `bar`.`_foo_old`, `bar`.`_foo_new` TO `bar`.`foo`"};
        String[] cmdsDash1 = {"rename table `foo-dash` to test.bar",
                "rename table `foo-dash` to bar",
                "RENAME TABLE `foo-dash` TO `_foo_old`, `_foo_new` TO `foo`"};
        String[] cmdsDash2 = {
                "rename table `bar-dash`.`foo-dash` to bar.bar",
                "rename table `bar-dash`.`foo-dash` to bar",
                "RENAME TABLE `bar-dash`.`foo-dash` TO `bar`.`_foo_old`, `bar`.`_foo_new` TO `bar`.`foo`"};

        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.RENAME,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
            Assert.assertTrue("Is autocommit: " + cmd, sqlName.isAutoCommit());
        }

        for (String cmd : cmds2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.RENAME,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar",
                    sqlName.getSchema());
        }
        for (String cmd : cmdsDash1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.RENAME,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo-dash",
                    sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
            Assert.assertTrue("Is autocommit: " + cmd, sqlName.isAutoCommit());
        }

        for (String cmd : cmdsDash2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.RENAME,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo-dash",
                    sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar-dash",
                    sqlName.getSchema());
        }

    }

    /**
     * Test insert with and without db name.
     */
    @Test
    public void testInsert() throws Exception
    {
        String[] cmds1 = {"insert into foo values(1)",
                "INSERT INTO foo(id,msg) values(1, 'data')",
                "InSeRt  LOW_PRIORITY  InTo \"foo\" values (1, 'data')",
                "insert  delayed into    `foo` /* hello*/ (one,too) values(1,2)"};
        String[] cmds2 = {"insert into bar.foo values(1)",
                "INSERT INTO bar.foo(id,msg) values(1, 'data')",
                "InSeRt InTo \"bar\".\"foo\" values (1, 'data')",
                "insert   ignore  into    `bar`.`foo` /* hello*/ (one,too) values(1,2)"};

        String[] cmdsDash1 = {"insert into `foo-dash` values(1)",
                "INSERT INTO `foo-dash`(id,msg) values(1, 'data')",
                "InSeRt  LOW_PRIORITY  InTo \"foo-dash\" values (1, 'data')",
                "insert  delayed into    `foo-dash` /* hello*/ (one,too) values(1,2)"};
        String[] cmdsDash2 = {
                "insert into `bar-dash`.`foo-dash` values(1)",
                "INSERT INTO `bar-dash`.`foo-dash`(id,msg) values(1, 'data')",
                "InSeRt InTo \"bar-dash\".\"foo-dash\" values (1, 'data')",
                "insert   ignore  into    `bar-dash`.`foo-dash` /* hello*/ (one,too) values(1,2)"};

        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.INSERT,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
            Assert.assertFalse("Is autocommit: " + cmd, sqlName.isAutoCommit());
        }

        for (String cmd : cmds2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.INSERT,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar",
                    sqlName.getSchema());
        }

        for (String cmd : cmdsDash1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.INSERT,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo-dash",
                    sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
            Assert.assertFalse("Is autocommit: " + cmd, sqlName.isAutoCommit());
        }

        for (String cmd : cmdsDash2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.INSERT,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo-dash",
                    sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar-dash",
                    sqlName.getSchema());
        }

    }

    /**
     * Test create and drop of indexes with and without the db name.
     */
    @Test
    public void testCreateDropIndex() throws Exception
    {
        // CREATE INDEX COMMANDS.
        String[] cmds1 = {"CREATE INDEX my_index ON foo(c1)",
                "CREATE ONLINE INDEX `index` ON `foo`(c2_2)",
                "   CREATE FULLTEXT INDEX `index` ON \"foo\"(c2_2)"};
        String[] cmds2 = {"CREATE INDEX my_index ON bar.foo(c1)",
                "CREATE ONLINE INDEX `index` ON `bar`.`foo`(c2_2)",
                "   CREATE FULLTEXT INDEX `index` ON \"bar\".\"foo\"(c2_2)"};
        String[] cmdsDash1 = {"CREATE INDEX my_index ON `foo-dash`(c1)",
                "CREATE ONLINE INDEX `index` ON `foo-dash`(c2_2)",
                "   CREATE FULLTEXT INDEX `index` ON \"foo-dash\"(c2_2)"};
        String[] cmdsDash2 = {
                "CREATE INDEX my_index ON `bar-dash`.`foo-dash`(c1)",
                "CREATE ONLINE INDEX `index` ON `bar-dash`.`foo-dash`(c2_2)",
                "   CREATE FULLTEXT INDEX `index` ON \"bar-dash\".\"foo-dash\"(c2_2)"};

        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.INDEX,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.CREATE,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
        }

        for (String cmd : cmds2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.INDEX,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.CREATE,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar",
                    sqlName.getSchema());
        }

        for (String cmd : cmdsDash1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.INDEX,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.CREATE,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo-dash",
                    sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
        }

        for (String cmd : cmdsDash2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.INDEX,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.CREATE,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo-dash",
                    sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar-dash",
                    sqlName.getSchema());
        }

        // DROP INDEX COMMANDS.
        String[] cmds3 = {"DROP INDEX my_index ON foo(c1)",
                "DROP INDEX `index` ON `foo`(c2_2)",
                "   DROP INDEX `index` ON \"foo\"(c2_2)"};
        String[] cmds4 = {"DROP INDEX my_index ON bar.foo(c1)",
                "DROP INDEX `index` ON `bar`.`foo`(c2_2)",
                "   DROP INDEX `index` ON \"bar\".\"foo\"(c2_2)"};
        String[] cmdsDash3 = {"DROP INDEX my_index ON `foo-dash`(c1)",
                "DROP INDEX `index` ON `foo-dash`(c2_2)",
                "   DROP INDEX `index` ON \"foo-dash\"(c2_2)"};
        String[] cmdsDash4 = {
                "DROP INDEX my_index ON `bar-dash`.`foo-dash`(c1)",
                "DROP INDEX `index` ON `bar-dash`.`foo-dash`(c2_2)",
                "   DROP INDEX `index` ON \"bar-dash\".\"foo-dash\"(c2_2)"};

        for (String cmd : cmds3)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.INDEX,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.DROP,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
        }

        for (String cmd : cmds4)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.INDEX,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.DROP,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar",
                    sqlName.getSchema());
        }

        for (String cmd : cmdsDash3)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.INDEX,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.DROP,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo-dash",
                    sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
        }

        for (String cmd : cmdsDash4)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.INDEX,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.DROP,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo-dash",
                    sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar-dash",
                    sqlName.getSchema());
        }

    }

    /**
     * Test replace with and without db name.
     */
    @Test
    public void testReplace() throws Exception
    {
        String[] cmds1 = {"replace into foo values(1)",
                "REPLACE foo(id,msg) values(1, 'data')",
                "RePlACe InTo \"foo\" values (1, 'data')",
                "replace   into    `foo` /* hello*/ (one,too) values(1,2)"};
        String[] cmds2 = {"replace into bar.foo values(1)",
                "REPLACE bar.foo(id,msg) values(1, 'data')",
                "RePlAcE InTo \"bar\".\"foo\" values (1, 'data')",
                "replace   into    `bar`.`foo` /* hello*/ (one,too) values(1,2)"};

        String[] cmdsDash1 = {"replace into `foo-dash` values(1)",
                "REPLACE `foo-dash`(id,msg) values(1, 'data')",
                "RePlACe InTo \"foo-dash\" values (1, 'data')",
                "replace   into    `foo-dash` /* hello*/ (one,too) values(1,2)"};
        String[] cmdsDash2 = {"replace into `bar-dash`.`foo-dash` values(1)",
                "REPLACE `bar-dash`.`foo-dash`(id,msg) values(1, 'data')",
                "RePlAcE InTo \"bar-dash\".\"foo-dash\" values (1, 'data')",
                "replace   into    `bar-dash`.`foo-dash` /* hello*/ (one,too) values(1,2)"};

        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd,
                    SqlOperation.REPLACE, sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
            Assert.assertFalse("Is autocommit: " + cmd, sqlName.isAutoCommit());
        }

        for (String cmd : cmds2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd,
                    SqlOperation.REPLACE, sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar",
                    sqlName.getSchema());
        }

        for (String cmd : cmdsDash1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd,
                    SqlOperation.REPLACE, sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo-dash",
                    sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
            Assert.assertFalse("Is autocommit: " + cmd, sqlName.isAutoCommit());
        }

        for (String cmd : cmdsDash2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd,
                    SqlOperation.REPLACE, sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo-dash",
                    sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar-dash",
                    sqlName.getSchema());
        }

    }

    /**
     * Test update with and without db name.
     */
    @Test
    public void testUpdate() throws Exception
    {
        String[] cmds1 = {"update /* comment */ foo set id=1",
                "UPDATE foo set id=1,msg='data' where \"msg\" = 'value'",
                "UpDaTe LOW_PRIORITY \"foo\" set id=1 WHere msg= 'data'",
                "update  `foo` /* hello*/ set id=1"};
        String[] cmds2 = {"update bar.foo set id=1",
                "UPDATE bar.foo set id=1,msg='data' where \"msg\" = 'value'",
                "UpDaTe \"bar\".\"foo\" set id=1 WHere msg= 'data'",
                "update  LOW_PRIORITY IGNORE   `bar`.`foo` /* hello*/ set id=1"};

        String[] cmdsDash1 = {
                "update /* comment */ `foo-dash` set id=1",
                "UPDATE `foo-dash` set id=1,msg='data' where \"msg\" = 'value'",
                "UpDaTe LOW_PRIORITY \"foo-dash\" set id=1 WHere msg= 'data'",
                "update  `foo-dash` /* hello*/ set id=1"};
        String[] cmdsDash2 = {
                "update `bar-dash`.`foo-dash` set id=1",
                "UPDATE `bar-dash`.`foo-dash` set id=1,msg='data' where \"msg\" = 'value'",
                "UpDaTe \"bar-dash\".\"foo-dash\" set id=1 WHere msg= 'data'",
                "update  LOW_PRIORITY IGNORE   `bar-dash`.`foo-dash` /* hello*/ set id=1"};

        String[] cmds3 = {
                "UPDATE `users` SET `confirmation_token` = NULL, `confirmed_at` = '2014-11-30 19:33:56', `updated_at` = '2014-11-30 19:33:56' WHERE `users`.`id` = 521",
                "UPDATE users SET `confirmation_token` = NULL, `confirmed_at` = '2014-11-30 19:33:56', `updated_at` = '2014-11-30 19:33:56' WHERE `users`.`id` = 521",
                "UPDATE \"users\" SET \"confirmation_token\" = NULL, \"confirmed_at\" = '2014-11-30 19:33:56', \"updated_at\" = '2014-11-30 19:33:56' WHERE \"users\".\"id\" = 521"};

        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.UPDATE,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
            Assert.assertFalse("Is autocommit: " + cmd, sqlName.isAutoCommit());
        }

        for (String cmd : cmds2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.UPDATE,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar",
                    sqlName.getSchema());
        }

        for (String cmd : cmdsDash1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.UPDATE,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo-dash",
                    sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
            Assert.assertFalse("Is autocommit: " + cmd, sqlName.isAutoCommit());
        }

        for (String cmd : cmdsDash2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.UPDATE,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo-dash",
                    sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar-dash",
                    sqlName.getSchema());
        }

        for (String cmd : cmds3)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.UPDATE,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "users",
                    sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
            Assert.assertFalse("Is autocommit: " + cmd, sqlName.isAutoCommit());
        }

    }

    /**
     * Test delete with and without db name.
     */
    @Test
    public void testDelete() throws Exception
    {
        String[] cmds1 = {
                "/* comment */ delete /* comment */ from foo where id=1",
                "DELETE from foo WHERE \"msg\" = 'value'",
                "DELETE from \"foo\" WHere msg= 'data'",
                "delete  from   `foo` /* hello*/ where id=1",
                "DElete LOW_PRIORITY QUICK IGNORE from \"foo\""};
        String[] cmds2 = {"delete from bar.foo where id=1",
                "DELETE from bar.foo WHERE \"msg\" = 'value'",
                "DELETE from \"bar\".\"foo\" WHere msg= 'data'",
                "delete      from  `bar`.`foo` /* hello*/ where id=1",
                "DElete LOW_PRIORITY QUICK IGNORE from bar.\"foo\"",
                "delete a from bar.foo a join bar.foo b on a.id = b.id where b.val = 2"};

        String[] cmdsDash1 = {
                "/* comment */ delete /* comment */ from `foo-dash` where id=1",
                "DELETE from `foo-dash` WHERE \"msg\" = 'value'",
                "DELETE from \"foo-dash\" WHere msg= 'data'",
                "delete  from   `foo-dash` /* hello*/ where id=1",
                "DElete LOW_PRIORITY QUICK IGNORE from \"foo-dash\""};
        String[] cmdsDash2 = {
                "delete from `bar-dash`.`foo-dash` where id=1",
                "DELETE from `bar-dash`.`foo-dash` WHERE \"msg\" = 'value'",
                "DELETE from \"bar-dash\".\"foo-dash\" WHere msg= 'data'",
                "delete      from  `bar-dash`.`foo-dash` /* hello*/ where id=1",
                "DElete LOW_PRIORITY QUICK IGNORE from `bar-dash`.\"foo-dash\"",
                "delete a from `bar-dash`.`foo-dash` a join `bar-dash`.`foo-dash` b on a.id = b.id where b.val = 2"};

        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.DELETE,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
            Assert.assertFalse("Is autocommit: " + cmd, sqlName.isAutoCommit());
        }

        for (String cmd : cmds2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.DELETE,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar",
                    sqlName.getSchema());
        }
        for (String cmd : cmdsDash1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.DELETE,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo-dash",
                    sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
            Assert.assertFalse("Is autocommit: " + cmd, sqlName.isAutoCommit());
        }

        for (String cmd : cmdsDash2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.DELETE,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo-dash",
                    sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar-dash",
                    sqlName.getSchema());
        }

    }

    /**
     * Test truncate with and without db name.
     */
    @Test
    public void testTruncate() throws Exception
    {
        String[] cmds1 = {"truncate table foo", "TRUNCATE TABLE foo",
                "TRUNCATE    tABlE  \"foo\" "};
        String[] cmds2 = {"truncate table bar.foo", "TRUNCATE TABLE bar.foo",
                "TRUNCATE    tABlE  \"bar\".\"foo\" "};

        String[] cmdsDash1 = {"truncate table `foo-dash`",
                "TRUNCATE TABLE `foo-dash`", "TRUNCATE    tABlE  \"foo-dash\" "};
        String[] cmdsDash2 = {"truncate table `bar-dash`.`foo-dash`",
                "TRUNCATE TABLE `bar-dash`.`foo-dash`",
                "TRUNCATE    tABlE  \"bar-dash\".\"foo-dash\" "};

        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd,
                    SqlOperation.TRUNCATE, sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
        }

        for (String cmd : cmds2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd,
                    SqlOperation.TRUNCATE, sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar",
                    sqlName.getSchema());
        }

        for (String cmd : cmdsDash1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd,
                    SqlOperation.TRUNCATE, sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo-dash",
                    sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
        }

        for (String cmd : cmdsDash2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd,
                    SqlOperation.TRUNCATE, sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo-dash",
                    sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar-dash",
                    sqlName.getSchema());
        }

    }

    /**
     * Test LOAD DATA with and without db name.
     */
    @Test
    public void testLoadData() throws Exception
    {
        String[] cmds1 = {
                "load data local infile '/tmp/ld.txt' into table foo FIELDS TERMINATED BY ','",
                "LOAD DATA INFILE '/tmp/ld.txt' INTO TABLE foo",
                "LOAD DATA INFILE '/tmp/ld.txt' INTO TABLE \"foo\"",
                "loAd   datA    lOcal iNfilE '/tmp/ld.txt' into   table \"foo\" FIELDS TERMINATED BY ','",
                "LOAD DATA INFILE '/tmp/SQL_LOAD-1701-10901-48716.data' IGNORE "
                        + "INTO TABLE \"foo\" FIELDS TERMINATED BY ',' "
                        + "OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\\' LINES TERMINATED "
                        + "BY '\n' STARTING BY '' IGNORE 0 LINES (@var0, @var1) SET id = "
                        + "@var0, data_10 = TRIM(TRIM(char(160) FROM TRIM(@var1))), "
                        + "call_now = '', upload_error_flag = 0, duplicate_flag = null"};
        String[] cmds2 = {
                "load data local infile '/tmp/ld.txt' into table bar.foo FIELDS TERMINATED BY ','",
                "LOAD DATA INFILE '/tmp/ld.txt' INTO TABLE bar.foo",
                "LOAD DATA INFILE '/tmp/ld.txt' INTO TABLE \"bar\".\"foo\"",
                "loAd   datA    lOcal iNfilE '/tmp/ld.txt' into   table bar.\"foo\" FIELDS TERMINATED BY ','",
                "LOAD DATA INFILE '/tmp/SQL_LOAD-1701-10901-48716.data' IGNORE "
                        + "INTO TABLE bar.foo FIELDS TERMINATED BY ',' "
                        + "OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\\' LINES TERMINATED "
                        + "BY '\n' STARTING BY '' IGNORE 0 LINES (@var0, @var1) SET id = "
                        + "@var0, data_10 = TRIM(TRIM(char(160) FROM TRIM(@var1))), "
                        + "call_now = '', upload_error_flag = 0, duplicate_flag = null"};

        String[] cmdsDash1 = {
                "load data local infile '/tmp/ld.txt' into table `foo-dash` FIELDS TERMINATED BY ','",
                "LOAD DATA INFILE '/tmp/ld.txt' INTO TABLE `foo-dash`",
                "LOAD DATA INFILE '/tmp/ld.txt' INTO TABLE \"foo-dash\"",
                "loAd   datA    lOcal iNfilE '/tmp/ld.txt' into   table \"foo-dash\" FIELDS TERMINATED BY ','",
                "LOAD DATA INFILE '/tmp/SQL_LOAD-1701-10901-48716.data' IGNORE "
                        + "INTO TABLE \"foo-dash\" FIELDS TERMINATED BY ',' "
                        + "OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\\' LINES TERMINATED "
                        + "BY '\n' STARTING BY '' IGNORE 0 LINES (@var0, @var1) SET id = "
                        + "@var0, data_10 = TRIM(TRIM(char(160) FROM TRIM(@var1))), "
                        + "call_now = '', upload_error_flag = 0, duplicate_flag = null"};
        String[] cmdsDash2 = {
                "load data local infile '/tmp/ld.txt' into table `bar-dash`.`foo-dash` FIELDS TERMINATED BY ','",
                "LOAD DATA INFILE '/tmp/ld.txt' INTO TABLE `bar-dash`.`foo-dash`",
                "LOAD DATA INFILE '/tmp/ld.txt' INTO TABLE \"bar-dash\".\"foo-dash\"",
                "loAd   datA    lOcal iNfilE '/tmp/ld.txt' into   table `bar-dash`.\"foo-dash\" FIELDS TERMINATED BY ','",
                "LOAD DATA INFILE '/tmp/SQL_LOAD-1701-10901-48716.data' IGNORE "
                        + "INTO TABLE `bar-dash`.`foo-dash` FIELDS TERMINATED BY ',' "
                        + "OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\\' LINES TERMINATED "
                        + "BY '\n' STARTING BY '' IGNORE 0 LINES (@var0, @var1) SET id = "
                        + "@var0, data_10 = TRIM(TRIM(char(160) FROM TRIM(@var1))), "
                        + "call_now = '', upload_error_flag = 0, duplicate_flag = null"};

        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd,
                    SqlOperation.LOAD_DATA, sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
            Assert.assertFalse("Is autocommit: " + cmd, sqlName.isAutoCommit());
        }

        for (String cmd : cmds2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd,
                    SqlOperation.LOAD_DATA, sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar",
                    sqlName.getSchema());
        }
        for (String cmd : cmdsDash1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd,
                    SqlOperation.LOAD_DATA, sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo-dash",
                    sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
            Assert.assertFalse("Is autocommit: " + cmd, sqlName.isAutoCommit());
        }

        for (String cmd : cmdsDash2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd,
                    SqlOperation.LOAD_DATA, sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo-dash",
                    sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar-dash",
                    sqlName.getSchema());
        }

    }

    /**
     * Test SET commands.
     */
    @Test
    public void testSet() throws Exception
    {
        String[] cmds1 = {"SET @var0 := NULL",
                "set session binlog_format = row"};
        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.SESSION,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.SET,
                    sqlName.getOperation());
            Assert.assertNull("Found name: " + cmd, sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
            Assert.assertFalse("Is autocommit: " + cmd, sqlName.isAutoCommit());
        }
    }

    /**
     * Test create procedure with and without db name.
     */
    @Test
    public void testCreateProcedure() throws Exception
    {
        String[] cmds1 = {
                "CREATE DEFINER=`root`@`localhost` PROCEDURE `foo`() begin select 1; end",
                "CREATE DEFINER=`root`@`localhost` PROCEDURE foo() begin select 1; end"};
        String[] cmds2 = {
                "CREATE DEFINER=`root`@`localhost` PROCEDURE `bar`.`foo`() begin select 1; end",
                "CREATE DEFINER=`root`@`localhost` PROCEDURE bar.foo() begin select 1; end"};

        String[] cmdsDash1 = {
                "CREATE DEFINER=`root`@`localhost` PROCEDURE `foo-dash`() begin select 1; end",
                "CREATE DEFINER=`root`@`localhost` PROCEDURE \"foo-dash\"() begin select 1; end"};
        String[] cmdsDash2 = {
                "CREATE DEFINER=`root`@`localhost` PROCEDURE `bar-dash`.`foo-dash`() begin select 1; end",
                "CREATE DEFINER=`root`@`localhost` PROCEDURE \"bar-dash\".\"foo-dash\"() begin select 1; end"};

        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.PROCEDURE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.CREATE,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
        }

        for (String cmd : cmds2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.PROCEDURE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.CREATE,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar",
                    sqlName.getSchema());
        }

        for (String cmd : cmdsDash1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.PROCEDURE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.CREATE,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo-dash",
                    sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
        }

        for (String cmd : cmdsDash2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.PROCEDURE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.CREATE,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo-dash",
                    sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar-dash",
                    sqlName.getSchema());
        }

    }

    /**
     * Test drop procedure with and without db name.
     */
    @Test
    public void testDropProcedure() throws Exception
    {
        String[] cmds1 = {"drop procedure foo", "DROP PROCEDURE foo",
                "DrOp PROCEDUre \"foo\""};
        String[] cmds2 = {"drop procedure bar.foo", "DROP PROCEDURE bar.foo",
                "DRop  PRocedurE \"bar\".\"foo\""};

        String[] cmdsDash1 = {"drop procedure `foo-dash`",
                "DROP PROCEDURE `foo-dash`", "DrOp PROCEDUre \"foo-dash\""};
        String[] cmdsDash2 = {"drop procedure `bar-dash`.`foo-dash`",
                "DROP PROCEDURE `bar-dash`.`foo-dash`",
                "DRop  PRocedurE \"bar-dash\".\"foo-dash\""};

        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.PROCEDURE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.DROP,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
        }

        for (String cmd : cmds2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.PROCEDURE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.DROP,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar",
                    sqlName.getSchema());
        }

        for (String cmd : cmdsDash1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.PROCEDURE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.DROP,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo-dash",
                    sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
        }

        for (String cmd : cmdsDash2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.PROCEDURE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.DROP,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo-dash",
                    sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar-dash",
                    sqlName.getSchema());
        }

    }

    /**
     * Test create view with and without db name.
     */
    @Test
    public void testCreateView() throws Exception
    {
        String[] cmds1 = {
                "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `foo` AS select 1",
                "CREATE OR REPLACE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `foo` AS select 1"};
        String[] cmds2 = {
                "CREATE OR REPLACE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `bar`.`foo` AS select 1",
                "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `bar`.`foo` AS select 1"};

        String[] cmdsDash1 = {
                "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `foo-dash` AS select 1",
                "CREATE OR REPLACE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `foo-dash` AS select 2"};
        String[] cmdsDash2 = {
                "CREATE OR REPLACE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `bar-dash`.`foo-dash` AS select 3",
                "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `bar-dash`.`foo-dash` AS select 4"};

        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.VIEW,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.CREATE,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
        }

        for (String cmd : cmds2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.VIEW,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.CREATE,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar",
                    sqlName.getSchema());
        }

        for (String cmd : cmdsDash1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.VIEW,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.CREATE,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo-dash",
                    sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
        }

        for (String cmd : cmdsDash2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.VIEW,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.CREATE,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo-dash",
                    sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar-dash",
                    sqlName.getSchema());
        }
    }

    /**
     * Test drop view with and without db name.
     */
    @Test
    public void testDropView() throws Exception
    {
        String[] cmds1 = {"drop view foo", "DROP VIEW foo",
                "DrOp VieW \"foo\"", "drop  view   `foo` /* hello*/",
                "drop    view  if   exists  foo"};
        String[] cmds2 = {"drop view bar.foo", "DROP VIEW bar.foo",
                "DRop VieW \"bar\".\"foo\"",
                "drop   view   `bar`.`foo` /* hello*/",
                "drop view  if  exists bar.foo"};

        String[] cmdsDash1 = {"drop view `foo-dash`", "DROP VIEW `foo-dash`",
                "DrOp VieW \"foo-dash\"", "drop  view   `foo-dash` /* hello*/",
                "drop    view  if   exists  `foo-dash`"};
        String[] cmdsDash2 = {"drop view `bar-dash`.`foo-dash`",
                "DROP VIEW `bar-dash`.`foo-dash`",
                "DRop VieW \"bar-dash\".\"foo-dash\"",
                "drop   view   `bar-dash`.`foo-dash` /* hello*/",
                "drop view  if  exists `bar-dash`.`foo-dash`"};

        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.VIEW,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.DROP,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
        }

        for (String cmd : cmds2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.VIEW,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.DROP,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar",
                    sqlName.getSchema());
        }
        for (String cmd : cmdsDash1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.VIEW,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.DROP,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo-dash",
                    sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
        }

        for (String cmd : cmdsDash2)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.VIEW,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.DROP,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo-dash",
                    sqlName.getName());
            Assert.assertEquals("Found database: " + cmd, "bar-dash",
                    sqlName.getSchema());
        }

    }

    /**
     * Test suppression of leading comments including comments generated by
     * mysqldump.
     */
    @Test
    public void testCommentHandling() throws Exception
    {
        String[] cmds1 = {"/* comment */ create table foo",
                "/*!50000 CREATE TABLE IF NOT EXISTS foo */",
                " /* another command */ creAtE TEMPORary TabLE \"foo\"",
                "/** a difficult comment */ create   table   `foo` /* hello*/",
                " /* comment*/create table foo",
                "-- this is a comment\n   create table foo",
                "-- this is a comment\n   create -- comment\ntable foo"};
        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.CREATE,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
        }
    }

    /**
     * Test suppression of very large and/or double comments.
     */
    @Test
    public void testCommentHandling2() throws Exception
    {
        String cmd1 = "/* "
                + "comment comment comment comment comment comment comment comment comment comment comment comment"
                + "comment comment comment comment comment comment comment comment comment comment comment comment"
                + "comment comment comment comment comment comment comment comment comment comment comment comment"
                + "comment comment comment comment comment comment comment comment comment comment comment comment"
                + "comment comment comment comment comment comment comment comment comment comment comment comment"
                + "comment comment comment comment comment comment comment comment comment comment comment comment"
                + "comment comment comment comment comment comment comment comment comment comment comment comment"
                + "comment comment comment comment comment comment comment comment comment comment comment comment"
                + "comment comment comment comment comment comment comment comment comment comment comment comment"
                + " */ create table foo";
        String cmd2 = "/* comment */ create table foo /* comment */";
        String[] cmds1 = {cmd1, cmd2};
        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.CREATE,
                    sqlName.getOperation());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
        }
    }

    /**
     * Test begin/start transaction.
     */
    @Test
    public void testBegin() throws Exception
    {
        String[] cmds1 = {" start transaction WITH CONSISTENT SNAPSHOT",
                "StarT TransactioN", "begin", " BEGIN WORK "};
        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd,
                    SqlOperation.TRANSACTION, sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.BEGIN,
                    sqlName.getOperation());
            Assert.assertNull("Found name: " + cmd, sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
        }
    }

    /**
     * Test commit
     */
    @Test
    public void testCommit() throws Exception
    {
        String[] cmds1 = {"COMMIT", "commit work", " cOMmit WorK", "commit"};
        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd,
                    SqlOperation.TRANSACTION, sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.COMMIT,
                    sqlName.getOperation());
            Assert.assertNull("Found name: " + cmd, sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
        }
    }

    /**
     * This is experimental--case copied from MySQLExtractor. Not sure it's even
     * legal in SQL.
     */
    @Test
    public void testBeginEnd() throws Exception
    {
        String[] cmds1 = {"begin select 1; end"};
        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.BLOCK,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd,
                    SqlOperation.BEGIN_END, sqlName.getOperation());
            Assert.assertNull("Found name: " + cmd, sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
        }
    }

    /**
     * Test FLUSH TABLES.
     */
    @Test
    public void testFlushTables() throws Exception
    {
        String[] cmds1 = {"FLUSH TABLES", "flush tables", " flUsh tABLES",
                "flush /* foo */ tables"};
        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.DBMS,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd,
                    SqlOperation.FLUSH_TABLES, sqlName.getOperation());
            Assert.assertNull("Found name: " + cmd, sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
            Assert.assertTrue("Is global: " + cmd, sqlName.isGlobal());
        }
    }

    /**
     * Identify a select. We don't select the db.table as select syntax is quite
     * convoluted.
     */
    @Test
    public void testSelect() throws Exception
    {
        String[] cmds1 = {
                "SELECT t1.*, t2.* FROM t1 INNER JOIN t2",
                "SELECT CONCAT(last_name,', ',first_name) AS full_name FROM mytable ORDER BY full_name"};
        SqlOperationMatcher m = new MySQLOperationMatcher();
        for (String cmd : cmds1)
        {
            SqlOperation sqlName = m.match(cmd);
            Assert.assertNotNull("Matched: " + cmd, sqlName);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found operation: " + cmd, SqlOperation.SELECT,
                    sqlName.getOperation());
            Assert.assertNull("Found name: " + cmd, sqlName.getName());
            Assert.assertNull("Found database: " + cmd, sqlName.getSchema());
        }
    }

    /**
     * Test performance over a large number of inserts.
     */
    @Test
    public void testInsertMany() throws Exception
    {
        String[] cmds1 = {
                "insert into foo values(1)",
                "INSERT INTO foo(id,msg) values(1, 'data')",
                "InSeRt InTo \"foo\" values (1, 'data')",
                "LOAD DATA INFILE '/tmp/SQL_LOAD-1701-10901-48716.data' IGNORE "
                        + "INTO TABLE \"foo\" FIELDS TERMINATED BY ',' "
                        + "OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\\' LINES TERMINATED "
                        + "BY '\n' STARTING BY '' IGNORE 0 LINES (@var0, @var1) SET id = "
                        + "@var0, data_10 = TRIM(TRIM(char(160) FROM TRIM(@var1))), "
                        + "call_now = '', upload_error_flag = 0, duplicate_flag = null"};
        SqlOperationMatcher m = new MySQLOperationMatcher();

        for (int i = 0; i < 1000000; i++)
        {
            String cmd = cmds1[i % cmds1.length];
            SqlOperation sqlName = m.match(cmd);
            Assert.assertEquals("Found object: " + cmd, SqlOperation.TABLE,
                    sqlName.getObjectType());
            Assert.assertEquals("Found name: " + cmd, "foo", sqlName.getName());

            if (i % 100000 == 0)
            {
                logger.info("Statements parsed: " + i);
            }
        }
    }
}