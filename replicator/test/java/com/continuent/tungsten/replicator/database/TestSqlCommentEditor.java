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
 * Contributor(s): Andreas Wederbrand, Stephane Giron
 */

package com.continuent.tungsten.replicator.database;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests SQL comment editin.
 * <p/>
 * As currently written, this test includes checks on MySQL schema. It needs to
 * be expanded to work with other database types, such as Drizzle.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class TestSqlCommentEditor
{
    SqlOperationMatcher matcher = new MySQLOperationMatcher();
    MySQLCommentEditor  editor  = new MySQLCommentEditor();

    /**
     * Test adding and recognizing comments on basic DDL statements.
     */
    @Test
    public void testBasicDdl() throws Exception
    {
        String[] cmds = {"create database foo",
                "create table if not exists `test`.`foo`",
                "drop procedure test.foo"};
        String comment = "___SERVICE = [mysvc]___";
        String commentRegex = "___SERVICE = \\[([a-zA-Z0-9-_]+)\\]___";
        editor.setCommentRegex(commentRegex);

        for (String cmd : cmds)
        {
            SqlOperation op = matcher.match(cmd);
            String newCmd = editor.addComment(cmd, op, comment);
            String foundComment = editor.fetchComment(newCmd, op);
            Assert.assertTrue("Comment added", newCmd.length() > cmd.length());
            Assert.assertEquals("Found original comment", comment, foundComment);
        }
    }

    /**
     * Confirm no comment is returned if none exists.
     */
    @Test
    public void testNonExistentComment() throws Exception
    {
        String[] cmds = {"create database foo",
                "create table if not exists `test`.`foo`",
                "drop procedure test.foo /* +++SERVICE = [mysvc]+++ */"};
        String commentRegex = "___SERVICE = \\[([a-zA-Z0-9-_]+)\\]___";
        editor.setCommentRegex(commentRegex);

        for (String cmd : cmds)
        {
            SqlOperation op = matcher.match(cmd);
            String foundComment = editor.fetchComment(cmd, op);
            Assert.assertNull("No comment found", foundComment);
        }
    }

    /**
     * Test adding and recognizing comments on stored procedure definitions.
     */
    @Test
    public void testSproc() throws Exception
    {
        String[] cmds = {
                "CREATE PROCEDURE simpleproc2 (OUT param1 INT) \nBEGIN\n"
                        + "SELECT 1 INTO param1;\nEND",
                "CREATE PROCEDURE `evilProc`(begin_comment VARCHAR(255), comment_body VARCHAR (255))\n"
                        + "    COMMENT 'will be removed'\n"
                        + "BEGIN\n"
                        + "INSERT INTO my_comments VALUES(begin_comment, comment_body, now());\n"
                        + "END",
                "CREATE FUNCTION `evilFunc`(begin_comment VARCHAR(255), comment_body VARCHAR (255))\nRETURNS int\n"
                        + "    COMMENT 'will be removed'\n"
                        + "BEGIN\n"
                        + "INSERT INTO my_comments VALUES(begin_comment, comment_body, now());return 1;"
                        + "END",
                "CREATE FUNCTION `evilFunc`(begin_comment VARCHAR(255),\n comment_body VARCHAR (255)) RETURNS int\nDETERMINISTIC\n"
                        + "BEGIN "
                        + "INSERT INTO my_comments VALUES(begin_comment, comment_body, now());return 1;"
                        + "END",
                "CREATE PROCEDURE `evilProc`(begin_comment VARCHAR(255), comment_body VARCHAR (255))\n"
                        + "    COMMENT 'multi lines comment\\nwill be removed'\n"
                        + "BEGIN\n"
                        + "INSERT INTO my_comments VALUES(begin_comment, comment_body, now());\n"
                        + "END",
                "create procedure `test`.`simpleproc2` (OUT param1 INT) \n"
                        + "BEGIN\nSELECT 1 INTO param1;\nEND",
                "CREATE DEFINER=`root`@`localhost` PROCEDURE `simpleproc2`(OUT param1 INT)\n"
                        + "    COMMENT 'this is a comment'\n"
                        + "begin SELECT 1 INTO param1;end",
                "CREATE DEFINER=`root`@`localhost` procedure `simpleproc2`(OUT param1 INT)\n"
                        + "    comment 'this is a comment'\n"
                        + "begin SELECT 1 INTO param1;end",
                "create function f1() returns int\n    deterministic\n return 1;",
                "create function f2() returns int\n    deterministic\n    comment 'this was generated by server 1'\nreturn 1;"};
        String comment = "___SERVICE = [mysvc]___";
        String commentRegex = "___SERVICE = \\[([a-zA-Z0-9-_]+)\\]___";
        editor.setCommentRegex(commentRegex);

        for (String cmd : cmds)
        {
            SqlOperation op = matcher.match(cmd);
            String newCmd = editor.addComment(cmd, op, comment);
            String foundComment = editor.fetchComment(newCmd, op);
            System.out.println("Command\n" + cmd + "\nwas changed into\n"
                    + newCmd + "\n");
            Assert.assertTrue("Comment must be added",
                    newCmd.length() > cmd.length()
                            || newCmd.indexOf("COMMENT") > 0);
            Assert.assertEquals("Found original comment", comment, foundComment);
        }
    }

    /**
     * Test adding and recognizing comments on drop table statements.
     */
    @Test
    public void testDropTable() throws Exception
    {
        String[] cmds = {
                "DROP /*!40005 TEMPORARY */ TABLE IF EXISTS `T`,`pertinent_bugs`",
                "DROP TABLE foo", "DROP TABLE IF EXISTS foo",
                "drop table if exists `bar`.`foo`"};
        String comment = "mysvc";
        String commentRegex = "TUNGSTEN_INFO.`[([a-zA-Z0-9-_]+)]`";
        editor.setCommentRegex(commentRegex);

        for (String cmd : cmds)
        {
            // Ensure basic comments are correct.
            SqlOperation op = matcher.match(cmd);
            String newCmd = editor.addComment(cmd, op, comment);
            String foundComment = editor.fetchComment(newCmd, op);
            System.out.println("Command\n" + cmd + "\nwas changed into\n"
                    + newCmd + "\n");
            Assert.assertTrue(
                    "Comment must be added",
                    newCmd.length() > cmd.length()
                            || newCmd.indexOf("TUNGSTEN_INFO") > 0);
            Assert.assertEquals("Found original comment", comment, foundComment);

            // Ensure we don't add them twice.
            SqlOperation op2 = matcher.match(newCmd);
            String newCmd2 = editor.addComment(newCmd, op2, comment);
            Assert.assertEquals("Checking for double add of comments", newCmd,
                    newCmd2);
        }
    }

    /**
     * Verify that we return appendable comments when safe to do so, otherwise
     * null.
     */
    @Test
    public void testAppendableComment() throws Exception
    {
        String[] cmds1 = {"create database foo",
                "create table if not exists `test`.`foo`",
                "drop procedure test.foo /* +++SERVICE = [mysvc]+++ */"};
        String[] cmds2 = {
                "CREATE PROCEDURE simpleproc2 (OUT param1 INT) \nBEGIN\n"
                        + "SELECT 1 INTO param1;\nEND",
                "create procedure `test`.`simpleproc2` (OUT param1 INT) \n"
                        + "BEGIN\nSELECT 1 INTO param1;\nEND",
                "CREATE DEFINER=`root`@`localhost` PROCEDURE `simpleproc2`(OUT param1 INT)\n"
                        + "    COMMENT 'this is a comment'\n"
                        + "begin SELECT 1 INTO param1;end",
                "CREATE DEFINER=`root`@`localhost` procedure `simpleproc2`(OUT param1 INT)\n"
                        + "    comment 'this is a comment'\n"
                        + "begin SELECT 1 INTO param1;end"};

        String comment = "___SERVICE = [mysvc]___";
        String commentRegex = "___SERVICE = \\[([a-zA-Z0-9-_]+)\\]___";
        editor.setCommentRegex(commentRegex);

        // Test cases that generate comments.
        for (String cmd : cmds1)
        {
            SqlOperation op = matcher.match(cmd);
            String formattedComment = editor.formatAppendableComment(op,
                    comment);
            Assert.assertNotNull("Must have comment", formattedComment);
        }

        // Test cases that do not.
        for (String cmd : cmds2)
        {
            SqlOperation op = matcher.match(cmd);
            String formattedComment = editor.formatAppendableComment(op,
                    comment);
            Assert.assertNull("Must not have comment", formattedComment);
        }
    }

    /**
     * Confirm that comment editing is turned off if we disable comment editing.
     */
    @Test
    public void testEditingDisabled() throws Exception
    {
        String[] cmds = {
                "create database foo",
                "create table if not exists `test`.`foo`",
                "drop table test.foo",
                "CREATE DEFINER=`root`@`localhost` procedure `simpleproc2`(OUT param1 INT)\n"
                        + "    comment 'this is a comment'\n"
                        + "begin SELECT 1 INTO param1;end"};
        String comment = "___SERVICE = [mysvc]___";
        String commentRegex = "___SERVICE = \\[([a-zA-Z0-9-_]+)\\]___";
        editor.setCommentRegex(commentRegex);
        editor.setCommentEditingEnabled(false);

        for (String cmd : cmds)
        {
            SqlOperation op = matcher.match(cmd);
            String formattedComment = editor.formatAppendableComment(op,
                    comment);
            Assert.assertNull("Must not have comment", formattedComment);
            String newCmd = editor.addComment(cmd, op, comment);
            Assert.assertEquals("No comment added", cmd, newCmd);
        }
    }

}