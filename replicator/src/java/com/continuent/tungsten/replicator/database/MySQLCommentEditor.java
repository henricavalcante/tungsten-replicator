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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses SQL statements to extract the SQL operation and the object, identified
 * by type, name and schema, to which it pertains.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class MySQLCommentEditor implements SqlCommentEditor
{
    private enum CreateProcedureStage
    {
        CREATE, PARAMETERS, CHARACTERISTICS, BODY;

        public CreateProcedureStage next()
        {
            if (this.ordinal() < CreateProcedureStage.values().length - 1)
            {
                return CreateProcedureStage.values()[this.ordinal() + 1];
            }
            else
            {
                return null;
            }
        }
    }

    // Flag to enable comment editing.
    protected boolean commentEditingEnabled = true;

    // Patterns to pull out metadata.
    protected Pattern standardPattern;
    protected Pattern sprocPattern;
    protected Pattern dropTablePattern;

    // Patterns used to add comments to DROP TABLE.
    protected Pattern ifExistsPattern       = Pattern
                                                    .compile(
                                                            "^(.*if\\s+exists\\s+)(.*)",
                                                            Pattern.CASE_INSENSITIVE);
    protected Pattern dropTable             = Pattern
                                                    .compile(
                                                            "^(\\s*drop.*\\s+table\\s+)(.*)",
                                                            Pattern.CASE_INSENSITIVE);

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.SqlCommentEditor#setCommentEditingEnabled(boolean)
     */
    public void setCommentEditingEnabled(boolean enabled)
    {
        this.commentEditingEnabled = enabled;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.SqlCommentEditor#addComment(java.lang.String,
     *      com.continuent.tungsten.replicator.database.SqlOperation,
     *      java.lang.String)
     */
    public String addComment(String statement, SqlOperation sqlOp,
            String comment)
    {
        // If editing is enabled, return now.
        if (!this.commentEditingEnabled)
            return statement;

        // Look for a stored procedure or function creation.
        if (sqlOp.getOperation() == SqlOperation.CREATE)
        {
            int objectType = sqlOp.getObjectType();
            if (objectType == SqlOperation.PROCEDURE
                    || objectType == SqlOperation.FUNCTION)
            {
                return processCreateProcedure(statement, comment);
            }
        }
        else if (sqlOp.dropTable())
        {
            return processDropTable(statement, comment);
        }

        // For any others just append the comment.
        return statement + " /* " + comment + " */";
    }

    /**
     * Handles CREATE (PROCEDURE|FUNCTION) statements.<BR>
     * The format of a CREATE PROCEDURE seems pretty well formed once in the
     * binlog. No matter where you put line breaks on the master this is the
     * format in the binlog.<BR>
     * On the first line comes :
     * "CREATE DEFINER=`user`@`host` PROCEDURE `procedure_name`("<BR>
     * Then comes the parameters list. Line breaks are valid inside the
     * parameter list so we can't know for sure that the line ends with a ")"
     * There is a unknown number of lines. The last one of those ends with a ")"
     * that matches the "(" on on the first line. Make sure to count the "(" and
     * ")" so the last one is really the last one. <BR>
     * Following that block there will be 0 or more rows containing
     * characteristics. They are all indented with 4 spaces. We're only
     * interested in COMMENT (and COMMENT always seems to be the last one).
     * Following that comes the routine body that may or may not be surrounded
     * with "BEGIN ... END"<BR>
     * CREATE FUNCTION is exactly the same except that after the closing ")" it
     * always says RETURNS. As it turns out, the same code will work for both
     * cases.
     * 
     * @param statement Create Procedure statement that requires a comment to be
     *            inserted.
     * @param comment Comment string to be added.
     */
    @SuppressWarnings("fallthrough")
    private String processCreateProcedure(String statement, String comment)
    {
        // Processing for CREATE PROCEDURE/FUNCTION -- add a COMMENT.
        // Following regex splits on line boundaries.
        String[] lines = statement.split("(?m)$");
        StringBuffer sb = new StringBuffer();

        CreateProcedureStage stage = CreateProcedureStage.CREATE;
        int parentheses = 0;
        boolean hasComment = false;

        for (String line : lines)
        {
            switch (stage)
            {
                case CREATE :
                    // Do nothing but advance to the next stage and start
                    // processing parameters
                    stage = stage.next();
                    // note, no break
                case PARAMETERS :
                    // Count number of parentheses and advance to
                    // Stage.CHARACTERISTICS if we're down to 0.
                    parentheses += countParentheses(line);
                    if (parentheses == 0)
                    {
                        stage = stage.next();
                    }
                    sb.append(line);
                    break;
                case CHARACTERISTICS :
                    if (line.matches("(\\n)?\\s{4}.*"))
                    {
                        // found characteristics
                        if (line.matches("(\\n)?\\s{4}(COMMENT|comment).*'"))
                        {
                            // Replace COMMENT with our own COMMENT if found
                            // NOTE: the upper replace doesn't replace the
                            // original comment, it just adds to it.
                            // line = line.replaceAll("COMMENT\\s*'(.*)'",
                            // "COMMENT '$1 " + comment + "'");

                            line = line.replaceAll(
                                    "(COMMENT|comment)\\s*'(.*)'", "COMMENT '"
                                            + comment + "'");
                            hasComment = true;
                        }
                        sb.append(line);
                        break;
                    }
                    else
                    {
                        // actually first line of Stage.BODY
                        // Advance to Stage.BODY (and redo this line)
                        // note, no break
                        stage = stage.next();
                    }
                case BODY :
                    if (!hasComment)
                    {
                        // no comment this far, add it
                        sb.append("\n    COMMENT '").append(comment)
                                .append("'");
                        hasComment = true;
                    }
                    sb.append(line);
                    break;
            }
        }

        return sb.toString();
    }

    private int countParentheses(String line)
    {
        int left = line.length() - line.replace("(", "").length();
        int right = line.length() - line.replace(")", "").length();
        return left - right;
    }

    /**
     * Handles DROP TABLE. Drop table suppresses comments so we instead add a
     * fake table with the comment in the following form:
     * 
     * <pre><code>
     *    DROP TABLE, TUNGSTEN_INFO.`comment text`, othertable1, othertable2,...
     * </code></pre>
     * 
     * This requires special handling to ensure we deal with multiple tables,
     * comments in the DROP TABLE command, and extra syntax like IF EXISTS.
     * 
     * @param statement DROP TABLE statement that requires a comment to be
     *            inserted.
     * @param comment Comment string to be added.
     */
    private String processDropTable(String statement, String comment)
    {
        // Ensure we don't edit in the values twice. Look for the edit
        // values and skip out if they are already there.
        if (statement.contains(" TUNGSTEN_INFO.`"))
            return statement;

        // Now process the edit, since it has not been done before.
        Matcher m = ifExistsPattern.matcher(statement);
        boolean foundIfExists = m.find();
        if (foundIfExists)
        {
            // This is the easy case. Split the DROP table and put our
            // comment in between.
            String before = m.group(1);
            String after = m.group(2);
            String newStatement = before + " TUNGSTEN_INFO.`" + comment + "`, "
                    + after;
            return newStatement;
        }
        else
        {
            // This is trickier. We need to split the statement before the
            // first table name and add our comment preceded by IF EXISTS.
            // Another regex to the rescue. This regex much split properly even
            // there are comments embedded in the DROP TABLE or multiple
            // tables.
            Matcher m2 = dropTable.matcher(statement);
            if (m2.find())
            {
                String before = m2.group(1);
                String after = m2.group(2);
                String newStatement = before + " IF EXISTS TUNGSTEN_INFO.`"
                        + comment + "`, " + after;
                return newStatement;
            }
        }

        // We didn't succeed in matching. We'll hand back the original statement
        // and hope for the best.
        return statement;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.SqlCommentEditor#formatAppendableComment(SqlOperation,
     *      String)
     */
    public String formatAppendableComment(SqlOperation sqlOp, String comment)
    {
        // If editing is enabled, return now.
        if (!this.commentEditingEnabled)
            return null;

        // Look for a stored procedure or function and return null. They are not
        // safe for appending.
        if (sqlOp.getOperation() == SqlOperation.CREATE)
        {
            int objectType = sqlOp.getObjectType();
            if (objectType == SqlOperation.PROCEDURE
                    || objectType == SqlOperation.FUNCTION)
            {
                return null;
            }
        }
        else if (sqlOp.dropTable())
        {
            // Drop table requires special handling as MySQL 5.5+ drops
            // comments.
            return null;
        }

        // For any others return a properly formatted comment.
        return " /* " + comment + " */";
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.SqlCommentEditor#fetchComment(String,
     *      SqlOperation)
     */
    public String fetchComment(String statement, SqlOperation sqlOp)
    {
        // Select correct comment pattern.
        Pattern commentPattern = standardPattern;
        if (sqlOp.getOperation() == SqlOperation.CREATE)
        {
            int objectType = sqlOp.getObjectType();
            if (objectType == SqlOperation.PROCEDURE)
                commentPattern = sprocPattern;
            else if (objectType == SqlOperation.FUNCTION)
                commentPattern = sprocPattern;
        }
        else if (sqlOp.dropTable())
        {
            commentPattern = dropTablePattern;
        }

        // Look for pattern match and return value if found.
        Matcher m = commentPattern.matcher(statement);
        if (m.find())
            return m.group(1);
        else
            return null;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.SqlCommentEditor#setCommentRegex(java.lang.String)
     */
    public void setCommentRegex(String regex)
    {
        String standardRegex = "\\/\\* (" + regex + ") \\*\\/";
        String sprocRegex = "COMMENT\\s*'(" + regex + ").*'";
        // This is a hack to handle DROP TABLE.
        String dropTableRegex = "TUNGSTEN_INFO.`([a-zA-Z0-9-_]+)`";
        standardPattern = Pattern.compile(standardRegex);
        sprocPattern = Pattern.compile(sprocRegex);
        dropTablePattern = Pattern.compile(dropTableRegex);
    }
}