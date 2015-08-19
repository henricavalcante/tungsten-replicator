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
 * Initial developer(s): Stephane Giron
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.replicator.database;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

/**
 * Matches schema and table names. Patterns used in matching are comma separated
 * lists, where each entry may have the following form:
 * <ul>
 * <li>A schema name, for example "test"</li>
 * <li>A fully qualified table name, for example "test.foo"</li>
 * </ul>
 * Schema and table names may contain * and ? characters, which substitute for a
 * series of characters or a single character, respectively. For example,
 * "test.*" matches all tables in database test, and "test?.foo" matches tables
 * "test1.foo" and "test2.foo" but not "test.foo".
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class TableMatcher
{
    private static Logger logger = Logger.getLogger(TableMatcher.class);

    /** Comma separated list of schema/table patterns. */
    private String        patternString;

    private Pattern       dbPattern;
    private Matcher       dbMatcher;

    private Pattern       tablePattern;
    private Matcher       tableMatcher;

    /**
     * Prepares matcher for use.
     * 
     * @param patternString Schema/table pattern list.
     */
    // Prepares extract filters.
    public void prepare(String patternString)
    {
        this.patternString = patternString;

        // Clear patterns.
        dbPattern = null;
        dbMatcher = null;
        tablePattern = null;
        tableMatcher = null;

        // If empty, we do nothing.
        if (patternString == null || patternString.length() == 0)
            return;

        // Prepare to look for schema and table matches.
        boolean haveSchemaPattern = false;
        StringBuffer db = new StringBuffer("^(");

        boolean haveTablePattern = false;
        StringBuffer table = new StringBuffer("^(");

        String[] filterArr = patternString.split(",");

        for (int i = 0; i < filterArr.length; i++)
        {
            // Get the next filter specification.
            String filter = filterArr[i].trim();
            if (filter.length() == 0)
                continue;

            // Decide whether this is a table or database.
            boolean useSchemaPattern = false;
            if (filter.contains("."))
            {
                // This is a table.
                filter = filter.replace(".", "\\.");
                if (haveTablePattern)
                    table.append("|");
                else
                    haveTablePattern = true;
            }
            else
            {
                // This is a schema
                if (haveSchemaPattern)
                    db.append("|");
                else
                    haveSchemaPattern = true;
                useSchemaPattern = true;
            }

            // Substitute for * and ? wildcards.
            filter = filter.replace("*", ".*").replace("?", ".");

            if (useSchemaPattern)
            {
                db.append(filter);
            }
            else
                table.append(filter);
        }

        // Create patterns if we got more than ^()$ (empty string).
        String tableRegex = table.append(")$").toString();
        String dbRegex = db.append(")$").toString();

        if (haveSchemaPattern)
        {
            dbPattern = Pattern.compile(dbRegex);
            if (logger.isDebugEnabled())
                logger.debug("Matching schemas using " + dbRegex);
        }
        if (haveTablePattern)
        {
            tablePattern = Pattern.compile(tableRegex);
            if (logger.isDebugEnabled())
                logger.debug("Matching tables using " + tableRegex);
        }
    }

    /**
     * Performs a scan of all rules to see if we have a match.
     * 
     * @param schema Schema name
     * @param table Table name or null to match on schema only
     * @return True if there is a match
     */
    public boolean match(String schema, String table)
    {
        // Check for an explicitly replicated schema.
        if (dbPattern != null)
        {
            if (logger.isDebugEnabled())
                logger.debug("Checking if database matches: " + schema);
            if (dbMatcher == null)
                dbMatcher = dbPattern.matcher(schema);
            else
                dbMatcher.reset(schema);

            if (dbMatcher.matches())
            {
                if (logger.isDebugEnabled())
                    logger.debug("Match db filter");
                return true;
            }
        }

        // Perform additional filtering if we have a table.
        if (table != null && table.length() > 0)
        {
            String searchedTable = fullyQualifiedName(schema, table);

            if (tablePattern != null)
            {
                if (tableMatcher == null)
                    tableMatcher = tablePattern.matcher(searchedTable);
                else
                    tableMatcher.reset(searchedTable);

                if (tableMatcher.matches())
                    return true;
            }
        }

        // We did not get a match.
        return false;
    }

    // Returns the fully qualified schema and/or table name, which can be used
    // as a key.
    private String fullyQualifiedName(String schema, String table)
    {
        StringBuffer fqn = new StringBuffer();
        fqn.append(schema);
        if (table != null)
            fqn.append(".").append(table);
        return fqn.toString();
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        return this.getClass().getSimpleName() + ": " + patternString;
    }
}