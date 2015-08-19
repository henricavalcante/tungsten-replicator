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

import java.util.HashMap;

/**
 * Handles parsing of SQL statements to derive parsing information. This class
 * encapsulates logic to select a parser based on a particular dialect.
 * <p/>
 * The call to fetch the singleton parser is synchronized to guarantee
 * visibility across all threads. Also, we final variables for all instances to
 * avoid unnecessary object creation, as the parsing function is called
 * potentially many times.
 * 
 * WARNING : this is not thread safe !
 */
public class SqlStatementParser
{
    // Singleton parser.
    private static SqlStatementParser                  parser       = new SqlStatementParser();

    // Map of available operation matchers.
    private final HashMap<String, SqlOperationMatcher> matchers     = new HashMap<String, SqlOperationMatcher>();
    // Singleton matcher for MySQL to eliminate unnecessary object creation.
    private final SqlOperationMatcher                  mysqlMatcher = new MySQLOperationMatcher();

    /** Instantiates a SqlStatementParser and loads the map. */
    private SqlStatementParser()
    {
        matchers.put(Database.MYSQL, mysqlMatcher);

        // This is lame but we only support MySQL statement parsing at this
        // point.
        matchers.put(Database.ORACLE, mysqlMatcher);
        matchers.put(Database.POSTGRESQL, mysqlMatcher);
        matchers.put(Database.UNKNOWN, mysqlMatcher);
    }

    /**
     * Returns a SQL statement parser.
     */
    public static synchronized SqlStatementParser getParser()
    {
        return parser;
    }

    /**
     * Parse a SQL statement.
     * 
     * @param statement A query, presumably written in some form of SQL
     * @param dbmsType The DBMS type, using one of the string names provided by
     *            the Database class.
     * @return A SqlOperation containing parsing metadata
     */
    public SqlOperation parse(String statement, String dbmsType)
    {
        SqlOperationMatcher matcher = matchers.get(dbmsType);
        if (matcher == null)
            matcher = new MySQLOperationMatcher();
        return matcher.match(statement);
    }
}