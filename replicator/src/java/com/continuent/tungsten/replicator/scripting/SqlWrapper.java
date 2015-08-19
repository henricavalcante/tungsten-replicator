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
 * Contributor(s): Linas Virbalas, Stephane Giron
 */

package com.continuent.tungsten.replicator.scripting;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.database.Database;

/**
 * Provides a simple wrapper for JDBC connections that is suitable for exposure
 * in scripted environments. This class may be extended to allow additional
 * methods for specific DBMS types.
 */
public class SqlWrapper
{
    private static Logger    logger = Logger.getLogger(SqlWrapper.class);

    // DBMS connection and statement.
    protected final Database connection;
    protected Statement      statement;

    /** Creates a new instance. */
    public SqlWrapper(Database connection) throws SQLException
    {
        this.connection = connection;
        statement = connection.createStatement();
    }

    /**
     * Executes a SQL statement.
     */
    public int execute(String sql) throws SQLException
    {
        if (logger.isDebugEnabled())
            logger.debug("Executing SQL: " + sql);
        return statement.executeUpdate(sql);
    }

    /**
     * Does a COUNT on a given table.
     * 
     * @param tableName Fully qualified table name (with schema name).
     * @return Row count in the table.
     */
    public int retrieveRowCount(String tableName) throws SQLException
    {
        ResultSet rs = null;
        int rowCount = -1;
        try
        {
            rs = statement.executeQuery("SELECT COUNT(*) FROM " + tableName);
            rs.next();
            rowCount = rs.getInt(1);
        }
        finally
        {
            if (rs != null)
            {
                rs.close();
            }
        }
        return rowCount;
    }

    /**
     * Begins a DBMS transaction.
     */
    public void begin() throws SQLException
    {
        if (logger.isDebugEnabled())
            logger.debug("Beginning transaction");
        connection.setAutoCommit(false);
    }

    /**
     * Commits a DBMS transaction.
     */
    public void commit() throws SQLException
    {
        if (logger.isDebugEnabled())
            logger.debug("Committing transaction");
        connection.commit();
    }

    /**
     * Rolls back a DBMS transaction.
     */
    public void rollback() throws SQLException
    {
        if (logger.isDebugEnabled())
            logger.debug("Rolling back transaction");
        connection.rollback();
    }

    /**
     * Releases the statement.
     */
    public void close()
    {
        // Release statement.
        if (statement != null)
        {
            try
            {
                statement.close();
            }
            catch (SQLException e)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Unable to close statement", e);
            }
        }
    }

    public String getDatabaseObjectName(String obj)
    {
        return connection.getDatabaseObjectName(obj);
    }
}