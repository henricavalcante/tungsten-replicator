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
 * Initial developer(s): Marcus Eriksson
 * INITIAL CODE DONATED UNDER TUNGSTEN CODE CONTRIBUTION AGREEMENT
 */

package com.continuent.tungsten.replicator.database;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * Implements DBMS-specific operations for MySQL.
 * 
 * @author <a href="mailto:scott.martin@continuent.com">Scott Martin</a>
 * @author Marcus Eriksson
 */
public class DrizzleDatabase extends AbstractDatabase
{
    private static Logger logger                        = Logger.getLogger(DrizzleDatabase.class);

    private boolean       sessionLevelLoggingSuppressed = false;

    public DrizzleDatabase() throws SQLException
    {
        dbms = DBMS.DRIZZLE;
        // Hard code the driver so it gets loaded correctly.
        dbDriver = "org.drizzle.jdbc.Driver";
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#getSqlNameMatcher()
     */
    @Override
    public SqlOperationMatcher getSqlNameMatcher() throws ReplicatorException
    {
        // Return MySQL matcher for now. 
        return new MySQLOperationMatcher();
    }

    protected String columnToTypeString(Column c, String tableType)
    {
        switch (c.getType())
        {
            case Types.TINYINT :
                return "INT";

            case Types.SMALLINT :
                return "INT";

            case Types.INTEGER :
                return "INT";

            case Types.BIGINT :
                return "BIGINT";

            case Types.CHAR :
                return "CHAR(" + c.getLength() + ")";

            case Types.VARCHAR :
                return "VARCHAR(" + c.getLength() + ")";

            case Types.DATE :
                return "DATETIME";

            case Types.TIMESTAMP :
                return "TIMESTAMP";

            case Types.CLOB :
                return "LONGTEXT";

            case Types.BLOB :
                return "LONGBLOB";

            default :
                return "UNKNOWN";
        }
    }

    /**
     * Connect to a MySQL database, which includes setting the wait_timeout to a
     * very high value so we don't lose our connection. {@inheritDoc}
     * 
     * @see AbstractDatabase#connect()
     */
    public void connect() throws SQLException
    {
        // Use superclass method to avoid missing things like loading the
        // driver class.
        super.connect();

        // set connection timeout to maximum to prevent timeout on the
        // server side
        // TREP-285 - Need to trap SQL error as some MySQL versions don't accept
        // an out of bounds number.
        try
        {
            executeUpdate("SET oldlibdrizzle_read_timeout = 99999999");
        }
        catch (SQLException e)
        {
            logger.debug("Unable to set read_timeout to maximum value of 99999999");
            logger.debug("Please consider using an explicit JDBC URL setting to avoid connection timeouts");
        }
    }

    public void createTable(Table t, boolean replace) throws SQLException
    {
        boolean comma = false;
        String SQL;

        if (replace)
        {
            this.dropTable(t);
        }

        SQL = "CREATE TABLE ";
        SQL += (replace ? "" : "IF NOT EXISTS ");
        SQL += t.getSchema() + "." + t.getName();
        SQL += " (";

        Iterator<Column> i = t.getAllColumns().iterator();
        while (i.hasNext())
        {
            Column c = i.next();
            SQL += (comma ? ", " : "") + c.getName() + " "
                    + columnToTypeString(c, null)
                    + (c.isNotNull() ? " NOT NULL" : " NULL");

            comma = true;
        }
        Iterator<Key> j = t.getKeys().iterator();

        while (j.hasNext())
        {
            Key key = j.next();
            SQL += ", ";
            switch (key.getType())
            {
                case Key.Primary :
                    SQL += "PRIMARY KEY (";
                    break;
                case Key.Unique :
                    SQL += "UNIQUE KEY (";
                    break;
                case Key.NonUnique :
                    SQL += "KEY (";
                    break;
            }
            i = key.getColumns().iterator();
            comma = false;
            while (i.hasNext())
            {
                Column c = i.next();
                SQL += (comma ? ", " : "") + c.getName();
                comma = true;
            }
            SQL += ")";
        }
        SQL += ") ENGINE=InnoDB";
        execute(SQL);
    }

    public boolean supportsReplace()
    {
        return true;
    }

    public boolean supportsUseDefaultSchema()
    {
        return true;
    }

    public void useDefaultSchema(String schema) throws SQLException
    {
        execute(getUseSchemaQuery(schema));
        this.defaultSchema = schema;
    }

    public String getUseSchemaQuery(String schema)
    {
        return "USE " + schema;
    }

    public boolean supportsCreateDropSchema()
    {
        return true;
    }

    public void createSchema(String schema) throws SQLException
    {
        String SQL = "CREATE DATABASE IF NOT EXISTS " + schema;
        execute(SQL);
    }

    public void dropSchema(String schema) throws SQLException
    {
        String SQL = "DROP DATABASE IF EXISTS " + schema;
        execute(SQL);
    }

    /**
     * {@inheritDoc}
     * 
     * @see Database#supportsControlSessionLevelLogging()
     */
    public boolean supportsControlSessionLevelLogging()
    {
        return true;
    }

    /**
     * {@inheritDoc}
     * 
     * @see Database#controlSessionLevelLogging(boolean)
     */
    public void controlSessionLevelLogging(boolean suppressed)
            throws SQLException
    {
        if (suppressed != this.sessionLevelLoggingSuppressed)
        {
            /*
             * if (suppressed) executeUpdate("SET SQL_LOG_BIN=0"); else
             * executeUpdate("SET SQL_LOG_BIN=1");
             */
            this.sessionLevelLoggingSuppressed = suppressed;
        }
    }

    public boolean supportsControlTimestamp()
    {
        return true;
    }

    /**
     * MySQL supports the 'set timestamp' command, which is what we return.
     */
    public String getControlTimestampQuery(Long timestamp)
    {
        return "SET TIMESTAMP=" + (timestamp / 1000);
    }

    /**
     * MySQL supports session variables.
     * 
     * @see Database#supportsSessionVariables()
     */
    public boolean supportsSessionVariables()
    {
        return true;
    }

    /**
     * Sets a variable on the current session using MySQL SET command.
     * {@inheritDoc}
     * 
     * @see Database#setSessionVariable(String, String)
     */
    public void setSessionVariable(String name, String value)
            throws SQLException
    {
        String escapedValue = value.replaceAll("'", "\'");
        execute("SET @" + name + "='" + escapedValue + "'");
    }

    /**
     * Gets a variable on the current session.
     * 
     * @see Database#getSessionVariable(String)
     */
    public String getSessionVariable(String name) throws SQLException
    {
        Statement s = null;
        ResultSet rs = null;
        String value = null;
        try
        {
            s = dbConn.createStatement();
            rs = s.executeQuery("SELECT @" + name);
            while (rs.next())
            {
                value = rs.getString(1);
            }
            rs.close();
        }
        finally
        {
            if (rs != null)
            {
                try
                {
                    rs.close();
                }
                catch (SQLException e)
                {
                }
            }
            if (s != null)
            {
                try
                {
                    s.close();
                }
                catch (SQLException e)
                {
                }
            }
        }
        return value;
    }

    public ArrayList<String> getSchemas() throws SQLException
    {
        ArrayList<String> schemas = new ArrayList<String>();

        try
        {
            DatabaseMetaData md = this.getDatabaseMetaData();
            ResultSet rs = md.getCatalogs();
            while (rs.next())
            {
                schemas.add(rs.getString("TABLE_CAT"));
            }
            rs.close();
        }
        finally
        {
        }

        return schemas;
    }

    public ResultSet getColumnsResultSet(DatabaseMetaData md,
            String schemaName, String tableName) throws SQLException
    {
        return md.getColumns(schemaName, null, tableName, null);
    }

    protected ResultSet getPrimaryKeyResultSet(DatabaseMetaData md,
            String schemaName, String tableName) throws SQLException
    {
        return md.getPrimaryKeys(schemaName, null, tableName);
    }

    protected ResultSet getIndexResultSet(DatabaseMetaData md,
            String schemaName, String tableName, boolean unique)
            throws SQLException
    {
        return md.getIndexInfo(schemaName, null, tableName, unique, true);
    }

    protected ResultSet getTablesResultSet(DatabaseMetaData md,
            String schemaName, boolean baseTablesOnly) throws SQLException
    {
        // Returns tables and views but drizzle does not have views anyway. 
        return md.getTables(schemaName, null, null, null);
    }

    public String getNowFunction()
    {
        return "now()";
    }

    /**
     * getTimeDiff returns the database-specific way of subtracting two "dates"
     * and return the result in seconds complete with space for the two bind
     * variables. E.g. in MySQL it might be "time_to_sec(timediff(?, ?))". If
     * either of the string variables are null, replace with the bind character
     * (e.g. "?") else use the string given. For example getTimeDiff(null,
     * "myTimeCol") -> time_to_sec(timediff(?, myTimeCol))
     */
    public String getTimeDiff(String string1, String string2)
    {
        String retval = "time_to_sec(timediff(";
        if (string1 == null)
            retval += "?";
        else
            retval += string1;
        retval += ",";
        if (string2 == null)
            retval += "?";
        else
            retval += string2;
        retval += "))";

        return retval;
    }
}