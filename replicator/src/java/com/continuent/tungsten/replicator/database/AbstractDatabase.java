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
 * Initial developer(s): Scott Martin
 * Contributor(s): Robert Hodges, Stephane Giron
 */

package com.continuent.tungsten.replicator.database;

import java.io.BufferedWriter;
import java.io.Serializable;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.csv.CsvSpecification;
import com.continuent.tungsten.common.csv.CsvWriter;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.channel.ShardChannelTable;
import com.continuent.tungsten.replicator.consistency.ConsistencyCheck;
import com.continuent.tungsten.replicator.consistency.ConsistencyException;
import com.continuent.tungsten.replicator.consistency.ConsistencyTable;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.extractor.ExtractorException;
import com.continuent.tungsten.replicator.heartbeat.HeartbeatTable;
import com.continuent.tungsten.replicator.shard.ShardTable;

/**
 * Provides a generic implementation for Database interface. Subclasses must
 * supply at least the implementation for method columnToTypeString(), which
 * converts values from java.sql.Types to DBMS-specific column specifications.
 * 
 * @author <a href="mailto:scott.martin@continuent.com">Scott Martin</a>
 * @version 1.0
 */
/*
 * public abstract class Database implements Runnable
 */
public abstract class AbstractDatabase implements Database
{
    private static Logger                  logger        = Logger.getLogger(AbstractDatabase.class);

    protected DBMS                         dbms;
    protected String                       dbDriver      = null;
    protected String                       dbUri         = null;
    protected String                       dbUser        = null;
    protected String                       dbPassword    = null;
    protected boolean                      privileged    = false;
    protected Connection                   dbConn        = null;
    protected boolean                      autoCommit    = false;
    protected String                       defaultSchema = null;

    protected static Map<String, Class<?>> drivers       = new HashMap<String, Class<?>>();
    protected boolean                      connected     = false;

    protected String                       initScript    = null;

    protected CsvSpecification             csvSpec       = null;

    /**
     * Create a new database instance. To use the database instance you must at
     * minimum set the url, host, and password properties.
     */
    public AbstractDatabase()
    {
    }

    public Connection getConnection()
    {
        return dbConn;
    }

    public DBMS getType()
    {
        return dbms;
    }

    public abstract SqlOperationMatcher getSqlNameMatcher()
            throws ReplicatorException;

    public void setUrl(String dbUri)
    {
        this.dbUri = dbUri;
    }

    public void setUser(String dbUser)
    {
        this.dbUser = dbUser;
    }

    public void setPassword(String dbPassword)
    {
        this.dbPassword = dbPassword;
    }

    public boolean isPrivileged()
    {
        return privileged;
    }

    public void setPrivileged(boolean privileged)
    {
        this.privileged = privileged;
    }

    public CsvSpecification getCsvSpecification()
    {
        return csvSpec;
    }

    public void setCsvSpecification(CsvSpecification csvSpec)
    {
        this.csvSpec = csvSpec;
    }

    public String getPlaceHolder(OneRowChange.ColumnSpec col, Object colValue,
            String typeDesc)
    {
        return " ? ";
    }

    public boolean nullsBoundDifferently(OneRowChange.ColumnSpec col)
    {
        return false;
    }

    public boolean nullsEverBoundDifferently()
    {
        return false;
    }

    /**
     * Return a properly constructed type specification for the column. Concrete
     * Database subclasses must implement at least this method if no others.
     * 
     * @param c Column for which specification is required
     * @return String containing specification
     */
    abstract protected String columnToTypeString(Column c, String tableType);

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#connect()
     */
    public synchronized void connect() throws SQLException
    {
        if (dbConn == null)
        {
            if (dbDriver != null && drivers.get(dbDriver) == null)
            {
                try
                {
                    logger.info("Loading database driver: " + dbDriver);
                    Class<?> driverClass = Class.forName(dbDriver);
                    drivers.put(dbDriver, driverClass);
                }
                catch (Exception e)
                {
                    throw new RuntimeException("Unable to load driver: "
                            + dbDriver, e);
                }
            }

            dbConn = DriverManager.getConnection(dbUri, dbUser, dbPassword);
            connected = (dbConn != null);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#disconnect()
     */
    public synchronized void disconnect()
    {
        if (dbConn != null)
        {
            try
            {
                dbConn.close();
            }
            catch (SQLException e)
            {
                logger.warn("Unable to close connection", e);
            }
            dbConn = null;
            connected = false;
        }
    }

    public DatabaseMetaData getDatabaseMetaData() throws SQLException
    {
        return dbConn.getMetaData();
    }

    /**
     * Returns false by default as only some database types allow schema to be
     * created dynamically.
     * 
     * @see com.continuent.tungsten.replicator.database.Database#supportsCreateDropSchema()
     */
    public boolean supportsCreateDropSchema()
    {
        return false;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#createSchema(java.lang.String)
     */
    public void createSchema(String schema) throws SQLException
    {
        throw new UnsupportedOperationException(
                "Creating schema is not supported");
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#dropSchema(java.lang.String)
     */
    public void dropSchema(String schema) throws SQLException
    {
        throw new UnsupportedOperationException(
                "Dropping schema is not supported");
    }

    /**
     * Returns false by default as only some database types allow schema to
     * change.
     * 
     * @see com.continuent.tungsten.replicator.database.Database#supportsUseDefaultSchema()
     */
    public boolean supportsUseDefaultSchema()
    {
        return false;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#supportsBLOB()
     */
    public boolean supportsBLOB()
    {
        return true;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#useDefaultSchema(java.lang.String)
     */
    public void useDefaultSchema(String schema) throws SQLException
    {
        throw new UnsupportedOperationException(
                "Setting the default schema is not supported");
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#getUseSchemaQuery(java.lang.String)
     */
    public String getUseSchemaQuery(String schema)
    {
        throw new UnsupportedOperationException(
                "Getting the default schema is not supported");
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#supportsControlSessionLevelLogging()
     */
    public boolean supportsControlSessionLevelLogging()
    {
        return false;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#controlSessionLevelLogging(boolean)
     */
    public void controlSessionLevelLogging(boolean suppressed)
            throws SQLException
    {
        throw new UnsupportedOperationException(
                "Controlling session level logging is not supported");
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.UniversalConnection#setLogged(boolean)
     */
    public void setLogged(boolean logged) throws ReplicatorException
    {
        // This is harmless if session logging is not supported.
        if (supportsControlSessionLevelLogging())
        {
            try
            {
                controlSessionLevelLogging(!logged);
            }
            catch (SQLException e)
            {
                throw new ReplicatorException("Unable to set logging: "
                        + e.getMessage(), e);
            }
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#supportsNativeSlaveSync()
     */
    public boolean supportsNativeSlaveSync()
    {
        return false;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#syncNativeSlave(java.lang.String)
     */
    public void syncNativeSlave(String eventId) throws SQLException
    {
        throw new UnsupportedOperationException(
                "Native slave synchronization is not supported");
    }

    /**
     * By default we do not support controlling the timestamp. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#supportsControlTimestamp()
     */
    public boolean supportsControlTimestamp()
    {
        return false;
    }

    /**
     * Returns a query that can be used to set the timestamp.
     * 
     * @param timestamp Time in milliseconds according to Java standard
     * @see #supportsControlTimestamp()
     */
    public String getControlTimestampQuery(Long timestamp)
    {
        throw new UnsupportedOperationException(
                "Controlling session level logging is not supported");
    }

    /**
     * By default we do not support setting session variables. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#supportsSessionVariables()
     */
    public boolean supportsSessionVariables()
    {
        return false;
    }

    /**
     * Sets a variable on the current session. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#setSessionVariable(java.lang.String,
     *      java.lang.String)
     */
    public void setSessionVariable(String name, String value)
            throws SQLException
    {
        throw new UnsupportedOperationException(
                "Session variables are not supported");
    }

    /**
     * Gets a variable on the current session. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#getSessionVariable(java.lang.String)
     */
    public String getSessionVariable(String name) throws SQLException
    {
        throw new UnsupportedOperationException(
                "Session variables are not supported");
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#supportsUserManagement()
     */
    public boolean supportsUserManagement()
    {
        return false;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#createUser(com.continuent.tungsten.replicator.database.User)
     */
    public void createUser(User user) throws SQLException
    {
        throw new UnsupportedOperationException(
                "User management is not supported");
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#dropUser(com.continuent.tungsten.replicator.database.User,
     *      boolean)
     */
    public void dropUser(User user, boolean ignore) throws SQLException
    {
        throw new UnsupportedOperationException(
                "User management is not supported");
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#listSessions()
     */
    public List<Session> listSessions() throws SQLException
    {
        throw new UnsupportedOperationException(
                "User management is not supported");
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#kill(com.continuent.tungsten.replicator.database.Session)
     */
    public void kill(Session session) throws SQLException, ReplicatorException
    {
        throw new UnsupportedOperationException(
                "User management is not supported");
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#execute(java.lang.String)
     */
    public void execute(String SQL) throws SQLException
    {
        Statement sqlStatement = null;
        try
        {
            sqlStatement = dbConn.createStatement();
            if (logger.isDebugEnabled())
                logger.debug(SQL);
            sqlStatement.execute(SQL);
        }
        finally
        {
            if (sqlStatement != null)
                sqlStatement.close();
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#executeUpdate(java.lang.String)
     */
    public void executeUpdate(String SQL) throws SQLException
    {
        Statement sqlStatement = null;

        try
        {
            sqlStatement = dbConn.createStatement();
            if (logger.isDebugEnabled())
                logger.debug(SQL);
            sqlStatement.executeUpdate(SQL);
        }
        finally
        {
            sqlStatement.close();
        }
    }

    private String buildWhereClause(ArrayList<Column> columns)
    {
        if (columns.size() == 0)
            return "";

        StringBuffer retval = new StringBuffer(" WHERE ");

        Iterator<Column> i = columns.iterator();
        boolean comma = false;
        while (i.hasNext())
        {
            Column c = i.next();
            if (comma)
                retval.append(" AND ");
            comma = true;
            retval.append(assignString(c));
        }
        return retval.toString();
    }

    private String buildCommaAssign(ArrayList<Column> columns)
    {
        StringBuffer retval = new StringBuffer();
        Iterator<Column> i = columns.iterator();
        boolean comma = false;
        while (i.hasNext())
        {
            Column c = i.next();
            if (comma)
                retval.append(", ");
            comma = true;
            retval.append(assignString(c));
        }
        return retval.toString();
    }

    private String buildCommaValues(ArrayList<Column> columns)
    {
        StringBuffer retval = new StringBuffer();
        for (int i = 0; i < columns.size(); i++)
        {
            if (i > 0)
                retval.append(", ");
            retval.append("?");
        }
        return retval.toString();
    }

    private String assignString(Column c)
    {
        return c.getName() + "= ?";
    }

    /**
     * Executes a prepared statement using values supplied as arguments.
     * 
     * @param columns List of values
     * @param statement The prepared statement instance
     * @return Number of rows updated
     */
    protected int executePrepareStatement(List<Column> columns,
            PreparedStatement statement) throws SQLException
    {
        int bindNo = 1;

        for (Column c : columns)
        {
            statement.setObject(bindNo++, c.getValue());
        }

        return statement.executeUpdate();
    }

    protected int executePrepareStatement(Table table,
            PreparedStatement statement) throws SQLException
    {
        return executePrepareStatement(table.getAllColumns(), statement);
    }

    protected int executePrepare(Table table, List<Column> columns, String SQL)
            throws SQLException
    {
        return executePrepare(table, columns, SQL, false, -1);
    }

    protected int executePrepare(Table table, String SQL) throws SQLException
    {
        return executePrepare(table, table.getAllColumns(), SQL, false, -1);
    }

    /**
     * Executes a prepared statement using values supplied as arguments.
     * 
     * @param table Table on which to run
     * @param columns List of values
     * @param SQL The SQL statement to execute
     * @param keep If true cache prepared statement in table instance
     * @param type Statement type assigned by caller (and used to tag
     *            statements)
     * @return Number of rows updated
     */
    protected int executePrepare(Table table, List<Column> columns, String SQL,
            boolean keep, int type) throws SQLException
    {
        int bindNo = 1;

        PreparedStatement statement = null;
        int affectedRows = 0;

        try
        {
            statement = dbConn.prepareStatement(SQL);

            for (Column c : columns)
            {
                Serializable val = c.getValue();
                statement.setObject(bindNo++, val);
            }
            affectedRows = statement.executeUpdate();
        }
        finally
        {
            if (statement != null && !keep)
            {
                statement.close();
                statement = null;
            }
        }
        if (keep && type > -1)
            table.setStatement(type, statement);

        return affectedRows;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#insert(com.continuent.tungsten.replicator.database.Table)
     */
    public int insert(Table table) throws SQLException
    {
        String SQL = "";
        PreparedStatement statement = null;
        boolean caching = table.getCacheStatements();
        ArrayList<Column> allColumns = table.getAllColumns();

        if (caching && (statement = table.getStatement(Table.INSERT)) != null)
        {
            return executePrepareStatement(table, statement);
        }
        else
        {
            SQL += "INSERT INTO " + table.getSchema() + "." + table.getName()
                    + " VALUES (";
            SQL += buildCommaValues(allColumns);
            SQL += ")";
        }

        return executePrepare(table, allColumns, SQL, caching, Table.INSERT);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#update(com.continuent.tungsten.replicator.database.Table,
     *      java.util.ArrayList, java.util.ArrayList)
     */
    public int update(Table table, ArrayList<Column> whereClause,
            ArrayList<Column> values) throws SQLException
    {
        StringBuffer sb = new StringBuffer("UPDATE ");
        sb.append(table.getSchema());
        sb.append(".");
        sb.append(table.getName());
        sb.append(" SET ");
        sb.append(buildCommaAssign(values));
        if (whereClause != null)
        {
            sb.append(" ");
            sb.append(buildWhereClause(whereClause));
        }
        String SQL = sb.toString();

        ArrayList<Column> allColumns = new ArrayList<Column>(values);
        if (whereClause != null)
        {
            allColumns.addAll(whereClause);
        }
        return this.executePrepare(table, allColumns, SQL);
    }

    public boolean supportsReplace()
    {
        return false;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#replace(com.continuent.tungsten.replicator.database.Table)
     */
    public void replace(Table table) throws SQLException
    {
        if (supportsReplace())
        {
            String SQL = "";
            SQL += "REPLACE INTO " + table.getSchema() + "." + table.getName()
                    + " VALUES (";
            SQL += buildCommaValues(table.getAllColumns());
            SQL += ")";

            executePrepare(table, SQL);
        }
        else
        {
            try
            {
                delete(table, false);
            }
            catch (SQLException e)
            {
            }
            insert(table);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#delete(com.continuent.tungsten.replicator.database.Table,
     *      boolean)
     */
    public int delete(Table table, boolean allRows) throws SQLException
    {
        String SQL = "DELETE FROM " + table.getSchema() + "." + table.getName()
                + " ";
        if (!allRows)
        {
            SQL += buildWhereClause(table.getPrimaryKey().getColumns());
            return executePrepare(table, table.getPrimaryKey().getColumns(),
                    SQL);
        }
        else
            return executePrepare(table, new ArrayList<Column>(), SQL);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#prepareStatement(java.lang.String)
     */
    public PreparedStatement prepareStatement(String statement)
            throws SQLException
    {
        if (logger.isDebugEnabled())
            logger.debug("prepareStatement" + statement);
        return dbConn.prepareStatement(statement);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#createStatement()
     */
    public Statement createStatement() throws SQLException
    {
        if (logger.isDebugEnabled())
            logger.debug("createStatement");
        return dbConn.createStatement();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#commit()
     */
    public void commit() throws SQLException
    {
        if (logger.isDebugEnabled())
            logger.debug("commit");
        dbConn.commit();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#rollback()
     */
    public void rollback() throws SQLException
    {
        if (logger.isDebugEnabled())
            logger.debug("rollback");
        dbConn.rollback();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#setAutoCommit(boolean)
     */
    public void setAutoCommit(boolean autoCommit) throws SQLException
    {
        this.autoCommit = autoCommit;
        if (logger.isDebugEnabled())
            logger.debug("setAutoCommit = " + autoCommit);
        if (dbConn.getAutoCommit() != autoCommit)
            dbConn.setAutoCommit(autoCommit);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#setInitScript(java.lang.String)
     */
    @Override
    public void setInitScript(String pathToScript)
    {
        this.initScript = pathToScript;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#createTable(com.continuent.tungsten.replicator.database.Table,
     *      boolean)
     */
    public void createTable(Table t, boolean replace) throws SQLException
    {
        boolean comma = false;

        if (replace)
            dropTable(t);

        String temporary = t.isTemporary() ? "TEMPORARY " : "";

        String SQL = "CREATE " + temporary + "TABLE " + t.getSchema() + "."
                + t.getName() + " (";

        Iterator<Column> i = t.getAllColumns().iterator();
        while (i.hasNext())
        {
            Column c = i.next();
            SQL += (comma ? ", " : "") + c.getName() + " "
                    + columnToTypeString(c, null)
                    + (c.isNotNull() ? " NOT NULL" : "");
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
                    SQL += "UNIQUE (";
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
        SQL += ")";

        // Create the table.
        execute(SQL);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#dropTable(com.continuent.tungsten.replicator.database.Table)
     */
    public void dropTable(Table table)
    {
        String SQL = "DROP TABLE " + table.getSchema() + "." + table.getName()
                + " ";

        try
        {
            execute(SQL);
        }
        catch (SQLException e)
        {
            if (logger.isDebugEnabled())
                logger.debug("Unable to drop table; this may be expected", e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#close()
     */
    public void close()
    {
        disconnect();
    }

    public int nativeTypeToJavaSQLType(int nativeType) throws SQLException
    {
        return nativeType;
    }

    public int javaSQLTypeToNativeType(int javaSQLType) throws SQLException
    {
        return javaSQLType;
    }

    public Table findTable(int tableID) throws SQLException
    {
        return null;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#findTable(int,
     *      java.lang.String)
     */
    public Table findTable(int tableID, String scn) throws SQLException
    {
        return null;
    }

    /**
     * This function should be implemented in concrete class.
     * 
     * @param md DatabaseMetaData object
     * @param schemaName schema name
     * @param tableName table name
     * @return ResultSet as produced by DatabaseMetaData.getColumns() for a
     *         given schema and table
     * @throws SQLException
     */
    public abstract ResultSet getColumnsResultSet(DatabaseMetaData md,
            String schemaName, String tableName) throws SQLException;

    /**
     * This function should be implemented in concrete class.
     * 
     * @param md DatabaseMetaData object
     * @param schemaName schema name
     * @param tableName table name
     * @return ResultSet as produced by DatabaseMetaData.getPrimaryKeys() for a
     *         given schema and table
     * @throws SQLException
     */
    protected abstract ResultSet getPrimaryKeyResultSet(DatabaseMetaData md,
            String schemaName, String tableName) throws SQLException;

    /**
     * This function should be implemented in concrete class.
     * 
     * @param md DatabaseMetaData object.
     * @param schemaName Schema name.
     * @param tableName Table name.
     * @param unique When true, return only indices for unique values; when
     *            false, return indices regardless of whether unique or not.
     * @return ResultSet as produced by DatabaseMetaData.getIndexInfo() for a
     *         given schema and table.
     * @throws SQLException
     */
    protected abstract ResultSet getIndexResultSet(DatabaseMetaData md,
            String schemaName, String tableName, boolean unique)
            throws SQLException;

    /**
     * This function should be implemented in concrete class.
     * 
     * @param md DatabaseMetaData object
     * @param schemaName schema name
     * @param baseTablesOnly If true, return only base tables, not catalogs or
     *            views
     * @return ResultSet as produced by DatabaseMetaData.getTables() for a given
     *         schema
     * @throws SQLException
     */
    protected abstract ResultSet getTablesResultSet(DatabaseMetaData md,
            String schemaName, boolean baseTablesOnly) throws SQLException;

    /**
     * Override in specific database classes. Adds column from a metadata result
     * set with database-specific logic.
     * 
     * @param rs Metadata resultset
     * @return the column definition
     * @throws SQLException if an error occurs
     */
    protected Column addColumn(ResultSet rs) throws SQLException
    {
        String colName = rs.getString("COLUMN_NAME");
        int colType = rs.getInt("DATA_TYPE");
        long colLength = rs.getLong("COLUMN_SIZE");
        boolean isNotNull = rs.getInt("NULLABLE") == DatabaseMetaData.columnNoNulls;
        String valueString = rs.getString("COLUMN_DEF");
        String typeDesc = rs.getString("TYPE_NAME").toUpperCase();
        int columnIdx = rs.getInt("ORDINAL_POSITION");

        Column column = new Column(colName, colType, colLength, isNotNull,
                valueString);
        column.setPosition(columnIdx);
        column.setTypeDescription(typeDesc);
        return column;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#findTable(java.lang.String,
     *      java.lang.String, boolean)
     */
    public Table findTable(String schemaName, String tableName,
            boolean withUniqueIndex) throws SQLException
    {
        DatabaseMetaData md = this.getDatabaseMetaData();
        Table table = null;

        ResultSet rsc = getColumnsResultSet(md, schemaName, tableName);
        List<Column> columns = new LinkedList<Column>();
        while (rsc.next())
        {
            Column column = addColumn(rsc);
            columns.add(column);
        }
        rsc.close();

        // If we found no columns, the table does not exist.
        if (columns.size() == 0)
            return null;

        // Create the table instance and add the columns.
        Map<String, Column> cm = new HashMap<String, Column>();
        table = new Table(schemaName, tableName);
        for (Column col : columns)
        {
            table.AddColumn(col);
            cm.put(col.getName(), col);
        }

        // Look for primary key columns.
        ResultSet rsk = getPrimaryKeyResultSet(md, schemaName, tableName);
        Key pKey = new Key(Key.Primary);
        while (rsk.next())
        {
            String colName = rsk.getString("COLUMN_NAME");
            Column column = cm.get(colName);
            // Adding columns in the primary key order
            pKey.AddColumn(column, rsk.getShort("KEY_SEQ"));
        }
        rsk.close();

        // Add the Primary key if we found any columns.
        if (pKey.columns.size() > 0)
        {
            table.AddKey(pKey);
        }

        // Find unique indexes.
        if (withUniqueIndex)
            findUniqueIndexes(md, schemaName, tableName, cm, table);

        return table;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#findTable(java.lang.String,
     *      java.lang.String)
     */
    @Override
    public Table findTable(String schemaName, String tableName)
            throws SQLException
    {
        return findTable(schemaName, tableName, false);
    }

    /**
     * Finds unique indexes from the metadata and adds them to the table.
     * Primary key is included too, if it exists.
     * 
     * @throws SQLException
     */
    private void findUniqueIndexes(DatabaseMetaData md, String schemaName,
            String tableName, Map<String, Column> cm, Table table)
            throws SQLException
    {
        // Find unique indexes.
        try
        {
            ResultSet rsi = getIndexResultSet(md, schemaName, tableName, true);
            if (rsi.isBeforeFirst())
            {
                String lastIdxName = null;
                Key uIdx = null;
                while (rsi.next())
                {
                    short idxType = rsi.getShort("TYPE");
                    if (idxType != DatabaseMetaData.tableIndexStatistic)
                    {
                        // Results are ordered by NON_UNIQUE, TYPE,
                        // INDEX_NAME, and ORDINAL_POSITION.
                        String idxName = rsi.getString("INDEX_NAME");
                        if (!idxName.equals(lastIdxName))
                        {
                            if (uIdx != null)
                                table.AddKey(uIdx);
                            uIdx = new Key(Key.Unique);
                            uIdx.setName(idxName);
                            lastIdxName = idxName;
                        }

                        String colName = rsi.getString("COLUMN_NAME");
                        Column column = cm.get(colName);
                        uIdx.AddColumn(column);
                    }
                }
                if (uIdx != null)
                    table.AddKey(uIdx);
            }
            rsi.close();
        }
        catch (UnsupportedOperationException e)
        {
            if (logger.isDebugEnabled())
                logger.debug("Can't search for unique indexes, because this function is unsupported: "
                        + e);
        }
    }

    /**
     * Implement ability to fetch tables. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#getTables(java.lang.String,
     *      boolean, boolean)
     */
    public ArrayList<Table> getTables(String schemaName,
            boolean baseTablesOnly, boolean withUniqueIndex)
            throws SQLException
    {
        DatabaseMetaData md = this.getDatabaseMetaData();
        ArrayList<Table> tables = new ArrayList<Table>();

        try
        {
            ResultSet rst = getTablesResultSet(md, schemaName, baseTablesOnly);
            if (rst.isBeforeFirst())
            {
                while (rst.next())
                {
                    String tableName = rst.getString("TABLE_NAME");
                    Table table = findTable(schemaName, tableName,
                            withUniqueIndex);
                    if (table != null)
                    {
                        tables.add(table);
                    }
                }
            }
            rst.close();
        }
        finally
        {
        }

        return tables;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#getTables(java.lang.String,
     *      boolean)
     */
    @Override
    public ArrayList<Table> getTables(String schema, boolean baseTablesOnly)
            throws SQLException
    {
        return getTables(schema, baseTablesOnly, false);
    }

    // this is part of TREP-232 workaround
    static final String insertColumnsValues = " ("
                                                    + ConsistencyTable.idColumnName
                                                    + ", "
                                                    + ConsistencyTable.dbColumnName
                                                    + ", "
                                                    + ConsistencyTable.tblColumnName
                                                    + ", "
                                                    + ConsistencyTable.offsetColumnName
                                                    + ", "
                                                    + ConsistencyTable.limitColumnName
                                                    + ", "
                                                    + ConsistencyTable.methodColumnName
                                                    + ") VALUES (";

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#consistencyCheck(com.continuent.tungsten.replicator.database.Table,
     *      com.continuent.tungsten.replicator.consistency.ConsistencyCheck)
     */
    public void consistencyCheck(Table ct, ConsistencyCheck cc)
            throws SQLException, ConsistencyException
    {
        consistencyCheck(ct, cc, null, -1);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#consistencyCheck(com.continuent.tungsten.replicator.database.Table,
     *      com.continuent.tungsten.replicator.consistency.ConsistencyCheck,
     *      String, int)
     */
    public void consistencyCheck(Table ct, ConsistencyCheck cc,
            String masterCrc, int masterCnt) throws SQLException,
            ConsistencyException
    {
        String tableName = cc.getTableName();
        String schemaName = cc.getSchemaName();
        int id = cc.getCheckId();

        ArrayList<Column> ctColumns = ct.getAllColumns();

        // Initialize primary key columns - used in WHERE clauses
        ctColumns.get(ConsistencyTable.dbColumnIdx).setValue(schemaName);
        ctColumns.get(ConsistencyTable.tblColumnIdx).setValue(tableName);
        ctColumns.get(ConsistencyTable.idColumnIdx).setValue(id);

        // Initialize row defaults
        ctColumns.get(ConsistencyTable.offsetColumnIdx).setValue(
                cc.getRowOffset());
        ctColumns.get(ConsistencyTable.limitColumnIdx).setValue(
                cc.getRowLimit());
        ctColumns.get(ConsistencyTable.methodColumnIdx)
                .setValue(cc.getMethod());

        // if (supportsUseDefaultSchema())
        // {
        // /*
        // * on MySQL this circumvents the replication protection on tungsten
        // * schema. Thus, updates to tungsten.consistency table will get into
        // * binlog
        // */
        // logger.info("Consistency check: switching to schema '" + schemaName
        // + "'...");
        // useDefaultSchema(schemaName);
        // }

        // Start consistency check transaction
        setAutoCommit(false);
        // Prepare row that will hold consistency check values
        // TENT-134: Delete holds a lock that causes LOCK WAIT TIMEOUT on
        // InnoDB. Commented out for now.
        // delete(ct); // WHERE is taken from prim key

        // Database.insert() does not work with TIMESTAMP fields on MySQL: I'm
        // getting
        // com.mysql.jdbc.MysqlDataTruncation: Data truncation: Incorrect
        // datetime value: 'CURRENT_TIMESTAMP' for column 'ts' at row 1
        // Looks like JDBC puts this magic symbol there which MySQL refuses to
        // recognize. Very sad.
        // Have to compose own INSERT statement as a workaround which might be
        // not DBMS-portable.
        StringBuffer insert = new StringBuffer(256);
        insert.append("INSERT INTO ");
        insert.append(ct.getSchema());
        insert.append('.');
        insert.append(ct.getName());
        insert.append(insertColumnsValues);
        insert.append(id);
        insert.append(", '");
        insert.append(schemaName);
        insert.append("', '");
        insert.append(tableName);
        insert.append("', ");
        insert.append(cc.getRowOffset());
        insert.append(", ");
        insert.append(cc.getRowLimit());
        insert.append(", '");
        insert.append(cc.getMethod());
        insert.append("')");

        Statement st = dbConn.createStatement();
        try
        {
            st.execute(insert.toString());
        }
        catch (Exception e)
        {
            String msg = insert.toString() + " failed: " + e.getMessage();
            logger.error(msg);
            throw new ConsistencyException(msg, e);
        }

        // Perform consistency check
        try
        {
            ResultSet rs = cc.performConsistencyCheck(this);

            if (rs.next())
            {
                // Create SET array
                Column col;
                ArrayList<Column> setColumns = new ArrayList<Column>();
                if (masterCrc == null)
                {
                    // We are the master, so put CC results into master columns.
                    col = ctColumns.get(ConsistencyTable.masterCrcColumnIdx);
                    col.setValue(rs
                            .getString(ConsistencyTable.thisCrcColumnName));
                    setColumns.add(col);
                    col = ctColumns.get(ConsistencyTable.masterCntColumnIdx);
                    col.setValue(rs.getInt(ConsistencyTable.thisCntColumnName));
                    setColumns.add(col);
                }
                else
                {
                    // We got CC values from the master up-front.
                    col = ctColumns.get(ConsistencyTable.masterCrcColumnIdx);
                    col.setValue(masterCrc);
                    setColumns.add(col);
                    col = ctColumns.get(ConsistencyTable.masterCntColumnIdx);
                    col.setValue(masterCnt);
                    setColumns.add(col);
                    // Save calculated CC values to "this" fields.
                    col = ctColumns.get(ConsistencyTable.thisCrcColumnIdx);
                    col.setValue(rs
                            .getString(ConsistencyTable.thisCrcColumnName));
                    setColumns.add(col);
                    col = ctColumns.get(ConsistencyTable.thisCntColumnIdx);
                    col.setValue(rs.getInt(ConsistencyTable.thisCntColumnName));
                    setColumns.add(col);
                }
                rs.close();

                // record CC values obtained on master
                update(ct, ct.getPrimaryKey().getColumns(), setColumns);
                // commit consistency check transaction
            }
            else
            {
                rs.close();
                String msg = "Consistency check returned empty ResultSet.";
                logger.warn(msg);
                // throw new ConsistencyException(msg);
            }
            commit();
        }
        finally
        {
            st.close();
            // Ensure rollback after an error to release locks.
            try
            {
                rollback();
            }
            catch (Exception e)
            {
            }
        }
    }

    /**
     * Stub routine that ignores table type. MySQL databases must override this.
     * 
     * @see com.continuent.tungsten.replicator.database.Database#createTable(com.continuent.tungsten.replicator.database.Table,
     *      boolean, java.lang.String)
     */
    public void createTable(Table table, boolean replace,
            String tungstenTableType) throws SQLException
    {
        createTable(table, replace);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#createTable(com.continuent.tungsten.replicator.database.Table,
     *      boolean, java.lang.String, java.lang.String)
     */
    @Override
    public void createTable(Table table, boolean replace,
            String tungstenSchema, String tungstenTableType)
            throws SQLException
    {
        createTable(table, replace, tungstenTableType);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#createTable(com.continuent.tungsten.replicator.database.Table,
     *      boolean, java.lang.String, java.lang.String, java.lang.String)
     */
    @Override
    public void createTable(Table table, boolean replace,
            String tungstenSchema, String tungstenTableType, String serviceName)
            throws SQLException
    {
        createTable(table, replace, tungstenSchema, tungstenTableType);

    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#prepareOptionSetStatement(java.lang.String,
     *      java.lang.String)
     */
    public String prepareOptionSetStatement(String optionName,
            String optionValue)
    {
        return null;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#getBlobAsBytes(ResultSet,
     *      int)
     */
    public byte[] getBlobAsBytes(ResultSet resultSet, int column)
            throws SQLException
    {
        Blob blob = resultSet.getBlob(column);
        return blob.getBytes(1L, (int) blob.length());
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#getDatabaseObjectName(java.lang.String)
     */
    public String getDatabaseObjectName(String name)
    {
        return name;
    }

    /**
     * {@inheritDoc}
     */
    public ArrayList<String> getReservedWords()
    {
        throw new UnsupportedOperationException(
                "List of reserved words is not implemented");
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#dropTungstenCatalogTables(java.lang.String,
     *      java.lang.String, java.lang.String)
     */
    public void dropTungstenCatalogTables(String schemaName,
            String tungstenTableType, String serviceName) throws SQLException
    {
        dropTable(new Table(schemaName, ConsistencyTable.TABLE_NAME));
        dropTable(new Table(schemaName, ShardChannelTable.TABLE_NAME));
        dropTable(new Table(schemaName, ShardTable.TABLE_NAME));
        dropTable(new Table(schemaName, HeartbeatTable.TABLE_NAME));
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#isSystemSchema(java.lang.String)
     */
    @Override
    public boolean isSystemSchema(String schemaName)
    {
        return false;
    }

    /**
     * {@inheritDoc}
     * 
     * @throws ExtractorException
     */
    @Override
    public String getCurrentPosition(boolean flush) throws ReplicatorException
    {
        return null;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#supportsFlashbackQuery()
     */
    @Override
    public boolean supportsFlashbackQuery()
    {
        return false;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#getFlashbackQuery(java.lang.String)
     */
    @Override
    public String getFlashbackQuery(String position)
    {
        return null;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#getCsvWriter(java.io.BufferedWriter)
     */
    public CsvWriter getCsvWriter(BufferedWriter writer)
    {
        if (this.csvSpec == null)
        {
            throw new UnsupportedOperationException(
                    "CSV output specification is not configured");
        }
        else
            return csvSpec.createCsvWriter(writer);
    }

    public boolean hasMicrosecondsSupport()
    {
        return false;
    }
}
