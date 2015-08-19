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
 * Contributor(s): Robert Hodges, Stephane Giron, Linas Virbalas
 */

package com.continuent.tungsten.replicator.database;

import java.io.BufferedWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.continuent.tungsten.common.csv.CsvSpecification;
import com.continuent.tungsten.common.csv.CsvWriter;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.consistency.ConsistencyCheck;
import com.continuent.tungsten.replicator.consistency.ConsistencyException;
import com.continuent.tungsten.replicator.datasource.UniversalConnection;
import com.continuent.tungsten.replicator.dbms.OneRowChange;

/**
 * Defines the contract for a driver that implements database-specific SQL
 * operations. There is an implementation of this interface for each DBMS
 * implementation.
 * 
 * @author <a href="mailto:scott.martin@continuent.com">Scott Martin</a>
 * @version 1.0
 */
public interface Database extends UniversalConnection
{
    /** String to denote MySQL DBMS dialect in metadata. */
    public static String MYSQL      = "mysql";

    /** String to denote Oracle dialect in metadata. */
    public static String ORACLE     = "oracle";

    /** String to denote PostgreSQL dialect in metadata. */
    public static String POSTGRESQL = "postgresql";

    /** String to denote PostgreSQL dialect in metadata. */
    public static String UNKNOWN    = "unknown";

    // START UNIVERSALCONNECTOR API.
    /**
     * Commit the current transaction.
     */
    @Override
    public void commit() throws SQLException;

    /**
     * Rollback the current transaction.
     */
    @Override
    public void rollback() throws SQLException;

    /**
     * Toggles autocommit by calling Connection.setAutocommit().
     */
    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException;

    // END UNIVERSALCONNECTOR API.

    /** Returns the type of DBMS behind the interface */
    public DBMS getType();

    /**
     * Returns a SQL name matcher for this database type. You can get a matcher
     * without calling connect() first.
     * 
     * @throws ReplicatorException
     */
    public SqlOperationMatcher getSqlNameMatcher() throws ReplicatorException;

    /** Sets the JDBC URL used for database connections. */
    public void setUrl(String url);

    /** Sets the database user. */
    public void setUser(String user);

    /** Sets the database password. */
    public void setPassword(String password);

    /**
     * Determines whether this connection has superuser privileges (e.g., SYSDBA
     * or SUPER depending on the DBMS type). Superusers can create logins, kill
     * sessions, and manipulate replication.
     * 
     * @param privileged If true this account is a superuser
     */
    public void setPrivileged(boolean privileged);

    /** Returns true if this account is a super user. */
    public boolean isPrivileged();

    /**
     * Sets the CSV specification use to generate CSV output parameters.
     */
    public void setCsvSpecification(CsvSpecification csvSpecification);

    /**
     * Connects to the database. You must set the url, user, and password then
     * do this. Connection does not log queries by default.
     */
    public void connect() throws SQLException;

    /**
     * Disconnects from the database. This is also accomplished by close().
     */
    public void disconnect();

    /**
     * Returns JDBC DatabaseMetadata for the current connection.
     */
    public DatabaseMetaData getDatabaseMetaData() throws SQLException;

    /**
     * Creates a table using the supplied table definition.
     * 
     * @param table Table specification
     * @param replace If true, replace an existing table
     * @throws SQLException
     */
    public void createTable(Table table, boolean replace) throws SQLException;

    /**
     * Returns true if this implementation supports schema create and drop
     * operations.
     */
    public boolean supportsCreateDropSchema() throws SQLException;

    /**
     * Creates the named schema.
     * 
     * @see #supportsCreateDropSchema()
     */
    public void createSchema(String schema) throws SQLException;

    /**
     * Drops the named schema.
     * 
     * @see #supportsCreateDropSchema()
     */
    public void dropSchema(String schema) throws SQLException;

    /**
     * Returns true if this implementation supports changing the default schema,
     * for example via a "USE <database>" command.
     */
    public boolean supportsUseDefaultSchema() throws SQLException;

    /**
     * Changes the default schema to the named schema.
     * 
     * @see #supportsUseDefaultSchema()
     */
    public void useDefaultSchema(String schema) throws SQLException;

    /**
     * Returns a query that can be used to set the schema.
     * 
     * @see #supportsUseDefaultSchema()
     */
    public String getUseSchemaQuery(String schema);

    /**
     * Returns true if this implementation allow clients to turn logging of SQL
     * updates on and off at the session level. (Currently only MySQL supports
     * this feature.)
     */
    public boolean supportsControlSessionLevelLogging() throws SQLException;

    /**
     * Sets session-level logging of updates.
     * 
     * @param suppressed If true, updates are not logged; otherwise logging is
     *            turned on
     */
    public void controlSessionLevelLogging(boolean suppressed)
            throws SQLException;

    /**
     * Returns true if this implementation supports synchronization of native
     * slave replication with Tungsten position. (Currently only MySQL supports
     * this feature.)
     */
    public boolean supportsNativeSlaveSync();

    /**
     * Synchronize the native slave position with Tungsten so that native
     * replication can start properly after Tungsten goes offline.
     * 
     * @param eventId Tungsten event ID containing native log coordinates
     */
    public void syncNativeSlave(String eventId) throws SQLException;

    /**
     * Returns true if this implementation supports changing the timestamp value
     * used by functions that return the current time.
     */
    public boolean supportsControlTimestamp() throws SQLException;

    /**
     * Returns true if this implementation supports setting session variables.
     */
    public boolean supportsSessionVariables() throws SQLException;

    /**
     * Sets a session variable. This works only if the database type supports
     * session variables.
     * 
     * @param name Name of the variable to set
     * @param value Value to set
     * @throws SQLException Thrown if setting variable is unsuccessful
     */
    public void setSessionVariable(String name, String value)
            throws SQLException;

    /**
     * Get session variable. This works only if the database type supports
     * session variables.
     * 
     * @param name Name of the variable to get
     * @return Value of variable or null if unset
     * @throws SQLException Thrown if getting variable is unsuccessful
     */
    public String getSessionVariable(String name) throws SQLException;

    /**
     * Returns true if this implementation supports user management commands.
     */
    public boolean supportsUserManagement();

    /**
     * Creates a user. Works only if the database type supports user management.
     * This call is intended for testing.
     * 
     * @param user User definition
     */
    public void createUser(User user) throws SQLException;

    /**
     * Deletes a user. Works only if the database type supports user management.
     * This call is intended for testing.
     * 
     * @param user User definition
     * @param ignore If true, ignore error
     */
    public void dropUser(User user, boolean ignore) throws SQLException;

    /**
     * Lists user sessions in the DBMS server.
     */
    public List<Session> listSessions() throws SQLException;

    /**
     * Kills a particular session.
     */
    public void kill(Session session) throws SQLException, ReplicatorException;

    /**
     * Return the Table with all its current accompanying Columns that matches
     * tableID. tableID is meant to be some sort of unique "object number"
     * interpreted within the current connection. The exact nature of of what
     * this tableID will likely vary from rdbms to rdbms but in Oracle parlance
     * it is an object number. Returns null if no such table exists.
     */
    public Table findTable(int tableID) throws SQLException;

    /**
     * Return the Table with all its accompanying Columns at the provided scn
     * that matches tableID. tableID is meant to be some sort of unique "object
     * number" interpreted within the current connection. The exact nature of of
     * what this tableID will likely vary from rdbms to rdbms but in Oracle
     * parlance it is an object number. Returns null if no such table exists.
     * 
     * @param tableID the object which is looked for
     * @param scn when the object is search for
     * @return a Table if matching was found
     * @throws SQLException if an error occurs
     */
    public Table findTable(int tableID, String scn) throws SQLException;

    /**
     * Return the Table with all its accompanying Columns.
     * 
     * @param schemaName name of schema containing the table
     * @param tableName name of the table
     * @return a Table if matching was found
     * @throws SQLException if an error occurs
     */
    public Table findTable(String schemaName, String tableName)
            throws SQLException;

    /**
     * Return the Table with all its accompanying Columns.
     * 
     * @param schemaName name of schema containing the table
     * @param tableName name of the table
     * @param withUniqueIndex should unique indexes be fetched or not ?
     * @return a Table if matching was found
     * @throws SQLException if an error occurs
     */
    public Table findTable(String schemaName, String tableName,
            boolean withUniqueIndex) throws SQLException;

    /**
     * Returns a query that can be used to set the timestamp.
     * 
     * @param timestamp A Java time value consisting of milliseconds since
     *            January 1, 1970 00:00:00 GMT
     * @see #supportsControlTimestamp()
     */
    public String getControlTimestampQuery(Long timestamp);

    /**
     * Executes a SQL request.
     */
    public void execute(String SQL) throws SQLException;

    /**
     * Executes a SQL request containing an update.
     */
    public void executeUpdate(String SQL) throws SQLException;

    /**
     * Inserts a row into a table. Values are taken from the current Column
     * instances stored in the table definition.
     * 
     * @param table Table instance containing column data
     * @return the number of inserted rows
     */
    public int insert(Table table) throws SQLException;

    /**
     * Updates on or more rows in a table.
     * 
     * @param table Table instance to update
     * @param whereClause List of columns containing where clause values, which
     *            are ANDed
     * @param values List of columns containing values to set for matching rows
     * @return the number of updated rows
     */
    public int update(Table table, ArrayList<Column> whereClause,
            ArrayList<Column> values) throws SQLException;

    /**
     * Returns true if the implementation supports a SQL REPLACE command.
     */
    public boolean supportsReplace();

    /**
     * Returns true if the implementation supports a SQL REPLACE command.
     */
    public boolean supportsBLOB();

    /**
     * Replaces a row in the table using the data supplied by the Table
     * specification. The Table instance's primary key is used to locate the
     * matching row. This uses a REPLACE command if it is available.
     * 
     * @param table Table definition with column data and primary key
     * @see #supportsReplace()
     */
    public void replace(Table table) throws SQLException;

    /**
     * Deletes a row in a table.
     * 
     * @param table Table specification with primary key; columns for the key
     *            must be defined.
     * @param allRows flag indicating that all rows from the underlying table
     *            should be deleted
     * @return the number of deleted rows
     * @throws SQLException
     */
    public int delete(Table table, boolean allRows) throws SQLException;

    /**
     * Generate a JDBC prepared statement.
     * 
     * @param statement SQL statement to prepare
     */
    public PreparedStatement prepareStatement(String statement)
            throws SQLException;

    /**
     * Generate a JDBC statement.
     */
    public Statement createStatement() throws SQLException;

    /**
     * Provides a script file to be executed when initializing connection.
     */
    public void setInitScript(String pathToScript);

    /**
     * Return a place holder in a prepared statement for a column of type
     * ColumnSpec. Typically "?" as is INSERT INTO FOO VALUES(?)
     */
    public String getPlaceHolder(OneRowChange.ColumnSpec col, Object colValue,
            String typeDesc);

    /**
     * Return TRUE IFF NULL values are bound differently in SQL statement from
     * non null values for the given column type. For example, in Oracle, the
     * datatype XML must look like "XMLTYPE(?)" in most SQL statements, but in
     * the case of a NULL value, it would look simply like "?".
     */
    public boolean nullsBoundDifferently(OneRowChange.ColumnSpec col);

    /**
     * return true IFF nulls are sometimes treated differently in
     * nullsBoundDifferently() as non nulls.
     */
    public boolean nullsEverBoundDifferently();

    /**
     * Returns the database connection
     */
    public Connection getConnection();

    /**
     * Drops an existing table.
     */
    public void dropTable(Table table);

    /**
     * Closes the instance and frees all resources.
     */
    public void close();

    /**
     * Databases have various column types usually identified by some vendor
     * defined integer value. e.g. 2 = Oracle number, 12 = Oracle date. This
     * function converts the vendor specific type numbers into the java.sql.Type
     * numbers we shall use internally.
     */
    public int nativeTypeToJavaSQLType(int nativeType) throws SQLException;

    /**
     * Opposite of the above function. I cannot image why any of the portable
     * code should ever need to call this function. But it seems like a fine
     * place to create a place holder for such a function declaration even if
     * only called from vendor specific code.
     */
    public int javaSQLTypeToNativeType(int javaSQLType) throws SQLException;

    /**
     * Returns a list of schemas available on the server.
     * 
     * @return list of available schemas
     * @throws SQLException
     */
    public ArrayList<String> getSchemas() throws SQLException;

    /**
     * Returns a list of tables available in the schema
     * 
     * @param schema Name of the schema
     * @param baseTablesOnly If true, only return real tables and not catalogs
     *            or views
     * @param withUniqueIndex should unique indexes be fetched or not ?
     * @return list of tables in the schema
     * @throws SQLException
     */
    public ArrayList<Table> getTables(String schema, boolean baseTablesOnly,
            boolean withUniqueIndex) throws SQLException;

    /**
     * Returns a list of tables available in the schema
     * 
     * @param schema Name of the schema
     * @param baseTablesOnly If true, only return real tables and not catalogs
     *            or views
     * @return list of tables in the schema
     * @throws SQLException
     */
    public ArrayList<Table> getTables(String schema, boolean baseTablesOnly)
            throws SQLException;

    /**
     * Returns a result set containing columns for a specific table.
     * 
     * @param md DatabaseMetaData object
     * @param schemaName schema name
     * @param tableName table name
     * @return ResultSet as produced by DatabaseMetaData.getColumns() for a
     *         given schema and table
     * @throws SQLException
     */
    public ResultSet getColumnsResultSet(DatabaseMetaData md,
            String schemaName, String tableName) throws SQLException;

    /**
     * Runs consistency check transaction
     * 
     * @param ct consitency table
     * @param cc ConsistencyCheck specification
     * @throws SQLException
     */
    public void consistencyCheck(Table ct, ConsistencyCheck cc)
            throws SQLException, ConsistencyException;

    /**
     * Runs consistency check transaction:<br/>
     * a.) If masterCrc==null - normal (master side) consistency check is
     * executed, which is expected to be replicated to the slaves by
     * replication.<br/>
     * b.) If masterCrc!=null, then masterCrc and masterCnt are put into
     * corresponding columns, while executed check's results are put into
     * this_crc and this_cnt columns. The idea behind this is that if there's no
     * replication running, that would usually copy over the consistency check
     * from master to slave, it inserts consistency check results of the master
     * to this slave, runs consistency check on this slave and updates the slave
     * part (this_crc and this_cnt) in the created consistency row accordingly.
     * 
     * @param ct Consistency table.
     * @param cc ConsistencyCheck specification.
     * @param masterCrc CRC value of this check on the master.
     * @param masterCnt Count of rows of this check on the master.
     */
    public void consistencyCheck(Table ct, ConsistencyCheck cc,
            String masterCrc, int masterCnt) throws SQLException,
            ConsistencyException;

    /**
     * getNowFunction returns the database-specific way to get current date and
     * time from the database.
     * 
     * @return the name of the function to be called at the database level to
     *         get the current date and time
     */
    public String getNowFunction();

    /**
     * getTimeDiff returns the database-specific way of subtracting two "dates"
     * and return the result in seconds complete with space for the two bind
     * variables. E.g. in MySQL it might be "time_to_sec(timediff(?, ?))". If
     * either of the string variables are null, replace with the bind character
     * (e.g. "?") else use the string given. For example getTimeDiff(null,
     * "myTimeCol") -> time_to_sec(timediff(?, myTimeCol))
     */
    public String getTimeDiff(String string1, String string2);

    /**
     * Creates a table using the supplied table definition and the provided
     * table type.
     * 
     * @param table Table specification
     * @param replace If true, replace an existing table
     * @param tungstenTableType table type to be used for tungsten catalog
     *            tables
     * @throws SQLException
     */
    public void createTable(Table table, boolean replace,
            String tungstenTableType) throws SQLException;

    public void createTable(Table table, boolean replace,
            String tungstenSchema, String tungstenTableType)
            throws SQLException;

    public void createTable(Table hbTable, boolean b, String schema,
            String tableType, String serviceName) throws SQLException;

    /**
     * prepareOptionSetStatement generates the sql statement that is to be used
     * to set an option (or a session variable) at the database connection
     * level.
     * 
     * @param optionName the option to be set
     * @param optionValue the value to be used
     * @return a string that contains the statement that should be executed, or
     *         null if this does not exist for a database
     */
    public String prepareOptionSetStatement(String optionName,
            String optionValue);

    /**
     * Fetches the given column as a byte[] array. For MySQL and Oracle this
     * means converting BLOB field into byte[] array, while for PostgreSQL -
     * returning the bytea type field's value.
     * 
     * @param resultSet ResultSet which has this blob column.
     * @param column Index of the column to fetch value from.
     * @return byte[] array containing underlying BLOB or bytea field value.
     * @throws SQLException As this method is operating on ResultSet, it might
     *             throw a SQLException.
     */
    public byte[] getBlobAsBytes(ResultSet resultSet, int column)
            throws SQLException;

    /**
     * Returns the eventually quoted database object name. For example, with
     * mysql, database object names should be backticked (`example`).
     * 
     * @param name unquoted database object name
     * @return eventually quoted database object name
     */
    public String getDatabaseObjectName(String name);

    /**
     * Returns a properly configured CsvWriter to generate CSV according to the
     * preferred conventions of this DBMS type.
     * 
     * @param writer A buffered writer to receive CSV output
     * @return A property configured CsvWriter instance
     */
    public CsvWriter getCsvWriter(BufferedWriter writer);

    /**
     * Returns a list of reserved words used by the DBMS, which cannot be used
     * as table and column names.<br/>
     * Words are expected to be upper case and applications checking with
     * something similar to
     * getReservedWords().contains(table.getName().toUpperCase()).
     * 
     * @return A list of reserved words.
     */
    public ArrayList<String> getReservedWords();

    /**
     * dropTungstenCatalog removes Tungsten catalog tables.
     * 
     * @param schemaName The schema name where Tungsten catalog is stored
     * @param tungstenTableType The type of table used to store Tungsten
     *            metadata
     * @param serviceName The service name for which the catalog has to be
     *            dropped
     * @throws SQLException when an error occurs
     */
    public void dropTungstenCatalogTables(String schemaName,
            String tungstenTableType, String serviceName) throws SQLException;

    /**
     * Returns true if the given schema is a system schema.
     * 
     * @param schemaName a schema
     * @return true if schemaName matches a system schema
     */
    public boolean isSystemSchema(String schemaName);

    /**
     * Returns the current position of the database (binary log position for
     * MySQL, SCN for Oracle, for example).
     * 
     * @param flush Should the database be flushed before reading the position?
     * @return The current position as a string
     * @throws ReplicatorException if an error occurs
     */
    public String getCurrentPosition(boolean flush) throws ReplicatorException;

    /**
     * Does the database support flashback query?
     * 
     * @return true if the database support flashback queries, false otherwise
     */
    public boolean supportsFlashbackQuery();

    /**
     * Returns the flashback clause based on the given position
     * 
     * @param position The position to be used for the flashback query
     * @return the generated flashback clause based on the position
     */
    public String getFlashbackQuery(String position);

    public boolean hasMicrosecondsSupport();
}