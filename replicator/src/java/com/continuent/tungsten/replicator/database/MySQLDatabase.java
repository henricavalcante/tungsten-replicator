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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.csv.CsvWriter;
import com.continuent.tungsten.common.csv.NullPolicy;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.extractor.ExtractorException;

/**
 * Implements DBMS-specific operations for MySQL.
 * 
 * @author <a href="mailto:scott.martin@continuent.com">Scott Martin</a>
 */
public class MySQLDatabase extends AbstractDatabase
{
    private static Logger                  logger                        = Logger.getLogger(MySQLDatabase.class);

    private boolean                        sessionLevelLoggingSuppressed = false;

    // SET TIMESTAMP support requires to know whether the timestamp can be a
    // double (MySQL 5.6 or later) or is a LONGLONG (older releases)
    private static final int               MYSQL_DOUBLE                  = 8;
    // private static final int MYSQL_LONGLONG = -5;

    /** A list of words that can't be used in table and column names. */
    private static final ArrayList<String> reservedWords                 = new ArrayList<String>(
                                                                                 Arrays.asList(new String[]{
            "ACCESSIBLE", "ALTER", "AS", "BEFORE", "BINARY", "BY", "CASE",
            "CHARACTER", "COLUMN", "CONTINUE", "CROSS", "CURRENT_TIMESTAMP",
            "DATABASE", "DAY_MICROSECOND", "DEC", "DEFAULT", "DESC",
            "DISTINCT", "DOUBLE", "EACH", "ENCLOSED", "EXIT", "FETCH",
            "FLOAT8", "FOREIGN", "GRANT", "HIGH_PRIORITY", "HOUR_SECOND", "IN",
            "INNER", "INSERT", "INT2", "INT8", "INTO", "JOIN", "KILL", "LEFT",
            "LINEAR", "LOCALTIME", "LONG", "LOOP", "MATCH", "MEDIUMTEXT",
            "MINUTE_SECOND", "NATURAL", "NULL", "OPTIMIZE", "OR", "OUTER",
            "PRIMARY", "RANGE", "READ_WRITE", "REGEXP", "REPEAT", "RESTRICT",
            "RIGHT", "SCHEMAS", "SENSITIVE", "SHOW", "SPECIFIC", "SQLSTATE",
            "SQL_CALC_FOUND_ROWS", "STARTING", "TERMINATED", "TINYINT",
            "TRAILING", "UNDO", "UNLOCK", "USAGE", "UTC_DATE", "VALUES",
            "VARCHARACTER", "WHERE", "WRITE", "ZEROFILL", "ALL", "AND",
            "ASENSITIVE", "BIGINT", "BOTH", "CASCADE", "CHAR", "COLLATE",
            "CONSTRAINT", "CREATE", "CURRENT_TIME", "CURSOR", "DAY_HOUR",
            "DAY_SECOND", "DECLARE", "DELETE", "DETERMINISTIC", "DIV", "DUAL",
            "ELSEIF", "EXISTS", "FALSE", "FLOAT4", "FORCE", "FULLTEXT",
            "HAVING", "HOUR_MINUTE", "IGNORE", "INFILE", "INSENSITIVE", "INT1",
            "INT4", "INTERVAL", "ITERATE", "KEYS", "LEAVE", "LIMIT", "LOAD",
            "LOCK", "LONGTEXT", "MASTER_SSL_VERIFY_SERVER_CERT", "MEDIUMINT",
            "MINUTE_MICROSECOND", "MODIFIES", "NO_WRITE_TO_BINLOG", "ON",
            "OPTIONALLY", "OUT", "PRECISION", "PURGE", "READS", "REFERENCES",
            "RENAME", "REQUIRE", "REVOKE", "SCHEMA", "SELECT", "SET",
            "SPATIAL", "SQLEXCEPTION", "SQL_BIG_RESULT", "SSL", "TABLE",
            "TINYBLOB", "TO", "TRUE", "UNIQUE", "UPDATE", "USING",
            "UTC_TIMESTAMP", "VARCHAR", "WHEN", "WITH", "YEAR_MONTH", "ADD",
            "ANALYZE", "ASC", "BETWEEN", "BLOB", "CALL", "CHANGE", "CHECK",
            "CONDITION", "CONVERT", "CURRENT_DATE", "CURRENT_USER",
            "DATABASES", "DAY_MINUTE", "DECIMAL", "DELAYED", "DESCRIBE",
            "DISTINCTROW", "DROP", "ELSE", "ESCAPED", "EXPLAIN", "FLOAT",
            "FOR", "FROM", "GROUP", "HOUR_MICROSECOND", "IF", "INDEX", "INOUT",
            "INT", "INT3", "INTEGER", "IS", "KEY", "LEADING", "LIKE", "LINES",
            "LOCALTIMESTAMP", "LONGBLOB", "LOW_PRIORITY", "MEDIUMBLOB",
            "MIDDLEINT", "MOD", "NOT", "NUMERIC", "OPTION", "ORDER", "OUTFILE",
            "PROCEDURE", "READ", "REAL", "RELEASE", "REPLACE", "RETURN",
            "RLIKE", "SECOND_MICROSECOND", "SEPARATOR", "SMALLINT", "SQL",
            "SQLWARNING", "SQL_SMALL_RESULT", "STRAIGHT_JOIN", "THEN",
            "TINYTEXT", "TRIGGER", "UNION", "UNSIGNED", "USE", "UTC_TIME",
            "VARBINARY", "VARYING", "WHILE", "XOR"                               }));

    private static final List<String>      SYSTEM_SCHEMAS                = Arrays.asList(new String[]{
            "mysql", "performance_schema", "information_schema"          });

    private boolean                        supportsMicroseconds;

    public MySQLDatabase() throws SQLException
    {
        dbms = DBMS.MYSQL;
        // Hard code the driver so it gets loaded correctly.
        dbDriver = "com.mysql.jdbc.Driver";
    }

    protected String columnToTypeString(Column c, String tableType)
    {
        switch (c.getType())
        {
            case Types.TINYINT :
                return "TINYINT";

            case Types.SMALLINT :
                return "SMALLINT";

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
                if (tableType != null
                        && "infinidb".equals(tableType.toLowerCase()))
                    return "DATETIME";
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
     * {@inheritDoc}
     */
    @Override
    protected Column addColumn(ResultSet rs) throws SQLException
    {
        // Generic initialization.
        Column column = super.addColumn(rs);
        String typeDesc = column.getTypeDescription();
        String columnName = column.getName();

        // MySQL specifics.
        boolean isSigned = !typeDesc.contains("UNSIGNED");
        int dataType = rs.getInt("DATA_TYPE");
        if (logger.isDebugEnabled())
            logger.debug("Adding column " + columnName + " (TYPE " + dataType
                    + " - " + (isSigned ? "SIGNED" : "UNSIGNED") + ")");
        column.setSigned(isSigned);

        return column;
    }

    /**
     * Connect to a MySQL database, which includes setting the wait_timeout to a
     * very high value so we don't lose our connection. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#connect()
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
            executeUpdate("SET wait_timeout = 2147483");
        }
        catch (SQLException e)
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Unable to set wait_timeout to maximum value of 2147483");
                logger.debug("Please consider using an explicit JDBC URL setting to avoid connection timeouts");
            }
        }

        // Set the time zone to UTC to ensure consistent handling of timestamp
        // values.
        try
        {
            executeUpdate("SET session time_zone='+00:00'");
        }
        catch (SQLException e)
        {
            logger.warn("Unable to set time zone to +00:00: message="
                    + e.getMessage());
            if (logger.isDebugEnabled())
            {
                logger.debug(e);
            }
        }

        Statement sqlStatement = null;
        ResultSet rs = null;
        try
        {
            sqlStatement = dbConn.createStatement();
            rs = sqlStatement.executeQuery("SELECT @@timestamp");
            supportsMicroseconds = rs.getMetaData().getColumnType(1) == MYSQL_DOUBLE;
        }
        finally
        {
            if (rs != null)
                try
                {
                    rs.close();
                }
                catch (SQLException ignore)
                {
                }
            if (sqlStatement != null)
                try
                {
                    sqlStatement.close();
                }
                catch (SQLException ignore)
                {
                }
        }

        if (initScript != null)
        {
            // Load the file : one sql statement per line
            File file = new File(initScript);
            FileReader reader = null;
            Statement stmt = null;
            try
            {
                reader = new FileReader(file);
            }
            catch (FileNotFoundException e)
            {
                throw new SQLException("Init script not found", e);
            }

            try
            {
                // Add to support misleading Eclipse warning.
                @SuppressWarnings("resource")
                BufferedReader br = new BufferedReader(reader);
                String sql = null;
                stmt = dbConn.createStatement();

                while ((sql = br.readLine()) != null)
                {
                    sql = sql.trim();
                    if (sql.startsWith("#") || sql.length() == 0)
                        continue;

                    // For now, we don't care for the results.
                    stmt.execute(sql);
                }
            }
            catch (IOException e)
            {
                throw new SQLException("Failed reading init script ("
                        + initScript + ")", e);
            }
            finally
            {
                if (reader != null)
                {
                    try
                    {
                        reader.close();
                    }
                    catch (IOException e)
                    {
                    }
                }
                if (stmt != null)
                    stmt.close();
            }
        }
    }

    /**
     * This should not be called for MySQL but we have a version of it anyway
     * because it's better not to have broken code. This will default to the
     * default engine type.
     */
    public void createTable(Table t, boolean replace) throws SQLException
    {
        createTable(t, replace, null);
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
        return "USE " + getDatabaseObjectName(schema);
    }

    public boolean supportsCreateDropSchema()
    {
        // MySQL allows schema creation but setting this to true creates extra
        // transactions in the binlog. So we set it to false, since any needed
        // schema will be created via the JDBC URL.
        return false;
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
     * @see com.continuent.tungsten.replicator.database.Database#supportsControlSessionLevelLogging()
     */
    public boolean supportsControlSessionLevelLogging()
    {
        // This is a privileged command.
        if (isPrivileged())
            return true;
        else
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
        if (suppressed != this.sessionLevelLoggingSuppressed)
        {
            if (suppressed)
                executeUpdate("SET SQL_LOG_BIN=0");
            else
                executeUpdate("SET SQL_LOG_BIN=1");

            this.sessionLevelLoggingSuppressed = suppressed;
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#supportsNativeSlaveSync()
     */
    @Override
    public boolean supportsNativeSlaveSync()
    {
        // This is a privileged command.
        if (isPrivileged())
            return true;
        else
            return false;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#syncNativeSlave(java.lang.String)
     */
    @Override
    public void syncNativeSlave(String eventId) throws SQLException
    {
        // Parse the event ID, which has the following format:
        // <file>:<offset>[;<session id>]
        int colonIndex = eventId.indexOf(':');
        String binlogFile = eventId.substring(0, colonIndex);

        int semicolonIndex = eventId.indexOf(";");
        int binlogOffset;
        if (semicolonIndex != -1)
            binlogOffset = Integer.valueOf(eventId.substring(colonIndex + 1,
                    semicolonIndex));
        else
            binlogOffset = Integer.valueOf(eventId.substring(colonIndex + 1));

        // Create a CHANGE MASTER TO command.
        String changeMaster = String.format(
                "CHANGE MASTER TO master_log_file = '%s', master_log_pos = %s",
                binlogFile, binlogOffset);
        executeUpdate(changeMaster);
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
     * @see com.continuent.tungsten.replicator.database.Database#supportsSessionVariables()
     */
    public boolean supportsSessionVariables()
    {
        return true;
    }

    /**
     * Sets a variable on the current session using MySQL SET command.
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#setSessionVariable(java.lang.String,
     *      java.lang.String)
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
     * @see com.continuent.tungsten.replicator.database.Database#getSessionVariable(java.lang.String)
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
        String types[] = null;
        if (baseTablesOnly)
            types = new String[]{"TABLE"};

        return md.getTables(schemaName, null, null, types);
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

    public String getNowFunction()
    {
        return "now()";
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
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#prepareOptionSetStatement(java.lang.String,
     *      java.lang.String)
     */
    @Override
    public String prepareOptionSetStatement(String optionName,
            String optionValue)
    {
        return "set @@session." + optionName + "=" + optionValue;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#createTable(com.continuent.tungsten.replicator.database.Table,
     *      boolean, java.lang.String)
     */
    @Override
    public void createTable(Table t, boolean replace, String tableType)
            throws SQLException
    {
        boolean comma = false;
        String SQL;

        if (replace)
        {
            this.dropTable(t);
        }
        String temporary = t.isTemporary() ? "TEMPORARY " : "";

        SQL = "CREATE " + temporary + "TABLE ";
        SQL += (replace ? "" : "IF NOT EXISTS ");
        SQL += t.getSchema() + "." + t.getName();
        SQL += " (";

        Iterator<Column> i = t.getAllColumns().iterator();
        while (i.hasNext())
        {
            Column c = i.next();
            SQL += (comma ? ", " : "")
                    + c.getName()
                    + " "
                    + columnToTypeString(c, tableType)
                    + (supportsNotNull(tableType) ? (c.isNotNull()
                            ? " NOT NULL"
                            : " NULL") : "");

            comma = true;
        }

        // Add primary keys if supported by this table type.
        if (supportsPrimaryKeys(tableType))
        {
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
        }
        SQL += ")";
        if (tableType != null && tableType.length() > 0)
            SQL += " ENGINE=" + tableType;

        if (supportsCharset(tableType))
            SQL += " CHARSET=utf8";
        execute(SQL);
    }

    // Returns true if the table type supports primary keys.
    protected boolean supportsPrimaryKeys(String tableType)
    {
        if (tableType == null)
            return false;
        String lowerTableType = tableType.toLowerCase();
        if ("brighthouse".equals(lowerTableType))
            return false;
        else if ("infinidb".equals(lowerTableType))
            return false;
        else
            return true;
    }

    // Returns true if the table type supports primary keys.
    protected boolean supportsCharset(String tableType)
    {
        if (tableType == null)
            return true;
        String lowerTableType = tableType.toLowerCase();
        if ("infinidb".equals(lowerTableType))
            return false;
        else
            return true;
    }

    // Returns true if the table type supports primary keys.
    protected boolean supportsNotNull(String tableType)
    {
        if (tableType == null)
            return true;
        String lowerTableType = tableType.toLowerCase();
        if ("infinidb".equals(lowerTableType))
            return false;
        else
            return true;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#getDatabaseObjectName(java.lang.String)
     */
    @Override
    public String getDatabaseObjectName(String name)
    {
        return "`" + name + "`";
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#getSqlNameMatcher()
     */
    @Override
    public SqlOperationMatcher getSqlNameMatcher() throws ReplicatorException
    {
        return new MySQLOperationMatcher();
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
            CsvWriter csv = new CsvWriter(writer);
            csv.setQuoteChar('"');
            csv.setQuoted(true);
            csv.setEscapeChar('\\');
            csv.setEscapedChars("\\");
            csv.setNullPolicy(NullPolicy.nullValue);
            csv.setNullValue("\\N");
            csv.setWriteHeaders(false);
            return csv;
        }
        else
            return csvSpec.createCsvWriter(writer);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#supportsUserManagement()
     */
    public boolean supportsUserManagement()
    {
        // This requires a privileged account.
        if (isPrivileged())
            return true;
        else
            return false;
    }

    /**
     * Creates a user that can connect from any location. If the user is a
     * superuser, grant all on *.*, otherwise just grant select.
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#createUser(com.continuent.tungsten.replicator.database.User)
     */
    @Override
    public void createUser(User user) throws SQLException
    {
        String skeleton;
        if (user.isPrivileged())
        {
            skeleton = "grant all on *.* to %s@'%%' identified by '%s' with grant option";
            String sql = String.format(skeleton, user.getLogin(),
                    user.getPassword());
            execute(sql);
        }
        else
        {
            // Grant select on all schemas.
            skeleton = "grant select on *.* to %s@'%%' identified by '%s' with grant option";
            String sql = String.format(skeleton, user.getLogin(),
                    user.getPassword());
            execute(sql);

            // Grant all on current schema. This is a hack to get around a
            // limitation of the drizzle JDBC driver, which issues a CREATE
            // DATABASE even for non-privileged accounts.
            Statement stmt = null;
            ResultSet rs = null;
            String currentSchema = null;
            try
            {
                stmt = dbConn.createStatement();
                rs = stmt.executeQuery("select database() as \"database\"");
                while (rs.next())
                {
                    currentSchema = rs.getString("database");
                }
            }
            finally
            {
                if (rs != null)
                    rs.close();
                if (stmt != null)
                    stmt.close();
            }

            skeleton = "grant all on %s.* to %s@'%%' identified  by '%s' with grant option";
            String sql2 = String.format(skeleton, currentSchema,
                    user.getLogin(), user.getPassword());
            execute(sql2);
        }
    }

    /**
     * Drops user, ignoring errors if desired by caller.
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#dropUser(com.continuent.tungsten.replicator.database.User,
     *      boolean)
     */
    @Override
    public void dropUser(User user, boolean ignore) throws SQLException
    {
        String sql = String.format("drop user %s", user.getLogin());
        try
        {
            execute(sql);
        }
        catch (SQLException e)
        {
            if (!ignore)
            {
                throw e;
            }
            else if (logger.isDebugEnabled())
            {
                logger.debug("Drop user failed: " + sql, e);
            }
        }
    }

    /**
     * Issue SHOW PROCESSLIST command to get a list of all currently available
     * sessions.
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#listSessions()
     */
    @Override
    public List<Session> listSessions() throws SQLException
    {
        String sql = "show processlist";
        Statement stmt = null;
        ResultSet rs = null;
        LinkedList<Session> sessions = new LinkedList<Session>();
        try
        {
            stmt = dbConn.createStatement();
            rs = stmt.executeQuery(sql);
            while (rs.next())
            {
                Session session = new Session();
                session.setIdentifier(rs.getString("Id"));
                session.setLogin(rs.getString("User"));
                sessions.add(session);
            }
        }
        finally
        {
            if (rs != null)
                rs.close();
            if (stmt != null)
                stmt.close();
        }

        return sessions;
    }

    /**
     * Issue a KILL command to remove a particular session.
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#kill(com.continuent.tungsten.replicator.database.Session)
     */
    @Override
    public void kill(Session session) throws SQLException, ReplicatorException
    {
        // This requires a privileged account.
        if (!isPrivileged())
        {
            throw new ReplicatorException(
                    "Attempt to issue a kill command on a non-privileged connection");
        }
        String sql = String.format("kill %s", session.getIdentifier());
        execute(sql);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#isSystemSchema(java.lang.String)
     */
    @Override
    public boolean isSystemSchema(String schemaName)
    {
        return SYSTEM_SCHEMAS.contains(schemaName);
    }

    /**
     * {@inheritDoc}
     * 
     * @see <a
     *      href="http://dev.mysql.com/doc/mysqld-version-reference/en/mysqld-version-reference-reservedwords-5-6.html">MySQL
     *      Docs</a>
     */
    @Override
    public ArrayList<String> getReservedWords()
    {
        return reservedWords;
    }

    /**
     * {@inheritDoc}
     * 
     * @throws ExtractorException
     */
    @Override
    public String getCurrentPosition(boolean flush) throws ReplicatorException
    {
        Statement st = null;
        ResultSet rs = null;
        try
        {
            st = createStatement();
            if (flush)
            {
                logger.debug("Flushing logs");
                st.executeUpdate("FLUSH LOGS");
            }
            logger.debug("Seeking head position in binlog");
            rs = st.executeQuery("SHOW MASTER STATUS");
            if (!rs.next())
                throw new ReplicatorException(
                        "Error getting master status; is the MySQL binlog enabled?");
            String binlogFile = rs.getString(1);
            long binlogOffset = rs.getLong(2);

            logger.info("Starting from current position: " + binlogFile + ":"
                    + binlogOffset);
            return binlogFile + ":" + binlogOffset;
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(
                    "Error getting master status; is the MySQL binlog enabled?",
                    e);
        }
        finally
        {
            if (rs != null)
            {
                try
                {
                    rs.close();
                }
                catch (SQLException ignore)
                {
                }
            }
            if (st != null)
            {
                try
                {
                    st.close();
                }
                catch (SQLException ignore)
                {
                }
            }
        }
    }

    public boolean hasMicrosecondsSupport()
    {
        return supportsMicroseconds;
    }
}