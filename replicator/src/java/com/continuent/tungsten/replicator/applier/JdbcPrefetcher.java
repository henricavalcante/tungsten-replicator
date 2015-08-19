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
 * Contributor(s):  
 */

package com.continuent.tungsten.replicator.applier;

import java.io.UnsupportedEncodingException;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.database.Column;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;
import com.continuent.tungsten.replicator.database.MySQLOperationMatcher;
import com.continuent.tungsten.replicator.database.SqlOperation;
import com.continuent.tungsten.replicator.database.SqlOperationMatcher;
import com.continuent.tungsten.replicator.database.Table;
import com.continuent.tungsten.replicator.database.TableMetadataCache;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.LoadDataFileDelete;
import com.continuent.tungsten.replicator.dbms.LoadDataFileFragment;
import com.continuent.tungsten.replicator.dbms.LoadDataFileQuery;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.RowIdData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.DBMSEmptyEvent;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSFilteredEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplOption;
import com.continuent.tungsten.replicator.event.ReplOptionParams;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.thl.THLManagerCtrl;

/**
 * Implements a JDBC prefetcher.
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class JdbcPrefetcher implements RawApplier
{
    private static Logger             logger               = Logger.getLogger(JdbcPrefetcher.class);

    // DELETE [LOW_PRIORITY] [QUICK] [IGNORE] FROM tbl_name
    private Pattern                   delete               = Pattern
                                                                   .compile(
                                                                           "^\\s*delete\\s*(?:low_priority\\s*)?(?:quick\\s*)?(?:ignore\\s*)?(?:from\\s*)(.*)",
                                                                           Pattern.CASE_INSENSITIVE);

    // UPDATE [LOW_PRIORITY] [IGNORE] table_reference
    private Pattern                   update               = Pattern
                                                                   .compile(
                                                                           "^\\s*update\\s*(?:low_priority\\s*)?(?:ignore\\s*)?((?:[`\"]*(?:[a-zA-Z0-9_]+)[`\"]*\\.){0,1}[`\"]*(?:[a-zA-Z0-9_]+)[`\"]*(?:\\s*,\\s*(?:[`\"]*(?:[a-zA-Z0-9_]+)[`\"]*\\.){0,1}[`\"]*(?:[a-zA-Z0-9_]+)[`\"]*)*)\\s+SET\\s+(?:.*)?\\s+(WHERE\\s+.*)",
                                                                           Pattern.CASE_INSENSITIVE);

    // INSERT [LOW_PRIORITY | HIGH_PRIORITY] [IGNORE] [INTO] tbl_name
    // [(col_name,...)] SELECT ...[ ON DUPLICATE KEY UPDATE col_name=expr
    // [,col_name=expr] ... ]
    private Pattern                   insert               = Pattern
                                                                   .compile(
                                                                           "^\\s*insert\\s*(?:(?:low_priority|high_priority)\\s*)?(?:ignore\\s*)?(?:into\\s*)?(?:(?:[`\\\"]*(?:[a-zA-Z0-9_]+)[`\\\"]*\\.){0,1}[`\\\"]*(?:[a-zA-Z0-9_]+)[`\\\"]*)\\s+(?:\\((?:.*)?\\)\\s*)?(?:(?:(SELECT.*?)(?:ON\\s+DUPLICATE\\s+KEY\\s+UPDATE\\s+.*))|(SELECT.*))",
                                                                           Pattern.CASE_INSENSITIVE);

    protected int                     taskId               = 0;
    protected ReplicatorRuntime       runtime              = null;
    protected String                  driver               = null;
    protected String                  url                  = null;
    protected String                  user                 = "root";
    protected String                  password             = "rootpass";
    protected String                  ignoreSessionVars    = null;

    protected String                  metadataSchema       = null;
    protected Database                conn                 = null;
    protected Statement               statement            = null;
    protected Pattern                 ignoreSessionPattern = null;

    // Values of schema, timestamp and session variables which are buffered to
    // avoid unnecessary commands on the SQL connection.
    protected String                  currentSchema        = null;
    protected long                    currentTimestamp     = -1;
    protected HashMap<String, String> currentOptions;

    // Statistics.
    protected long                    eventCount           = 0;

    /**
     * Maximum length of SQL string to log in case of an error. This is needed
     * because some statements may be very large. 
     */
    protected int                     maxSQLLogLength      = 1000;

    private TableMetadataCache        tableMetadataCache;

    private ReplDBMSHeader            lastProcessedEvent   = null;

    // SQL parser.
    private SqlOperationMatcher       sqlMatcher           = new MySQLOperationMatcher();

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#setTaskId(int)
     */
    public void setTaskId(int id)
    {
        this.taskId = id;
        if (logger.isDebugEnabled())
            logger.debug("Set task id: id=" + taskId);
    }

    public void setDriver(String driver)
    {
        this.driver = driver;
    }

    public Database getDatabase()
    {
        return conn;
    }

    public void setUrl(String url)
    {
        this.url = url;
    }

    public void setUser(String user)
    {
        this.user = user;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    public void setIgnoreSessionVars(String ignoreSessionVars)
    {
        this.ignoreSessionVars = ignoreSessionVars;
    }

    enum PrintMode
    {
        ASSIGNMENT, NAMES_ONLY, VALUES_ONLY, PLACE_HOLDER
    }

    /**
     * @param keyValues Is used to identify NULL values, in which case, if the
     *            mode is ASSIGNMENT, "x IS ?" is constructed instead of "x =
     *            ?".
     */
    protected void printColumnSpec(StringBuffer stmt,
            ArrayList<OneRowChange.ColumnSpec> cols,
            ArrayList<OneRowChange.ColumnVal> keyValues, PrintMode mode,
            String separator)
    {
        boolean first = true;
        for (int i = 0; i < cols.size(); i++)
        {
            OneRowChange.ColumnSpec col = cols.get(i);
            if (!first)
                stmt.append(separator);
            else
                first = false;
            if (mode == PrintMode.ASSIGNMENT)
            {
                if (keyValues != null)
                {
                    if (keyValues.get(i).getValue() == null)
                    {
                        // TREP-276: use "IS NULL" vs. "= NULL"
                        // stmt.append(col.getName() + " IS ? ");
                        // Oracle cannot handle "IS ?" and then binding a NULL
                        // value. It needs
                        // an explicit "IS NULL".
                        stmt.append(conn.getDatabaseObjectName(col.getName())
                                + " IS NULL ");
                    }
                    else
                    {
                        stmt.append(conn.getDatabaseObjectName(col.getName())
                                + " = "
                                + conn.getPlaceHolder(col, keyValues.get(i)
                                        .getValue(), col.getTypeDescription()));
                    }
                }
            }
        }
    }

    /**
     * Queries database for column names of a table that OneRowChange is
     * affecting. Fills in column names and key names for the given
     * OneRowChange.
     * 
     * @param data
     * @return Number of columns that a table has. Zero, if no columns were
     *         retrieved (table does not exist or has no columns).
     * @throws SQLException
     */
    protected int fillColumnNames(OneRowChange data) throws SQLException
    {
        Table t = tableMetadataCache.retrieve(data.getSchemaName(),
                data.getTableName());
        if (t == null)
        {
            // Not yet in cache
            t = new Table(data.getSchemaName(), data.getTableName());
            DatabaseMetaData meta = conn.getDatabaseMetaData();
            ResultSet rs = null;

            try
            {
                rs = conn.getColumnsResultSet(meta, data.getSchemaName(),
                        data.getTableName());
                while (rs.next())
                {
                    String columnName = rs.getString("COLUMN_NAME");
                    int columnIdx = rs.getInt("ORDINAL_POSITION");

                    Column column = addColumn(rs, columnName);
                    column.setPosition(columnIdx);
                    t.AddColumn(column);
                }
                tableMetadataCache.store(t);
            }
            finally
            {
                if (rs != null)
                {
                    rs.close();
                }
            }
        }

        // Set column names.
        for (Column column : t.getAllColumns())
        {
            ListIterator<OneRowChange.ColumnSpec> litr = data.getColumnSpec()
                    .listIterator();
            for (; litr.hasNext();)
            {
                OneRowChange.ColumnSpec cv = litr.next();
                if (cv.getIndex() == column.getPosition())
                {
                    cv.setName(column.getName());
                    cv.setSigned(column.isSigned());
                    cv.setTypeDescription(column.getTypeDescription());

                    // Check whether column is real blob on the applier side
                    if (cv.getType() == Types.BLOB)
                        cv.setBlob(column.isBlob());

                    break;
                }
            }

            litr = data.getKeySpec().listIterator();
            for (; litr.hasNext();)
            {
                OneRowChange.ColumnSpec cv = litr.next();
                if (cv.getIndex() == column.getPosition())
                {
                    cv.setName(column.getName());
                    cv.setSigned(column.isSigned());
                    cv.setTypeDescription(column.getTypeDescription());

                    // Check whether column is real blob on the applier side
                    if (cv.getType() == Types.BLOB)
                        cv.setBlob(column.isBlob());

                    break;
                }
            }

        }
        return t.getColumnCount();
    }

    /**
     * Returns a new column definition.
     * 
     * @param rs Metadata resultset
     * @param columnName Name of the column to be added
     * @return the column definition
     * @throws SQLException if an error occurs
     */
    protected Column addColumn(ResultSet rs, String columnName)
            throws SQLException
    {
        return new Column(columnName, rs.getInt("DATA_TYPE"));
    }

    protected int bindValues(PreparedStatement prepStatement,
            ArrayList<OneRowChange.ColumnVal> values, int startBindLoc,
            ArrayList<OneRowChange.ColumnSpec> specs, boolean skipNulls)
            throws SQLException
    {
        int bindLoc = startBindLoc;
        // prepared statement variable index starts from 1

        for (int idx = 0; idx < values.size(); idx++)
        {
            OneRowChange.ColumnVal value = values.get(idx);
            if (value.getValue() == null)
            {
                if (skipNulls)
                    continue;
                if (conn.nullsBoundDifferently(specs.get(idx)))
                    continue;
            }
            setObject(prepStatement, bindLoc, value, specs.get(idx));

            bindLoc += 1;
        }
        return bindLoc;
    }

    protected void setObject(PreparedStatement prepStatement, int bindLoc,
            OneRowChange.ColumnVal value, ColumnSpec columnSpec)
            throws SQLException
    {
        // By default, type is not used. If specific operations have to be done,
        // this should happen in specific classes (e.g. OracleApplier).
        prepStatement.setObject(bindLoc, value.getValue());
    }

    protected void applyRowIdData(RowIdData data) throws ReplicatorException
    {
        String query = "SET ";

        switch (data.getType())
        {
            case RowIdData.LAST_INSERT_ID :
                query += "LAST_INSERT_ID";
                break;
            case RowIdData.INSERT_ID :
                query += "INSERT_ID";
                break;
            default :
                // Old behavior
                query += "INSERT_ID";
                break;
        }
        query += " = " + data.getRowId();

        try
        {
            try
            {
                statement.execute(query);
            }
            catch (SQLWarning e)
            {
                String msg = "While applying SQL event:\n" + data.toString()
                        + "\nWarning: " + e.getMessage();
                logger.warn(msg);
            }
            statement.clearBatch();

            if (logger.isDebugEnabled())
            {
                logger.debug("Applied event: " + query);
            }
        }
        catch (SQLException e)
        {
            logFailedStatementSQL(query, e);
            throw new ApplierException(e);
        }
    }

    protected void prefetchStatementData(StatementData data)
            throws ReplicatorException
    {
        String sqlQuery = null;
        try
        {
            // Parse query first in order to avoid changing cached session
            // variables, schema and timestamps to values that are not going to
            // be applied, if for instance the statement is skipped.
            if (data.getQuery() != null)
                sqlQuery = data.getQuery();
            else
            {
                try
                {
                    sqlQuery = new String(data.getQueryAsBytes(),
                            data.getCharset());
                }
                catch (UnsupportedEncodingException e)
                {
                    sqlQuery = new String(data.getQueryAsBytes());
                }
            }

            // Clear the statement batch to ensure there is no left-over data.
            statement.clearBatch();

            // Step through looking for DML statements to transform.
            boolean hasTransform = false;
            if (logger.isDebugEnabled())
            {
                logger.debug("Seeking prefetch transformation query: "
                        + sqlQuery);
            }
            SqlOperation parsing = (SqlOperation) data.getParsingMetadata();
            if (parsing.getOperation() == SqlOperation.INSERT)
            {
                Matcher m = insert.matcher(sqlQuery);
                if (m.matches())
                {
                    if (m.group(1) != null)
                        sqlQuery = m.group(1);
                    else
                        sqlQuery = m.group(2);

                    if (logger.isDebugEnabled())
                        logger.debug("Transformed INSERT to prefetch query: "
                                + sqlQuery);
                    hasTransform = true;
                }
                // else do nothing
                else
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Unable to match INSERT for transformation: "
                                + sqlQuery);
                }
            }
            else if (parsing.getOperation() == SqlOperation.DELETE)
            {
                Matcher m = delete.matcher(sqlQuery);
                if (m.matches())
                {
                    sqlQuery = "SELECT * FROM " + m.group(1);
                    if (logger.isDebugEnabled())
                        logger.debug("Transformed DELETE to prefetch query: "
                                + sqlQuery);
                    hasTransform = true;
                }
                else
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Unable to match DELETE for transformation: "
                                + sqlQuery);
                }
            }
            else if (parsing.getOperation() == SqlOperation.UPDATE)
            {
                Matcher m = update.matcher(sqlQuery);
                if (m.matches())
                {
                    sqlQuery = "SELECT * FROM " + m.group(1) + " " + m.group(2);
                    if (logger.isDebugEnabled())
                        logger.debug("Transformed UPDATE to prefetch query: "
                                + sqlQuery);
                    hasTransform = true;
                }
                else
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Unable to match UPDATE for transformation: "
                                + sqlQuery);
                }
            }
            else if (parsing.getOperation() == SqlOperation.SET)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Allowing SET operation to proceed: "
                            + sqlQuery);
                hasTransform = true;
            }
            // else do nothing
            else
            {
                if (logger.isDebugEnabled())
                    logger.debug("Ignoring unmatched statement: " + sqlQuery);
            }

            String schema = data.getDefaultSchema();
            Long timestamp = data.getTimestamp();
            List<ReplOption> options = data.getOptions();

            // Set the session context. This must happen even if there was no
            // transform.
            applyUseSchema(schema);
            applySetTimestamp(timestamp);
            applySessionVariables(options);

            int[] updateCount;

            // Only add the
            if (hasTransform)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Adding transformed query to batch: "
                            + sqlQuery);
                statement.addBatch(sqlQuery);
            }
            statement.setEscapeProcessing(false);
            try
            {
                updateCount = statement.executeBatch();
            }
            catch (SQLWarning e)
            {
                String msg = "Warning generated by prefetch query: transform="
                        + sqlQuery + " original=" + data.toString()
                        + " warning=" + e.getMessage();
                logger.warn(msg);
                updateCount = new int[1];
                updateCount[0] = statement.getUpdateCount();
            }
            catch (SQLException e)
            {
                if (data.getErrorCode() == 0)
                {
                    String msg = "Error generated by prefetch query: transform="
                            + sqlQuery + " original=" + data.toString();
                    SQLException sqlException = new SQLException(msg);
                    sqlException.initCause(e);
                    throw sqlException;
                }
            }
            finally
            {
                statement.clearBatch();
            }
        }
        catch (SQLException e)
        {
            logFailedStatementSQL(data.getQuery(), e);
            throw new ApplierException(e);
        }
    }

    /**
     * applySetTimestamp adds to the batch the query used to change the server
     * timestamp, if needed and if possible (if the database support such a
     * feature)
     * 
     * @param timestamp the timestamp to be used
     * @throws SQLException if an error occurs
     */
    protected void applySetTimestamp(Long timestamp) throws SQLException
    {
        if (timestamp != null && conn.supportsControlTimestamp())
        {
            if (timestamp.longValue() != currentTimestamp)
            {
                currentTimestamp = timestamp.longValue();
                statement.addBatch(conn.getControlTimestampQuery(timestamp));
            }
        }
    }

    /**
     * applySetUseSchema adds to the batch the query used to change the current
     * schema where queries should be executed, if needed and if possible (if
     * the database support such a feature)
     * 
     * @param schema the schema to be used
     * @throws SQLException if an error occurs
     */
    protected void applyUseSchema(String schema) throws SQLException
    {
        boolean schemaSet = false;
        if (schema != null && schema.length() > 0
                && !schema.equals(this.currentSchema))
        {
            currentSchema = schema;
            if (conn.supportsUseDefaultSchema())
            {
                String useQuery = conn.getUseSchemaQuery(schema);
                if (logger.isDebugEnabled())
                {
                    logger.debug("Setting default schema: " + useQuery);
                }
                statement.addBatch(useQuery);
                schemaSet = true;
            }
        }

        if (!schemaSet)
        {
            // Post debug message if we do not set the schema.
            if (logger.isDebugEnabled())
            {
                logger.debug("Schema was not set: schema=" + schema
                        + " currentSchema=" + currentSchema);
            }
        }
    }

    /**
     * applyOptionsToStatement adds to the batch queries used to change the
     * connection options, if needed and if possible (if the database support
     * such a feature)
     * 
     * @param options
     * @return true if any option changed
     * @throws SQLException
     */
    protected boolean applySessionVariables(List<ReplOption> options)
            throws SQLException
    {
        boolean sessionVarChange = false;

        if (options != null && conn.supportsSessionVariables())
        {
            if (currentOptions == null)
                currentOptions = new HashMap<String, String>();

            for (ReplOption statementDataOption : options)
            {
                // if option already exists and have the same value, skip it
                // Otherwise, we need to set it on the current connection
                String optionName = statementDataOption.getOptionName();
                String optionValue = statementDataOption.getOptionValue();

                // Ignore internal Tungsten options.
                if (optionName
                        .startsWith(ReplOptionParams.INTERNAL_OPTIONS_PREFIX))
                    continue;

                // If we are ignoring this option, just continue.
                if (ignoreSessionPattern != null)
                {
                    if (ignoreSessionPattern.matcher(optionName).matches())
                    {
                        if (logger.isDebugEnabled())
                            logger.debug("Ignoring session variable: "
                                    + optionName);
                        continue;
                    }
                }

                if (optionName.equals(StatementData.CREATE_OR_DROP_DB))
                {
                    // Clearing current used schema, so that it will force a new
                    // "use" statement to be issued for the next query
                    currentSchema = null;
                    continue;
                }

                String currentOptionValue = currentOptions.get(optionName);
                if (currentOptionValue == null
                        || !currentOptionValue.equalsIgnoreCase(optionValue))
                {
                    String optionSetStatement = conn.prepareOptionSetStatement(
                            optionName, optionValue);
                    if (optionSetStatement != null)
                    {
                        if (logger.isDebugEnabled())
                            logger.debug("Issuing " + optionSetStatement);
                        statement.addBatch(optionSetStatement);
                    }
                    currentOptions.put(optionName, optionValue);
                    sessionVarChange = true;
                }
            }
        }
        return sessionVarChange;
    }

    /**
     * Logs SQL into error log stream. Trims the message if it exceeds
     * maxSQLLogLength.<br/>
     * In addition, extracts and logs next exception of the SQLException, if
     * available. This extends logging detail that is provided by general
     * exception logging mechanism.
     * 
     * @see #maxSQLLogLength
     * @param sql the sql statement to be logged
     */
    protected void logFailedStatementSQL(String sql, SQLException ex)
    {
        try
        {
            String log = "Statement failed: " + sql;
            if (log.length() > maxSQLLogLength)
                log = log.substring(0, maxSQLLogLength);
            logger.error(log);

            // Sometimes there's more details to extract from the exception.
            if (ex != null && ex.getCause() != null
                    && ex.getCause() instanceof SQLException)
            {
                SQLException nextException = ((SQLException) ex.getCause())
                        .getNextException();
                if (nextException != null)
                {
                    logger.error(nextException.getMessage());
                }
            }
        }
        catch (Exception e)
        {
            if (logger.isDebugEnabled())
                logger.debug("logFailedStatementSQL failed to log, because: "
                        + e.getMessage());
        }
    }

    /**
     * Constructs a SQL statement template later used for prepared statement.
     * 
     * @param action INSERT/UPDATE/DELETE
     * @param schemaName Database name to work on.
     * @param tableName Table name to work on.
     * @param columns Columns to INSERT/UPDATE.
     * @param keys Columns to search on.
     * @param keyValues Column values that are being searched for. Used for
     *            identifying NULL values and constructing "x IS NULL" instead
     *            of "x = NULL". May be null, in which case "x = NULL" is always
     *            used.
     * @return Constructed SQL statement with "?" instead of real values.
     */
    private StringBuffer buildSelectQuery(String schemaName, String tableName,
            ArrayList<OneRowChange.ColumnSpec> keys,
            ArrayList<OneRowChange.ColumnVal> keyValues)
    {
        StringBuffer stmt = new StringBuffer();
        stmt.append("SELECT * FROM ");
        stmt.append(conn.getDatabaseObjectName(schemaName) + "."
                + conn.getDatabaseObjectName(tableName));
        stmt.append(" WHERE ");
        printColumnSpec(stmt, keys, keyValues, PrintMode.ASSIGNMENT, " AND ");
        return stmt;
    }

    /**
     * Compares current key values to the previous key values and determines
     * whether null values changed. Eg. {1, 3, null} vs. {5, 2, null} returns
     * false, but {1, 3, null} vs. {1, null, null} returns true.<br/>
     * Size of both arrays must be the same.
     * 
     * @param currentKeyValues Current key values.
     * @param previousKeyValues Previous key values.
     * @return true, if positions of null values in currentKeyValues changed
     *         compared to previousKeyValues.
     */
    private static boolean didNullKeysChange(
            ArrayList<OneRowChange.ColumnVal> currentKeyValues,
            ArrayList<OneRowChange.ColumnVal> previousKeyValues)
    {
        for (int i = 0; i < currentKeyValues.size(); i++)
        {
            if (previousKeyValues.get(i).getValue() == null
                    || currentKeyValues.get(i).getValue() == null)
                if (!(previousKeyValues.get(i).getValue() == null && currentKeyValues
                        .get(i).getValue() == null))
                    return true;
        }
        return false;
    }

    protected boolean needNewSQLStatement(int row,
            ArrayList<ArrayList<OneRowChange.ColumnVal>> keyValues)
    {
        if (keyValues.size() > row
                && didNullKeysChange(keyValues.get(row), keyValues.get(row - 1)))
            return true;
        return false;
    }

    protected void prefetchOneRowChangePrepared(OneRowChange oneRowChange)
            throws ReplicatorException
    {
        PreparedStatement prepStatement = null;

        if (oneRowChange.getAction() == RowChangeData.ActionType.INSERT)
            // Nothing to do for now
            return;

        else
        {
            // UPDATE or DELETE
            try
            {
                int colCount = fillColumnNames(oneRowChange);
                if (colCount <= 0)
                {
                    logger.warn("No column information found for table (perhaps table is missing?): "
                            + oneRowChange.getSchemaName()
                            + "."
                            + oneRowChange.getTableName());
                    // While prefetching, it is possible that the table we try
                    // to prefetch does not exist yet. In that case, just
                    // return.
                    return;
                }
            }
            catch (SQLException e1)
            {
                logger.error("column name information could not be retrieved");
                return;
            }

            StringBuffer stmt = null;

            ArrayList<OneRowChange.ColumnSpec> key = oneRowChange.getKeySpec();

            try
            {
                ArrayList<ArrayList<OneRowChange.ColumnVal>> keyValues = oneRowChange
                        .getKeyValues();
                ArrayList<ArrayList<OneRowChange.ColumnVal>> columnValues = oneRowChange
                        .getColumnValues();

                int row = 0;
                for (row = 0; row < columnValues.size()
                        || row < keyValues.size(); row++)
                {
                    if (row == 0 || needNewSQLStatement(row, keyValues))
                    {
                        // Build a new statement only if needed
                        ArrayList<OneRowChange.ColumnVal> keyValuesOfThisRow = null;
                        if (keyValues.size() > 0)
                            keyValuesOfThisRow = keyValues.get(row);

                        stmt = buildSelectQuery(oneRowChange.getSchemaName(),
                                oneRowChange.getTableName(), key,
                                keyValuesOfThisRow);

                        // runtime.getMonitor().incrementEvents(columnValues.size());
                        prepStatement = conn.prepareStatement(stmt.toString());
                    }

                    int bindLoc = 1; /* Start binding at index 1 */

                    /* bind key values */
                    if (keyValues.size() > 0)
                    {
                        bindLoc = bindValues(prepStatement, keyValues.get(row),
                                bindLoc, key, true);
                    }
                    ResultSet rs = null;
                    try
                    {
                        rs = prepStatement.executeQuery();
                    }
                    catch (SQLWarning e)
                    {
                        String msg = "While applying SQL event:\n"
                                + stmt.toString() + "\nWarning: "
                                + e.getMessage();
                        logger.warn(msg);
                    }
                    finally
                    {
                        if (rs != null)
                            rs.close();
                    }
                }

                if (logger.isDebugEnabled())
                {
                    logger.debug("Prefetched event " + " : " + stmt.toString());
                }
            }
            catch (SQLException e)
            {
                ApplierException applierException = new ApplierException(e);
                applierException.setExtraData(logFailedRowChangeSQL(stmt,
                        oneRowChange));
                throw applierException;
            }
            finally
            {
                if (prepStatement != null)
                {
                    try
                    {
                        prepStatement.close();
                    }
                    catch (SQLException ignore)
                    {
                    }
                }
            }
        }
    }

    /**
     * Logs prepared statement and it's arguments into error log stream. Trims
     * the message if it exceeds maxSQLLogLength.
     * 
     * @see #maxSQLLogLength
     * @param stmt SQL template for PreparedStatement
     */
    private String logFailedRowChangeSQL(StringBuffer stmt,
            OneRowChange oneRowChange)
    {
        try
        {
            ArrayList<OneRowChange.ColumnSpec> keys = oneRowChange.getKeySpec();
            ArrayList<OneRowChange.ColumnSpec> columns = oneRowChange
                    .getColumnSpec();
            ArrayList<ArrayList<OneRowChange.ColumnVal>> keyValues = oneRowChange
                    .getKeyValues();
            ArrayList<ArrayList<OneRowChange.ColumnVal>> columnValues = oneRowChange
                    .getColumnValues();
            String log = "Failing statement : " + stmt.toString()
                    + "\nArguments:";
            for (int row = 0; row < columnValues.size()
                    || row < keyValues.size(); row++)
            {
                log += "\n - ROW# = " + row;
                // Print column values.
                for (int c = 0; c < columns.size(); c++)
                {
                    if (columnValues.size() > 0)
                    {
                        OneRowChange.ColumnSpec colSpec = columns.get(c);
                        ArrayList<OneRowChange.ColumnVal> values = columnValues
                                .get(row);
                        OneRowChange.ColumnVal value = values.get(c);
                        log += "\n"
                                + THLManagerCtrl.formatColumn(colSpec, value,
                                        "COL", null, false, true, null);
                    }
                }
                // Print key values.
                for (int k = 0; k < keys.size(); k++)
                {
                    if (keyValues.size() > 0)
                    {
                        OneRowChange.ColumnSpec colSpec = keys.get(k);
                        ArrayList<OneRowChange.ColumnVal> values = keyValues
                                .get(row);
                        OneRowChange.ColumnVal value = values.get(k);
                        log += "\n"
                                + THLManagerCtrl.formatColumn(colSpec, value,
                                        "KEY", null, false, true, null);
                    }
                }
            }
            // Output the error message, truncate it if too large.
            if (log.length() > maxSQLLogLength)
                log = log.substring(0, maxSQLLogLength);

            return log;
        }
        catch (Exception e)
        {
            if (logger.isDebugEnabled())
                logger.debug("logFailedRowChangeSQL failed to log, because: "
                        + e.getMessage());
        }
        return null;
    }

    protected void prefetchRowChangeData(RowChangeData data,
            List<ReplOption> options) throws ReplicatorException
    {
        if (options != null)
        {
            try
            {
                if (applySessionVariables(options))
                {
                    // Apply session variables to the connection only if
                    // something changed
                    statement.executeBatch();
                    statement.clearBatch();
                }
            }
            catch (SQLException e)
            {
                throw new ApplierException("Failed to apply session variables",
                        e);
            }
        }

        for (OneRowChange row : data.getRowChanges())
        {
            prefetchOneRowChangePrepared(row);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#apply(com.continuent.tungsten.replicator.event.DBMSEvent,
     *      com.continuent.tungsten.replicator.event.ReplDBMSHeader, boolean,
     *      boolean)
     */
    public void apply(DBMSEvent event, ReplDBMSHeader header, boolean doCommit,
            boolean doRollback)
    {
        // Ensure we are not trying to apply a previously applied event.
        // This case can arise during restart.
        if (lastProcessedEvent != null && lastProcessedEvent.getLastFrag()
                && lastProcessedEvent.getSeqno() >= header.getSeqno()
                && !(event instanceof DBMSEmptyEvent))
        {
            logger.info("Skipping over previously applied event: seqno="
                    + header.getSeqno() + " fragno=" + header.getFragno());
            return;
        }

        if (logger.isDebugEnabled())
            logger.debug("Prefetch for event: seqno=" + header.getSeqno()
                    + " fragno=" + header.getFragno());

        try
        {
            if (event instanceof DBMSEmptyEvent)
            {
                return;
            }
            else if (header instanceof ReplDBMSFilteredEvent)
            {
                return;
            }
            else
            {
                ArrayList<DBMSData> data = event.getData();
                for (DBMSData dataElem : data)
                {
                    if (dataElem instanceof RowChangeData)
                    {
                        prefetchRowChangeData((RowChangeData) dataElem,
                                event.getOptions());
                    }
                    else if (dataElem instanceof LoadDataFileFragment)
                    {
                        // Don't do anything with prefetch
                    }
                    else if (dataElem instanceof LoadDataFileQuery)
                    {
                        // Don't do anything with prefetch
                    }
                    else if (dataElem instanceof LoadDataFileDelete)
                    {
                        // Don't do anything with prefetch
                    }
                    else if (dataElem instanceof StatementData)
                    {
                        StatementData sdata = (StatementData) dataElem;

                        // Check for table metadata cache invalidation.
                        SqlOperation sqlOperation = (SqlOperation) sdata
                                .getParsingMetadata();

                        String query = sdata.getQuery();
                        if (sqlOperation == null)
                        {
                            if (query == null)
                                query = new String(sdata.getQueryAsBytes());
                            sqlOperation = sqlMatcher.match(query);
                            sdata.setParsingMetadata(sqlOperation);
                        }

                        prefetchStatementData(sdata);

                        int invalidated = tableMetadataCache.invalidate(
                                sqlOperation, sdata.getDefaultSchema());
                        if (invalidated > 0)
                        {
                            if (logger.isDebugEnabled())
                                logger.debug("Table metadata invalidation: stmt="
                                        + query + " invalidated=" + invalidated);
                        }

                    }
                    else if (dataElem instanceof RowIdData)
                    {
                        logger.debug("RowIdData");
                        applyRowIdData((RowIdData) dataElem);
                    }
                }
            }
        }
        catch (ReplicatorException e)
        {
            logger.warn("Failed to prefetch event " + header.getSeqno()
                    + "... Skipping", e);
        }

        // Update the last processed
        lastProcessedEvent = header;

        // Update statistics.
        this.eventCount++;

        return;

    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#commit()
     */
    public void commit() throws ReplicatorException, InterruptedException
    {
        return;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#rollback()
     */
    public void rollback() throws InterruptedException
    {
        return;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#getLastEvent()
     */
    public ReplDBMSHeader getLastEvent() throws ReplicatorException
    {
        return null;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException
    {
        try
        {
            // Load driver if provided.
            if (driver != null)
            {
                try
                {
                    Class.forName(driver);
                }
                catch (Exception e)
                {
                    throw new ReplicatorException("Unable to load driver: "
                            + driver, e);
                }
            }

            // Create the database.
            conn = DatabaseFactory.createDatabase(url, user, password);
            conn.connect();
            statement = conn.createStatement();

            // Create table metadata cache.
            tableMetadataCache = new TableMetadataCache(5000);
            eventCount = 0;
        }
        catch (SQLException e)
        {
            String message = String.format("Failed using url=%s, user=%s", url,
                    user);
            throw new ReplicatorException(message, e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException
    {
        runtime = (ReplicatorRuntime) context;
        metadataSchema = context.getReplicatorSchemaName();
        if (ignoreSessionVars != null)
        {
            ignoreSessionPattern = Pattern.compile(ignoreSessionVars);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException
    {
        // if (commitSeqnoTable != null)
        // {
        // commitSeqnoTable.release();
        // commitSeqnoTable = null;
        // }

        currentOptions = null;

        statement = null;
        if (conn != null)
        {
            conn.close();
            conn = null;
        }

        if (tableMetadataCache != null)
        {
            tableMetadataCache.invalidateAll();
            tableMetadataCache = null;
        }
    }
}
