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
 * Initial developer(s): Stephane Giron, Robert Hodges
 * Contributor(s):  
 */

package com.continuent.tungsten.replicator.prefetch;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.applier.ApplierException;
import com.continuent.tungsten.replicator.applier.RawApplier;
import com.continuent.tungsten.replicator.database.Column;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;
import com.continuent.tungsten.replicator.database.Key;
import com.continuent.tungsten.replicator.database.MySQLOperationMatcher;
import com.continuent.tungsten.replicator.database.PreparedStatementCache;
import com.continuent.tungsten.replicator.database.SqlOperation;
import com.continuent.tungsten.replicator.database.SqlOperationMatcher;
import com.continuent.tungsten.replicator.database.Table;
import com.continuent.tungsten.replicator.database.TableMetadataCache;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.LoadDataFileDelete;
import com.continuent.tungsten.replicator.dbms.LoadDataFileFragment;
import com.continuent.tungsten.replicator.dbms.LoadDataFileQuery;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
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

/**
 * Implements a JDBC prefetcher. This class is currently for MySQL only as it
 * assumes MySQL syntax and index structure.
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class PrefetchApplier implements RawApplier
{
    private static Logger             logger                 = Logger.getLogger(PrefetchApplier.class);

    // DELETE [LOW_PRIORITY] [QUICK] [IGNORE] FROM tbl_name
    private Pattern                   delete                 = Pattern
                                                                     .compile(
                                                                             "^\\s*delete\\s*(?:low_priority\\s*)?(?:quick\\s*)?(?:ignore\\s*)?(?:from\\s*)(.*)",
                                                                             Pattern.CASE_INSENSITIVE);

    // UPDATE [LOW_PRIORITY] [IGNORE] table_reference
    private Pattern                   update                 = Pattern
                                                                     .compile(
                                                                             "^\\s*update\\s*(?:low_priority\\s*)?(?:ignore\\s*)?((?:[`\"]*(?:[a-zA-Z0-9_]+)[`\"]*\\.){0,1}[`\"]*(?:[a-zA-Z0-9_]+)[`\"]*(?:\\s*,\\s*(?:[`\"]*(?:[a-zA-Z0-9_]+)[`\"]*\\.){0,1}[`\"]*(?:[a-zA-Z0-9_]+)[`\"]*)*)\\s+SET\\s+(?:.*)?\\s+(WHERE\\s+.*)",
                                                                             Pattern.CASE_INSENSITIVE);

    // INSERT [LOW_PRIORITY | HIGH_PRIORITY] [IGNORE] [INTO] tbl_name
    // [(col_name,...)] SELECT ...[ ON DUPLICATE KEY UPDATE col_name=expr
    // [,col_name=expr] ... ]
    private Pattern                   insert                 = Pattern
                                                                     .compile(
                                                                             "^\\s*insert\\s*(?:(?:low_priority|high_priority)\\s*)?(?:ignore\\s*)?(?:into\\s*)?(?:(?:[`\\\"]*(?:[a-zA-Z0-9_]+)[`\\\"]*\\.){0,1}[`\\\"]*(?:[a-zA-Z0-9_]+)[`\\\"]*)\\s+(?:\\((?:.*)?\\)\\s*)?(?:(?:(SELECT\\s+[*A-Za-z].*?)(?:ON\\s+DUPLICATE\\s+KEY\\s+UPDATE\\s+.*))|(SELECT\\s+[*A-Za-z].*))",
                                                                             Pattern.CASE_INSENSITIVE);

    // Plugin parameters.
    protected int                     taskId                 = 0;
    protected String                  driver                 = null;
    protected String                  url                    = null;
    protected String                  user                   = "root";
    protected String                  password               = "rootpass";
    protected String                  ignoreSessionVars      = null;

    protected int                     slowQueryCacheSize     = 10000;
    protected int                     slowQueryRows          = 100;
    protected double                  slowQuerySelectivity   = .05;
    protected int                     slowQueryCacheDuration = 60;

    // Maximum number of rows to prefetch on transformed statements.
    protected int                     prefetchRowLimit       = 25000;

    // Fail the applier after this number of errors.
    protected int                     maxErrors              = 1000;

    protected PluginContext           runtime                = null;
    protected String                  metadataSchema         = null;
    protected Database                conn                   = null;
    protected Statement               statement              = null;
    protected Pattern                 ignoreSessionPattern   = null;

    // Values of schema, timestamp and session variables which are buffered to
    // avoid unnecessary commands on the SQL connection.
    protected String                  currentSchema          = null;
    protected long                    currentTimestamp       = -1;
    protected HashMap<String, String> currentOptions;

    // Statistics.
    private static long               events                 = 0;
    private static long               statements             = 0;
    private static long               rowUpdates             = 0;
    private static long               transformed            = 0;
    private static long               prefetchedQueries      = 0;
    private static long               skippedSlowQueries     = 0;
    private static long               errors                 = 0;

    /**
     * Maximum length of SQL string to log in case of an error. This is needed
     * because some statements may be very large. 
     */
    protected int                     maxSQLLogLength        = 1000;

    // Table and prepared statement caches.
    private TableMetadataCache        tableMetadataCache;
    private PreparedStatementCache    preparedStatementCache;

    // Slow query cache. This is managed by task 0 but is shared across
    // threads.
    private static SlowQueryCache     slowQueryCache;

    private ReplDBMSHeader            lastProcessedEvent     = null;

    // SQL parsing and transformation.
    private SqlOperationMatcher       sqlMatcher             = new MySQLOperationMatcher();
    private PrefetchSqlTransformer    prefetchTransformer    = new PrefetchSqlTransformer();

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

    public synchronized void setSlowQueryCacheSize(int slowQueryCacheSize)
    {
        this.slowQueryCacheSize = slowQueryCacheSize;
    }

    public synchronized void setSlowQueryRows(int slowQueryRows)
    {
        this.slowQueryRows = slowQueryRows;
    }

    public synchronized void setSlowQuerySelectivity(double slowQuerySelectivity)
    {
        this.slowQuerySelectivity = slowQuerySelectivity;
    }

    public synchronized void setSlowQueryCacheDuration(
            int slowQueryCacheDuration)
    {
        this.slowQueryCacheDuration = slowQueryCacheDuration;
    }

    public int getPrefetchRowLimit()
    {
        return prefetchRowLimit;
    }

    public void setPrefetchRowLimit(int prefetchRowLimit)
    {
        this.prefetchRowLimit = prefetchRowLimit;
    }

    public int getMaxErrors()
    {
        return maxErrors;
    }

    public void setMaxErrors(int maxErrors)
    {
        this.maxErrors = maxErrors;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#apply(com.continuent.tungsten.replicator.event.DBMSEvent,
     *      com.continuent.tungsten.replicator.event.ReplDBMSHeader, boolean,
     *      boolean)
     */
    public void apply(DBMSEvent event, ReplDBMSHeader header, boolean doCommit,
            boolean doRollback) throws ReplicatorException
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

        // Determine prefetch action according to replicated data type.
        try
        {
            if (event instanceof DBMSEmptyEvent)
            {
                // No prefetch necessary.
                return;
            }
            else if (header instanceof ReplDBMSFilteredEvent)
            {
                // No prefetch necessary.
                return;
            }
            else
            {
                ArrayList<DBMSData> data = event.getData();
                for (DBMSData dataElem : data)
                {
                    if (dataElem instanceof RowChangeData)
                    {
                        // Fetch data for changed rows.
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
                        // Parse the statement, then prefetch.
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

                        // Prefetch for this statement.
                        prefetchStatementData(sdata);

                        // Check for data that should be invalidated due to
                        // DDL statement.
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
            // Update error count.
            errors++;

            // Check for maxErrors and fail.
            if (maxErrors < 0 || errors < maxErrors)
            {
                logger.warn("Failed to prefetch event " + header.getSeqno()
                        + "... Skipping", e);
            }
            else
            {
                logger.info("Maximum number of prefetch errors exceeded: "
                        + errors);
                throw e;
            }
        }

        // Update the last processed
        lastProcessedEvent = header;

        // Update statistics.
        events++;

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
            conn = DatabaseFactory.createDatabase(url, user, password,
                    context.isPrivilegedSlave());
            conn.connect();
            statement = conn.createStatement();

            // Instantiate local caches.
            tableMetadataCache = new TableMetadataCache(5000);
            preparedStatementCache = new PreparedStatementCache(500);

            // If we are task 0, we need to initialized the shared slow query
            // cache.
            if (this.taskId == 0)
            {
                slowQueryCache = new SlowQueryCache();
                slowQueryCache
                        .setSlowQueryCacheDuration(slowQueryCacheDuration);
                slowQueryCache.setSlowQueryCacheSize(slowQueryCacheSize);
                slowQueryCache.setSlowQueryRows(slowQueryRows);
                slowQueryCache.setSlowQuerySelectivity(slowQuerySelectivity);
                slowQueryCache.init();
            }
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
        runtime = context;
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
        // Release local resources.
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

        // If we are task 0, print prefetch stats, then free the slow
        // query cache.
        if (taskId == 0)
        {
            if (slowQueryCache != null)
            {
                StringBuffer stats = new StringBuffer("Prefetch statistics:");
                stats.append(" events=").append(events);
                stats.append(" statements=").append(statements);
                stats.append(" rowUpdates=").append(rowUpdates);
                stats.append(" transformed=").append(transformed);
                stats.append(" prefetchedQueries=").append(prefetchedQueries);
                stats.append(" skippedSlowQueries=").append(skippedSlowQueries);
                stats.append(" errors=").append(errors);
                stats.append(" slowQueryCache=[")
                        .append(slowQueryCache.toString()).append("]");
                logger.info(stats.toString());

                slowQueryCache = null;
            }
        }
    }

    /**
     * Prefetch data for statements.
     */
    private void prefetchStatementData(StatementData data)
            throws ReplicatorException
    {
        statements++;
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
            boolean fetchSecondaryIndexes = false;
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
                    transformed++;
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
                    transformed++;
                    hasTransform = true;
                    fetchSecondaryIndexes = true;
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
                    transformed++;
                    hasTransform = true;
                    fetchSecondaryIndexes = true;
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
            try
            {
                statement.executeBatch();
            }
            catch (SQLWarning e)
            {
                String msg = "Warning generated when setting context of query: original="
                        + data.toString() + " warning=" + e.getMessage();
                logger.warn(msg);
            }
            catch (SQLException e)
            {
                if (data.getErrorCode() == 0)
                {
                    String msg = "Error generated when seting context of prefetch query: original="
                            + data.toString();
                    SQLException sqlException = new SQLException(msg);
                    sqlException.initCause(e);
                    throw sqlException;
                }
            }
            finally
            {
                statement.clearBatch();
            }

            // If we have a transform, issue a call for the data.
            ResultSet rs = null;
            if (hasTransform)
            {
                // Add a limit clause
                if (prefetchRowLimit > 0)
                    sqlQuery = prefetchTransformer.addLimitToQuery(sqlQuery,
                            prefetchRowLimit);

                // Load the row.
                try
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Executing transformed query: " + sqlQuery);
                    rs = statement.executeQuery(sqlQuery);
                    prefetchedQueries++;
                    if (fetchSecondaryIndexes)
                    {
                        // Fetch secondary indexes if this value exists.
                        prefetchIndexesForStatement(data, parsing, rs);
                    }
                }
                finally
                {
                    closeResultSet(rs);
                }
            }
        }
        catch (SQLException e)
        {
            logFailedStatementSQL(data.getQuery(), e);
            throw new ApplierException(e);
        }
    }

    // Fetch secondary index pages for a statement.
    private void prefetchIndexesForStatement(StatementData data,
            SqlOperation parsing, ResultSet rs) throws ReplicatorException
    {
        // Establish identity of table. Only the first statement in a
        // transaction has the current schema, so we fall back on our own
        // records if it differs.
        String schemaName = parsing.getSchema();
        String tableName = parsing.getName();
        if (schemaName == null)
            schemaName = currentSchema;

        if (logger.isDebugEnabled())
        {
            logger.debug("Seeking indexes for statement: schema=" + schemaName
                    + " table=" + tableName);
        }
        Table table = this.fetchTableDefinition(schemaName, tableName);
        List<Key> keys = table.getKeys();

        int rows = 0;
        try
        {
            // Get result set metadata so we know the number of columns.
            ResultSetMetaData metadata = rs.getMetaData();
            int columns = metadata.getColumnCount();

            // Iterate over result set.
            while (rs.next())
            {
                rows++;

                // Load up row values.
                Map<String, Object> valueMap = new HashMap<String, Object>();
                for (int i = 1; i <= columns; i++)
                {
                    String name = metadata.getColumnName(i);
                    int colType = metadata.getColumnType(i);
                    if (colType == Types.DATE)
                    {
                        String safeValue = rs.getString(i);
                        valueMap.put(name, safeValue);
                    }
                    else
                    {
                        valueMap.put(name, rs.getObject(i));
                    }
                }

                // If we have values for any secondary key, see if we can
                // construct a list of column name pairs that does not include
                // nulls.
                for (Key key : keys)
                {
                    // Skip primary key.
                    if (!key.isSecondaryKey())
                        continue;

                    // Try to get the values corresponding to the key.
                    KeySelect keySelect = new KeySelect(table, key);
                    for (Column col : key.getColumns())
                    {
                        Object value = valueMap.get(col.getName());
                        keySelect.setValue(col.getName(), value);
                    }

                    // If we got entries for each key value, try to execute a
                    // SQL query for same.
                    if (!keySelect.hasNulls())
                    {
                        executeIndexQuery(keySelect);
                    }
                }
            }
        }
        catch (SQLException e)
        {
            throw new ApplierException(
                    "Failure while seeking table metadata: schema="
                            + schemaName + " table=" + tableName, e);
        }
        finally
        {
            closeResultSet(rs);
        }

        // Check for an excessively large query.
        if (rows >= prefetchRowLimit)
        {
            logger.info("Statement row count exceeded prefetch row limit: rows="
                    + rows + " statement=" + data.getQuery());
        }
    }

    /**
     * Generate and execute a query designed to load pages from a secondary
     * index.
     * 
     * @throws SQLException
     */
    private void executeIndexQuery(KeySelect keySelect)
            throws ReplicatorException
    {
        // Extract table and key definitions.
        Table table = keySelect.getTable();
        Key key = keySelect.getKey();
        if (logger.isDebugEnabled())
        {
            logger.debug("Executing prefetch query for key: " + key);
        }

        // See if we have a slow query that does not need to be repeated.
        if (!slowQueryCache.shouldExecute(keySelect))
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Skipping slow query: keySelect=" + keySelect);
            }
            skippedSlowQueries++;
            return;
        }

        // Fetch prepared statement from cache.
        String pstmtName = String.format("%s.%s.%s-statement",
                table.getSchema(), table.getName(), key.getName());
        PreparedStatement pstmt = this.preparedStatementCache
                .retrieve(pstmtName);
        if (pstmt == null)
        {
            String query = keySelect.createPrefetchSelect();
            if (logger.isDebugEnabled())
            {
                logger.debug("Generating prepared statement for index load: key="
                        + pstmtName + " query=" + query);
            }
            try
            {
                pstmt = conn.getConnection().prepareStatement(query);
            }
            catch (SQLException e)
            {
                throw new ApplierException(
                        "Unable to prepare statement: query=" + query, e);
            }
            preparedStatementCache.store(pstmtName, pstmt, query);
        }

        // Load query.
        ResultSet rs = null;
        try
        {
            // Populate values in prepared statement and execute.
            for (int i = 1; i <= keySelect.size(); i++)
            {
                pstmt.setObject(i, keySelect.getValue(i));
            }
            if (logger.isDebugEnabled())
            {
                // Show values we are about to use.
                StringBuffer sb = new StringBuffer();
                sb.append("Executing index prefetch: key=").append(key);
                sb.append(" values=[");
                List<Column> columns = keySelect.getKey().getColumns();
                for (int i = 0; i < columns.size(); i++)
                {
                    if (i > 0)
                        sb.append(",");
                    sb.append(columns.get(i).getName());
                    sb.append("=");
                    sb.append(keySelect.getValue(i + 1));
                }
                sb.append("]");
                logger.debug(sb.toString());
            }

            // Execute the query and compute the number of rows that were
            // touched.
            rs = pstmt.executeQuery();
            prefetchedQueries++;
            long rowCount = -1;
            if (rs.next())
            {
                if (key.isPrimaryKey())
                    rowCount = 1;
                else
                    rowCount = rs.getLong(1);
            }
            if (logger.isDebugEnabled())
            {
                logger.debug("Executed index prefetch: key=" + key
                        + " rowCount=" + rowCount);
            }

            // Offer result to slow query cache.
            slowQueryCache.updateCache(keySelect, rowCount);
        }
        catch (SQLException e)
        {
            throw new ApplierException("Unable to prefetch secondary index: "
                    + key.toString(), e);
        }
        finally
        {
            if (rs != null)
                closeResultSet(rs);
        }
    }

    // Prefetch data for one or one or more rows.
    private void prefetchRowChangeData(RowChangeData data,
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

    // Prefetches data for a set of changes on a single table.
    private void prefetchOneRowChangePrepared(OneRowChange oneRowChange)
            throws ReplicatorException
    {
        // Fill in column names in column specifications.
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

        // Construct table change set and handle prefetch based on the type of
        // change.
        RbrTableChangeSet tableChangeSet = new RbrTableChangeSet(oneRowChange);
        if (logger.isDebugEnabled())
        {
            StringBuffer sb = new StringBuffer(
                    "Handling table change set: schema=");
            sb.append(tableChangeSet.getSchemaName());
            sb.append(" table=").append(tableChangeSet.getTableName());
            sb.append(" isInsert=").append(tableChangeSet.isInsert());
            sb.append(" isUpdate=").append(tableChangeSet.isUpdate());
            sb.append(" isDelete=").append(tableChangeSet.isDelete());
            sb.append(" size=").append(tableChangeSet.size());
            logger.debug(sb.toString());
        }
        if (tableChangeSet.isInsert())
        {
            // INSERT: Prefetch data for the after images.
            for (RbrRowChange rowChange : tableChangeSet.getRowChanges())
            {
                prefetchSimpleRowIndexes(rowChange.getAfterImage());
            }
        }
        else if (tableChangeSet.isDelete())
        {
            // DELETE: Prefetch data for the before images.
            for (RbrRowChange rowChange : tableChangeSet.getRowChanges())
            {
                prefetchSimpleRowIndexes(rowChange.getBeforeImage());
            }
        }
        else
        {
            // UPDATE: Prefetch on the before image.
            for (RbrRowChange rowChange : tableChangeSet.getRowChanges())
            {
                prefetchSimpleRowIndexes(rowChange.getBeforeImage());
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
    private int fillColumnNames(OneRowChange data) throws ReplicatorException
    {

        Table t = fetchTableDefinition(data.getSchemaName(),
                data.getTableName());

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
     * Identify and prefetch indexes for a single row image.
     */
    private void prefetchSimpleRowIndexes(RbrRowImage image)
            throws ReplicatorException
    {
        rowUpdates++;
        if (logger.isDebugEnabled())
        {
            logger.debug("Seeking indexes for row image: schema="
                    + image.getSchemaName() + " table=" + image.getTableName());
        }
        // Fetch out fixed information about the table.
        String schemaName = image.getSchemaName();
        String tableName = image.getTableName();
        Table table = this.fetchTableDefinition(schemaName, tableName);
        logger.debug(table);
        List<Key> keys = table.getKeys();

        // If we have values for any key, see if we can construct
        // a list of column name pairs that does not include nulls.
        for (Key key : keys)
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Looking for values for key: " + key);
            }
            // Try to get the values corresponding to the key.
            KeySelect keySelect = new KeySelect(table, key);
            for (Column col : key.getColumns())
            {
                int imageColIndex = image.getColumnIndex(col.getName());
                ColumnVal colValue = image.getValue(imageColIndex);
                keySelect.setValue(col.getName(), colValue.getValue());
            }

            // If we got entries for each key value, try to execute a
            // SQL query for same.
            if (!keySelect.hasNulls())
            {
                executeIndexQuery(keySelect);
            }
        }
    }

    // Fetches a table definition from the table metadata cache.
    private Table fetchTableDefinition(String schemaName, String tableName)
            throws ReplicatorException
    {
        Table t = tableMetadataCache.retrieve(schemaName, tableName);
        if (t == null)
        {
            Statement stmt = null;
            ResultSet rs = null;
            try
            {
                // Not yet in cache. Fetch the base definition, which includes
                // the primary key.
                t = conn.findTable(schemaName, tableName, false);
                if (logger.isDebugEnabled())
                {
                    logger.debug("Adding table to prefetch table metadata: "
                            + t);
                }

                // Add primary keys. This currently only works for Drizzle
                // driver.
                Connection connection = conn.getConnection();
                Map<String, Key> keyMap = new HashMap<String, Key>();
                stmt = connection.createStatement();
                String showKeys = String.format(
                        "SHOW KEYS IN %s.%s /* tungsten prefetch */",
                        schemaName, tableName);
                rs = stmt.executeQuery(showKeys);

                while (rs.next())
                {
                    // Get the index name and dereference key, creating a new
                    // one if necessary.
                    String name = rs.getString("Key_name");
                    Key key = keyMap.get(name);
                    if (key == null)
                    {
                        key = new Key();
                        key.setName(name);
                        boolean nonUnique = rs.getBoolean("Non_unique");
                        if ("PRIMARY".equals(name))
                            key.setType(Key.Primary);
                        else if (nonUnique)
                            key.setType(Key.NonUnique);
                        else
                            key.setType(Key.Unique);
                        keyMap.put(name, key);
                    }

                    // Fetch column attributes and add to key.
                    String columnName = rs.getString("Column_name");
                    int columnIdx = rs.getInt("Seq_in_index");

                    Column column = new Column(columnName, Types.NULL);
                    column.setPosition(columnIdx);

                    key.AddColumn(column);

                    // Update cardinality if greater than current max.
                    long cardinality = rs.getLong("Cardinality");
                    if (cardinality > key.getMaxCardinality())
                        key.setMaxCardinality(cardinality);
                }

                // Add keys to the table, clearing any previous keys.
                t.clearKeys();
                for (Key key : keyMap.values())
                {
                    t.AddKey(key);
                    if (logger.isDebugEnabled())
                    {
                        logger.debug("Adding key to table: table="
                                + t.fullyQualifiedName() + " key=" + key);
                    }
                }

                // Store the resulting description.
                tableMetadataCache.store(t);
            }
            catch (SQLException e)
            {
                throw new ApplierException(
                        "Failure while seeking table metadata: schema="
                                + schemaName + " table=" + tableName, e);
            }
            finally
            {
                closeResultSet(rs);
                closeStatement(stmt);
            }
        }
        if (logger.isDebugEnabled())
        {
            logger.debug("Table metadata found: " + t.toExtendedString());
        }
        return t;

    }

    private void applyRowIdData(RowIdData data) throws ReplicatorException
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

    /**
     * applySetTimestamp adds to the batch the query used to change the server
     * timestamp, if needed and if possible (if the database support such a
     * feature)
     * 
     * @param timestamp the timestamp to be used
     * @throws SQLException if an error occurs
     */
    private void applySetTimestamp(Long timestamp) throws SQLException
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
    private void applyUseSchema(String schema) throws SQLException
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
    private boolean applySessionVariables(List<ReplOption> options)
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
    private void logFailedStatementSQL(String sql, SQLException ex)
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

    // Close a result set.
    private void closeResultSet(ResultSet rs)
    {
        try
        {
            if (rs != null)
                rs.close();
        }
        catch (SQLException e)
        {
        }
    }

    // Close a statement.
    private void closeStatement(Statement stmt)
    {
        try
        {
            if (stmt != null)
                stmt.close();
        }
        catch (SQLException e)
        {
        }
    }
}