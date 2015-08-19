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
 * Contributor(s): Linas Virbalas
 */

package com.continuent.tungsten.replicator.applier.batch;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.csv.CsvException;
import com.continuent.tungsten.common.csv.CsvWriter;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.applier.RawApplier;
import com.continuent.tungsten.replicator.consistency.ConsistencyException;
import com.continuent.tungsten.replicator.consistency.ConsistencyTable;
import com.continuent.tungsten.replicator.csv.CsvDataFormat;
import com.continuent.tungsten.replicator.csv.CsvFile;
import com.continuent.tungsten.replicator.csv.CsvFileSet;
import com.continuent.tungsten.replicator.csv.CsvInfo;
import com.continuent.tungsten.replicator.csv.CsvKey;
import com.continuent.tungsten.replicator.database.Column;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.Key;
import com.continuent.tungsten.replicator.database.Table;
import com.continuent.tungsten.replicator.database.TableMetadataCache;
import com.continuent.tungsten.replicator.datasource.CommitSeqno;
import com.continuent.tungsten.replicator.datasource.CommitSeqnoAccessor;
import com.continuent.tungsten.replicator.datasource.DataSourceService;
import com.continuent.tungsten.replicator.datasource.HdfsConnection;
import com.continuent.tungsten.replicator.datasource.UniversalConnection;
import com.continuent.tungsten.replicator.datasource.UniversalDataSource;
import com.continuent.tungsten.replicator.datatypes.MySQLUnsignedNumeric;
import com.continuent.tungsten.replicator.datatypes.Numeric;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.LoadDataFileFragment;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.RowChangeData.ActionType;
import com.continuent.tungsten.replicator.dbms.RowIdData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplOptionParams;
import com.continuent.tungsten.replicator.filter.SchemaTableFilter;
import com.continuent.tungsten.replicator.heartbeat.HeartbeatTable;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.scripting.HdfsWrapper;
import com.continuent.tungsten.replicator.scripting.JavascriptExecutor;
import com.continuent.tungsten.replicator.scripting.ScriptExecutor;
import com.continuent.tungsten.replicator.scripting.ScriptExecutorService;
import com.continuent.tungsten.replicator.scripting.ScriptExecutorTaskStatus;
import com.continuent.tungsten.replicator.scripting.ScriptMethodRequest;
import com.continuent.tungsten.replicator.scripting.ScriptMethodResponse;
import com.continuent.tungsten.replicator.scripting.SqlWrapper;

/**
 * Implements an applier that bulk loads data into a SQL database via CSV files.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class SimpleBatchApplier implements RawApplier
{
    private static Logger               logger              = Logger.getLogger(SimpleBatchApplier.class);

    /**
     * Denotes an insert operation.
     */
    public static String                INSERT              = "I";

    /**
     * Denotes a delete operation.
     */
    public static String                DELETE              = "D";

    /**
     * Denotes an update operation. Updates are normally represented as DELETE
     * followed by INSERT unless update opcodes are enabled.
     */
    public static String                UPDATE              = "U";

    /**
     * Denotes a delete operation resulting from an update.
     */
    public static String                UPDATE_DELETE       = "UD";

    /**
     * Denotes an insert operation resulting from an update.
     */
    public static String                UPDATE_INSERT       = "UI";

    // Names of staging header columns. These are prefixed when writing data.
    public static String                OPCODE              = "opcode";
    public static String                SEQNO               = "seqno";
    public static String                ROW_ID              = "row_id";
    public static String                COMMIT_TIMESTAMP    = "commit_timestamp";
    public static String                SERVICE             = "service";

    // Task management information.
    private int                         taskId;

    // Properties.
    protected String                    dataSource;
    protected String                    stageDirectory;
    protected String                    loadScript;
    protected boolean                   cleanUpFiles        = true;
    protected String                    charset             = "UTF-8";
    protected String                    timezone            = "GMT";
    protected String                    stageSchemaPrefix;
    protected String                    stageTablePrefix;
    protected String                    stageColumnPrefix   = "tungsten_";
    protected List<String>              stageColumnNames;
    protected String                    partitionBy;
    protected String                    partitionByClass;
    protected String                    partitionByFormat;
    protected int                       parallelization     = 1;
    protected boolean                   useUpdateOpcode     = false;
    protected boolean                   distinguishUpdates  = false;

    // Replication context
    PluginContext                       context;

    // Load file directory for this task.
    private File                        stageDir;

    // Character set for writing CSV files.
    private Charset                     outputCharset;

    // Open CSV file map for current transaction.
    private Map<String, CsvFileSet>     openCsvSets         = new TreeMap<String, CsvFileSet>();

    // Formatter to use when writing objects to CSV.
    private CsvDataFormat               csvDataFormat;

    // Script executors, which are stored as an array to enable parallel load.
    private List<ScriptExecutor>        loadScriptExecutors;
    private boolean                     hasBeginMethod;
    private boolean                     hasCommitMethod;

    // Latest event.
    private ReplDBMSHeader              latestHeader;

    // First sequence number in current transaction.
    private long                        startSeqno          = -1;

    // Table metadata for base tables.
    private TableMetadataCache          fullMetadataCache;

    // Stage table Tungsten header columns with data types and
    // distribute by information. This information is a bit messy
    // because we need to record the following information.
    //
    // 1. Column definitions to generate staging table meta
    // 2. ColumnSpec definitions to write values properly in CSV.
    // 3. A map of Column instances.
    //
    // The first two are stored in order, whereas the map tracks
    // which header columns are actually enabled.
    private List<Column>                stageHeaderColumns;
    private OneRowChange                stageHeader;
    private Map<String, Integer>        stageHeaderColumnIndexMap;

    // This tracks the row ID column as well as the partition by
    // column for generating multiple CSV files per table split
    // by a data value.
    String                              rowIdColumn;
    int                                 partitionByColumn   = -1;
    ValuePartitioner                    valuePartitioner;

    // DBMS connection information.
    protected String                    metadataSchema      = null;
    protected String                    consistencyTable    = null;
    protected String                    consistencySelect   = null;

    // Catalog information.
    protected UniversalDataSource       dataSourceImpl      = null;
    protected List<UniversalConnection> connections         = null;
    protected CommitSeqnoAccessor       commitSeqnoAccessor = null;

    // Old catalog tables.
    protected HeartbeatTable            heartbeatTable      = null;

    private SchemaTableFilter           filter              = null;

    public void setDataSource(String dataSource)
    {
        this.dataSource = dataSource;
    }

    /** Set the name of the load script. */
    public void setLoadScript(String loadScript)
    {
        this.loadScript = loadScript;
    }

    /** Set the schema prefix for staging tables. */
    public void setStageSchemaPrefix(String stageSchemaPrefix)
    {
        this.stageSchemaPrefix = stageSchemaPrefix;
    }

    public void setStageTablePrefix(String stageTablePrefix)
    {
        this.stageTablePrefix = stageTablePrefix;
    }

    /** Set the prefix for staging table columns. */
    public void setStageColumnPrefix(String stageColumnPrefix)
    {
        this.stageColumnPrefix = stageColumnPrefix;
    }

    /** Specifies the names and order of staging columns added by Tungsten */
    public void setStageColumnNames(List<String> stageColumnNames)
    {
        this.stageColumnNames = stageColumnNames;
    }

    /** Set the name of the staging directory. */
    public void setStageDirectory(String stageDirectory)
    {
        this.stageDirectory = stageDirectory;
    }

    /** If true, clean up files automatically. */
    public void setCleanUpFiles(boolean cleanUpFiles)
    {
        this.cleanUpFiles = cleanUpFiles;
    }

    /** Sets the platform charset name. */
    public void setCharset(String charset)
    {
        this.charset = charset;
    }

    /** Sets the timezone. */                                                                                                                                         
    public void setTimezone(String timezone)
    {
        this.timezone = timezone;
    }

    /**
     * Selects a header column by which to partition CSV data for a single table
     * into separate files. This must be the full name including prefix, for
     * example tungsten_commit_timestamp.
     */
    public void setPartitionBy(String distributeBy)
    {
        this.partitionBy = distributeBy;
    }

    /** Set the name of the value partitioner. */
    public void setPartitionByClass(String partitionByClass)
    {
        this.partitionByClass = partitionByClass;
    }

    /**
     * Specifies a format string for the partitioning class used to split CSV
     * file data.
     */
    public void setPartitionByFormat(String partitionByFormat)
    {
        this.partitionByFormat = partitionByFormat;
    }

    /**
     * Specifies the number of loads to run in parallel when applying.
     */
    public void setParallelization(int parallelization)
    {
        this.parallelization = parallelization;
    }

    /**
     * If true use 'U' opcode for update operations. Otherwise updates are split
     * into delete followed by insert.
     */
    public void setUseUpdateOpcode(boolean useUpdateOpcode)
    {
        this.useUpdateOpcode = useUpdateOpcode;
    }

    /**
     * If true, use 'UI' and 'UD' opcodes for update operations, as opposed to
     * plain 'I' and 'D'.
     */
    public void setDistinguishUpdates(boolean distinguishUpdates)
    {
        this.distinguishUpdates = distinguishUpdates;
    }

    /**
     * Applies row updates using a batch loading scheme. Statements are
     * discarded. {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.applier.RawApplier#apply(com.continuent.tungsten.replicator.event.DBMSEvent,
     *      com.continuent.tungsten.replicator.event.ReplDBMSHeader, boolean,
     *      boolean)
     */
    @Override
    public void apply(DBMSEvent event, ReplDBMSHeader header, boolean doCommit,
            boolean doRollback) throws ReplicatorException,
            ConsistencyException, InterruptedException
    {
        long seqno = header.getSeqno();
        Timestamp commitTimestamp = header.getExtractedTstamp();
        String service = event.getMetadataOptionValue(ReplOptionParams.SERVICE);
        ArrayList<DBMSData> dbmsDataValues = event.getData();

        // Update the starting sequence number in the range.
        if (startSeqno < 0)
            startSeqno = seqno;

        // Apply heartbeat directly, skipping batch loading.
        String hbName = event
                .getMetadataOptionValue(ReplOptionParams.HEARTBEAT);
        if (hbName != null)
        {
            try
            {
                commit();
            }
            catch (ReplicatorException e)
            {
                throw e;
            }
            catch (Exception e)
            {
                throw new ReplicatorException("Error updating heartbeat table",
                        e);
            }
            return;
        }

        // Process consistency checks. These are currently not supported.
        String consistencyWhere = event
                .getMetadataOptionValue(ReplOptionParams.CONSISTENCY_WHERE);
        if (consistencyWhere != null)
        {
            logger.warn("Consistency checks are not supported: where clause="
                    + consistencyWhere);
            return;
        }

        // Iterate through values inferring the database name.
        for (DBMSData dbmsData : dbmsDataValues)
        {
            if (dbmsData instanceof StatementData)
            {
                if (logger.isDebugEnabled())
                {
                    StatementData stmtData = (StatementData) dbmsData;
                    logger.debug("Ignoring statement: " + stmtData.getQuery());
                }
            }
            else if (dbmsData instanceof RowChangeData)
            {
                RowChangeData rd = (RowChangeData) dbmsData;
                for (OneRowChange orc : rd.getRowChanges())
                {
                    // Get the action as well as the schema & table name.
                    ActionType action = orc.getAction();
                    String schema = orc.getSchemaName();
                    String table = orc.getTableName();
                    if (logger.isDebugEnabled())
                    {
                        logger.debug("Processing row update: action=" + action
                                + " schema=" + schema + " table=" + table);
                    }

                    // Process the action.
                    if (action.equals(ActionType.INSERT))
                    {
                        // Fetch column names and values.
                        List<ColumnSpec> colSpecs = orc.getColumnSpec();
                        ArrayList<ArrayList<ColumnVal>> colValues = orc
                                .getColumnValues();
                        // PK should be put in here by the PrimaryKeyFilter.
                        List<ColumnSpec> keySpecs = orc.getKeySpec();

                        // Get information the table definition.
                        Table tableMetadata = this.getTableMetadata(schema,
                                table, colSpecs, keySpecs);

                        // Insert each column into the CSV file.
                        writeValues(seqno, commitTimestamp, service,
                                tableMetadata, colSpecs, colValues, INSERT);
                    }
                    else if (action.equals(ActionType.UPDATE))
                    {
                        // Fetch column names and values.
                        List<ColumnSpec> keySpecs = orc.getKeySpec();
                        List<ColumnSpec> colSpecs = orc.getColumnSpec();
                        ArrayList<ArrayList<ColumnVal>> keyValues = orc
                                .getKeyValues();
                        ArrayList<ArrayList<ColumnVal>> colValues = orc
                                .getColumnValues();

                        // Get information the table definition.
                        Table tableMetadata = this.getTableMetadata(schema,
                                table, colSpecs, keySpecs);

                        // If we are programmed to use a U opcode, see if it
                        // is safe to do a single line update.
                        boolean updateOpcode;
                        if (useUpdateOpcode)
                        {
                            updateOpcode = true;

                            if (keySpecs.size() == colSpecs.size())
                            {
                                // Having equal-sized arrays indicates there is
                                // no primary key and updates are a full-row
                                // match. We must delete followed by insert.
                                updateOpcode = false;
                            }
                            else
                            {
                                // The key values may have changed. If not, we
                                // can use the U opcode.
                                updateOpcode = this.keysUnchanged(keySpecs,
                                        keyValues, colSpecs, colValues);
                            }
                        }
                        else
                        {
                            updateOpcode = false;
                        }

                        // Now write the update.
                        if (updateOpcode)
                        {
                            // It is safe to write a U value.
                            writeValues(seqno, commitTimestamp, service,
                                    tableMetadata, colSpecs, colValues, UPDATE);
                        }
                        else
                        {
                            // Split update into delete followed by update.
                            // Write keys for deletion and columns for insert.
                            if (distinguishUpdates)
                            {
                                writeValues(seqno, commitTimestamp, service,
                                        tableMetadata, keySpecs, keyValues,
                                        UPDATE_DELETE);
                                writeValues(seqno, commitTimestamp, service,
                                        tableMetadata, colSpecs, colValues,
                                        UPDATE_INSERT);
                            }
                            else
                            {
                                writeValues(seqno, commitTimestamp, service,
                                        tableMetadata, keySpecs, keyValues,
                                        DELETE);
                                writeValues(seqno, commitTimestamp, service,
                                        tableMetadata, colSpecs, colValues,
                                        INSERT);
                            }
                        }
                    }
                    else if (action.equals(ActionType.DELETE))
                    {
                        // Fetch column names and values.
                        List<ColumnSpec> keySpecs = orc.getKeySpec();
                        List<ColumnSpec> colSpecs = orc.getColumnSpec();
                        ArrayList<ArrayList<ColumnVal>> keyValues = orc
                                .getKeyValues();

                        // If the colspecs are empty, use keyspecs. This
                        // gets around an upstream bug in column metadata
                        // generation for tables without primary keys (Issue
                        // 916).
                        if (colSpecs.size() == 0)
                            colSpecs = keySpecs;

                        // Get information about the table definition.
                        Table tableMetadata = this.getTableMetadata(schema,
                                table, colSpecs, keySpecs);

                        // Insert each column into the CSV file.
                        writeValues(seqno, commitTimestamp, service,
                                tableMetadata, keySpecs, keyValues, DELETE);
                    }
                    else
                    {
                        logger.warn("Unrecognized action type: " + action);
                        return;
                    }
                }
            }
            else if (dbmsData instanceof LoadDataFileFragment)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Ignoring load data file fragment");
            }
            else if (dbmsData instanceof RowIdData)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Ignoring row ID data");
            }
            else
            {
                logger.warn("Unsupported DbmsData class: "
                        + dbmsData.getClass().getName());
            }
        }

        // Mark the current header and commit position if requested.
        this.latestHeader = header;
        if (doCommit)
            commit();
    }

    /**
     * Determines whether keys have changed. This is a relatively costly and
     * complex call that is only undertaken when using the U opcode. This code
     * is copied from the OptimizeUpdatesFilter class.
     *
     * @see OptimizeUpdatesFilter
     */
    private boolean keysUnchanged(List<ColumnSpec> keySpecs,
            ArrayList<ArrayList<ColumnVal>> keyValues,
            List<ColumnSpec> colSpecs, ArrayList<ArrayList<ColumnVal>> colValues)
    {
        // Iterate over the key values.
        for (int k = 0; k < keySpecs.size(); k++)
        {
            // Find the key and matching column specifications.
            ColumnSpec keySpec = keySpecs.get(k);
            int keyIndex = keySpec.getIndex() - 1;
            ColumnSpec colSpec = null;
            if (keyIndex < colSpecs.size())
            {
                colSpec = colSpecs.get(keyIndex);
            }
            else
            {
                // We can't find the column value for this key,
                // so we give up.
                return false;
            }

            // Iterate through multiple rows being updated.
            for (int row = 0; row < colValues.size() || row < keyValues.size(); row++)
            {
                ColumnVal keyValueHolder = keyValues.get(row).get(k);
                Object keyValue = keyValueHolder.getValue();

                // Is corresponding column value different from key value?
                ColumnVal colValueHolder = colValues.get(row).get(keyIndex);
                Object colValue = colValueHolder.getValue();
                if (!(keySpec.getType() == colSpec.getType()
                        && keySpec.getIndex() == colSpec.getIndex() && ((keyValue == null && colValue == null) || (keyValue != null && keyValue
                        .equals(colValue)))))
                {
                    // Value is different, so we note that and return.
                    return false;
                }
                else
                {
                    logger.debug("Col " + colSpec.getIndex() + " @ Row " + row
                            + " is static: " + keyValue.toString() + " = "
                            + colValue.toString());
                }
            }
        }

        // If we get here the key value did not change.
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.applier.RawApplier#commit()
     */
    @Override
    public void commit() throws ReplicatorException, InterruptedException
    {
        // If we don't have a last header, there is nothing to be done.
        if (latestHeader == null)
        {
            if (logger.isDebugEnabled())
                logger.debug("Unable to commit; last header is null");
            return;
        }

        // Make sure we have set the starting sequence number in the transaction
        // batch.
        if (startSeqno < 0)
            startSeqno = latestHeader.getSeqno();

        // Invoke begin method on load scripts to show transaction is starting.
        if (hasBeginMethod)
        {
            for (ScriptExecutor exec : loadScriptExecutors)
            {
                exec.execute("begin", null);
            }
        }

        // Flush open CSV files now so that data become visible in case we
        // abort. Count them along the way so we know how big the request
        // queue should be.
        int pendingCsvCount = 0;
        for (CsvFileSet fileSet : this.openCsvSets.values())
        {
            fileSet.flushAndCloseCsvFiles();
            pendingCsvCount += fileSet.size();
        }

        // Load each open CSV file into a request queue. We update the seqno of
        // this commit in CsvInfo as that helps the batch load scripts generate
        // unique file names that associate easily with the trep_commit_seqno
        // position.
        long endSeqno = latestHeader.getSeqno();
        ScriptExecutorService execService = new ScriptExecutorService(
                "batch-load", loadScriptExecutors, Math.max(1, pendingCsvCount));
        for (CsvFileSet fileSet : this.openCsvSets.values())
        {
            // Set the transaction boundaries.
            fileSet.setStartSeqno(startSeqno);
            fileSet.setEndSeqno(endSeqno);

            // Load requests to process all pending CSV files in the set.
            for (CsvInfo info : fileSet.getCsvInfoList())
            {
                ScriptMethodRequest request = new ScriptMethodRequest("apply",
                        info);
                execService.addRequest(request);
            }
        }

        // Process all requests. If there is a failure we need to search for the
        // failed task and log the error properly.
        try
        {
            boolean succeeded = execService.process();
            if (succeeded)
            {
                if (logger.isDebugEnabled())
                {
                    logger.debug("Loading tasks completed successfully");
                }
            }
            else
            {
                List<ScriptExecutorTaskStatus> statusValues = execService
                        .getTaskStatusList();
                for (ScriptExecutorTaskStatus status : statusValues)
                {
                    if (!status.isSuccessful())
                    {
                        ScriptMethodResponse response = status
                                .getFailedResponse();
                        ScriptMethodRequest request = response.getRequest();
                        Throwable rootCause = response.getThrowable();
                        CsvInfo info = (CsvInfo) request.getArgument();
                        String message = "CSV loading failed: schema="
                                + info.schema + " table=" + info.table
                                + " CSV file=" + info.file.getAbsolutePath()
                                + " message=" + rootCause.getMessage();
                        throw new ReplicatorException(message, rootCause);
                    }
                }
            }
        }
        finally
        {
            execService.shutdownNow();
        }

        // Ensure that the number of files loaded matches the number of files we
        // expected.
        int loadCount = execService.getMethodInvocationCount();
        if (loadCount != pendingCsvCount)
        {
            throw new ReplicatorException(
                    "Actual loaded file count does not match expected pending CSV file count: actual="
                            + loadCount + " expected=" + pendingCsvCount);
        }

        // Update trep_commit_seqno.
        commitSeqnoAccessor.updateLastCommitSeqno(this.latestHeader, 0);

        // Commit on data source.
        try
        {
            // Commit on connections.
            for (UniversalConnection conn : connections)
            {
                conn.commit();
                conn.setAutoCommit(false);
            }
            // Call script method if and only if previous commits succeed.
            if (hasCommitMethod)
            {
                for (ScriptExecutor exec : loadScriptExecutors)
                {
                    exec.execute("commit", null);
                }
            }
        }
        catch (Exception e)
        {
            throw new ReplicatorException("Unable to commit transaction", e);
        }

        // Clear the starting sequence number in anticipation of the next
        // transaction.
        startSeqno = -1;

        // Clear the CSV file cache.
        this.openCsvSets.clear();

        // Clear the metadata cache. Otherwise we will get errors if there is a
        // schema change between commits.
        fullMetadataCache.invalidateAll();

        // Clear the load directories if desired.
        if (cleanUpFiles)
            purgeDirIfExists(stageDir, false);
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.applier.RawApplier#getLastEvent()
     */
    @Override
    public ReplDBMSHeader getLastEvent() throws ReplicatorException,
            InterruptedException
    {
        return latestHeader;
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.applier.RawApplier#rollback()
     */
    @Override
    public void rollback() throws InterruptedException
    {
        // Roll back connection.
        for (UniversalConnection conn : connections)
        {
            try
            {
                conn.rollback();
                conn.setAutoCommit(true);
            }
            catch (Exception e)
            {
                logger.info("Unable to roll back transaction");
                if (logger.isDebugEnabled())
                    logger.debug("Transaction rollback error", e);
            }
        }

        // Clear the CSV file cache.
        openCsvSets.clear();

        // Clear the load directories if desired.
        if (cleanUpFiles)
        {
            try
            {
                purgeDirIfExists(stageDir, false);
            }
            catch (ReplicatorException e)
            {
                logger.error(
                        "Unable to purge staging directory; "
                                + stageDir.getAbsolutePath(), e);
            }
        }
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.applier.RawApplier#setTaskId(int)
     */
    @Override
    public void setTaskId(int id)
    {
        this.taskId = id;
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        // Store the context for future reference.
        this.context = context;

        // Ensure basic properties are not null.
        assertNotNull(dataSource, "dataSource");
        assertNotNull(stageDirectory, "stageDirectory");
        assertNotNull(stageTablePrefix, "stageTablePrefix");
        assertNotNull(stageColumnPrefix, "stageRowIdColumn");
        assertNotNull(loadScript, "loadScript");

        // Get metadata schema.
        metadataSchema = context.getReplicatorSchemaName();
        consistencyTable = metadataSchema + "." + ConsistencyTable.TABLE_NAME;
        consistencySelect = "SELECT * FROM " + consistencyTable + " ";

        // Set default for header columns if not already set.
        if (this.stageColumnNames == null)
        {
            stageColumnNames = new ArrayList<String>();
            stageColumnNames.add(OPCODE);
            stageColumnNames.add(SEQNO);
            stageColumnNames.add(ROW_ID);
            stageColumnNames.add(COMMIT_TIMESTAMP);
        }
    }

    // Ensure value is not null.
    public void assertNotNull(String property, String name)
            throws ReplicatorException
    {
        if (property == null)
        {
            throw new ReplicatorException(String.format(
                    "Property %s may not be null", name));
        }
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void prepare(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        // Find the data source.
        DataSourceService datasourceService = (DataSourceService) context
                .getService("datasource");
        if (datasourceService == null)
        {
            throw new ReplicatorException(
                    "Unable to locate service for data source configuration; "
                            + "check replicator configuration files: service name=datasource");
        }
        dataSourceImpl = datasourceService.find(dataSource);

        // Establish connections for each parallel thread we plan to run.
        connections = new ArrayList<UniversalConnection>(parallelization);
        for (int i = 0; i < parallelization; i++)
        {
            UniversalConnection conn = dataSourceImpl.getConnection();
            connections.add(conn);
        }

        // Look up the time zone for use with CSV, then get an appropriate
        // formatter from the data source.

        TimeZone tz = context.getReplicatorTimeZone();
        csvDataFormat = dataSourceImpl.getCsvStringFormatter(tz);

        // Look up the output character set.
        if (charset == null)
            outputCharset = Charset.defaultCharset();
        else
        {
            try
            {
                outputCharset = Charset.forName(charset);
            }
            catch (Exception e)
            {
                throw new ReplicatorException("Unable to load character set: "
                        + charset, e);
            }
        }
        if (logger.isDebugEnabled())
        {
            logger.debug("Using output character set:"
                    + outputCharset.toString());
        }

        // Set up the staging directory.
        File staging = new File(stageDirectory);
        createDirIfNotExist(staging);

        // Define and create the load sub-directory.
        stageDir = new File(staging, "staging" + taskId);
        purgeDirIfExists(stageDir, true);
        createDirIfNotExist(stageDir);

        // Initialize table metadata cache.
        fullMetadataCache = new TableMetadataCache(5000);

        // Prepare accessor(s) to data.
        CommitSeqno commitSeqno = dataSourceImpl.getCommitSeqno();
        commitSeqnoAccessor = commitSeqno.createAccessor(taskId,
                connections.get(0));

        // Fetch the last event.
        latestHeader = commitSeqnoAccessor.lastCommitSeqno();

        // Ensure we are not in auto-commit mode.
        try
        {
            for (UniversalConnection conn : connections)
            {
                conn.setAutoCommit(false);
            }
        }
        catch (Exception e)
        {
            throw new ReplicatorException(
                    "Unable to start transaction: message=" + e.getMessage(), e);
        }

        // Initialize load script.
        logger.info("Initializing load script: name=" + loadScript
                + " parallelization=" + parallelization);
        this.loadScriptExecutors = new ArrayList<ScriptExecutor>(
                parallelization);
        for (int i = 0; i < parallelization; i++)
        {
            // Create the executor.
            ScriptExecutor exec = createScriptExecutor(loadScript);

            // Create a long-running connection for loading.
            Map<String, Object> contextMap = new HashMap<String, Object>();
            UniversalConnection conn = connections.get(i);
            if (conn instanceof Database)
            {
                try
                {
                    // Create a SQL connection wrapper if we have a SQL
                    // connection and add to environment as 'sql'.
                    contextMap.put("sql", new SqlWrapper((Database) conn));
                }
                catch (SQLException e)
                {
                    throw new ReplicatorException(
                            "Unable to initialize connection for load script: script="
                                    + loadScript + " message=" + e.getMessage(),
                            e);
                }
            }
            else if (conn instanceof HdfsConnection)
            {
                // If we have an HDFS connection wrap that and put into
                // environment as 'hdfs'.
                contextMap.put("hdfs", new HdfsWrapper((HdfsConnection) conn));
            }

            filter = new SchemaTableFilter();
            contextMap.put("filter", filter);

            exec.setContextMap(contextMap);

            // Prepare the executor for use.
            exec.prepare(context);
            hasBeginMethod = exec.register("begin");
            hasCommitMethod = exec.register("commit");
            loadScriptExecutors.add(exec);
        }

        // Prepare the header columns. We also identify the row id column name
        // if it exists.
        stageHeaderColumns = new ArrayList<Column>(stageColumnNames.size());
        stageHeaderColumnIndexMap = new HashMap<String, Integer>();
        for (int i = 0; i < stageColumnNames.size(); i++)
        {
            // Fetch the current stage header name and start the header column
            // specification.
            String headerName = stageColumnNames.get(i);

            // Now check to see if we have a match on the distribute by column
            // so we can complete the specification.
            Column headerCol;
            if (OPCODE.equals(headerName))
            {
                headerCol = new Column(stageColumnPrefix + OPCODE, Types.CHAR,
                        1);
            }
            else if (SEQNO.equals(headerName))
            {
                headerCol = new Column(stageColumnPrefix + SEQNO, Types.INTEGER);
            }
            else if (ROW_ID.equals(headerName))
            {
                // We also need to mark the row ID column.
                headerCol = new Column(stageColumnPrefix + ROW_ID,
                        Types.INTEGER);
                rowIdColumn = headerCol.getName();
            }
            else if (COMMIT_TIMESTAMP.equals(headerName))
            {
                headerCol = new Column(stageColumnPrefix + COMMIT_TIMESTAMP,
                        Types.TIMESTAMP);
            }
            else if (SERVICE.equals(headerName))
            {
                headerCol = new Column(stageColumnPrefix + SERVICE,
                        Types.VARCHAR);
            }
            else
            {
                throw new ReplicatorException(
                        "Unrecognized header column name: " + headerName);
            }

            // Add the column and add a map entry so we can confirm it is
            // enabled later.
            stageHeaderColumns.add(headerCol);
            stageHeaderColumnIndexMap.put(headerName, i);
        }

        // Now that we have the column definitions, also create a set of
        // column specifications so that we can write values.
        stageHeader = new OneRowChange();
        for (int i = 0; i < stageColumnNames.size(); i++)
        {
            // Start the header column specification.
            ColumnSpec headerColSpec = stageHeader.new ColumnSpec();
            headerColSpec.setIndex(i);

            // Add metadata specific to the column.
            Column col = stageHeaderColumns.get(i);
            headerColSpec.setName(col.getName());
            headerColSpec.setType(col.getType());
            headerColSpec.setLength((int) col.getLength());

            // Add the column and check to ensure we have a distribute by
            // column.
            stageHeader.getColumnSpec().add(headerColSpec);

            if (headerColSpec.getName().equals(partitionBy))
            {
                logger.info("Data files will be partitioned by column: name="
                        + headerColSpec.getName() + " index=" + (i + 1));
                partitionByColumn = i;
            }
        }

        // If we have a distributeBy column, see if we can find a
        // formatter.
        if (partitionByColumn > -1)
        {
            if (partitionByClass == null)
            {
                throw new ReplicatorException(
                        "Property partitionBy is set but there is no distributeByClass specified to divide CSV data");
            }
            try
            {
                logger.info("Output file partitioning requested: column="
                        + partitionBy + " partitioner=" + partitionByClass
                        + " format=" + partitionByFormat);
                Class<?> distributorClass = Class
                        .forName(this.partitionByClass);
                valuePartitioner = (ValuePartitioner) distributorClass
                        .newInstance();
                valuePartitioner.setTimeZone(tz);
                valuePartitioner.setFormat(partitionByFormat);
                logger.info("Initialized value partition class: "
                        + distributorClass.getName());
            }
            catch (Exception e)
            {
                throw new ReplicatorException(
                        "Unable to instantiate partitioner for CSV values: class="
                                + partitionByClass + " message="
                                + e.getMessage(), e);
            }
        }
    }

    /**
     * Creates a script executor for a particular script.
     */
    private ScriptExecutor createScriptExecutor(String script)
            throws ReplicatorException, InterruptedException
    {
        ScriptExecutor exec;
        if (script.toLowerCase().endsWith(".js"))
            exec = new JavascriptExecutor();
        else
        {
            throw new ReplicatorException(
                    "Unrecognized batch script suffix; only .js is supported: "
                            + script);
        }

        // Set parameters and prepare.
        exec.setScript(script);
        exec.setDefaultDataSourceName(this.dataSource);
        return exec;
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void release(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        // Release load script. This calls the release method.
        if (loadScriptExecutors != null)
        {
            try
            {
                for (ScriptExecutor exec : loadScriptExecutors)
                {
                    exec.release(context);
                }
            }
            catch (Exception e)
            {
                logger.warn(
                        "Unable to release load script: name=" + loadScript, e);
            }
            loadScriptExecutors = null;
        }

        // Release staging directory if cleanup is requested.
        if (stageDir != null && cleanUpFiles)
        {
            purgeDirIfExists(stageDir, true);
            stageDir = null;
        }

        // Release table cache.
        if (fullMetadataCache != null)
        {
            fullMetadataCache.invalidateAll();
            fullMetadataCache = null;
        }

        // Release our connection. This prevents all manner of trouble.
        if (connections != null)
        {
            for (UniversalConnection conn : connections)
            {
                conn.close();
            }
            connections = null;
        }
    }

    /**
     * Create stage table definition by prefixing the base name, removing PK
     * constraint (if requested) and adding a stageRowIdColumn column.
     *
     * @param baseTable Table for which to create corresponding stage table
     * @return Stage table definition
     * @throws ReplicatorException Thrown if stage table definition cannot be
     *             created
     */
    private Table getStageTable(Table baseTable) throws ReplicatorException
    {
        // Generate schema and table names.
        String stageSchema = baseTable.getSchema();
        if (stageSchemaPrefix != null)
            stageSchema = stageSchemaPrefix + baseTable.getSchema();
        String stageName = stageTablePrefix + baseTable.getName();

        // Create table definition.
        Table stageTable = new Table(stageSchema, stageName);

        // Prepend the header columns.
        for (Column headerCol : this.stageHeaderColumns)
        {
            stageTable.AddColumn(headerCol);
        }

        // Add columns from base table.
        for (Column baseCol : baseTable.getAllColumns())
        {
            stageTable.AddColumn(baseCol);
        }

        // Return the result.
        return stageTable;
    }

    // Returns an open CSV file corresponding to a given schema and table name.
    private CsvFileSet getCsvFileSet(Table tableMetadata)
            throws ReplicatorException
    {
        // Create a key and search for the corresponding CSV set.
        String key = tableMetadata.getSchema() + "." + tableMetadata.getName();
        CsvFileSet fileSet = this.openCsvSets.get(key);
        if (fileSet == null)
        {
            // Get stage table metadata.
            Table stageTableMetadata = getStageTable(tableMetadata);

            // Generate and configure the CSV set, since it is missing.
            fileSet = new CsvFileSet(stageTableMetadata, tableMetadata,
                    startSeqno);
            fileSet.setConnection(connections.get(0));
            fileSet.setRowIdColumn(rowIdColumn);
            fileSet.setStageDir(stageDir);
            fileSet.setOutputCharset(outputCharset);
            this.openCsvSets.put(key, fileSet);
        }

        return fileSet;
    }

    // Write values into a CSV file.
    private void writeValues(long seqno, Timestamp commitTimestamp,
            String service, Table tableMetadata, List<ColumnSpec> colSpecs,
            ArrayList<ArrayList<ColumnVal>> colValues, String opcode)
            throws ReplicatorException
    {
        // Look up header field locations and put them in an array so that we
        // can write efficiently.
        int headerSize = stageHeaderColumns.size();
        Object[] headerValues = new Object[headerSize];
        Integer opcodeIndex = this.stageHeaderColumnIndexMap.get(OPCODE);
        if (opcodeIndex != null)
        {
            headerValues[opcodeIndex] = opcode;
        }
        Integer seqnoIndex = this.stageHeaderColumnIndexMap.get(SEQNO);
        if (seqnoIndex != null)
        {
            headerValues[seqnoIndex] = seqno;
        }
        Integer commitTimestampIndex = this.stageHeaderColumnIndexMap
                .get(COMMIT_TIMESTAMP);
        if (commitTimestampIndex != null)
        {
            headerValues[commitTimestampIndex] = commitTimestamp;
        }
        Integer sourceIndex = this.stageHeaderColumnIndexMap.get(SERVICE);
        if (sourceIndex != null)
        {
            headerValues[sourceIndex] = service;
        }

        // Compute the CSV key.
        CsvKey key;
        if (partitionByColumn < 0)
        {
            // No distribute by support enabled, so there is just one
            // CSV file per table.
            key = CsvKey.emptyKey();
        }
        else
        {
            // We have a distribute by column, so we need to find the value
            // and feed it to the value partitioner to generate a key.
            key = new CsvKey(
                    valuePartitioner.partition(headerValues[partitionByColumn]));
        }

        // Fetch a CSV writer.
        CsvFileSet fileSet = getCsvFileSet(tableMetadata);
        CsvFile csvFile = fileSet.getCsvFile(key);
        CsvWriter csv = csvFile.getWriter();

        try
        {
            // Iterate over updates.
            Iterator<ArrayList<ColumnVal>> colIterator = colValues.iterator();
            while (colIterator.hasNext())
            {
                // Insert header values. We don't bother with the row_id value
                // at that will be filled in automatically.
                int headerIdx = 0;
                for (int i = 0; i < headerSize; i++)
                {
                    headerIdx++;
                    Object headerValue = headerValues[i];
                    ColumnSpec headerColSpec = stageHeader.getColumnSpec().get(
                            i);
                    if (headerColSpec.getName().equals(rowIdColumn))
                        continue;
                    String value = getCsvString(headerValue, headerColSpec);
                    csv.put(headerIdx, value);
                }

                // Now add the row data. Note that we skip the 3rd column as
                // that has the row_id value and is filled in automatically.
                ArrayList<ColumnVal> row = colIterator.next();
                for (int i = 0; i < row.size(); i++)
                {
                    // Get the column and its type value.
                    ColumnVal columnVal = row.get(i);
                    ColumnSpec columnSpec = colSpecs.get(i);
                    int type = columnSpec.getType();
                    Object rawValue;
                    if (type == Types.INTEGER)
                    {
                        // Issue 798 - MySQLExtractor extracts UNSIGNED numbers
                        // in a non-
                        // platform-independent way. We need to fix data here.
                        Numeric numeric = new Numeric(columnSpec, columnVal);
                        if (columnSpec.isUnsigned() && numeric.isNegative())
                        {
                            // We assume that if column is unsigned - it's MySQL
                            // on the
                            // master side, as Oracle does not have UNSIGNED
                            // modifier.
                            rawValue = MySQLUnsignedNumeric
                                    .negativeToMeaningful(numeric).toString();
                        }
                        else
                        {
                            rawValue = columnVal.getValue();
                        }
                    }
                    else
                    {
                        rawValue = columnVal.getValue();
                    }
                    String value = getCsvString(rawValue, columnSpec);

                    int colIdx = columnSpec.getIndex();
                    csv.put(colIdx + headerIdx, value);
                }
                csv.write();
            }
        }
        catch (CsvException e)
        {
            // Enumerate table columns.
            StringBuffer colBuffer = new StringBuffer();
            for (Column col : tableMetadata.getAllColumns())
            {
                if (colBuffer.length() > 0)
                    colBuffer.append(",");
                colBuffer.append(col.getName());
            }

            // Enumerate CSV columns.
            StringBuffer csvBuffer = new StringBuffer();
            for (String name : csv.getNames())
            {
                if (csvBuffer.length() > 0)
                    csvBuffer.append(",");
                csvBuffer.append(name);
            }

            throw new ReplicatorException("Invalid write to CSV file: name="
                    + csvFile.getFile().getAbsolutePath() + " table="
                    + tableMetadata.fullyQualifiedName() + " table_columns="
                    + colBuffer.toString() + " csv_columns="
                    + csvBuffer.toString(), e);
        }
        catch (IOException e)
        {
            throw new ReplicatorException(
                    "Unable to append value to CSV file: "
                            + csvFile.getFile().getAbsolutePath(), e);
        }
    }

    // Get full table metadata. Cache for table metadata is populated
    // automatically.
    private Table getTableMetadata(String schema, String name,
            List<ColumnSpec> colSpecs, List<ColumnSpec> keySpecs)
    {
        // Look in the cache first.
        Table t = fullMetadataCache.retrieve(schema, name);

        // Create if missing and add to cache.
        if (t == null)
        {
            // Create table definition.
            t = new Table(schema, name);

            // Add column definitions.
            for (ColumnSpec colSpec : colSpecs)
            {
                Column col = new Column(colSpec.getName(), colSpec.getType());
                t.AddColumn(col);
            }

            // Store the new definition.
            fullMetadataCache.store(t);
            if (logger.isDebugEnabled())
            {
                logger.debug("Added metadata for table: schema=" + schema
                        + " table=" + name + " metadata="
                        + t.toExtendedString());
            }
        }

        // If keys are missing and we have them, add them now. This extra
        // step is necessary because insert operations do not have keys,
        // whereas update and delete do. So if we added the insert later,
        // we will need it now.
        if (t.getKeys().size() == 0 && keySpecs != null && keySpecs.size() > 0)
        {
            // Fetch the column definition matching each element of the key
            // we receive from replication and construct a key definition.
            Key key = new Key(Key.Primary);
            for (ColumnSpec keySpec : keySpecs)
            {
                for (Column col : t.getAllColumns())
                {
                    String colName = col.getName();
                    if (colName != null && colName.equals(keySpec.getName()))
                    {
                        key.AddColumn(col);
                        break;
                    }
                }
            }

            // Add the key.
            t.AddKey(key);
            if (logger.isDebugEnabled())
            {
                logger.debug("Added keys for table: schema=" + schema
                        + " table=" + name + " metadata="
                        + t.toExtendedString());
            }
        }

        // Return the table.
        return t;
    }

    /**
     * Converts a column value to a suitable String for CSV loading.
     *
     * @param value Column value
     * @param columnSpec Column metadata containing type and whether underlying
     *            data is actually binary regardless of what Java type says
     * @return String for loading
     */
    protected String getCsvString(Object value, ColumnSpec columnSpec)
            throws ReplicatorException
    {
        return csvDataFormat.csvString(value, columnSpec.getType(),
                columnSpec.isBlob());
    }

    // Create a directory if it does not exist.
    private void createDirIfNotExist(File dir) throws ReplicatorException
    {
        if (!dir.exists())
        {
            if (!dir.mkdirs())
            {
                throw new ReplicatorException(
                        "Unable to create staging directory: "
                                + dir.getAbsolutePath());
            }
        }
    }

    // Clear and optionally delete a directory if it exists.
    private void purgeDirIfExists(File dir, boolean delete)
            throws ReplicatorException
    {
        // Return if there's nothing to do.
        if (!dir.exists())
            return;

        // Remove any files.
        for (File child : dir.listFiles())
        {
            if (!child.delete())
            {
                throw new ReplicatorException("Unable to delete staging file: "
                        + child.getAbsolutePath());
            }
        }

        // Remove directory if desired.
        if (delete && !dir.delete())
        {
            if (!dir.delete())
            {
                throw new ReplicatorException(
                        "Unable to delete staging directory: "
                                + dir.getAbsolutePath());
            }
        }
    }

}