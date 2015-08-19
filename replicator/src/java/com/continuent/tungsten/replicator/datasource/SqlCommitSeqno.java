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
 * Contributor(s): Stephane Giron, Linas Virbalas
 */

package com.continuent.tungsten.replicator.datasource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Column;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.Key;
import com.continuent.tungsten.replicator.database.Table;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplDBMSHeaderData;

/**
 * Implements the CommitSeqno interface for SQL DBMS servers.
 */
public class SqlCommitSeqno implements CommitSeqno
{
    private static Logger              logger          = Logger.getLogger(SqlCommitSeqno.class);

    public static final String         TABLE_NAME      = "trep_commit_seqno";

    // Properties.
    private int                        channels        = -1;
    private final String               schema;
    private final SqlConnectionManager connectionManager;

    private Table                      commitSeqnoTable;
    private Column                     commitSeqnoTableTaskId;
    private Column                     commitSeqnoTableSeqno;
    private Column                     commitSeqnoTableFragno;
    private Column                     commitSeqnoTableLastFrag;
    private Column                     commitSeqnoTableSourceId;
    private Column                     commitSeqnoTableEpochNumber;
    private Column                     commitSeqnoTableEventId;
    private Column                     commitSeqnoTableAppliedLatency;
    private Column                     commitSeqnoTableExtractTimestamp;
    private Column                     commitSeqnoTableUpdateTimestamp;
    private Column                     commitSeqnoTableShardId;

    private String                     allSeqnoQuery;

    private String                     tableType;

    // Low water mark for committing to the trep_seqno_table. This prevents
    // restart points from being set backwards in time by accident.
    long                               lowSeqno        = Long.MIN_VALUE;
    boolean                            lowSeqnoWarning = false;

    /**
     * Creates a new instance.
     */
    public SqlCommitSeqno(SqlConnectionManager connectionManager,
            String schema, String tableType)
    {
        this.connectionManager = connectionManager;
        this.schema = schema;
        this.tableType = tableType;

        allSeqnoQuery = "SELECT seqno, fragno, last_frag, source_id, epoch_number, eventid, shard_id, extract_timestamp, applied_latency, update_timestamp, task_id from "
                + schema + "." + TABLE_NAME;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.CommitSeqno#setChannels(int)
     */
    public void setChannels(int channels)
    {
        this.channels = channels;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.CatalogEntity#configure()
     */
    public void configure() throws ReplicatorException, InterruptedException
    {
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.CatalogEntity#prepare()
     */
    public void prepare() throws ReplicatorException, InterruptedException
    {
        // Check channels.
        if (channels < 0)
        {
            throw new ReplicatorException(
                    "Channels are not set for commit seqno");
        }

        // Set up table definitions.
        defineTableData();
    }

    // Set up SQL structures for table.
    private void defineTableData()
    {
        // Define schema.
        commitSeqnoTable = new Table(schema, TABLE_NAME);
        commitSeqnoTableTaskId = new Column("task_id", java.sql.Types.INTEGER,
                true); // true => isNotNull
        commitSeqnoTableSeqno = new Column("seqno", java.sql.Types.BIGINT);
        commitSeqnoTableFragno = new Column("fragno", Types.SMALLINT);
        commitSeqnoTableLastFrag = new Column("last_frag", Types.CHAR, 1);
        commitSeqnoTableSourceId = new Column("source_id", Types.VARCHAR, 128);
        commitSeqnoTableEpochNumber = new Column("epoch_number", Types.BIGINT);
        commitSeqnoTableEventId = new Column("eventid", Types.VARCHAR, 128);
        commitSeqnoTableAppliedLatency = new Column("applied_latency",
                Types.INTEGER);
        commitSeqnoTableUpdateTimestamp = new Column("update_timestamp",
                Types.TIMESTAMP);
        commitSeqnoTableShardId = new Column("shard_id", Types.VARCHAR, 128);
        commitSeqnoTableExtractTimestamp = new Column("extract_timestamp",
                Types.TIMESTAMP);

        commitSeqnoTable.AddColumn(commitSeqnoTableTaskId);
        commitSeqnoTable.AddColumn(commitSeqnoTableSeqno);
        commitSeqnoTable.AddColumn(commitSeqnoTableFragno);
        commitSeqnoTable.AddColumn(commitSeqnoTableLastFrag);
        commitSeqnoTable.AddColumn(commitSeqnoTableSourceId);
        commitSeqnoTable.AddColumn(commitSeqnoTableEpochNumber);
        commitSeqnoTable.AddColumn(commitSeqnoTableEventId);
        commitSeqnoTable.AddColumn(commitSeqnoTableAppliedLatency);
        commitSeqnoTable.AddColumn(commitSeqnoTableUpdateTimestamp);
        commitSeqnoTable.AddColumn(commitSeqnoTableShardId);
        commitSeqnoTable.AddColumn(commitSeqnoTableExtractTimestamp);

        Key pkey = new Key(Key.Primary);
        pkey.AddColumn(commitSeqnoTableTaskId);
        commitSeqnoTable.AddKey(pkey);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.CatalogEntity#release()
     */
    public void release() throws ReplicatorException, InterruptedException
    {
    }

    /**
     * Create the trep_commit_seqno table, if necessary, and ensure the number
     * of channels matches the number of channels.
     * 
     * @see com.continuent.tungsten.replicator.datasource.CommitSeqno#initialize()
     */
    public void initialize() throws ReplicatorException, InterruptedException
    {
        Database database = null;
        CommitSeqnoAccessor accessor = null;
        try
        {
            database = connectionManager.getCatalogConnection();

            // Create the table if it does not exist.
            initTable(database);

            // Count rows to see if the table is empty and if so add a dummy
            // first row (taskID = 0).
            int rows = count(database);
            if (rows == 0)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Adding dummy first row to " + TABLE_NAME
                            + " table");

                // Set defaults.
                commitSeqnoTableTaskId.setValue(0);
                commitSeqnoTableSeqno.setValue(-1L);
                commitSeqnoTableFragno.setValue(-1);
                commitSeqnoTableEventId.setValue(null);
                commitSeqnoTableUpdateTimestamp.setValue(new Timestamp(System
                        .currentTimeMillis()));

                // Insert and retrieve default value. We can only get
                // the accessor once the table is created and has a row in it.
                database.insert(commitSeqnoTable);
                accessor = this.createAccessor(0, database);
                ReplDBMSHeader task0CommitSeqno = accessor.lastCommitSeqno();

                // Update to set default values correctly.
                accessor.updateLastCommitSeqno(task0CommitSeqno, 0);
            }

            // Count again and check the number of rows in the table.
            //
            // a) If the number equals the number of channels, we leave it
            // alone.
            // b) If there is just one row, we expand to the number of channels.
            //
            // Any other number is an error.
            rows = count(database);
            if (rows == channels)
            {
                logger.info("Validated that trep_commit_seqno row count matches channels: rows="
                        + rows + " channels=" + channels);
            }
            else if (rows == 1)
            {
                expandTasks();
            }
            else
            {
                String msg = String
                        .format("Rows in trep_commit_seqno are inconsistent with channel count: channels=%d rows=%d",
                                channels, rows);
                logger.error("Replication configuration error: table trep_commit_seqno does not match channels");
                logger.info("This may be due to resetting the number of channels after an unclean replicator shutdown");
                throw new ReplicatorException(msg);
            }
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(
                    "Unable to initialize commit seqno table: "
                            + e.getMessage(), e);
        }
        finally
        {
            if (accessor != null)
                accessor.close();
            connectionManager.releaseCatalogConnection(database);
        }
    }

    /**
     * Create the table if it does not exist already.
     */
    private void initTable(Database database) throws SQLException
    {
        if (database.findTable(commitSeqnoTable.getSchema(),
                commitSeqnoTable.getName()) == null)
        {
            if (logger.isDebugEnabled())
                logger.debug("Initializing " + TABLE_NAME + " table");
            database.createTable(commitSeqnoTable, false, tableType);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void initPosition(long seqno, String sourceId, long epoch,
            String eventId) throws ReplicatorException, InterruptedException
    {
        Database database = null;
        CommitSeqnoAccessor accessor = null;
        try
        {
            database = connectionManager.getCatalogConnection();

            // Create the table if it does not exist.
            initTable(database);

            // Count rows to see if the table is empty and if so add the
            // position row (taskID = 0).
            int rows = count(database);
            if (rows == 0)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Adding first row to " + TABLE_NAME + " table");

                // Set position.
                commitSeqnoTableTaskId.setValue(0);
                commitSeqnoTableSeqno.setValue(seqno);
                commitSeqnoTableFragno.setValue(-1);
                // Set last frag to true, so the pipeline would start from the
                // *next* available event, as opposed to given one.
                commitSeqnoTableLastFrag.setValue(1);
                commitSeqnoTableSourceId.setValue(sourceId);
                commitSeqnoTableEpochNumber.setValue(epoch);
                commitSeqnoTableEventId.setValue(eventId);
                commitSeqnoTableUpdateTimestamp.setValue(new Timestamp(System
                        .currentTimeMillis()));
                commitSeqnoTableExtractTimestamp.setValue(new Timestamp(System
                        .currentTimeMillis()));

                database.insert(commitSeqnoTable);
            }
            else
            {
                throw new ReplicatorException(
                        "Cannot set position unless existing position data is removed - reset first");
            }
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(
                    "Unable to initialize commit seqno table: "
                            + e.getMessage(), e);
        }
        finally
        {
            if (accessor != null)
                accessor.close();
            connectionManager.releaseCatalogConnection(database);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.CatalogEntity#clear()
     */
    public boolean clear()
    {
        Database database = null;
        try
        {
            database = connectionManager.getCatalogConnection();

            // Try to suppress logging. This keeps dropping the table from
            // accidentally replicating. (CONT-
            try
            {
                database.controlSessionLevelLogging(true);
            }
            catch (Exception e)
            {
                // This may fail due to lack of privileges or if it's not
                // supported.
                logger.debug("Unable to suppress logging when clearing catalog tables");
            }

            // Drop the table if it exists.
            if (logger.isDebugEnabled())
                logger.debug("Dropping " + TABLE_NAME + " table");
            database.dropTable(commitSeqnoTable);
            return true;
        }
        catch (SQLException e)
        {
            logger.warn("Unable to connect to DBMS to drop table: name="
                    + commitSeqnoTable.fullyQualifiedName() + " message="
                    + e.getMessage());
            return false;
        }
        catch (ReplicatorException e)
        {
            logger.warn("Unable to connect to DBMS to drop table: name="
                    + commitSeqnoTable.fullyQualifiedName() + " message="
                    + e.getMessage());
            return false;
        }
        finally
        {
            connectionManager.releaseCatalogConnection(database);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.CommitSeqno#getHeaders()
     */
    public List<ReplDBMSHeader> getHeaders() throws ReplicatorException,
            InterruptedException
    {
        ArrayList<ReplDBMSHeader> headers = new ArrayList<ReplDBMSHeader>();
        Database database = null;
        CommitSeqnoAccessor accessor = null;
        try
        {
            database = connectionManager.getWrappedConnection();
            accessor = this.createAccessor(0, database);

            Statement stmt = database.createStatement();
            ResultSet res = stmt.executeQuery(allSeqnoQuery);
            while (res.next())
            {
                ReplDBMSHeaderData header = headerFromResult(res);
                headers.add(header);
            }

            return headers;
        }
        catch (SQLException e)
        {
            throw new ReplicatorException("Unabled to get position header(s): "
                    + e.getMessage(), e);
        }
        finally
        {
            if (accessor != null)
                accessor.close();
            connectionManager.releaseConnection(database);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.CommitSeqno#minCommitSeqno()
     */
    public ReplDBMSHeader minCommitSeqno() throws ReplicatorException,
            InterruptedException
    {
        Database database = null;
        CommitSeqnoAccessor accessor = null;
        try
        {
            database = connectionManager.getWrappedConnection();
            accessor = this.createAccessor(0, database);

            ReplDBMSHeaderData minHeader = null;
            Statement stmt = database.createStatement();
            ResultSet res = stmt.executeQuery(allSeqnoQuery);
            while (res.next())
            {
                ReplDBMSHeaderData header = headerFromResult(res);
                if (minHeader == null
                        || header.getSeqno() < minHeader.getSeqno())
                    minHeader = header;
            }

            // Return whatever we found, including null value.
            return minHeader;
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(
                    "Unabled to determine minimum commit seqno: "
                            + e.getMessage(), e);
        }
        finally
        {
            if (accessor != null)
                accessor.close();
            connectionManager.releaseConnection(database);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.CommitSeqno#maxCommitSeqno()
     */
    public ReplDBMSHeader maxCommitSeqno() throws ReplicatorException,
            InterruptedException
    {
        Database database = null;
        CommitSeqnoAccessor accessor = null;
        try
        {
            database = connectionManager.getWrappedConnection();
            accessor = this.createAccessor(0, database);

            ReplDBMSHeaderData maxHeader = null;
            Statement stmt = database.createStatement();
            ResultSet res = stmt.executeQuery(allSeqnoQuery);
            while (res.next())
            {
                ReplDBMSHeaderData header = headerFromResult(res);
                if (maxHeader == null
                        || header.getSeqno() > maxHeader.getSeqno())
                    maxHeader = header;
            }

            // Return whatever we found, including null value.
            return maxHeader;
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(
                    "Unabled to determine maximum commit seqno: "
                            + e.getMessage(), e);
        }
        finally
        {
            if (accessor != null)
                accessor.close();
            connectionManager.releaseConnection(database);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.CommitSeqno#createAccessor(int,
     *      com.continuent.tungsten.replicator.datasource.UniversalConnection)
     */
    public CommitSeqnoAccessor createAccessor(int taskId,
            UniversalConnection conn) throws ReplicatorException
    {
        SqlCommitSeqnoAccessor accessor = new SqlCommitSeqnoAccessor(
                commitSeqnoTable, connectionManager);
        accessor.setTaskId(taskId);
        accessor.setConnection((Database) conn);
        accessor.prepare();
        return accessor;
    }

    /**
     * {@inheritDoc}
     * 
     * @throws InterruptedException
     * @see com.continuent.tungsten.replicator.datasource.CommitSeqno#expandTasks()
     */
    public void expandTasks() throws ReplicatorException, InterruptedException
    {
        Database database = null;
        CommitSeqnoAccessor accessor = null;
        try
        {
            database = connectionManager.getCatalogConnection();
            accessor = this.createAccessor(0, database);

            // Fetch the task 0 position.
            ReplDBMSHeader task0CommitSeqno = accessor.lastCommitSeqno();
            if (task0CommitSeqno == null)
            {
                throw new ReplicatorException(
                        "Unable to expand tasks as task 0 row is missing from trep_commit_seqno; check for misconfiguration");
            }

            // Copy the task 0 position to create channels - 1 new rows.
            logger.info("Expanding task 0 entry in trep_commit_seqno for parallel apply: channels="
                    + channels);
            for (int taskId = 1; taskId < channels; taskId++)
            {
                // Always initialize to default value.
                commitSeqnoTableTaskId.setValue(taskId);
                commitSeqnoTableSeqno.setValue(-1L);
                commitSeqnoTableEventId.setValue(null);
                commitSeqnoTableUpdateTimestamp.setValue(new Timestamp(System
                        .currentTimeMillis()));

                // Add base row and commit to base value.
                if (logger.isDebugEnabled())
                {
                    logger.debug("Add trep_commit_seqno entry for task_id: "
                            + taskId);
                }
                database.insert(commitSeqnoTable);
                accessor.setTaskId(taskId);
                accessor.updateLastCommitSeqno(task0CommitSeqno, 0);
            }
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(
                    "Unabled to expand tasks in commit seqno table: "
                            + e.getMessage(), e);
        }
        finally
        {
            if (accessor != null)
                accessor.close();
            connectionManager.releaseCatalogConnection(database);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.CommitSeqno#reduceTasks()
     */
    public boolean reduceTasks() throws ReplicatorException
    {
        boolean reduced = false;
        boolean hasTask0 = false;
        boolean hasCommonSeqno = true;
        long commonSeqno = -1;
        PreparedStatement allSeqnosQuery = null;
        PreparedStatement deleteQuery = null;
        ResultSet rs = null;

        Database database = null;
        try
        {
            database = connectionManager.getCatalogConnection();

            // Scan task positions.
            allSeqnosQuery = database.prepareStatement(allSeqnoQuery);
            String lastEventId = null;
            int rows = 0;
            rs = allSeqnosQuery.executeQuery();
            while (rs.next())
            {
                // Increment row count.
                rows++;

                // Look for a common sequence number.
                ReplDBMSHeader header = headerFromResult(rs);
                if (commonSeqno == -1)
                    commonSeqno = header.getSeqno();
                else if (commonSeqno != header.getSeqno())
                    hasCommonSeqno = false;

                // Store the event ID. This is only used if we reduce, in which
                // case event IDs on all rows are the same.
                if (lastEventId == null)
                    lastEventId = rs.getString(6);

                // Check for task 0.
                int task_id = rs.getInt(9);
                if (task_id == 0)
                    hasTask0 = true;
            }

            // See if we can reduce the table to task 0.
            if (!hasTask0)
            {
                logger.warn("No task 0 present; cannot reduce task entries: "
                        + schema + "." + TABLE_NAME);
            }
            else if (!hasCommonSeqno)
            {
                logger.warn("Sequence numbers do not match; cannot reduce task entries: "
                        + schema + "." + TABLE_NAME);
            }
            else if (rows != channels)
            {
                logger.warn("Task entry rows do not match channels:  rows="
                        + rows + " channels=" + channels);
            }
            else
            {
                // Reduce rows.
                deleteQuery = database.prepareStatement("DELETE FROM " + schema
                        + "." + TABLE_NAME + " WHERE task_id > 0");
                int reducedRows = deleteQuery.executeUpdate();
                logger.info("Reduced " + reducedRows + " task entries: "
                        + schema + "." + TABLE_NAME);
                reduced = true;
            }
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(
                    "Unabled to reduce tasks in commit seqno table: "
                            + e.getMessage(), e);
        }
        finally
        {
            connectionManager.releaseCatalogConnection(database);
        }

        return reduced;
    }

    /**
     * Returns the count of rows in the trep_commit_seqno table.
     */
    private int count(Database conn) throws SQLException
    {
        Statement stmt = null;
        ResultSet res = null;
        int taskRows = 0;
        try
        {
            stmt = conn.createStatement();
            res = stmt.executeQuery(allSeqnoQuery);
            while (res.next())
            {
                taskRows++;
            }
        }
        finally
        {
            connectionManager.close(res);
            connectionManager.close(stmt);
        }

        return taskRows;
    }

    // Return a header from a trep_commit_seqno result.
    private ReplDBMSHeaderData headerFromResult(ResultSet rs)
            throws SQLException
    {
        long seqno = rs.getLong(1);
        short fragno = rs.getShort(2);
        boolean lastFrag = rs.getBoolean(3);
        String sourceId = rs.getString(4);
        long epochNumber = rs.getLong(5);
        String eventId = rs.getString(6);
        String shardId = rs.getString(7);
        Timestamp extractTimestamp = rs.getTimestamp(8);
        long appliedLatency = rs.getLong("applied_latency");
        Timestamp updateTimestamp = rs.getTimestamp("update_timestamp");
        long taskId = rs.getLong("task_id");
        return new ReplDBMSHeaderData(seqno, fragno, lastFrag, sourceId,
                epochNumber, eventId, shardId, extractTimestamp,
                appliedLatency, updateTimestamp, taskId);
    }
}