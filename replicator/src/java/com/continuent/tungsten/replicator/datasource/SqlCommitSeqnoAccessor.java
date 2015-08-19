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

package com.continuent.tungsten.replicator.datasource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.MySQLDatabase;
import com.continuent.tungsten.replicator.database.MySQLDrizzleDatabase;
import com.continuent.tungsten.replicator.database.Table;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplDBMSHeaderData;

/**
 * Implements an RDBMS accessor to update the trep_commit_seqno table from
 * within tasks. This uses prepared statements and should therefore be allocated
 * and kept for a while to amortize the cost of statement preparation.
 */
public class SqlCommitSeqnoAccessor implements CommitSeqnoAccessor
{
    private static Logger        logger          = Logger.getLogger(SqlCommitSeqnoAccessor.class);
    private Database             conn;
    private int                  taskId;
    private Table                commitSeqnoTable;
    private SqlConnectionManager connectionManager;

    // Prepared statements to improve performance.
    private PreparedStatement    commitSeqnoUpdate;
    private PreparedStatement    lastSeqnoQuery;

    // Low water mark for committing to the trep_seqno_table. This prevents
    // restart points from being set backwards in time by accident.
    long                         lowSeqno        = Long.MIN_VALUE;
    boolean                      lowSeqnoWarning = false;

    /** Create a new instance. */
    public SqlCommitSeqnoAccessor(Table commitSeqnoTable,
            SqlConnectionManager connectionManager)
    {
        this.commitSeqnoTable = commitSeqnoTable;
        this.connectionManager = connectionManager;
    }

    public void setTaskId(int taskId)
    {
        this.taskId = taskId;
    }

    public void setConnection(Database conn)
    {
        this.conn = conn;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.CommitSeqnoAccessor#prepare()
     */
    public void prepare() throws ReplicatorException
    {
        // Prepare SQL.
        try
        {
            // Hack to add correct syntax to fix MySQL timestamp values. For
            // MySQL we have to convert the GMT value of trep_commit_seqno
            // timestamp columns into the connection time zone.
            boolean isMySQL;
            if (conn instanceof MySQLDatabase
                    || conn instanceof MySQLDrizzleDatabase)
            {
                isMySQL = true;
            }
            else
            {
                isMySQL = false;
            }

            lastSeqnoQuery = conn
                    .prepareStatement("SELECT seqno, fragno, last_frag, source_id, epoch_number, eventid, shard_id, extract_timestamp, applied_latency from "
                            + commitSeqnoTable.getSchema()
                            + "."
                            + commitSeqnoTable.getName() + " WHERE task_id=?");

            commitSeqnoUpdate = conn
                    .prepareStatement("UPDATE "
                            + commitSeqnoTable.getSchema()
                            + "."
                            + commitSeqnoTable.getName()
                            + " SET seqno=?, "
                            + "fragno=?, "
                            + "last_frag=?, "
                            + "source_id=?, "
                            + "epoch_number=?, "
                            + "eventid=?, "
                            + "applied_latency=?, "
                            + (isMySQL
                                    ? "update_timestamp=convert_tz(?, '+0:00', @@time_zone), "
                                    : "update_timestamp=?, ")
                            + "shard_id=?, "
                            + (isMySQL
                                    ? "extract_timestamp=convert_tz(?, '+0:00', @@time_zone) "
                                    : "extract_timestamp=? ")
                            + "WHERE task_id=?");
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(
                    "Unable to prepare SQL to access commit seqno position: "
                            + e.getMessage(), e);
        }

        // Ensure there is a row for this task ID.
        if (lastCommitSeqno() == null)
        {
            String msg = String.format(
                    "Missing entry in trep_commit_seqno for task: taskId=%d",
                    taskId);
            logger.error(msg);
            logger.info("This may indicate a replicator misconfiguration or manual deletion of rows");
            throw new ReplicatorException(msg);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.CommitSeqnoAccessor#close()
     */
    public void close()
    {
        // Free statements but *do not* close the connection as it is from
        // client.
        if (lastSeqnoQuery != null)
            connectionManager.close(lastSeqnoQuery);
        if (commitSeqnoUpdate != null)
            connectionManager.close(commitSeqnoUpdate);
    }

    /**
     * Updates the last committed seqno for a single channel. This is a client
     * call used by appliers to mark the restart position.
     */
    public void updateLastCommitSeqno(ReplDBMSHeader header, long appliedLatency)
            throws ReplicatorException
    {
        try
        {
            // Ensure we have a low-watermark for commits to prevent committing
            // an older seqno value.
            if (lowSeqno == Long.MIN_VALUE)
            {
                ReplDBMSHeader lowHeader = lastCommitSeqno();
                if (lowHeader == null)
                    lowSeqno = -1;
                else
                    lowSeqno = lowHeader.getSeqno();
                if (logger.isDebugEnabled())
                    logger.debug("Fetching low seqno for task: " + lowSeqno);
            }

            // Only commit if the offered value is greater than or equal to
            // the low water mark.
            if (header.getSeqno() >= lowSeqno)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Updating last committed event header: "
                            + header.getSeqno());

                commitSeqnoUpdate.setLong(1, header.getSeqno());
                commitSeqnoUpdate.setShort(2, header.getFragno());
                commitSeqnoUpdate.setBoolean(3, header.getLastFrag());
                commitSeqnoUpdate.setString(4, header.getSourceId());
                commitSeqnoUpdate.setLong(5, header.getEpochNumber());
                commitSeqnoUpdate.setString(6, header.getEventId());
                // Latency can go negative due to clock differences. Round up to
                // 0.
                commitSeqnoUpdate.setLong(7, Math.max(appliedLatency, 0));
                commitSeqnoUpdate.setTimestamp(8,
                        new Timestamp(System.currentTimeMillis()));
                commitSeqnoUpdate.setString(9, header.getShardId());
                commitSeqnoUpdate.setTimestamp(10, header.getExtractedTstamp());
                commitSeqnoUpdate.setInt(11, taskId);

                commitSeqnoUpdate.executeUpdate();
            }
            else
            {
                // Since restart points are critical, we warn the first time
                // we skip a commit update.
                if (lowSeqnoWarning)
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Skipping update of last committed event header: seqno="
                                + header.getSeqno() + " lowSeqno=" + lowSeqno);
                }
                else
                {
                    logger.warn("Skipping attempted update of last committed event header to avoid resetting restart point: seqno="
                            + header.getSeqno() + " lowSeqno=" + lowSeqno);
                    lowSeqnoWarning = true;
                }
            }
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(
                    "Unable to update last commit seqno: " + e.getMessage(), e);
        }
    }

    /**
     * Fetches header data for last committed transaction for a particular
     * channel. This is a client call to get the restart position.
     */
    public ReplDBMSHeader lastCommitSeqno() throws ReplicatorException
    {
        try
        {
            ReplDBMSHeaderData header = null;
            ResultSet res = null;

            try
            {
                lastSeqnoQuery.setInt(1, taskId);
                res = lastSeqnoQuery.executeQuery();
                if (res.next())
                {
                    header = headerFromResult(res);
                }
            }
            finally
            {
                connectionManager.close(res);
            }

            // Return whatever we found, including null value.
            return header;
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(
                    "Unable to retrieve last commit seqno: " + e.getMessage(),
                    e);
        }
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
        return new ReplDBMSHeaderData(seqno, fragno, lastFrag, sourceId,
                epochNumber, eventId, shardId, extractTimestamp, appliedLatency);
    }
}