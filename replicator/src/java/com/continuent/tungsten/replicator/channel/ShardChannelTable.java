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
 * Contributor(s): 
 */

package com.continuent.tungsten.replicator.channel;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Column;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.Key;
import com.continuent.tungsten.replicator.database.Table;

/**
 * Encapsulates management of shard to channel assignments. Such assignments are
 * used to manage parallel apply with dynamic mapping of new shards to channels.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class ShardChannelTable
{
    private static Logger      logger       = Logger.getLogger(ShardChannelTable.class);

    public static final String TABLE_NAME   = "trep_shard_channel";

    public static final String SHARD_ID_COL = "shard_id";
    public static final String CHANNEL_COL  = "channel";

    private String             select;
    private String             selectMax;

    private String             tableType;
    private Table              channelTable;
    private Column             shardId;
    private Column             channel;

    /**
     * Create and initialize a new shard channel table.
     * 
     * @param schema
     */
    public ShardChannelTable(String schema, String tableType)
    {
        this.tableType = tableType;
        initialize(schema);
    }

    /**
     * Initialize DBMS access structures.
     */
    private void initialize(String schema)
    {
        channelTable = new Table(schema, TABLE_NAME);
        shardId = new Column(SHARD_ID_COL, Types.VARCHAR, 128, true); // true =>
                                                                      // isNotNull
        channel = new Column(CHANNEL_COL, Types.INTEGER);

        Key shardKey = new Key(Key.Primary);
        shardKey.AddColumn(shardId);

        channelTable.AddColumn(shardId);
        channelTable.AddColumn(channel);
        channelTable.AddKey(shardKey);

        select = "SELECT " + SHARD_ID_COL + ", " + CHANNEL_COL + " FROM "
                + schema + "." + TABLE_NAME + " ORDER BY " + SHARD_ID_COL;
        selectMax = "SELECT MAX(" + CHANNEL_COL + ") FROM " + schema + "."
                + TABLE_NAME;
    }

    /**
     * Set up the channel table.
     * 
     * @throws ReplicatorException Thrown if table appears to have invalid data
     */
    public void initializeShardTable(Database database, int channels)
            throws SQLException, ReplicatorException
    {
        if (logger.isDebugEnabled())
            logger.debug("Initializing channel table");

        // Create the table if it does not exist.
        if (database
                .findTable(channelTable.getSchema(), channelTable.getName()) == null)
        {
            database.createTable(this.channelTable, false, tableType);
        }

        // Validate the channel assignments.
        int maxChannel = this.listMaxChannel(database);
        if (maxChannel < channels)
        {
            if (logger.isDebugEnabled())
                logger.info("Validated channel assignments");
        }
        else
        {
            String msg = String
                    .format("Shard channel assignments are inconsistent with channel configuration: channels=%d max channel id allowed=%d max id assigned=%d",
                            channels, channels - 1, maxChannel);
            logger.error("Replication configuration error: table trep_shard_channel has value(s) that exceed available channels");
            logger.info("This may be due to resetting the number of channels after an unclean replicator shutdown");
            throw new ReplicatorException(msg);
        }
    }

    /**
     * Insert a shard/channel assignment into the database.
     */
    public int insert(Database database, String shardName, int channelNumber)
            throws SQLException
    {
        shardId.setValue(shardName);
        channel.setValue(channelNumber);
        return database.insert(channelTable);
    }

    /**
     * Drop all channel definitions, but only if there are no channel
     * assignments over the currently configured level.
     */
    public int reduceAssignments(Database conn, int channels)
            throws SQLException
    {
        // Find the max assigned channel.
        int maxChannel = this.listMaxChannel(conn);
        if (maxChannel < channels)
        {
            return conn.delete(channelTable, true);
        }
        else
        {
            logger.warn("Cannot reduce channel assignments as assignments exceed channels:  channels="
                    + channels
                    + " max assigned="
                    + maxChannel
                    + " max allowed=" + (channels - 1));
            return 0;
        }
    }

    /**
     * Return a list of currently known shard/channel assignments.
     */
    public List<Map<String, String>> list(Database conn) throws SQLException
    {
        ResultSet rs = null;
        Statement statement = conn.createStatement();
        List<Map<String, String>> shardToChannels = new ArrayList<Map<String, String>>();
        try
        {
            rs = statement.executeQuery(select);

            while (rs.next())
            {
                Map<String, String> shard = new HashMap<String, String>();

                shard.put(ShardChannelTable.SHARD_ID_COL,
                        rs.getString(ShardChannelTable.SHARD_ID_COL));
                shard.put(ShardChannelTable.CHANNEL_COL,
                        rs.getString(ShardChannelTable.CHANNEL_COL));
                shardToChannels.add(shard);
            }
        }
        finally
        {
            close(rs);
            close(statement);
        }
        return shardToChannels;
    }

    /**
     * Return the maximum assigned channel.
     */
    public int listMaxChannel(Database conn) throws SQLException
    {
        ResultSet rs = null;
        Statement statement = null;
        int maxChannel = -1;
        try
        {
            statement = conn.createStatement();
            rs = statement.executeQuery(selectMax);

            while (rs.next())
            {
                maxChannel = rs.getInt(1);
            }
        }
        finally
        {
            close(rs);
            close(statement);
        }
        return maxChannel;
    }

    // Close a result set properly.
    private void close(ResultSet rs)
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
    }

    // Close a statement properly.
    private void close(Statement s)
    {
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
}
