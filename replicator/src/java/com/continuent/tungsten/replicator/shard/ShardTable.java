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

package com.continuent.tungsten.replicator.shard;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.database.Column;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.Key;
import com.continuent.tungsten.replicator.database.Table;

/**
 * Provides a definition for the shard table, which is a catalog of currently
 * known shards.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ShardTable
{
    private static Logger       logger           = Logger.getLogger(ShardTable.class);

    public static final String  TABLE_NAME       = "trep_shard";

    public static final String  SHARD_ID_COL     = "shard_id";
    public static final String  SHARD_CRIT_COL   = "critical";
    public static final String  SHARD_MASTER_COL = "master";

    private static final String SELECT           = "SELECT " + SHARD_ID_COL
                                                         + ", "
                                                         + SHARD_MASTER_COL
                                                         + ", "
                                                         + SHARD_CRIT_COL
                                                         + " FROM "
                                                         + TABLE_NAME
                                                         + " ORDER BY "
                                                         + SHARD_MASTER_COL
                                                         + ", " + SHARD_ID_COL;

    private Table               shardTable;
    private Column              shardName;
    private Column              shardCritical;
    private Column              shardMaster;

    private String              tableType;

    public ShardTable(String schema, String tableType)
    {
        this.tableType = tableType;
        initialize(schema);
    }

    /**
     * Initialize DBMS access structures.
     */
    private void initialize(String schema)
    {
        shardTable = new Table(schema, TABLE_NAME);
        shardMaster = new Column(SHARD_MASTER_COL, Types.VARCHAR, 128);
        shardName = new Column(SHARD_ID_COL, Types.VARCHAR, 128, true); // true
                                                                        // =>
                                                                        // isNotNull
        shardCritical = new Column(SHARD_CRIT_COL, Types.TINYINT, 1);

        Key shardKey = new Key(Key.Primary);
        shardKey.AddColumn(shardName);

        shardTable.AddColumn(shardName);
        shardTable.AddColumn(shardMaster);
        shardTable.AddColumn(shardCritical);
        shardTable.AddKey(shardKey);
    }

    /**
     * Set up the shard table.
     */
    public void initializeShardTable(Database database) throws SQLException
    {
        // Create the table.
        if (database.findTable(shardTable.getSchema(), shardTable.getName()) == null)
        {
            if (logger.isDebugEnabled())
                logger.debug("Creating shard table");
            database.createTable(this.shardTable, false, tableType);
        }
    }

    /**
     * Insert a shard definition into the database. The shard may not already
     * exist.
     */
    public int insert(Database database, Shard shard) throws SQLException
    {
        shardName.setValue(shard.getShardId());
        shardMaster.setValue(shard.getMaster());
        shardCritical.setValue(shard.isCritical());
        return database.insert(shardTable);
    }

    /**
     * Update an existing shard definition. The shard must exist.
     */
    public int update(Database database, Shard shard) throws SQLException
    {
        ArrayList<Column> whereClause = new ArrayList<Column>();
        ArrayList<Column> values = new ArrayList<Column>();

        shardName.setValue(shard.getShardId());
        whereClause.add(shardName);

        shardCritical.setValue(shard.isCritical());
        shardMaster.setValue(shard.getMaster());
        values.add(shardMaster);
        values.add(shardCritical);

        return database.update(shardTable, whereClause, values);
    }

    /**
     * Drop all shard definitions.
     */
    public int deleleAll(Database database) throws SQLException
    {
        return database.delete(shardTable, true);
    }

    /**
     * Delete a single existing shard definition.
     */
    public int delete(Database database, String id) throws SQLException
    {
        shardName.setValue(id);

        return database.delete(shardTable, false);
    }

    /**
     * Return a list of currently known shards.
     */
    public List<Map<String, String>> list(Database conn) throws SQLException
    {
        ResultSet rs = null;
        Statement statement = conn.createStatement();
        List<Map<String, String>> shards = new ArrayList<Map<String, String>>();
        try
        {
            rs = statement.executeQuery(ShardTable.SELECT);

            while (rs.next())
            {
                Map<String, String> shard = new HashMap<String, String>();

                shard.put(ShardTable.SHARD_ID_COL,
                        rs.getString(ShardTable.SHARD_ID_COL));
                shard.put(ShardTable.SHARD_CRIT_COL, Boolean.toString(rs
                        .getBoolean(ShardTable.SHARD_CRIT_COL)));

                shard.put(ShardTable.SHARD_MASTER_COL,
                        rs.getString(ShardTable.SHARD_MASTER_COL));
                shards.add(shard);

            }
        }
        finally
        {
            statement.close();
        }
        return shards;
    }
}
