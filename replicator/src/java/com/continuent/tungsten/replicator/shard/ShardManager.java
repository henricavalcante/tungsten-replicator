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
 * Contributor(s): Linas Virbalas
 */

package com.continuent.tungsten.replicator.shard;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.jmx.DynamicMBeanHelper;
import com.continuent.tungsten.common.jmx.JmxManager;
import com.continuent.tungsten.common.jmx.MethodDesc;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class ShardManager implements ShardManagerMBean
{
    private static Logger logger = Logger.getLogger(ShardManager.class);
    private String        serviceName;
    private String        url;
    private String        user;
    private String        password;
    private String        schema;
    private String        tableType;
    private PluginContext context;

    public ShardManager(String serviceName, String url, String user,
            String password, String tungstenSchema, String tungstenTableType,
            PluginContext context)
    {
        this.serviceName = serviceName;
        this.url = url;
        this.user = user;
        this.password = password;
        this.schema = tungstenSchema;
        this.tableType = tungstenTableType;
        this.context = context;
    }

    @Override
    public boolean isAlive()
    {
        return true;
    }

    @Override
    @MethodDesc(description = "Inserts new shards for service", usage = "insert")
    public int insert(List<Map<String, String>> params) throws SQLException
    {
        assertNotUnprivilegedMaster();
        int shardsCount = 0;
        if (logger.isDebugEnabled())
        {
            logger.debug("Inserting new shards for service " + serviceName);
        }

        Database connection = null;
        ShardTable shardTable = new ShardTable(schema, tableType);

        try
        {
            connection = getConnection();
            for (Iterator<Map<String, String>> iterator = params.iterator(); iterator
                    .hasNext();)
            {
                Map<String, String> shard = iterator.next();
                try
                {
                    shardsCount += shardTable.insert(connection, new Shard(
                            shard));
                }
                catch (SQLException e)
                {
                    logger.warn("Failed to insert new shard definition ("
                            + shard + ") : " + e.getMessage());
                    if (logger.isDebugEnabled())
                        logger.debug("Full stack trace", e);
                }
            }
        }
        finally
        {
            connection.close();
        }
        return shardsCount;
    }

    @Override
    @MethodDesc(description = "Updates shards for service", usage = "update")
    public int update(List<Map<String, String>> params) throws SQLException
    {
        assertNotUnprivilegedMaster();
        if (logger.isDebugEnabled())
            logger.debug("Updating shards for service " + serviceName);

        int shardsCount = 0;
        Database connection = null;
        ShardTable shardTable = new ShardTable(schema, tableType);

        try
        {
            connection = getConnection();
            for (Iterator<Map<String, String>> iterator = params.iterator(); iterator
                    .hasNext();)
            {
                Map<String, String> shard = iterator.next();
                try
                {
                    shardsCount += shardTable.update(connection, new Shard(
                            shard));
                }
                catch (SQLException e)
                {
                    logger.warn("Failed to update shard definitions (" + shard
                            + ") : " + e.getMessage());
                    if (logger.isDebugEnabled())
                        logger.debug("Full stack trace", e);
                }
            }

        }
        finally
        {
            connection.close();
        }
        return shardsCount;
    }

    @Override
    @MethodDesc(description = "Deletes shards for service", usage = "delete")
    public int delete(List<Map<String, String>> params) throws SQLException
    {
        assertNotUnprivilegedMaster();
        if (logger.isDebugEnabled())
            logger.debug("Deleting shards for service " + serviceName);

        int shardsCount = 0;
        Database connection = null;

        ShardTable shardTable = new ShardTable(schema, tableType);
        try
        {
            connection = getConnection();
            for (Iterator<Map<String, String>> iterator = params.iterator(); iterator
                    .hasNext();)
            {
                Map<String, String> shard = iterator.next();
                String id = shard.get(ShardTable.SHARD_ID_COL);
                try
                {
                    shardsCount += shardTable.delete(connection, id);
                }
                catch (SQLException e)
                {
                    logger.warn("Failed to delete shard " + id + " : "
                            + e.getMessage());
                    if (logger.isDebugEnabled())
                        logger.debug("Full stack trace", e);
                }
            }
        }
        finally
        {
            connection.close();
        }
        return shardsCount;
    }

    @Override
    @MethodDesc(description = "Deletes all shards for service", usage = "deleteAll")
    public int deleteAll() throws SQLException, RuntimeException
    {
        assertNotUnprivilegedMaster();
        if (logger.isDebugEnabled())
            logger.debug("Deleting all shards for service " + serviceName);
        Database connection = null;
        ShardTable shardTable = new ShardTable(schema, tableType);

        try
        {
            connection = getConnection();
            return shardTable.deleleAll(connection);
        }
        finally
        {
            if (connection != null)
                connection.close();
        }
    }

    @Override
    @MethodDesc(description = "Lists defined shards for service", usage = "list")
    public List<Map<String, String>> list() throws SQLException
    {
        if (logger.isDebugEnabled())
            logger.debug("Listing defined shards for service " + serviceName);
        Database connection = null;
        ShardTable shardTable = new ShardTable(schema, tableType);

        try
        {
            connection = getConnection();
            return shardTable.list(connection);
        }
        finally
        {
            if (connection != null)
                connection.close();
        }
    }

    private Database getConnection() throws SQLException
    {
        Database db = null;
        db = DatabaseFactory.createDatabase(url, user, password, true);
        db.connect();
        if (context.isPrivilegedMaster() || context.isPrivilegedSlave())
        {
            // Suppress logging of transactions if we have the power to do so.
            if (db.supportsControlSessionLevelLogging())
            {
                db.controlSessionLevelLogging(true);
            }
        }

        return db;
    }

    /**
     * Ensures that we are not logging on a mastre.
     */
    public void assertNotUnprivilegedMaster() throws RuntimeException
    {
        if (context.isMaster() && !context.isPrivilegedMaster())
            throw new RuntimeException("Operation requires a privileged master");
    }

    public void advertiseInternal()
    {
        if (logger.isDebugEnabled())
            logger.debug("Registering ShardManagerMBean for service "
                    + serviceName);
        JmxManager.registerMBean(this, ShardManager.class, serviceName, true);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorManager#createHelper()
     */
    @MethodDesc(description = "Returns a DynamicMBeanHelper to facilitate dynamic JMX calls", usage = "createHelper")
    public DynamicMBeanHelper createHelper() throws Exception
    {
        return JmxManager.createHelper(getClass(), serviceName);
    }
}
