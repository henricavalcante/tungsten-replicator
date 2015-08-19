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

package com.continuent.tungsten.replicator.shard;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplOptionParams;
import com.continuent.tungsten.replicator.filter.Filter;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class ShardFilter implements Filter
{
    private static Logger logger = Logger.getLogger(ShardFilter.class);

    private enum Policy
    {
        /** Accept shard with unknown master */
        accept,
        /** Drop shard with unknown master */
        drop,
        /** Issue warning for unknown master and drop shard */
        warn,
        /** Throw exception for unknown master */
        error
    }

    // Plugin properties.
    private boolean    enabled                   = false;
    private boolean    autoCreate                = false;
    private boolean    enforceHome               = false;

    @SuppressWarnings("unused")
    private boolean    allowWhitelisted          = false;

    private Policy     unknownShardPolicy        = Policy.error;
    private String     unknownShardPolicyString  = null;
    private Policy     unwantedShardPolicy       = Policy.drop;
    private String     unwantedShardPolicyString = null;

    PluginContext      context;
    Map<String, Shard> shards;

    Database           conn                      = null;

    private String     user;
    private String     url;
    private String     password;
    private boolean    remote;
    private String     service;
    private String     schemaName;
    private String     tableType;

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        // Record schema name and table type.
        schemaName = context.getReplicatorSchemaName();
        tableType = ((ReplicatorRuntime) context).getTungstenTableType();

        // If policy string is set, convert to an enum.
        if (this.unknownShardPolicyString != null)
        {
            try
            {
                this.unknownShardPolicy = Policy
                        .valueOf(unknownShardPolicyString.toLowerCase());
            }
            catch (IllegalArgumentException e)
            {
                throw new ReplicatorException(
                        "Invalid value for unknownShardPolicy: "
                                + unknownShardPolicyString);
            }
        }

        // If policy string is set, convert to an enum.
        if (this.unwantedShardPolicyString != null)
        {
            try
            {
                this.unwantedShardPolicy = Policy
                        .valueOf(unwantedShardPolicyString.toLowerCase());
            }
            catch (IllegalArgumentException e)
            {
                throw new ReplicatorException(
                        "Invalid value for unwantedShardPolicy: "
                                + unwantedShardPolicyString);
            }
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
        this.context = context;

        shards = new HashMap<String, Shard>();

        // Read shard catalog
        // Load defaults for connection
        if (url == null)
            url = context.getJdbcUrl(null);
        if (user == null)
            user = context.getJdbcUser();
        if (password == null)
            password = context.getJdbcPassword();

        // Connect.
        try
        {
            conn = DatabaseFactory.createDatabase(url, user, password);
            conn.connect();
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(e);
        }

        ShardTable shardTable = new ShardTable(schemaName, tableType);

        try
        {
            List<Map<String, String>> list = shardTable.list(conn);
            for (Map<String, String> map : list)
            {
                Shard shard = new Shard(map);
                logger.warn("Adding shard " + shard.getShardId());
                shards.put(shard.getShardId(), shard);
            }
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(e);
        }

        remote = context.isRemoteService();
        service = context.getServiceName();
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
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.filter.Filter#filter(com.continuent.tungsten.replicator.event.ReplDBMSEvent)
     */
    @Override
    public ReplDBMSEvent filter(ReplDBMSEvent event)
            throws ReplicatorException, InterruptedException
    {
        // If we are not enforcing homes, we need to stop now.
        if (!enabled)
        {
            if (logger.isDebugEnabled())
                logger.debug("Shard filtering is not enabled");
            return event;
        }

        // Get the shard definition.
        String eventShard = event.getDBMSEvent().getMetadataOptionValue(
                ReplOptionParams.SHARD_ID);
        Shard shard = shards.get(eventShard);

        // If no shard definition, then we have a couple of options. We can auto
        // create a shard definition. Or we can fail.
        if (shard == null)
        {
            if (event.getDBMSEvent().getMetadataOptionValue(
                    ReplOptionParams.TUNGSTEN_METADATA) != null)
            {
                String shardService = event.getDBMSEvent()
                        .getMetadataOptionValue(ReplOptionParams.SERVICE);
                if (logger.isDebugEnabled())
                    logger.debug("Auto-creating shard definition for Tungsten metadata transaction: seqno="
                            + event.getSeqno()
                            + " shardId="
                            + eventShard
                            + " master=" + shardService);
                shard = updateShardCatalog(eventShard, shardService);
            }
            else if (autoCreate)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Auto-creating shard definition for new shard: seqno="
                            + event.getSeqno()
                            + " shardId="
                            + eventShard
                            + " master=" + service);
                shard = updateShardCatalog(eventShard, service);
            }
        }

        // If the shard is null, we refer to the policy for unknown shards.
        if (shard == null)
        {
            switch (this.unknownShardPolicy)
            {
                case accept :
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Accepting event from unknown shard: seqno="
                                + event.getSeqno() + " shard ID=" + eventShard);
                    return event;
                }
                case drop :
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Dropping event from unknown shard: seqno="
                                + event.getSeqno() + " shard ID=" + eventShard);
                    return null;
                }
                case warn :
                {
                    logger.warn("Dropping event from unknown shard: seqno="
                            + event.getSeqno() + " shard ID=" + eventShard);
                    return null;
                }
                case error :
                {
                    throw new ReplicatorException(
                            "Rejected event from unknown shard: seqno="
                                    + event.getSeqno() + " shard ID="
                                    + eventShard);
                }
                default :
                {
                    throw new ReplicatorException(
                            "No policy for unknown shard: seqno="
                                    + event.getSeqno() + " shard ID="
                                    + eventShard);
                }
            }
        }

        // Handle home enforcement if enabled.
        if (enforceHome)
        {
            // Home enforcement only applies to remote services, which will only
            // apply events mastered in the same service.
            if (remote)
            {
                /*
                 * The master service should be compared with the service that
                 * originated the event, rather than the service that is
                 * applying it. Otherwise, star topologies (where the hub
                 * service applies data originated from the spokes master
                 * services) could not work.
                 */
                String shardService = event.getDBMSEvent()
                        .getMetadataOptionValue(ReplOptionParams.SERVICE);
                if (shard.getMaster().equals(shardService))
                {
                    // Shard home matches the service name, apply this event
                    if (logger.isDebugEnabled())
                    {
                        logger.debug("Event mastered in this service; processing event: seqno="
                                + event.getSeqno()
                                + " shard ID="
                                + event.getShardId()
                                + " shard master="
                                + shard.getMaster() + " service=" + service);

                    }
                    logger.debug("Event shard matches shard home definition. Processing event.");
                    return event;
                }
                else
                {
                    /*
                     * Provisional fix for issue 443.
                     */

                    /*
                     * We need to exclude tungsten_* schemas from the
                     * evaluation. Tungsten schema DDL events are passed from
                     * master service to slave services and safely ignored (they
                     * are qualified by "IF EXISTS"). This should not be a
                     * problem, except that the shard filter recognizes these
                     * events as unknown, and thus rejects them. In the previous
                     * implementation, where 'drop' is the only action for
                     * enforceHome, the problem does not occur. If the policy is
                     * 'error', instead of drop, the result is not what we want.
                     * Replication stops, and when we skip the event it will
                     * propagate to the other services, resulting in a loop.
                     * Therefore, we drop all events that affect tungsten_*
                     * schemas.
                     */
                    Policy tempShardPolicy = this.unwantedShardPolicy;
                    String filterRemarks = "";
                    /*
                     * Remove events that come from the replicator itself.
                     */
                    if (event.getDBMSEvent().getMetadataOptionValue(
                            ReplOptionParams.TUNGSTEN_METADATA) != null)
                    {
                        tempShardPolicy = Policy.drop;
                        filterRemarks = " (Event is used for Tungsten Replicator)";
                    }

                    /*
                     * Accept events that have been explicitly whitelisted
                     */
                    if (shard.getMaster().equals("whitelisted"))
                    {
                        tempShardPolicy = Policy.accept;
                        filterRemarks = " (Event was whitelisted)";
                    }

                    switch (tempShardPolicy)
                    {
                        case accept :
                        {
                            if (logger.isDebugEnabled())
                                logger.debug("Accepting event from wrong shard: seqno="
                                        + event.getSeqno()
                                        + " shard ID="
                                        + eventShard
                                        + " shard master="
                                        + shard.getMaster()
                                        + " service="
                                        + service + filterRemarks);
                            return event;
                        }
                        case drop :
                        {
                            if (logger.isDebugEnabled())
                                logger.debug("Event master does not match this service; dropping event: seqno="
                                        + event.getSeqno()
                                        + " shard ID="
                                        + event.getShardId()
                                        + " shard master="
                                        + shard.getMaster()
                                        + " service="
                                        + service + filterRemarks);

                            return null;
                        }
                        case warn :
                        {
                            logger.warn("Dropping event from wrong shard: seqno="
                                    + event.getSeqno()
                                    + " shard ID="
                                    + eventShard
                                    + " shard ID="
                                    + event.getShardId()
                                    + " shard master="
                                    + shard.getMaster() + " service=" + service);
                            return null;
                        }
                        case error :
                        {
                            throw new ReplicatorException(
                                    "Rejected event from wrong shard: seqno="
                                            + event.getSeqno() + " shard ID="
                                            + event.getShardId()
                                            + " shard master="
                                            + shard.getMaster() + " service="
                                            + service);
                        }
                        default :
                        {
                            throw new ReplicatorException(
                                    "No policy for wrong shard: seqno="
                                            + event.getSeqno() + " shard ID="
                                            + event.getShardId()
                                            + " shard master="
                                            + shard.getMaster() + " service="
                                            + service);
                        }
                    }
                    /*
                     * Old code. Replaced by the above switch statement // Shard
                     * home does not match, discard this event if
                     * (logger.isDebugEnabled()) { logger.debug(
                     * "Event master does not match this service; dropping event: seqno="
                     * + event.getSeqno() + " shard ID=" + event.getShardId() +
                     * " shard master=" + shard.getMaster() + " service=" +
                     * service); } return null;
                     */
                }

            }
            else
            {
                // Local services do not enforce homes.
                if (logger.isDebugEnabled())
                    logger.debug("Local service - home enforcement does not apply");
                return event;
            }
        }
        else
        {
            if (logger.isDebugEnabled())
                logger.debug("Home enforcement is not enabled");
            return event;
        }

        // Otherwise if it matches the service, apply it.
    }

    /**
     * updateShardCatalog both update the shards hold in memory as well as in
     * shard table in database.
     * 
     * @param eventShard Id of the shard to be created
     * @param shardService Service to which shard is assigned
     * @return New shard definition
     * @throws ReplicatorException
     */
    private Shard updateShardCatalog(String eventShard, String shardService)
            throws ReplicatorException
    {
        if (logger.isDebugEnabled())
            logger.debug("Creating unknown shard " + eventShard + " for home "
                    + shardService);

        ShardManager manager = new ShardManager(service, url, user, password,
                schemaName, tableType, context);
        List<Map<String, String>> params = new ArrayList<Map<String, String>>();
        Map<String, String> newShard = new HashMap<String, String>();
        newShard.put(ShardTable.SHARD_ID_COL, eventShard);
        newShard.put(ShardTable.SHARD_MASTER_COL, shardService);
        params.add(newShard);
        try
        {
            manager.insert(params);
            shards.put(eventShard, new Shard(newShard));
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(e);
        }

        return shards.get(eventShard);
    }

    /**
     * If true the shard is enabled.
     */
    public void setEnabled(boolean enabled)
    {
        this.enabled = enabled;
    }

    /**
     * If true, we enforce shard homes in remote services.
     */
    public void setEnforceHome(boolean enforceHome)
    {
        this.enforceHome = enforceHome;
    }

    /**
     * If true, we allow whitelisted shards in remote services.
     */
    public void setAllowWhitelisted(boolean allowWhitelisted)
    {
        this.allowWhitelisted = allowWhitelisted;
    }

    /**
     * If true we auto-create shards when new shard appears.
     */
    public void setAutoCreate(boolean autoCreate)
    {
        this.autoCreate = autoCreate;
    }

    /**
     * Defines policy for unwanted shards.
     */
    public void setUnwantedShardPolicy(String policy)
    {
        this.unwantedShardPolicyString = policy;
    }

    /**
     * Defines policy for unknown shards.
     */
    public void setUnknownShardPolicy(String policy)
    {
        this.unknownShardPolicyString = policy;
    }
}
