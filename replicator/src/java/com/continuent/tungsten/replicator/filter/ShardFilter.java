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
 * Initial developer(s): Robert Hodges, Stephane Giron
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.filter;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplOptionParams;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Ignores or replicates a database using rules similar to MySQL ignore-db and
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class ShardFilter implements Filter
{
    private static Logger logger = Logger.getLogger(ShardFilter.class);

    private String        doShard;

    // Currently unsupported. 
    @SuppressWarnings("unused")
    private String        ignoreShard;

    /**
     * Sets a list of one or more shards to replicate. Shard names are
     * comma-separated.
     */
    public void setDoShard(String doShard)
    {
        this.doShard = doShard;
    }

    /**
     * Sets a list of one or more databases to replicate. Database names are
     * comma-separated.
     */
    public void setIgnoreShard(String ignoreShard)
    {
        this.ignoreShard = ignoreShard;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.filter.Filter#filter(com.continuent.tungsten.replicator.event.ReplDBMSEvent)
     */
    public ReplDBMSEvent filter(ReplDBMSEvent event)
            throws ReplicatorException, InterruptedException
    {
        String shardId = event.getDBMSEvent()
                .getMetadataOptionValue(ReplOptionParams.SHARD_ID);
        if (shardId.equals(doShard))
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Accepting event: seqno=" + event.getSeqno() + " shard_id=" + shardId);
            }
            return event;
        }
        else
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Dropping event: seqno=" + event.getSeqno() + " shard_id=" + shardId);
            }
            return null;
        }
    }
    
    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
    }
}
