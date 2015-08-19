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
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.replicator.shard;

import java.util.Map;

/**
 * Holds information about a single shard, whose name is given by the shardId
 * property.
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class Shard
{
    /**
     * Master name that denotes a short that is local to current service only.
     */
    public String   LOCAL = "#LOCAL";

    // Shard properties.
    private String  shardId;
    private boolean critical;
    private String  master;

    public Shard(String shardId, boolean critical, String master)
    {
        this.shardId = shardId;
        this.critical = critical;
        this.master = master;
    }

    public Shard(Map<String, String> shard)
    {
        this.shardId = shard.get(ShardTable.SHARD_ID_COL);
        this.critical = Boolean.valueOf(shard.get(ShardTable.SHARD_CRIT_COL));
        this.master = shard.get(ShardTable.SHARD_MASTER_COL);
    }

    /** Returns the shard name. */
    public String getShardId()
    {
        return shardId;
    }

    /** Returns true if shard is critical. */
    public boolean isCritical()
    {
        return critical;
    }

    /** Returns name of master service. */
    public String getMaster()
    {
        return master;
    }

    /**
     * Returns true if shard is local-only, i.e., does not cross services.
     */
    public boolean isLocal()
    {
        return LOCAL.equals(master);
    }
}
