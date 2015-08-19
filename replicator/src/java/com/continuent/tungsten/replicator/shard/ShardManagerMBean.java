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
import java.util.List;
import java.util.Map;

import com.continuent.tungsten.common.jmx.DynamicMBeanHelper;

/**
 * Defines API for shard management extensions.
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public interface ShardManagerMBean
{
    /**
     * Returns true so that clients can confirm connection liveness.
     * 
     * @return true if the service is up and running, false otherwise
     */
    public boolean isAlive();

    /**
     * Inserts a list of shards into the shard table. Each shard will be a map
     * of name-value parameters, for example, name -> my_shard, channel -> 2.
     * 
     * @param params a list of shards to be inserted
     * @throws SQLException
     */
    public int insert(List<Map<String, String>> params) throws SQLException;

    /**
     * Updates a list of shards into the shard table. Each shard will be a map
     * of name-value parameters, for example, name -> my_shard, channel -> 2,
     * the key to be used to update being the shard name.
     * 
     * @param params a list of shards to be updated
     * @throws SQLException
     */
    public int update(List<Map<String, String>> params) throws SQLException;

    /**
     * Deletes a list of shards based on shard ids (aka shard name). The list
     * will only contain shard ids.
     * 
     * @param params
     * @throws SQLException
     */
    public int delete(List<Map<String, String>> params) throws SQLException;

    /**
     * Deletes all shards from the shard table.
     * 
     * @throws SQLException
     */
    public int deleteAll() throws SQLException;

    /**
     * List all shards definitions
     * 
     * @throws SQLException
     * @return A list of shards represented by maps of name-value.
     */
    public List<Map<String, String>> list() throws SQLException;

    /**
     * Returns a helper that supplies MBean metadata.
     */
    public abstract DynamicMBeanHelper createHelper() throws Exception;
}