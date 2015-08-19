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

package com.continuent.tungsten.replicator.management;

import java.util.List;
import java.util.Map;

import com.continuent.tungsten.common.jmx.DynamicMBeanHelper;

/**
 * Management interface for main replicator control class.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface ReplicationServiceManagerMBean
{
    /**
     * Lists currently defined replicators and whether they are running or not.
     */
    public List<Map<String, String>> services() throws Exception;

    /**
     * Returns true if the MBean is alive. Used to test liveness of connections.
     */
    public boolean isAlive();

    /**
     * Returns status information.
     * 
     * @throws Exception
     */
    public Map<String, String> status() throws Exception;

    /**
     * Starts a replication service.
     * 
     * @param name Name of the replicator service
     * @return True if replicator service exists and was started
     * @throws Exception Thrown if service start-up fails
     */
    public boolean loadService(String name) throws Exception;

    /**
     * Stops a replication service.
     * 
     * @param name Name of the replicator service
     * @return True if replicator service exists and was stopped
     * @throws Exception Thrown if service stop fails
     */
    public boolean unloadService(String name) throws Exception;

    /**
     * Resets a replication service.
     * 
     * @param name Name of the replicator service
     * @return Map of strings that indicate actions taken.
     * @throws Exception Thrown if service stop fails
     */
    public Map<String, String> resetService(String name) throws Exception;

    /**
     * Resets a replication service or some of its components (thl, relay,
     * database).
     * 
     * @param name Name of the replicator service
     * @param controlParams 0 or more control parameters expressed as name-value
     *            pairs (option={-all|-thl|-relay|-db})
     * @return Map of strings that indicate actions taken.
     * @throws Exception Thrown if service stop fails
     */
    public Map<String, String> resetService(String name,
            Map<String, String> controlParams) throws Exception;

    /**
     * Returns a list of properties that have the status for each of the current
     * services.
     */
    public Map<String, String> replicatorStatus(String name) throws Exception;

    /**
     * Returns a map of status properties for all current replicators
     * 
     * @throws Exception
     */
    public Map<String, String> getStatus() throws Exception;

    /**
     * Stops all replication services and exits the process cleanly.
     * 
     * @throws Exception Thrown if service stop fails
     */
    public void stop() throws Exception;

    /**
     * Terminates the replicator process immediately without clean-up. This
     * command should be used only if stop does not work or for testing.
     */
    public void kill() throws Exception;

    /**
     * Returns a helper that supplies MBean metadata.
     */
    public abstract DynamicMBeanHelper createHelper() throws Exception;
}
