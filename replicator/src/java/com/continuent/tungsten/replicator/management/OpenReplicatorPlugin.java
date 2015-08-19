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
 * Contributor(s): Alexey Yurchenku, Teemu Ollakka, Edward Archibald, Seppo Jaakola
 */

package com.continuent.tungsten.replicator.management;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.continuent.tungsten.common.cluster.resource.physical.ReplicatorCapabilities;
import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;

/**
 * Replicator management plug-in definition. This interface defines basic
 * functions of a plug-in service that manages the overall replication service.
 * OpenReplicatorPlugin instances have the following life-cycle:
 * <ol>
 * <li>Instantiate from class name</li>
 * <li>Call setters on plug-in instance to load values from global property file
 * </li>
 * <li>Call prepare() to set event dispatcher and allocate resources used by
 * instance</li>
 * <li>(Calls to administrative methods)</li>
 * <li>Call release() to free resources</li>
 * </ol>
 * The default implementation of this interface controls the Tungsten native
 * replicator.
 * <p>
 * <strong>NOTE:</strong>This interface should not be confused with the
 * {@link com.continuent.tungsten.replicator.management.tungsten.TungstenPlugin}
 * interface, which is a plugin within the native Tungsten replicator.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin
 * @see com.continuent.tungsten.replicator.management.tungsten.TungstenPlugin
 */
public interface OpenReplicatorPlugin
{
    /**
     * Prepare plug-in for use. It is invoked once after setters have been
     * invoke but before the plug-in performs any operations and after
     * 
     * @param context Context with access to replicator service resources
     * @throws ReplicatorException Thrown if resource allocation fails
     */
    public void prepare(OpenReplicatorContext context)
            throws ReplicatorException;

    /**
     * Release all resources used by plug-in. This is called before the plug-in
     * is deallocated.
     * 
     * @throws ReplicatorException Thrown if resource deallocation fails
     */
    public void release() throws ReplicatorException;

    /**
     * Read configuration data and configure replication behavior appropriately.
     * This may be called multiple times to process configuration data. 
     * 
     * @throws ReplicatorException Thrown if configuration is incomplete or
     *             fails
     */
    public void configure(TungstenProperties properties)
            throws ReplicatorException;

    /**
     * Puts the replicator into the online state, which turns on replication.
     * 
     * @param params Plugin-specific name-value pairs that alter online operation
     */
    public void online(TungstenProperties params) throws Exception;

    /**
     * Puts the replicator into the offline state, which turns off replication.
     */
    public void offline(TungstenProperties params) throws Exception;

    /**
     * Issues a request to go offline at a later point in replication.  Not all 
     * plug-ins support deferred offline behavior. 
     */
    public void offlineDeferred(TungstenProperties params) throws Exception;

    /**
     * Inserts a heartbeat event into the transaction history. The replicator
     * plugin must be in the MASTER state for this call to be successful.
     * 
     * @return true if the heartbeat is inserted.
     */
    public boolean heartbeat(TungstenProperties params) throws Exception;

    /**
     * Implements a flush operation to synchronize the state of the database
     * with the replication log and return a comparable event ID that can be
     * used in a wait operation on a slave.
     * 
     * @param timeout Number of seconds to wait. 0 is indefinite.
     * @return The event ID at which the log is synchronized
     */
    public String flush(long timeout) throws Exception;

    /**
     * Wait for a particular event to be applied on the slave.
     * 
     * @param event Event to wait for
     * @param timeout Number of seconds to wait. 0 is indefinite.
     * @return true if requested sequence number or greater applied, else false
     *         if the wait timed out
     * @throws Exception if there is a timeout or we are canceled
     */
    public boolean waitForAppliedEvent(String event, long timeout)
            throws Exception;

    /**
     * Kill logins other than the connection(s) used for replication.  This works
     * on the master only. 
     * <p/>
     * The following control parameters are accepted:
     * <ul>
     * <li>timeout - Number of seconds to wait for kill operations to complete</li>
     * </ul>
     * 
     * @param params 0 or more control parameters expressed as name-value
     *            pairs
     * @return Number of sessions terminated
     * @throws Exception Thrown if we timeout or are canceled
     */
    public int purge(TungstenProperties params) throws Exception;

    /**
     * Returns the current replicator status as a set of name-value pairs.
     * Values are defined as constants beginning with "STATUS_".
     */
    public HashMap<String, String> status() throws Exception;

    /**
     * Returns a list of status instances for a particular list of items.
     * 
     * @param name Name of the status list. 'tasks' is supported by the native
     *            Tungsten replicator plugin.
     * @return List of TungstenProperties instances containing task status
     * @throws Exception
     */
    public List<Map<String, String>> statusList(String name) throws Exception;

    /**
     * Performs a provision operation. The provision operation is invoked on the
     * replicator to be provisioned. The optional URI value provides the source
     * for provisioning.
     * 
     * @param uri
     * @throws Exception
     */
    public void provision(String uri) throws Exception;

    /**
     * Sets the replicator role.
     * 
     * @param role A supported role name, such as 'master', 'slave', or
     *            'standby'
     * @param uri An optional URI referring to another replicator if required
     *            for role (e.g., a slave typically points to a master)
     * @throws Exception
     */
    public void setRole(String role, String uri) throws ReplicatorException;

    /**
     * Initiates consistency check transaction on a given table.
     * 
     * @param method consistency check method to use
     * @param schemaName name of the table schema
     * @param tableName name of the table, if null all tables in schema are
     *            checked
     * @param rowOffset start consistency check from this row (numeration starts
     *            with 0). If negative - whole table is checked.
     * @param rowLimit limit consistency check to that many rows. If rowOffset
     *            is negative this is ignored.
     * @return Executed consistency check's ID.
     * @throws Exception
     */
    public int consistencyCheck(String method, String schemaName,
            String tableName, int rowOffset, int rowLimit) throws Exception;

    /**
     * gets the capabilties for the replicator
     * 
     * @throws Exception
     */
    public ReplicatorCapabilities getCapabilities() throws Exception;

    // Key names used in response to status command. These are set
    // in properties files, etc. returned as responses.

    /** Current replication role, such as master, slave, or standby. */
    public static final String STATUS_ROLE            = "role";

    /** Current error message containing last error from replicator. */
    public static final String STATUS_ERRMSG          = "errmsg";

    /** ID of last replication event sent. */
    public static final String STATUS_LAST_SENT       = "last-sent";

    /** ID of last replication event applied. */
    public static final String STATUS_LAST_APPLIED    = "last-applied";

    /** ID of last replication event received. */
    public static final String STATUS_LAST_RECEIVED   = "last-received";

    /** Latency in seconds between sent and applied events. */
    public static final String STATUS_APPLIED_LATENCY = "applied-latency";

    public ReplicatorRuntime getReplicatorRuntime();
}
