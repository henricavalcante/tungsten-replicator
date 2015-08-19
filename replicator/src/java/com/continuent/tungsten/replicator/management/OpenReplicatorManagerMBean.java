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
 * Initial developer(s): Teemu Ollakka
 * Contributor(s): Alex Yurchenko, Robert Hodges, Seppo Jaakola, Stephane Giron
 */

package com.continuent.tungsten.replicator.management;

import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.jmx.DynamicMBeanHelper;

/**
 * Replicator Manager MBean interface definition. This contains all replicator
 * management functions.
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public interface OpenReplicatorManagerMBean
{
    /**
     * Returns true so that clients can confirm connection liveness.
     * 
     * @return true if the service is up and running, false otherwise
     */
    public boolean isAlive();

    /**
     * Gets the site name for this replication service
     * 
     * @return the site name as a string
     */
    public String getSiteName();

    /**
     * Gets the cluster name for this replication service
     * 
     * @return the cluster name as a string
     */
    public String getClusterName();

    /**
     * Gets the service name for this replication service
     * 
     * @return the service name as a string
     */
    public String getServiceName();

    /**
     * Gets the simple name for this replication service
     * 
     * @return the simple name of this service as a string
     */
    public String getSimpleServiceName();

    /**
     * Returns the replicator product version.
     * 
     * @return the version number as a string
     */
    public String getVersion();

    /**
     * Returns current instance unique source identifier.
     * 
     * @return the source identifier as a string
     */
    public String getSourceId();

    /**
     * Returns the current replicator role.
     * 
     * @return the role as a string
     */
    public String getRole();

    /**
     * Returns the master remote URI to which this replicator connects when
     * operating as a slave.stop
     */
    public String getMasterConnectUri();

    /**
     * Returns the URI on which this master listens when operating as a slave.
     */
    public String getMasterListenUri();

    /**
     * Returns true if the Replicator uses SSL connections.
     */
    public Boolean getUseSSLConnection() throws URISyntaxException;

    /**
     * Returns the port on which the replicator will listen when it's a master.
     */
    public int getMasterListenPort();

    /**
     * Returns clients (slaves) of this server.
     */
    public List<Map<String, String>> getClients() throws Exception;

    /**
     * Returns the current replicator state.
     */
    public String getState();

    /**
     * Returns description of last error if we are in OFFLINE:ERROR state or
     * null if no error is pending.
     */
    public String getPendingError();

    /**
     * Returns message from exception that provoked the last error
     */
    public String getPendingExceptionMessage();

    /**
     * Returns the highest applied sequence number in the local transaction
     * history log or -1 if log is not operating.
     * 
     * @throws Exception
     */
    public String getMaxSeqNo() throws Exception;

    /**
     * Returns the lowest sequence number in the local transaction history log
     * or -1 if log is not operating.
     * 
     * @throws Exception
     */
    public String getMinSeqNo() throws Exception;

    /**
     * Returns the lowest and the highest sequence numbers in the local
     * transaction history log or -1 if log is not operating.
     * 
     * @throws Exception
     */
    public String[] getMinMaxSeqNo() throws Exception;

    /**
     * Puts the replicator into the online state. This call returns when the
     * request to go online has been accepted. The replicator must be in the
     * OFFLINE state for this call to be processed.
     * <p/>
     * The online operation accepts name-value control parameters that allow
     * users to control replicator behavior when going online.
     * <table>
     * <theader>
     * <tr>
     * <td>Name</td>
     * <td>Description</td>
     * <td>Default</td>
     * </tr>
     * </theader> <tbody>stop
     * <tr>
     * <td>initEventId</td>
     * <td>Sets initial native event ID at which to start extraction, overriding
     * value from the last event extracted</td>
     * <td>Replication starts at either last event ID extracted or, if there is
     * none, the current event ID</td>
     * </tr>
     * <td>skipApplyEvents</td>
     * <td>Number of events to skip applying at startup</td>
     * <td>0</td> </tr> </tr>
     * <td>toEventId</td>
     * <td>Replicate up to the indicated event ID</td>
     * <td>None</td> </tr> </tr>
     * <td>toSeqno</td>
     * <td>Replicate up to the indicated sequence number</td>
     * <td>None</td> </tr> </tbody>
     * </table>
     * Due to the fact that the replicator may need to synchronize with a master
     * it may be some time before the replicator actually reaches the online
     * state. Use the {@link #waitForState(String, long)} call to wait
     * synchronously for the replicator to go fully online.
     * 
     * @param controlParams 0 or more control parameters expressed as name-value
     *            pairs
     * @throws Exception
     */
    public void online2(Map<String, String> controlParams) throws Exception;

    /**
     * Puts the replicator into the online state using defaults for all control
     * parameters.
     * 
     * @throws Exception
     */
    public void online() throws Exception;

    /**
     * Issues a request to go offline at a particular event or sequence number.
     * The replicator must be in the ONLINE state for this call to be processed.
     * <p/>
     * The offline operation accepts name-value control parameters that allow
     * users to control replicator behavior.
     * <table>
     * <theader>
     * <tr>
     * <td>Name</td>
     * <td>Description</td>
     * <td>Parameter</td>
     * <td>Default</td>
     * </tr>
     * </theader> <tbody></tr>
     * <td>atEventId</td>
     * <td>Go offline at the requested event</td>
     * <td>An event ID in native format</td>
     * <td>None</td> </tr> </tr>
     * <td>atHeartbeat</td>
     * <td>Go offline at the next heartbeat event.</td>
     * <td>A heartbeat name or * to select any heartbeat</td>
     * <td>*</td> </tr> </tbody>
     * <td>atSeqno</td>
     * <td>Go offline at the indicated sequence number</td>
     * <td>A valid sequence number</td>
     * <td>None</td> </tr> </tbody>
     * <td>atTransaction</td>
     * <td>Go offline cleanly at the next transaction. This is the best way to
     * take a replicator offline as it ensures the replicator can reload backups
     * when parallel apply is in effect or make change to parallel apply
     * parameters</td>
     * <td>Insert any value</td>
     * <td>None</td> </tr> </tbody>
     * <td>atTimestamp</td>
     * <td>Go offline cleanly at the next transaction. This is the best way to
     * take a replicator offline as it ensures the replicator can reload backups
     * when parallel apply is in effect or make change to parallel apply
     * parameters</td>
     * <td>A timestamp String in yyyy-MM-dd HH:mm:ss format</td>
     * <td>None</td></tr> </tbody>
     * </table>
     * 
     * @param controlParams 0 or more control parameters expressed as name-value
     *            pairs
     * @throws Exception
     */
    public void offlineDeferred(Map<String, String> controlParams)
            throws Exception;

    /**
     * Puts the replicator into the offline state immediately without clean-up,
     * returning once the replicator is offline. The replicator must be in the
     * ONLINE or GOING-ONLINE state for this call to be processed.
     * <p/>
     * <strong>This call is hazardous on slaves using parallel apply.</strong>
     * It does not do a clean offline operation, which means that it is unsafe
     * for doing failover operations, creating a backup that can restore when
     * using different numbers of parallel apply channels or for changing any
     * parameters associated with parallel apply. You should use offlineDeferred
     * for clean shutdown.
     * 
     * @throws Exception
     */
    public void offline() throws Exception;

    /**
     * Implements a flush operation to synchronize the state of the database
     * with the transaction history log and return the sequence number of the
     * log at the point where the database is synchronized. The replicator must
     * be in the ONLINE:MASTER state for this call to be processed. This call is
     * used to implement safe failover.
     * 
     * @param timeout Number of seconds to wait. 0 is indefinite.
     * @return The sequence number at which the log is synchronized
     * @throws Exception Thrown if we timeout or are canceled
     */
    public String flush(long timeout) throws Exception;

    /**
     * Attempts to kill all non-replication logins on the DBMS server. May be
     * called prior to flush() to ensure that all application activity stops
     * prior to a failover. This avoids problems with blocked or long-running
     * transactions that commit after failover starts. This operation works in
     * both ONLINE and OFFLINE states so that it can be used to clear
     * transactions during unplanned as well as planned failover.
     * <p/>
     * The following control parameters are accepted:
     * <ul>
     * <li>timeout - Number of seconds to wait for kill operations to complete</li>
     * </ul>
     * 
     * @param controlParams 0 or more control parameters expressed as name-value
     *            pairs
     * @return Number of sessions killed
     * @throws Exception Thrown if we timeout or are canceled
     */
    public int purge(Map<String, String> controlParams) throws Exception;

    /**
     * @deprecated Use
     *             {@link com.continuent.tungsten.replicator.management.OpenReplicatorManagerMBean#getStatus()}
     */
    @Deprecated
    public TungstenProperties getStatus() throws Exception;

    /**
     * Returns the detailed, current status information from the replicator.
     * 
     * @return detailed status information as a TungstenProperties instance
     * @throws Exception
     */
    public Map<String, String> status() throws Exception;

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
     * Stops the OpenReplicatorManager
     * 
     * @throws Exception
     */
    public void stop() throws Exception;

    /**
     * Run configuration on the replicator. The replicator must be in the
     * OFFLINE state. If the map instance is null the Replicator will read its
     * own properties file, whose location is defined by the System property
     * named "replicator.properties."
     * 
     * @param props A map instance or null to reread local file
     * @throws Exception
     */
    public void configure(Map<String, String> props) throws Exception;

    /**
     * Inserts a heartbeat event into the replicator transaction history. The
     * replicator manager must be in the MASTER state for this call to be
     * successful.
     * 
     * @param props A map instance containing heartbeat parameters
     */
    public void heartbeat(Map<String, String> props) throws Exception;

    /**
     * Waits for the replicator to attain a specific state, such as "SLAVE". The
     * wait time is configurable so that callers do not wait indefinitely. The
     * wait will terminate automatically with an exception if the replicator
     * goes into the error state (unless that is what you are waiting for).
     * 
     * @param state Name of state on which to wait. This can be a parent state
     *            name--"OFFLINE" will also detect "OFFLINE:NORMAL" and
     *            "OFFLINE:ERROR".
     * @param timeout Number of seconds to wait. 0 is indefinite.
     * @return true if requested wait state is achieved, else false if the wait
     *         timed out
     * @throws Exception
     */
    public boolean waitForState(String state, long timeout) throws Exception;

    /**
     * Wait for a particular event to be applied on the slave.
     * 
     * @param sequenceNo Id of the event to wait for
     * @param timeout Number of seconds to wait. 0 is indefinite.
     * @return true if requested sequence number or greater applied, else false
     *         if the wait timed out
     * @throws Exception if there is a timeout or we are canceled
     */
    public boolean waitForAppliedSequenceNumber(String sequenceNo, long timeout)
            throws Exception;

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
     * Spawns a backup of the database and optionally waits for completion. This
     * command can only be run in the off-line state.
     * 
     * @param backupAgentName Name of the backup agent to use or null to use the
     *            default backup
     * @param storageAgentName Name of the storage agent to use or null to use
     *            the default storage
     * @param timeout Number of seconds to wait. 0 is indefinite, negative means
     *            no wait.
     * @return A URI for the backup if successful, otherwise a null if the
     *         backup is still pending
     * @throws Exception if there is a backup failure
     */
    public String backup(String backupAgentName, String storageAgentName,
            long timeout) throws Exception;

    /**
     * Spawns a restore of the database and optionally waits for completion.
     * This command can only be run in the off-line state.
     * 
     * @param uri URI of the backup to load
     * @param timeout Number of seconds to wait. 0 is indefinite, negative means
     *            no wait.
     * @return the URI of the restored backup if the restore is known to have
     *         completed successfully, otherwise null, which means restore is
     *         still pending
     * @throws Exception if there is a restore failure
     */
    public String restore(String uri, long timeout) throws Exception;

    /**
     * Provisions a database from another copy and optionally waits for
     * completion. This command can only be run from the off-line state.
     * 
     * @param donorUri URI of donor replicator
     * @param timeout Number of seconds to wait. 0 is indefinite, negative means
     *            no wait.
     * @return true if the provision task is known to have completed
     *         successfully, otherwise false, which means provisioning is still
     *         pending
     * @throws Exception if there is a provisioning failure
     */
    public boolean provision(String donorUri, long timeout) throws Exception;

    /**
     * Starts the replicator service, which spawns all threads and underlying
     * components necessary to perform replication. It is the first call to a
     * new replication service. It also issues a call to put the replicator
     * services online if auto_enable is set in configuration file, except if
     * forceOffline is true.
     * 
     * @param forceOffline true to prevent the replicator from putting its
     *            replication services online (if auto-enable is set to true)
     * @throws Exception Thrown if start-up fails. This includes failure to go
     *             online if the replicator is auto-enabled.
     */
    public void start(boolean forceOffline) throws Exception;

    /**
     * Returns a map instance containing currently set properties, if any. This
     * call can be issued in any replicator state.
     * 
     * @param key optional key of a single property
     * @return the current property(ies)
     */
    public Map<String, String> properties(String key) throws Exception;

    /**
     * Returns a map instance containing currently set dynamic properties, if
     * any. This call can be issued in any replicator state.
     */
    public Map<String, String> getDynamicProperties() throws Exception;

    /**
     * Clears all dynamic properties. The replicator must be in the OFFLINE
     * state when this call is issued.
     * 
     * @throws Exception
     */
    public void clearDynamicProperties() throws Exception;

    /**
     * Sets replicator role. Not all replicators support all roles; check other
     * documentation for details.
     * 
     * @param role The new replicator role; must be 'slave', 'master', or
     *            'standby'
     * @param uri Optional uri to identify master or slave.
     */
    public void setRole(String role, String uri) throws Exception;

    /**
     * Returns the JMX API for a named extension.
     */
    public Object getExtensionMBean(String name) throws Exception;

    /**
     * Returns a list of the names of currently defined extension MBeans.
     */
    public List<String> listExtensionMBeans() throws Exception;

    /**
     * Returns a helper that supplies MBean metadata.
     */
    public abstract DynamicMBeanHelper createHelper() throws Exception;

    /**
     * Notification signaling methods. These sends a notification to the
     * replicator manager. These methods are used by replicator providers, which
     * need to signal of underlying state changes.
     * 
     * @param signal one of signal* strings listed in this interface
     * @param msg additional message passed along the signal
     * @throws Exception
     */
    public void signal(int signal, String msg) throws Exception;

    public static final int signalOfflineReached  = 1;
    public static final int signalShutdown        = 2;
    public static final int signalConfigured      = 3;
    public static final int signalSynced          = 4;
    public static final int signalRestored        = 5;
    public static final int signalConsistencyFail = 6;
    public static final int signalError           = 7;

    /**
     * Gets the replicator capabilities.
     */
    public Map<String, String> capabilities() throws Exception;

    /**
     * This makes the replicator go into the provisioning state before going
     * online.
     * 
     * @see OpenReplicatorManager#online2(Map)
     */
    public void provisionOnline(Map<String, String> map) throws Exception;
}
