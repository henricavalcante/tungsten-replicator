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
 * Initial developer(s): Edward Archibald
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.common.cluster.resource;

import java.io.Serializable;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

import com.continuent.tungsten.common.config.TungstenProperties;

/**
 * Defines a replicator resource. Among other things this class contains the
 * definitive reference to names that replicators must use for monitoring
 * properties.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Replicator extends Resource implements Serializable
{
    /**
     * 
     */
    private static final long  serialVersionUID               = 1L;

    /**
     * Last LSN (log sequence number) applied to slave database. On a master
     * this is the same as the last position read from the log.
     */
    public static final String APPLIED_LAST_SEQNO             = "appliedLastSeqno";

    /**
     * Last native transaction ID applied to slave database. On a master this is
     * the same as the last native transaction ID read from the log.
     */
    public static final String APPLIED_LAST_EVENT_ID          = "appliedLastEventId";

    /**
     * Lag in seconds between the time last event was applied and when it was
     * originally generated.
     */
    public static final String APPLIED_LATENCY                = "appliedLatency";
    
    /**
     * Time, in seconds, that has elapesed between the last update timestamp an 'now()'.
     * This value will increase, second-for-second, when a replicator is 'idle' i.e.
     * not processing any transactions.
     */
    public static final String RELATIVE_LATENCY                = "relativeLatency";

    /** Current epoch number used by replicator, if it has one. */
    public static final String LATEST_EPOCH_NUMBER            = "latestEpochNumber";

    /**
     * Lowest LSN stored in the replicator log. Value is null if there is no
     * log.
     */
    public static final String MIN_STORED_SEQNO               = "minimumStoredSeqNo";

    /**
     * Highest LSN stored in the replicator log. Value is null if there is no
     * log. This value is used for selecting slaves for failover.
     */
    public static final String MAX_STORED_SEQNO               = "maximumStoredSeqNo";

    /**
     * Current native transaction ID in the database. On a slave this value may
     * be null.
     */
    public static final String CURRENT_EVENT_ID               = "currentEventId";

    /** URI to which slave replicator connects. Undefined for master. */
    public static final String MASTER_CONNECT_URI             = "masterConnectUri";

    /** URI that slaves should use when connecting to this master. */
    public static final String MASTER_LISTEN_URI              = "masterListenUri";

    /** Indicates if SSL connection is used. */
    public static final String USE_SSL_CONNECTION             = "useSSLConnection";

    /** Name of the cluster to which this replicator belongs. */
    public static final String CLUSTERNAME                    = "clusterName";

    /**
     * Role of this replicator. By convention roles are either 'master' or
     * 'slave'.
     */
    public static final String ROLE                           = "role";

    /** Host name of this replicator. */
    public static final String DATASERVERHOST                 = "dataServerHost";

    /** Seconds since replicator has started. */
    public static final String UPTIME_SECONDS                 = "uptimeSeconds";

    /** Seconds that replicator has been in its current state. */
    public static final String TIME_IN_STATE_SECONDS          = "timeInStateSeconds";

    /** Current time on replicator expressed as milliseconds since Jan 1, 1970. */
    public static final String CURRENT_TIME_MILLIS            = "currentTimeMillis";

    /** Current replicator state. */
    public static final String STATE                          = "state";

    /** Source ID used to mark events for this replicator. */
    public static final String SOURCEID                       = "sourceId";

    /** Current exception that caused error, if there is one. */
    public static final String PENDING_EXCEPTION_MESSAGE      = "pendingExceptionMessage";

    /** Current error code, if there is one. */
    public static final String PENDING_ERROR_CODE             = "pendingErrorCode";

    /** Current error, if there is one. */
    public static final String PENDING_ERROR                  = "pendingError";

    /** Current failed log sequence number or -1 if there is none. */
    public static final String PENDING_ERROR_SEQNO            = "pendingErrorSeqno";

    /** Current failed event ID or null if there is none. */
    public static final String PENDING_ERROR_EVENTID          = "pendingErrorEventId";

    /** Pending offline requests in text form. */
    public static final String OFFLINE_REQUESTS               = "offlineRequests";

    /** URL to connect to underlying data source replicator serves. */
    static public final String RESOURCE_JDBC_URL              = "resourceJdbcUrl";

    /** Class name of Java data source. */
    static public final String RESOURCE_JDBC_DRIVER           = "resourceJdbcDriver";

    /**
     * Resource precedence for failover, which should be an integer > 0 or -1 to
     * indicate do not fail over to this resource.
     */
    public static final String RESOURCE_PRECEDENCE            = "resourcePrecedence";

    /** DBMS vendor string for this data source */
    static public final String RESOURCE_VENDOR                = "resourceVendor";

    static public final String RESOURCE_VIP_INTERFACE         = "vipInterface";

    static public final String RESOURCE_VIP_ADDRESS           = "vipAddress";

    static public final String RESOURCE_IS_STANDBY_DATASOURCE = "isStandbyDataSource";
    /**
     * Log sequence number type to allow managers to figure out how to sort
     * values.
     */
    public static final String SEQNO_TYPE                     = "seqnoType";

    /**
     * Denotes a numeric log sequence number type that is convertible to a Java
     * Long type.
     */
    public static final String SEQNO_TYPE_LONG                = "java.lang.Long";

    /**
     * Denotes a string log sequence number type whose values are comparable
     * strings.
     */
    public static final String SEQNO_TYPE_STRING              = "java.lang.String";

    private String             dataServiceName                = null;
    private String             host                           = null;
    private String             sourceId                       = null;
    private String             vendor                         = null;
    private String             resourceJdbcDriver             = null;
    private String             resourceJdbcUrl                = null;
    private String             role                           = null;
    private String             state                          = null;
    private boolean            isStandbyDataSource            = false;
    private long               latestEpochNumber              = -1L;
    private String             appliedLastEventId             = null;
    private long               appliedLastSeqno               = 01L;
    private double             appliedLatency                 = 0.0;
    private String             vipInterface                   = null;
    private String             vipAddress                     = null;
    private String             masterConnectUri               = null;
    private String             masterListenUri                = null;

    /** Replicator properties used for json output */
    private TungstenProperties replicatorProperties           = null;

    /**
     * Obsolete values provided for compatibility with branched open replicator.
     * 
     * @deprecated
     */
    public static final String MASTER_URI                     = "masterUri";

    // Default values
    public static final long   DEFAULT_LATEST_EPOCH_NUMBER    = -1;
    public static final String DEFAULT_LAST_EVENT_ID          = "0:0";

    /**
     * Creates a new <code>Replicator</code> object
     */
    public Replicator()
    {
        super(ResourceType.REPLICATOR, "unknown");
        this.dataServiceName = "unknown";
        this.host = "unknown";
    }

    @JsonCreator
    public Replicator(@JsonProperty("name") String key,
            @JsonProperty("dataServiceName") String clusterName,
            @JsonProperty("host") String host)
    {
        super(ResourceType.REPLICATOR, key);
        this.dataServiceName = clusterName;
        this.host = host;
    }

    static public Replicator createFromReplicatorStatus(
            TungstenProperties replicatorProps)
    {
        Replicator newReplicator = new Replicator(
                replicatorProps.getString(Replicator.SOURCEID),
                replicatorProps.getString(Replicator.CLUSTERNAME),
                replicatorProps.getString(Replicator.SOURCEID));

        newReplicator.setRole(replicatorProps.getString(Replicator.ROLE));

        newReplicator.setState(replicatorProps.getString(Replicator.STATE));

        newReplicator.setLatestEpochNumber(replicatorProps.getLong(
                Replicator.LATEST_EPOCH_NUMBER, "0", false));

        newReplicator.setAppliedLastEventId(replicatorProps
                .getString(Replicator.APPLIED_LAST_EVENT_ID));
        newReplicator.setAppliedLastSeqno(replicatorProps
                .getLong(Replicator.APPLIED_LAST_SEQNO));
        newReplicator.setAppliedLatency(replicatorProps
                .getDouble(Replicator.APPLIED_LATENCY));
        newReplicator.setMasterConnectUri(replicatorProps
                .getString(Replicator.MASTER_CONNECT_URI));
        newReplicator.setMasterListenUri(replicatorProps
                .getString(Replicator.MASTER_LISTEN_URI));

        // Add all of the Replicator properties
        // TUC-2351
        newReplicator.setReplicatorProperties(replicatorProps);

        return newReplicator;
    }

    /**
     * Returns the dataServiceName value.
     * 
     * @return Returns the dataServiceName.
     */
    public String getDataServiceName()
    {
        return dataServiceName;
    }

    /**
     * Sets the dataServiceName value.
     * 
     * @param dataServiceName The dataServiceName to set.
     */
    public void setDataServiceName(String dataServiceName)
    {
        this.dataServiceName = dataServiceName;
    }

    /**
     * Returns the host value.
     * 
     * @return Returns the host.
     */
    public String getHost()
    {
        return host;
    }

    /**
     * Sets the host value.
     * 
     * @param host The host to set.
     */
    public void setHost(String host)
    {
        this.host = host;
    }

    /**
     * Returns the sourceId value.
     * 
     * @return Returns the sourceId.
     */
    public String getSourceId()
    {
        return sourceId;
    }

    /**
     * Sets the sourceId value.
     * 
     * @param sourceId The sourceId to set.
     */
    public void setSourceId(String sourceId)
    {
        this.sourceId = sourceId;
    }

    /**
     * Returns the vendor value.
     * 
     * @return Returns the vendor.
     */
    public String getVendor()
    {
        return vendor;
    }

    /**
     * Sets the vendor value.
     * 
     * @param vendor The vendor to set.
     */
    public void setVendor(String vendor)
    {
        this.vendor = vendor;
    }

    /**
     * Returns the resourceJdbcDriver value.
     * 
     * @return Returns the resourceJdbcDriver.
     */
    public String getResourceJdbcDriver()
    {
        return resourceJdbcDriver;
    }

    /**
     * Sets the resourceJdbcDriver value.
     * 
     * @param resourceJdbcDriver The resourceJdbcDriver to set.
     */
    public void setResourceJdbcDriver(String resourceJdbcDriver)
    {
        this.resourceJdbcDriver = resourceJdbcDriver;
    }

    /**
     * Returns the role value.
     * 
     * @return Returns the role.
     */
    public String getRole()
    {
        return role;
    }

    /**
     * Sets the role value.
     * 
     * @param role The role to set.
     */
    public void setRole(String role)
    {
        this.role = role;
    }

    /**
     * Returns the isStandbyDataSource value.
     * 
     * @return Returns the isStandbyDataSource.
     */
    public boolean isStandbyDataSource()
    {
        return isStandbyDataSource;
    }

    /**
     * Sets the isStandbyDataSource value.
     * 
     * @param isStandbyDataSource The isStandbyDataSource to set.
     */
    public void setStandbyDataSource(boolean isStandbyDataSource)
    {
        this.isStandbyDataSource = isStandbyDataSource;
    }

    /**
     * Returns the resourceJdbcUrl value.
     * 
     * @return Returns the resourceJdbcUrl.
     */
    public String getResourceJdbcUrl()
    {
        return resourceJdbcUrl;
    }

    /**
     * Sets the resourceJdbcUrl value.
     * 
     * @param resourceJdbcUrl The resourceJdbcUrl to set.
     */
    public void setResourceJdbcUrl(String resourceJdbcUrl)
    {
        this.resourceJdbcUrl = resourceJdbcUrl;
    }

    /**
     * Returns the latestEpochNumber value.
     * 
     * @return Returns the latestEpochNumber.
     */
    public long getLatestEpochNumber()
    {
        return latestEpochNumber;
    }

    /**
     * Sets the latestEpochNumber value.
     * 
     * @param latestEpochNumber The latestEpochNumber to set.
     */
    public void setLatestEpochNumber(long latestEpochNumber)
    {
        this.latestEpochNumber = latestEpochNumber;
    }

    /**
     * Returns the appliedLastEventId value.
     * 
     * @return Returns the appliedLastEventId.
     */
    public String getAppliedLastEventId()
    {
        return appliedLastEventId;
    }

    /**
     * Sets the appliedLastEventId value.
     * 
     * @param appliedLastEventId The appliedLastEventId to set.
     */
    public void setAppliedLastEventId(String appliedLastEventId)
    {
        this.appliedLastEventId = appliedLastEventId;
    }

    /**
     * Returns the appliedLatency value.
     * 
     * @return Returns the appliedLatency.
     */
    public double getAppliedLatency()
    {
        return appliedLatency;
    }

    /**
     * Sets the appliedLatency value.
     * 
     * @param appliedLatency The appliedLatency to set.
     */
    public void setAppliedLatency(double appliedLatency)
    {
        this.appliedLatency = appliedLatency;
    }

    /**
     * Returns the vipInterface value.
     * 
     * @return Returns the vipInterface.
     */
    public String getVipInterface()
    {
        return vipInterface;
    }

    /**
     * Sets the vipInterface value.
     * 
     * @param vipInterface The vipInterface to set.
     */
    public void setVipInterface(String vipInterface)
    {
        this.vipInterface = vipInterface;
    }

    /**
     * Returns the vipAddress value.
     * 
     * @return Returns the vipAddress.
     */
    public String getVipAddress()
    {
        return vipAddress;
    }

    /**
     * Sets the vipAddress value.
     * 
     * @param vipAddress The vipAddress to set.
     */
    public void setVipAddress(String vipAddress)
    {
        this.vipAddress = vipAddress;
    }

    /**
     * Returns the masterConnectUri value.
     * 
     * @return Returns the masterConnectUri.
     */
    public String getMasterConnectUri()
    {
        return masterConnectUri;
    }

    /**
     * Sets the masterConnectUri value.
     * 
     * @param masterConnectUri The masterConnectUri to set.
     */
    public void setMasterConnectUri(String masterConnectUri)
    {
        this.masterConnectUri = masterConnectUri;
    }

    /**
     * Returns the masterListenUri value.
     * 
     * @return Returns the masterListenUri.
     */
    public String getMasterListenUri()
    {
        return masterListenUri;
    }

    /**
     * Sets the masterListenUri value.
     * 
     * @param masterListenUri The masterListenUri to set.
     */
    public void setMasterListenUri(String masterListenUri)
    {
        this.masterListenUri = masterListenUri;
    }

    /**
     * Returns the state value.
     * 
     * @return Returns the state.
     */
    public String getState()
    {
        return state;
    }

    /**
     * Sets the state value.
     * 
     * @param state The state to set.
     */
    public void setState(String state)
    {
        this.state = state;
    }

    /**
     * Returns the appliedLastSeqno value.
     * 
     * @return Returns the appliedLastSeqno.
     */
    public long getAppliedLastSeqno()
    {
        return appliedLastSeqno;
    }

    /**
     * Sets the appliedLastSeqno value.
     * 
     * @param appliedLastSeqno The appliedLastSeqno to set.
     */
    public void setAppliedLastSeqno(long appliedLastSeqno)
    {
        this.appliedLastSeqno = appliedLastSeqno;
    }

    /**
     * Returns the replicatorProperties value.
     * 
     * @return Returns the replicatorProperties.
     */
    public TungstenProperties getReplicatorProperties()
    {
        return replicatorProperties;
    }

    /**
     * Sets the replicatorProperties value.
     * 
     * @param replicatorProperties The replicatorProperties to set.
     */
    public void setReplicatorProperties(TungstenProperties replicatorProperties)
    {
        this.replicatorProperties = replicatorProperties;
    }

}
