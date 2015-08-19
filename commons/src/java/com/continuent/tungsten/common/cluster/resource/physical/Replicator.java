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

package com.continuent.tungsten.common.cluster.resource.physical;

/**
 * Defines a replicator resource. Among other things this class contains the
 * definitive reference to names that replicators must use for monitoring
 * properties.
 */
public class Replicator
{
    /**
     * Replication service name. In full clustering this should correspond with
     * a Tungsten service.
     */
    public static final String SERVICE_NAME                = "serviceName";

    /**
     * Replication simple service name. Service names are usually mangled names
     * and sometimes we just want the de-mangled name.
     */
    public static final String SIMPLE_SERVICE_NAME         = "simpleServiceName";

    /**
     * Replication service type, which can be local or remote.
     */
    public static final String SERVICE_TYPE                = "serviceType";

    /**
     * Last LSN (log sequence number) applied to slave database. On a master
     * this is the same as the last position read from the log.
     */
    public static final String APPLIED_LAST_SEQNO          = "appliedLastSeqno";

    /**
     * Last native transaction ID applied to slave database. On a master this is
     * the same as the last native transaction ID read from the log.
     */
    public static final String APPLIED_LAST_EVENT_ID       = "appliedLastEventId";

    /**
     * Lag in seconds between the time last event was applied and when it was
     * originally generated.
     */
    public static final String APPLIED_LATENCY             = "appliedLatency";

    /**
     * Lag in seconds between the timestamp of the last committed transaction
     * and the current time.
     */
    public static final String RELATIVE_LATENCY            = "relativeLatency";

    /** Current epoch number used by replicator, if it has one. */
    public static final String LATEST_EPOCH_NUMBER         = "latestEpochNumber";

    /**
     * Lowest LSN stored in the replicator log. Value is null if there is no
     * log.
     */
    public static final String MIN_STORED_SEQNO            = "minimumStoredSeqNo";

    /**
     * Highest LSN stored in the replicator log. Value is null if there is no
     * log. This value is used for selecting slaves for failover.
     */
    public static final String MAX_STORED_SEQNO            = "maximumStoredSeqNo";

    /**
     * Current native transaction ID in the database. On a slave this value may
     * be null.
     */
    public static final String CURRENT_EVENT_ID            = "currentEventId";

    /** URI to which slave replicator connects. Undefined for master. */
    public static final String MASTER_CONNECT_URI          = "masterConnectUri";

    /** URI that slaves should use when connecting to this master. */
    public static final String MASTER_LISTEN_URI           = "masterListenUri";

    /** Indicates if SSL connection is used. */
    public static final String USE_SSL_CONNECTION          = "useSSLConnection";

    /** Name of the site to which this replicator belongs. */
    public static final String SITENAME                    = "siteName";

    /** Name of the cluster to which this replicator belongs. */
    public static final String CLUSTERNAME                 = "clusterName";

    /**
     * Role of this replicator. By convention roles are either 'master' or
     * 'slave'.
     */
    public static final String ROLE                        = "role";

    /** Host name of this replicator. */
    public static final String HOST                        = "host";

    public static final String DATASERVER_HOST             = "dataServerHost";

    /** Seconds since replicator has started. */
    public static final String UPTIME_SECONDS              = "uptimeSeconds";

    /** Seconds that replicator has been in its current state. */
    public static final String TIME_IN_STATE_SECONDS       = "timeInStateSeconds";

    /** Current time on replicator expressed as milliseconds since Jan 1, 1970. */
    public static final String CURRENT_TIME_MILLIS         = "currentTimeMillis";

    /** Current replicator state. */
    public static final String STATE                       = "state";

    /** Pending replicator state following current state transition. */
    public static final String TRANSITIONING_TO            = "transitioningTo";

    /** Source ID used to mark events for this replicator. */
    public static final String SOURCEID                    = "sourceId";

    /** Replication database user this replicator. */
    public static final String USER                        = "user";

    /** Replication database user's password for this replicator. */
    public static final String PASSWORD                    = "password";

    /** Value of last port used by the replicator. */
    public static final String MAX_PORT                    = "maxPort";

    /** Current exception that caused error, if there is one. */
    public static final String PENDING_EXCEPTION_MESSAGE   = "pendingExceptionMessage";

    /** Current error code, if there is one. */
    public static final String PENDING_ERROR_CODE          = "pendingErrorCode";

    /** Current error, if there is one. */
    public static final String PENDING_ERROR               = "pendingError";

    /** Current failed log sequence number or -1 if there is none. */
    public static final String PENDING_ERROR_SEQNO         = "pendingErrorSeqno";

    /** Current failed event ID or null if there is none. */
    public static final String PENDING_ERROR_EVENTID       = "pendingErrorEventId";

    /** Total number of automatic recovery attempts following errors. */
    public static final String AUTO_RECOVERY_TOTAL         = "autoRecoveryTotal";

    /** Set to true if auto-recovery is enabled. */
    public static final String AUTO_RECOVERY_ENABLED       = "autoRecoveryEnabled";

    /** Pending offline requests in text form. */
    public static final String OFFLINE_REQUESTS            = "offlineRequests";

    public static final String RMI_PORT                    = "rmiPort";

    /** URL to connect to underlying data source replicator serves. */
    static public final String RESOURCE_JDBC_URL           = "resourceJdbcUrl";

    /** Class name of Java data source. */
    static public final String RESOURCE_JDBC_DRIVER        = "resourceJdbcDriver";

    /** Class name of Java data source. */
    static public final String RESOURCE_DATASERVER_HOST    = "resourceDataServerHost";

    /**
     * Resource precedence for failover, which should be an integer > 0 or -1 to
     * indicate do not fail over to this resource.
     */
    public static final String RESOURCE_PRECEDENCE         = "resourcePrecedence";

    /** DBMS vendor string for this data source */
    static public final String RESOURCE_VENDOR             = "resourceVendor";

    /** Directory where transaction logs are to be found */
    static public final String RESOURCE_LOGDIR             = "resourceLogDir";

    /** Directory where transaction logs are to be found */
    static public final String RESOURCE_DISK_LOGDIR        = "resourceDiskLogDir";

    /** Pattern used to identify logs */
    static public final String RESOURCE_LOGPATTERN         = "resourceLogPattern";

    /** Port on which apps access resource */
    static public final String RESOURCE_PORT               = "resourcePort";

    /**
     * Log sequence number type to allow managers to figure out how to sort
     * values.
     */
    public static final String SEQNO_TYPE                  = "seqnoType";

    /**
     * Denotes a numeric log sequence number type that is convertible to a Java
     * Long type.
     */
    public static final String SEQNO_TYPE_LONG             = "java.lang.Long";

    /**
     * Denotes a string log sequence number type whose values are comparable
     * strings.
     */
    public static final String SEQNO_TYPE_STRING           = "java.lang.String";

    /**
     * Contains the names of enabled extensions (e.g., sharding) on this
     * replicator.
     */
    public static final String EXTENSIONS                  = "extensions";

    /**
     * Denotes the source of the head extractor.
     */
    public static final String PIPELINE_SOURCE             = "pipelineSource";

    /**
     * Denotes the source of the head extractor.
     */
    public static final String SSL_LOG_CONNECTION          = "sslLogConnection";

    /**
     * Denotes the number of channels for parallel apply.
     */
    public static final String CHANNELS                    = "channels";

    /**
     * Denotes the time zone used by the replicator. 
     */
    public static final String TIME_ZONE                    = "timezone";

    /**
     * Denotes the replicator version.
     */
    public static final String VERSION                     = "version";

    // Default values
    public static final long   DEFAULT_LATEST_EPOCH_NUMBER = -1;
    public static final String DEFAULT_LAST_EVENT_ID       = "0:0";
}