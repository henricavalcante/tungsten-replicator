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
 * Contributor(s): Alex Yurchenko, Linas Virbalas, Stephane Giron, Robert Hodges
 */

package com.continuent.tungsten.replicator.conf;

/**
 * This class defines a ReplicatorConf
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class ReplicatorConf
{
    /** Applier name. */
    static public final String   OPEN_REPLICATOR                       = "replicator.plugin";

    /** Replicator role - slave or master. */
    static public final String   ROLE                                  = "replicator.role";
    static public final String   ROLE_MASTER                           = "master";
    static public final String   ROLE_SLAVE                            = "slave";

    /** Whether we are operating normally or in slave takeover mode. */
    static public final String   NATIVE_SLAVE_TAKEOVER                 = "replicator.nativeSlaveTakeover";
    static public final String   NATIVE_SLAVE_TAKEOVER_DEFAULT         = "false";

    /** URI to which we connect. */
    static public final String   MASTER_CONNECT_URI                    = "replicator.master.connect.uri";

    /** Port at which to start allocating master listeners */
    static public final String   MASTER_LISTEN_PORT_START              = "replicator.masterListenPortStart";

    /** Port at which to start allocating master listeners */
    static public final String   SERVICE_RMI_PORT_START                = "replicator.serviceRMIPortStart";

    /** URI on which we listen. */
    static public final String   MASTER_LISTEN_URI                     = "replicator.master.listen.uri";

    /**
     * Should the master checks that its THL is in sync with its database before
     * starting. By default, it is turned on
     */
    static public final String   MASTER_THL_CHECK                      = "replicator.master.thl_check";
    static public final String   MASTER_THL_CHECK_DEFAULT              = "true";

    /** Whether to go online automatically at startup time. */
    static public final String   AUTO_ENABLE                           = "replicator.auto_enable";
    static public final String   AUTO_ENABLE_DEFAULT                   = "false";

    /** How many times to attempt to go online automatically after an error. */
    static public final String   AUTO_RECOVERY_MAX_ATTEMPTS            = "replicator.autoRecoveryMaxAttempts";
    static public final String   AUTO_RECOVERY_MAX_ATTEMPTS_DEFAULT    = "0";

    /**
     * How long to delay when processing an online request initiated by
     * auto-recovery.
     */
    static public final String   AUTO_RECOVERY_DELAY_INTERVAL          = "replicator.autoRecoveryDelayInterval";
    static public final String   AUTO_RECOVERY_DELAY_INTERVAL_DEFAULT  = "300s";

    /**
     * How long a replicator must be online to reset the count of recovery
     * attempts.
     */
    static public final String   AUTO_RECOVERY_RESET_INTERVAL          = "replicator.autoRecoveryResetInterval";
    static public final String   AUTO_RECOVERY_RESET_INTERVAL_DEFAULT  = "300s";

    /**
     * Whether to reposition data extraction automatically on a master when the
     * source_id changes.
     */
    static public final String   AUTO_MASTER_REPOSITIONING             = "replicator.repositionOnSourceIdChange";
    static public final String   AUTO_MASTER_REPOSITIONING_DEFAULT     = "true";

    /** Whether to automatically provision this server at startup time. */
    static public final String   AUTO_PROVISION                        = "replicator.auto_provision";
    static public final String   AUTO_PROVISION_DEFAULT                = "false";

    /** Whether to automatically backup this server at startup time. */
    static public final String   AUTO_BACKUP                           = "replicator.auto_backup";
    static public final String   AUTO_BACKUP_DEFAULT                   = "false";

    /** Whether to go online automatically at startup time. */
    static public final String   DETACHED                              = "replicator.detached";
    static public final String   DETACHED_DEFAULT                      = "false";

    /** Source Identifier for THL and ReplDBMSEvents */
    static public final String   SOURCE_ID                             = "replicator.source_id";
    static public final String   SOURCE_ID_DEFAULT                     = "tungsten";

    /** Site name to which replicator belongs */
    static public final String   SITE_NAME                             = "site.name";
    static public final String   SITE_NAME_DEFAULT                     = "default";

    /** Cluster name to which replicator belongs */
    static public final String   CLUSTER_NAME                          = "cluster.name";
    static public final String   CLUSTER_NAME_DEFAULT                  = "default";

    /** Service name to which replication service belongs */
    static public final String   SERVICE_NAME                          = "service.name";

    /** Host where the replicator is running */
    static public final String   REPLICATOR_HOST                       = "replicator.host";

    /** Type of replicator service: local or remote. */
    static public final String   SERVICE_TYPE                          = "replicator.service.type";
    static public final String   SERVICE_TYPE_DEFAULT                  = "local";

    /** Out of sequence policy */
    static public final String   OOS_POLICY                            = "replicator.oos_policy";

    /** Where Replicator stores metadata */
    static public final String   METADATA_SCHEMA                       = "replicator.schema";
    static public final String   METADATA_SCHEMA_DEFAULT               = "tungsten_default";
    public static final String   METADATA_TABLE_TYPE                   = "replicator.table.engine";
    public static final String   METADATA_TABLE_TYPE_DEFAULT           = "";

    /** Shard assignment policies. */
    static public final String   SHARD_DEFAULT_DB_USAGE                = "replicator.shard.default.db";
    static public final String   SHARD_DEFAULT_DB_USAGE_DEFAULT        = "stringent";

    /** Whether to log slave updates. */
    static public final String   LOG_SLAVE_UPDATES                     = "replicator.log.slave.updates";
    static public final String   LOG_SLAVE_UPDATES_DEFAULT             = "false";

    /** Whether slave updates can assume a privileged account. */
    static public final String   PRIVILEGED_SLAVE                      = "replicator.privileged.slave";
    static public final String   PRIVILEGED_SLAVE_DEFAULT              = "true";

    /** Whether master extracts can assume a privileged account. */
    static public final String   PRIVILEGED_MASTER                     = "replicator.privileged.master";
    static public final String   PRIVILEGED_MASTER_DEFAULT             = "true";

    /**
     * Whether to allow SQL comments to distinguish statements for different
     * services in multi-master replication. This setting can corrupt statements
     * that contain binary data, double-encoded data, or multiple embedded
     * character sets in the same statement. It must be explicitly enabled. It
     * does not affect row data.
     */
    static public final String   SERVICE_COMMENTS_ENABLED              = "replicator.service.comments";
    static public final String   SERVICE_COMMENTS_ENABLED_DEFAULT      = "false";

    /** RMI port */
    static public final String   RMI_PORT                              = "replicator.rmi_port";
    static public final String   RMI_HOST                              = "replicator.rmi_host";

    /** Extension parameter names. */
    static public final String   EXTENSIONS                            = "replicator.extensions";
    static public final String   EXTENSION_ROOT                        = "replicator.extension";

    /** Pipeline and stage parameter names. */
    static public final String   PIPELINES                             = "replicator.pipelines";
    static public final String   PIPELINE_ROOT                         = "replicator.pipeline";
    static public final String   STAGE                                 = "stage";
    static public final String   STAGE_ROOT                            = "replicator.stage";

    /** Applier parameter names. */
    static public final String   APPLIER                               = "applier";
    static public final String   APPLIER_ROOT                          = "replicator.applier";

    /** Extractor parameter names. */
    static public final String   EXTRACTOR                             = "extractor";
    static public final String   EXTRACTOR_ROOT                        = "replicator.extractor";

    /** Prefix for filter property definitions. */
    static public final String   FILTER                                = "filter";
    static public final String   FILTERS                               = "filters";
    static public final String   FILTER_ROOT                           = "replicator.filter";

    /** Pipeline service parameter names. */
    static public final String   SERVICE                               = "service";
    static public final String   SERVICE_ROOT                          = "replicator.service";

    /** Store parameter names. */
    static public final String   STORE                                 = "store";
    static public final String   STORE_ROOT                            = "replicator.store";

    /** Data source parameter names. */
    static public final String   DATASOURCE                            = "datasource";
    static public final String   DATASOURCE_ROOT                       = "replicator.datasource";

    /** Applier failure policy */
    static public final String   APPLIER_FAILURE_POLICY                = "replicator.applier.failure_policy";
    static public final String   APPLIER_FAILURE_POLICY_DEFAULT        = "stop";

    /** Applier failure policy on zero row updates or deletes */
    static public final String   APPLIER_FAIL_ON_0_ROW_UPDATE          = "replicator.applier.failOnZeroRowUpdate";
    static public final String   APPLIER_FAIL_ON_0_ROW_UPDATE_DEFAULT  = "warn";

    /** Policy when consistency check fails (stop|warn) */
    static public final String   APPLIER_CONSISTENCY_POLICY            = "replicator.applier.consistency_policy";
    static public final String   APPLIER_CONSISTENCY_POLICY_DEFAULT    = "stop";

    /** Extractor failure policy (stop|skip) */
    static public final String   EXTRACTOR_FAILURE_POLICY              = "replicator.extractor.failure_policy";
    static public final String   EXTRACTOR_FAILURE_POLICY_DEFAULT      = "stop";

    /** Should consistency check be sensitive to column names (true|false) */
    static public final String   APPLIER_CONSISTENCY_COL_NAMES         = "replicator.applier.consistency_column_names";
    static public final String   APPLIER_CONSISTENCY_COL_NAMES_DEFAULT = "true";

    /** Should consistency check be sensitive to column types (true|false) */
    static public final String   APPLIER_CONSISTENCY_COL_TYPES         = "replicator.applier.consistency_column_types";
    static public final String   APPLIER_CONSISTENCY_COL_TYPES_DEFAULT = "true";

    /** RMI Defaults */
    static public final String   RMI_DEFAULT_PORT                      = "10000";
    static public final String   RMI_DEFAULT_SERVICE_NAME              = "replicator";
    static public final String   RMI_DEFAULT_HOST                      = "localhost";

    static public final String   THL_DB_URL                            = "replicator.store.thl.url";
    static public final String   THL_DB_USER                           = "replicator.store.thl.user";
    static public final String   THL_DB_PASSWORD                       = "replicator.store.thl.password";

    static public final String   THL_URI                               = "replicator.thl.uri";
    static public final String   THL_URI_DEFAULT                       = "thl://0.0.0.0/";
    static public final String   THL_REMOTE_URI_DEFAULT                = "thl://localhost/";

    static public final String   THL_STORAGE                           = "replicator.thl.storage";

    static public final String   THL_APPLIER_BLOCK_COMMIT_SIZE         = "replicator.thl.applier_block_commit_size";
    static public final String   THL_APPLIER_BLOCK_COMMIT_SIZE_DEFAULT = "0";

    static public final String   THL_SERVER_ACCEPT_TIMEOUT             = "replicator.thl.server.accept.timeout";
    static public final String   THL_SERVER_ACCEPT_TIMEOUT_DEFAULT     = "5000";

    public static final String   THL_PROTOCOL                          = "replicator.thl.protocol";
    public static final String   THL_PROTOCOL_DEFAULT                  = "com.continuent.tungsten.replicator.thl.Connector";
    public static final String   THL_PROTOCOL_BUFFER_SIZE              = "replicator.thl.protocol.buffer_size";
    public static final String   THL_PROTOCOL_BUFFER_SIZE_DEFAULT      = "0";

    static public final String   MONITOR_DETAIL_ENABLED                = "replicator.monitor.detail_enabled";

    /**
     * This information will be used by the sql router to create data sources
     * dynamically. It is also used by Heartbeat on the master side.
     */
    static public final String   RESOURCE_JDBC_URL                     = "replicator.resourceJdbcUrl";
    static public final String   RESOURCE_JDBC_INIT_SCRIPT             = "replicator.resourceJdbcInitScript";

    /** Default value provided to enable unit tests to run. */
    static public final String   RESOURCE_JDBC_URL_DEFAULT             = "jdbc:mysql://localhost/${DBNAME}";
    static public final String   RESOURCE_JDBC_DRIVER                  = "replicator.resourceJdbcDriver";
    static public final String   RESOURCE_PRECEDENCE                   = "replicator.resourcePrecedence";
    static public final String   RESOURCE_PRECEDENCE_DEFAULT           = "99";
    static public final String   RESOURCE_VENDOR                       = "replicator.resourceVendor";
    static public final String   RESOURCE_LOGDIR                       = "replicator.resourceLogDir";
    static public final String   RESOURCE_LOGPATTERN                   = "replicator.resourceLogPattern";
    static public final String   RESOURCE_DISKLOGDIR                   = "replicator.resourceDiskLogDir";
    static public final String   RESOURCE_PORT                         = "replicator.resourcePort";
    static public final String   RESOURCE_DATASERVER_HOST              = "replicator.resourceDataServerHost";

    static public final String   GLOBAL_DB_USER                        = "replicator.global.db.user";
    static public final String   GLOBAL_DB_PASSWORD                    = "replicator.global.db.password";

    /** script based replicator plugin properties */
    static public final String   SCRIPT_ROOT_DIR                       = "replicator.script.root_dir";
    static public final String   SCRIPT_CONF_FILE                      = "replicator.script.conf_file";
    static public final String   SCRIPT_PROCESSOR                      = "replicator.script.processor";

    /**
     * Replicator global time zone and standard default value. These are set in
     * services.properties.
     */
    static public final String   TIME_ZONE                             = "replicator.timezone";
    static public final String   TIME_ZONE_DEFAULT                     = "GMT";

    /**
     * Replicator start option to prevent services from going online
     * automatically
     */
    public static final String   FORCE_OFFLINE                         = "forceOffline";

    /** Dynamically settable property names. */
    static public final String[] DYNAMIC_PROPERTIES                    = {ROLE,
            AUTO_ENABLE, MASTER_CONNECT_URI, AUTO_PROVISION, AUTO_BACKUP};

}
