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
 * Contributor(s): Robert Hodges, Ludovic Launer
 */

package com.continuent.tungsten.common.config.cluster;

/**
 * This class defines a TSRouterConf
 * 
 * @author <a href="mailto:edward.archibald@continuent.com">Edward Archibald</a>
 * @version 1.0
 */
public class ConfigurationConstants
{
    /** SERVICE WIDE PROPERTIES */
    static public final String CLUSTER_HOME                                                   = "cluster.home";
    static public final String CLUSTER_CONF_DIR                                               = "conf";
    static public final String CLUSTER_DIR                                                    = "cluster";
    static public final String CLUSTER_DEFAULT_NAME                                           = "default";

    static public final String CLUSTER_SITENAME                                               = "siteName";
    static public final String CLUSTER_CLUSTERNAME                                            = "clusterName";
    static public final String CLUSTER_MEMBERNAME                                             = "memberName";
    static public final String CLUSTER_PORT                                                   = "port";
    static public final String CLUSTER_MANAGER_LIST                                           = "managerList";

    static public final String CLUSTER_STATE_MAP_OVERRIDE_PROPS                               = "statemap.properties";
    static public final String CLUSTER_STATE_MAP_DEFAULT_PROPS                                = "statemap.properties.defaults";

    /** SQLROUTER MANAGER */
    static public final String TR_PROPERTIES                                                  = "router.properties";
    static public final String TR_RMI_PORT                                                    = "router.rmi_port";
    static public final String TR_RMI_PORT_DEFAULT                                            = "10999";
    static public final String TR_RMI_DEFAULT_HOST                                            = "localhost";
    static public final String TR_SERVICE_NAME                                                = "router";
    static public final String TR_GW_PORT_DEFAULT                                             = "11999";
    static public final String TR_SERVICES_PROPS                                              = "dataservices.properties";

    /** POLICY MANAGER */
    static public final String PM_PROPERTIES                                                  = "policymgr.properties";
    static public final String PM_RMI_PORT                                                    = "policymgr.rmi_port";
    static public final String PM_NOTIFY_PORT                                                 = "policymgr.notify_port";
    static public final String PM_SERVICE_NAME                                                = "cluster-policy-mgr";

    static public final String PM_RMI_DEFAULT_HOST                                            = "localhost";
    static public final String PM_RMI_PORT_DEFAULT                                            = "10011";
    static public final String PM_NOTIFY_PORT_DEFAULT                                         = "10100";

    static public final int    KEEP_ALIVE_TIMEOUT_DEFAULT                                     = 30000;
    static public final int    KEEP_ALIVE_TIMEOUT_MAX                                         = 300000;
    public static final int    DELAY_BEFORE_OFFLINE_IF_NO_MANAGER_DEFAULT                     = 30;
    public static final int    DELAY_BEFORE_OFFLINE_IF_NO_MANAGER_MAX                         = 60;
    public static final int    DELAY_BEFORE_OFFLINE_IN_MAINTENANCE_MODE_IF_NO_MANAGER_DEFAULT = 5 * 60;
    public static final int    GATEWAY_CONNECT_TIMEOUT_MS_DEFAULT                             = 5000;
    public static final int    GATEWAY_CONNECT_TIMEOUT_MS_MAX                                 = 30000;
    public static final long   READ_COMMAND_RETRY_TIMEOUT_MS_DEFAULT                          = 10000;

}
