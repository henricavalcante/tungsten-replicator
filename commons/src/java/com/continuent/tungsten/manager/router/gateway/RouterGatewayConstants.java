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
 * Initial developer(s): Gilles Rayrat
 * Contributor(s): 
 */

package com.continuent.tungsten.manager.router.gateway;

/**
 * Defines static strings used by the router gateway system for exchanging data
 * and commands over the network
 * 
 * @author <a href="mailto:gilles.rayrat@continuent.com">Gilles Rayrat</a>
 * @version 1.0
 */
public class RouterGatewayConstants
{
    // Default gateway port
    public final static int    DEFAULT_GATEWAY_PORT        = 11999;

    // Commands
    public static final char   COMMAND_PING                = 'p';
    public static final char   COMMAND_UPDATE_DS           = 'u';
    public static final char   COMMAND_REMOVE_DS           = 'r';
    public static final char   COMMAND_STATUS              = 's';
    public static final char   COMMAND_STATISTICS          = 't';
    public static final char   COMMAND_CONFIGURE           = 'c';
    public static final char   COMMAND_DIAG                = 'd';
    public static final char   COMMAND_OFFLINE             = 'f';
    public static final char   COMMAND_ONLINE              = 'n';
    public static final char   COMMAND_DATASOURCE_MAP      = 'm';
    public static final char   COMMAND_QUIT                = 'q';
    public static final char   COMMAND_MAINTENANCE_MODE    = 'a';
    public static final char   COMMAND_RESET_CLUSTER_VIEW  = 'e';
    public static final char   COMMAND_RUN_GATEWAY         = 'U';
    public static final char   COMMAND_STOP_GATEWAY        = 'S';
    public static final char   COMMAND_GATEWAY_IS_RUNNING  = 'i';

    // Support for tracing
    public static final char   COMMAND_TRACE_GLOBAL_ENABLE = 'E';
    public static final char   COMMAND_TRACE               = 'T';
    public static final char   COMMAND_TRACE_LIST          = 'L';
    public static final char   COMMAND_TRACE_RESET         = 'R';

    // Property IDs inside Tungsten properties
    public static final String MANAGER_LIST                = "managers";
    public static final String ROUTER_ID                   = "router.id";

    public static final String ERROR                       = "error";
    public static final String SERVICE_NAME                = "serviceName";
    public static final String ROUTER_NAME                 = "memberName";
    public static final String METHOD_NAME                 = "methodName";
    public static final String TIMEOUT_MSECS               = "timeoutMsecs";
    public static final String ARGS                        = "args";
    public static final String RESULT                      = "result";
    public static final String NOTIFICATION_PREFIX         = "notification.";
    public static final String NOTIFICATION_ARGS_PREFIX    = "notificationArgs.";
    public static final String MAINTENANCE_MODE            = "maintenanceMode";
    public static final String FAILSAFE                    = "failSafe";

    public static String commandToText(final char command)
    {
        switch (command)
        {
            case COMMAND_PING :
                return "PING";
            case COMMAND_UPDATE_DS :
                return "UPDATE_DS";
            case COMMAND_REMOVE_DS :
                return "REMOVE_DS";
            case COMMAND_STATUS :
                return "STATUS";
            case COMMAND_STATISTICS :
                return "STATISTICS";
            case COMMAND_CONFIGURE :
                return "CONFIGURE";
            case COMMAND_OFFLINE :
                return "OFFLINE";
            case COMMAND_ONLINE :
                return "ONLINE";
            case COMMAND_DATASOURCE_MAP :
                return "DATASOURCE_MAP";
            case COMMAND_QUIT :
                return "QUIT";
            case COMMAND_MAINTENANCE_MODE :
                return "MAINTENANCE_MODE";
            case COMMAND_RESET_CLUSTER_VIEW :
                return "RESET_CLUSTER_VIEW";
            case COMMAND_TRACE_GLOBAL_ENABLE :
                return "TRACE_GLOBAL_ENABLE";
            case COMMAND_TRACE :
                return "TRACE";
            case COMMAND_TRACE_LIST :
                return "TRACE_LIST";
            case COMMAND_TRACE_RESET :
                return "TRACE_RESET";
            default :
                return "unknown";
        }
    }
}
