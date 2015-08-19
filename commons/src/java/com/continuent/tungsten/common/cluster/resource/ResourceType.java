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
 * Initial developer(s): Ed Archibald
 * Contributor(s): 
 */

package com.continuent.tungsten.common.cluster.resource;

import java.io.Serializable;

public enum ResourceType implements Serializable
{
    ROOT, /* the root resource of any resource tree */
    EVENT, /* any application */
    CLUSTER, /* a cluster */
    MANAGER, /* a manager of the cluster */
    MEMBER, /* a cluster member */
    FOLDER, /* a general purpose folder */
    QUEUE, /* The resource represents an instance of a queue */
    CONFIGURATION, /*
                    * any type of configuration that can be represented as
                    * properties
                    */
    PROCESS, /* a JVM/MBean server */
    RESOURCE_MANAGER, /*
                       * a class that is exported as a JMX MBean for a specific
                       * component
                       */
    POLICY_MANAGER, /* represents a policy manager */
    OPERATION, /* an operation exported by a JMX MBean */
    DATASOURCE, /* a sql-router datasource */
    MONITOR, DATASERVER, /* a database server */
    HOST, /* a node in a cluster */
    SQLROUTER, /* a sql-router component */
    REPLICATOR, /* a replicator component */
    REPLICATION_SERVICE, /* a single service in a replicator */
    SERVICE_MANAGER, /* a tungsten-manager */
    SERVICE, DIRECTORY, /* a Directory instance */
    DIRECTORY_SESSION, UNDEFINED, EXTENSION, NONE, ANY
    /* any resource */
}
