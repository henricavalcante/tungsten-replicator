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
 * Initial developer(s): Gilles Rayrat.
 * Contributor(s):
 */

package com.continuent.tungsten.common.cluster.resource.notification;

import com.continuent.tungsten.common.config.TungstenProperties;

/**
 * Additional status values dedicated to replicator instances
 * 
 * @author <a href="mailto:gilles.rayrat@continuent.com">Gilles Rayrat</a>
 * @version 1.0
 */
public class ReplicatorStatus extends DataSourceStatus
{
    /**
     * Creates a new <code>ReplicatorStatus</code> object filling in additional
     * latency information
     * 
     * @param type Type of RouterResource
     * @param name Possibly unique identifier for this resource
     * @param state Last known state
     * @param host hostname on which this replicator is executing
     * @param role slave or master
     * @param precedence precedence to use for failover
     * @param service service to which this datasource belongs
     * @param url url string used to connect to this datasource
     * @param driver driver as a string used for connection to this datasource
     * @param vendor vendor for driver
     * @param status arbitrary set of metrics and properties provided by the
     *            replicator
     */
    public ReplicatorStatus(String type, String name, String state,
            String host, String role, int precedence, String service,
            String url, String driver, String vendor, TungstenProperties status)
    {
        super(type, name, state, host, role, precedence, service, url, driver,
                vendor);
        setProperties(status.map());
    }
}
