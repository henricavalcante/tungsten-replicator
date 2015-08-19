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
 * Contributor(s): Edward Archibald.
 */

package com.continuent.tungsten.common.cluster.resource.notification;

import com.continuent.tungsten.common.cluster.resource.DataSourceRole;
import com.continuent.tungsten.common.config.TungstenProperties;

/**
 * Provides utilities for resource status specific properties, specialized in
 * data source representation
 * 
 * @author <a href="mailto:gilles.rayrat@continuent.com">Gilles Rayrat</a>
 * @version 1.0
 */
public class DataServerStatus extends ResourceStatus
{
    public static final String PROPERTY_KEY_SERVICE    = "dataServiceName";
    public static final String PROPERTY_KEY_HOST       = "host";
    public static final String PROPERTY_KEY_ROLE       = "role";
    public static final String PROPERTY_KEY_PRECEDENCE = "precedence";
    public static final String PROPERTY_KEY_URL        = "url";
    public static final String PROPERTY_KEY_DRIVER     = "driver";
    public static final String PROPERTY_KEY_VENDOR     = "vendor";

    /**
     * Inner class enumerating the different possible RouterResource Roles<br>
     * Roles are defined as strings so that they can be sent with native java
     * functions by the notifier
     * 
     * @author <a href="mailto:gilles.rayrat@continuent.com">Gilles Rayrat</a>
     * @version 1.0
     */
    public static final class Roles
    {
        public static final String SLAVE   = DataSourceRole.slave.toString();
        public static final String MASTER  = DataSourceRole.master.toString();
        public static final String UNKNOWN = DataSourceRole.undefined
                                                   .toString();
    }

    /**
     * Creates a new <code>DataSourceStatus</code> object filling in additional
     * role, service, url and driver information
     * 
     * @param type Type of RouterResource
     * @param name Possibly unique identifier for this resource
     * @param state Last known state
     * @param host hostname on which this data source runs
     * @param role slave or master
     * @param precedence used for failover
     * @param service service to which this datasource belongs
     * @param url url string used to connect to this datasource
     * @param driver driver as a string used for connection to this datasource
     * @param vendor vendor of the driver
     */
    public DataServerStatus(String type, String name, String state,
            String host, String role, int precedence, String service,
            String url, String driver, String vendor)
    {
        super(type, name, state, null);
        TungstenProperties datasourceProperties = new TungstenProperties();
        datasourceProperties.setString(PROPERTY_KEY_SERVICE, service);
        datasourceProperties.setString(PROPERTY_KEY_HOST, host);
        datasourceProperties.setString(PROPERTY_KEY_ROLE, role);
        datasourceProperties.setInt(PROPERTY_KEY_PRECEDENCE, precedence);
        datasourceProperties.setString(PROPERTY_KEY_URL, url);
        datasourceProperties.setString(PROPERTY_KEY_DRIVER, driver);
        datasourceProperties.setString(PROPERTY_KEY_VENDOR, vendor);
        setProperties(datasourceProperties.map());
    }
}
