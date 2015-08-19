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
 * Provides utilities for resource status specific properties, specialized in
 * data source representation
 * 
 * @author <a href="mailto:gilles.rayrat@continuent.com">Gilles Rayrat</a>
 * @version 1.0
 */
public class HostStatus extends ResourceStatus
{
    public static final String PROPERTY_KEY_CLUSTER  = "dataServiceName";
    public static final String PROPERTY_KEY_HOST     = "host";
    public static final String PROPERTY_KEY_UPTIME   = "uptime";
    public static final String PROPERTY_LOAD_AVG_1   = "loadAverage1";
    public static final String PROPERTY_LOAD_AVG_5   = "loadAverage5";
    public static final String PROPERTY_LOAD_AVG_15  = "loadAverage5";
    public static final String PROPERTY_KEY_CPUCOUNT = "cpuCount";

    private int                cpuCount              = 1;
    private double             loadAverage1          = 0.0;
    private double             loadAverage5          = 0.0;
    private double             loadAverage15         = 0.0;
    private String             uptime                = null;
    private String             host                  = null;
    private String             service               = null;

    public HostStatus(String type, String name, String state,
            String clusterName, String host, String uptime, int cpuCount,
            double loadAverage1, double loadAverage5, double loadAverage15)
    {
        super(type, name, state, null);
        TungstenProperties hostProperties = new TungstenProperties();
        hostProperties.setString(PROPERTY_KEY_CLUSTER, clusterName);
        hostProperties.setString(PROPERTY_KEY_HOST, host);
        hostProperties.setString(PROPERTY_KEY_UPTIME, uptime);
        hostProperties.setInt(PROPERTY_KEY_CPUCOUNT, cpuCount);
        hostProperties.setDouble(PROPERTY_LOAD_AVG_1, loadAverage1);
        hostProperties.setDouble(PROPERTY_LOAD_AVG_5, loadAverage5);
        hostProperties.setDouble(PROPERTY_LOAD_AVG_15, loadAverage5);

    }

    public int getCpuCount()
    {
        return cpuCount;
    }

    public String getUptime()
    {
        return uptime;
    }

    public String getHost()
    {
        return host;
    }

    public String getService()
    {
        return service;
    }

    public double getLoadAverage1()
    {
        return loadAverage1;
    }

    public double getLoadAverage5()
    {
        return loadAverage5;
    }

    public double getLoadAverage15()
    {
        return loadAverage15;
    }
}
