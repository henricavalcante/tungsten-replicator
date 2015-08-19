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
 * Initial developer(s):
 * Contributor(s):
 */

package com.continuent.tungsten.common.cluster.resource;

import java.io.Serializable;
import java.util.Map;

import com.continuent.tungsten.common.config.TungstenProperties;

public class DataServer extends Resource implements Serializable
{
    private static final long  serialVersionUID = 8153881753668230575L;
    private String             host             = "";
    private String             vendor           = "";
    private String             clusterName      = "";

    private long               lastResponseTime = 0;
    private long               maxResponseTime  = 0;
    private long               minResponseTime  = 0;

    public static final String HOST             = "host";

    public DataServer(TungstenProperties props)
    {
        super(ResourceType.DATASERVER, props.getString("name", "unknown", true));
        props.applyProperties(this, true);
    }

    /**
     * Creates a new <code>DataServer</code> object
     * 
     * @param name
     * @param host
     * @param vendor
     * @param clusterName
     */
    public DataServer(String name, String host, String vendor,
            String clusterName)
    {
        super(ResourceType.DATASERVER, name);
        this.host = host;
        this.vendor = vendor;
        this.clusterName = clusterName;
    }

    public DataServer(String key, String host)
    {
        super(ResourceType.DATASERVER, key);
        this.host = host;
    }

    public String getVendor()
    {
        if (vendor == null)
            return "";

        return vendor;
    }

    public void setVendor(String vendor)
    {
        this.vendor = vendor;
    }

    /**
     * Update a given datastore with values from a different datastore
     * 
     * @param ds
     */
    public void update(DataServer ds)
    {
        synchronized (this)
        {
            this.setName(ds.getName());
            this.setVendor(ds.getVendor());
            this.setClusterName(ds.getClusterName());
            this.notifyAll();
        }
    }

    public TungstenProperties toProperties()
    {
        TungstenProperties props = new TungstenProperties();

        props.setString("name", getName());
        props.setString("vendor", getVendor());
        props.setString("clusterName", getClusterName());
        props.setString("host", getHost());
        return props;
    }

    /**
     * 
     * @return properties representing this dataserver
     */
    public Map<String, String> toMap()
    {
        return toProperties().hashMap();
    }

    /**
     * Creates a new <code>DataSource</code> object
     * 
     * @param dsProperties
     */
    public DataServer(Map<String, String> dsProperties)
    {
        set(dsProperties);
    }

    public void set(Map<String, String> dsProperties)
    {
        TungstenProperties props = new TungstenProperties(dsProperties);
        props.applyProperties(this, true);
    }

    /**
     * Returns the clusterName value.
     * 
     * @return Returns the clusterName.
     */
    public String getClusterName()
    {
        return clusterName;
    }

    /**
     * Sets the clusterName value.
     * 
     * @param clusterName The clusterName to set.
     */
    public void setClusterName(String clusterName)
    {
        this.clusterName = clusterName;
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

    public long getLastResponseTime()
    {
        return lastResponseTime;
    }

    public void setLastResponseTime(long lastResponseTime)
    {
        this.lastResponseTime = lastResponseTime;
    }

    public long getMaxResponseTime()
    {
        return maxResponseTime;
    }

    public void setMaxResponseTime(long maxResponseTime)
    {
        this.maxResponseTime = maxResponseTime;
    }

    public long getMinResponseTime()
    {
        return minResponseTime;
    }

    public void setMinResponseTime(long minResponseTime)
    {
        this.minResponseTime = minResponseTime;
    }

}
