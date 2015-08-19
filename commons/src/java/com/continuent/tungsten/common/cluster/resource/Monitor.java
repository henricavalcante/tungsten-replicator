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

public class Monitor extends Resource implements Serializable
{
    private static final long  serialVersionUID = 8153881753668230575L;

    private String             memberName       = "";
    private String             clusterName      = "";

    public static final String MEMBERNAME       = "memberName";

    public Monitor(TungstenProperties props)
    {
        super(ResourceType.MONITOR, props.getString("name", "unknown", true));
        props.applyProperties(this, true);
    }

    /**
     * Creates a new <code>Monitor</code> object
     * 
     * @param name
     * @param memberName
     * @param clusterName
     */
    public Monitor(String name, String memberName, String clusterName)
    {
        super(ResourceType.MONITOR, name);
        this.memberName = memberName;
        this.clusterName = clusterName;
    }

    public Monitor(String key, String memberName)
    {
        super(ResourceType.MONITOR, key);
        this.memberName = memberName;
    }

    /**
     * Update a given monitor with values from a different monitor
     * 
     * @param ds
     */
    public void update(Monitor ds)
    {
        synchronized (this)
        {
            this.setName(ds.getName());
            this.setClusterName(ds.getClusterName());
            this.notifyAll();
        }
    }

    public TungstenProperties toProperties()
    {
        TungstenProperties props = new TungstenProperties();

        props.setString("name", getName());
        props.setString("clusterName", getClusterName());
        props.setString("memberName", getMemberName());
        return props;
    }

    /**
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
    public Monitor(Map<String, String> dsProperties)
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
     * Returns the memberName value.
     * 
     * @return Returns the memberName.
     */
    public String getMemberName()
    {
        return memberName;
    }

    /**
     * Sets the memberName value.
     * 
     * @param memberName The memberName to set.
     */
    public void setMemberName(String memberName)
    {
        this.memberName = memberName;
    }
}
