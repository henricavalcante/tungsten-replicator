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

import com.continuent.tungsten.common.config.TungstenProperties;

public class SQLRouter extends Resource
{

    private static final long serialVersionUID = 8153881753668230575L;
    private int               port             = 0;
    private String            host             = null;
    private String            role             = null;
    // the source for the THL for this replicator - valid if it is a slave.
    private String            source           = null;
    private String            dataServiceName  = null;

    public SQLRouter(TungstenProperties props)
    {
        super(ResourceType.SQLROUTER, props.getString("name", "router", true));
        props.applyProperties(this, true);
    }

    /**
     * @return the port
     */
    public int getPort()
    {
        return port;
    }

    /**
     * @param port the port to set
     */
    public void setPort(int port)
    {
        this.port = port;
    }

    /**
     * @return the host
     */
    public String getHost()
    {
        return host;
    }

    /**
     * @param host the host to set
     */
    public void setHost(String host)
    {
        this.host = host;
    }

    /**
     * @return the role
     */
    public String getRole()
    {
        return role;
    }

    /**
     * @param role the role to set
     */
    public void setRole(String role)
    {
        this.role = role;
    }

    /**
     * @return the source
     */
    public String getSource()
    {
        return source;
    }

    /**
     * @param source the source to set
     */
    public void setSource(String source)
    {
        this.source = source;
    }

    public String getDataServiceName()
    {
        return dataServiceName;
    }

    public void setDataServiceName(String dataServiceName)
    {
        this.dataServiceName = dataServiceName;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    @Override
    public String toString()
    {
        return describe(false);
    }

}
