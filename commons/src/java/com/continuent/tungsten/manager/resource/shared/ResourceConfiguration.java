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
 * Contributor(s): Linas Virbalas
 */

package com.continuent.tungsten.manager.resource.shared;

import com.continuent.tungsten.common.cluster.resource.Resource;
import com.continuent.tungsten.common.cluster.resource.ResourceType;
import com.continuent.tungsten.common.config.TungstenProperties;

public class ResourceConfiguration extends Resource
{

    private TungstenProperties properties       = new TungstenProperties();
    /**
     * 
     */
    private static final long  serialVersionUID = 1L;

    public ResourceConfiguration()
    {
        super(ResourceType.CONFIGURATION, "UNKNOWN");
        init();

    }

    public ResourceConfiguration(String name)
    {
        super(ResourceType.CONFIGURATION, name);
        init();
    }

    /**
     * Creates a new <code>ResourceConfiguration</code> object with underlying
     * TungstenProperties.
     */
    public ResourceConfiguration(String name, TungstenProperties properties)
    {
        super(ResourceType.CONFIGURATION, name);
        this.properties = properties;
        init();
    }

    /**
     * Returns TungstenProperties corresponding to this
     * <code>ResourceConfiguration</code>.
     */
    public TungstenProperties getProperties()
    {
        return properties;
    }

    private void init()
    {
        this.childType = ResourceType.ANY;
        this.isContainer = false;
    }

    public String describe(boolean detailed)
    {
        return TungstenProperties.formatProperties(
                getName() + " configuration", properties, "");
    }

}
