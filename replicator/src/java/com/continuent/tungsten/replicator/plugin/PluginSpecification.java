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
 * Initial developer(s): Robert Hodges
 * Contributor(s): 
 */

package com.continuent.tungsten.replicator.plugin;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.filter.FilterManualProperties;

/**
 * Specification for a component, including the implementation class and input
 * properties, and utility methods to manage the component life cycle.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class PluginSpecification
{
    private final String             prefix;
    private final String             name;
    private final Class<?>           pluginClass;
    private final TungstenProperties properties;

    public PluginSpecification(String prefix, String name,
            Class<?> pluginClass, TungstenProperties properties)
    {
        this.prefix = prefix;
        this.name = name;
        this.pluginClass = pluginClass;
        this.properties = properties;
    }

    public String getPrefix()
    {
        return prefix;
    }

    public String getName()
    {
        return name;
    }

    public Class<?> getPluginClass()
    {
        return pluginClass;
    }

    public TungstenProperties getProperties()
    {
        return properties;
    }

    /**
     * Instantiate the plugin and assign properties. 
     * 
     * @throws PluginException Thrown if instantiation fails
     */
    public ReplicatorPlugin instantiate(int id) throws ReplicatorException
    {
        ReplicatorPlugin plugin = PluginLoader.load(pluginClass.getName());
        if (plugin instanceof FilterManualProperties)
            ((FilterManualProperties) plugin).setConfigPrefix(prefix);
        else
            properties.applyProperties(plugin);
        return plugin;
    }
}
