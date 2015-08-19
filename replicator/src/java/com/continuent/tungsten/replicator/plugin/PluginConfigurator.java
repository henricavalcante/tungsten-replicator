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
 * Initial developer(s): Teemu Ollakka
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.plugin;

import java.lang.reflect.Method;

import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * This class defines a PluginConfigurator
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class PluginConfigurator
{
    /**
     * 
     * Call setter method for given ReplicatorPlugin.
     * 
     * @param plugin ReplicatorPlugin instance
     * @param name The name of the setter method to be called
     * @param value Argument to be passed for setter method
     * @throws ReplicatorException
     */
    static public void setParameter(ReplicatorPlugin plugin, String name,
            Object value) throws ReplicatorException
    {
        Method[] methods = plugin.getClass().getMethods();
        for (Method m : methods)
        {
            if (m.getName().equals(name) == false)
                continue;
            Class<?>[] types = m.getParameterTypes();
            if (types.length != 1)
                throw new PluginException("Method " + name + " for class "
                        + value.getClass() + " not found");
            try
            {
                m.invoke(plugin, value);
            }
            catch (Exception e)
            {
                throw new PluginException("Error in method invocation", e);
            }
            return;
        }
        throw new PluginException("Method " + name + " not found");
    }

    /**
     * 
     * Call getter method for given replicator plugin.
     * 
     * @param plugin ReplicatorPlugin instance
     * @param name The name of the getter method to be called
     * @return Return value of getter method
     * @throws ReplicatorException
     */
    static public Object getParameter(ReplicatorPlugin plugin, String name)
            throws ReplicatorException
    {
        Method[] methods = plugin.getClass().getMethods();
        for (Method m : methods)
        {
            if (m.getName().equals(name) == false)
                continue;
            Class<?>[] types = m.getParameterTypes();
            if (types.length != 0)
                throw new PluginException("Method " + name + " not found");
            try {
                return m.invoke(plugin);
            }
            catch (Exception e)
            {
                throw new PluginException("Error in method invocation", e);
            }
        }
        throw new PluginException("Method " + name + " not found");
    }
}
