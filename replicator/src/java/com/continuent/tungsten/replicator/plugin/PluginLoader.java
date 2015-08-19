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

import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * 
 * This class defines a PluginLoader
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class PluginLoader
{
    /**
     * Load plugin implementation.
     * 
     * @param name The name of the plugin implementation class to be loaded.
     * @return new plugin
     * @throws ReplicatorException
     */
    static public ReplicatorPlugin load(String name) throws ReplicatorException
    {
        if (name == null)
            throw new PluginException("Unable to load plugin with null name");
        try
        {
            return (ReplicatorPlugin) Class.forName(name).newInstance();
        }
        catch (Exception e)
        {
            throw new PluginException(e);
        }
    }

    /**
     * Load plugin class.
     * 
     * @param name The name of the plugin implementation class to be loaded.
     * @return new plugin class
     * @throws ReplicatorException
     */
    static public Class<?> loadClass(String name) throws ReplicatorException
    {
        if (name == null)
            throw new PluginException("Unable to load plugin with null name");
        try
        {
            return (Class<?>) Class.forName(name);
        }
        catch (Exception e)
        {
            throw new PluginException(e);
        }
    }
}
