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

package com.continuent.tungsten.replicator.scripting;

import java.util.Map;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.plugin.ReplicatorPlugin;

/**
 * Denotes a class capable of executing a batch load script. This interface
 * conforms to conventions for replicator plugins.
 */
public interface ScriptExecutor extends ReplicatorPlugin
{
    /** Sets the script name. */
    public void setScript(String script);

    /** Returns the script name. */
    public String getScript();

    /** Sets the default data source name, if it exists. */
    public void setDefaultDataSourceName(String name);

    /** Sets a map of objects to be inserted into the executor context. */
    public void setContextMap(Map<String, Object> contextMap);

    /**
     * Register a method name. This must be called prior to invoking any
     * individual script method.
     * 
     * @param method Name of the method in the script
     * @return True if the method is found and registered, otherwise false
     * @throws ReplicatorException Thrown if registration fails
     */
    public boolean register(String method) throws ReplicatorException;

    /**
     * Executes a registered script method including a single optional argument.
     * 
     * @param method Name of the method in the script
     * @param argument Argument to pass in during method invocation
     * @return An object or null if the method does not return a value
     * @throws ReplicatorException Thrown if execute operation fails
     */
    public Object execute(String method, Object argument)
            throws ReplicatorException;
}