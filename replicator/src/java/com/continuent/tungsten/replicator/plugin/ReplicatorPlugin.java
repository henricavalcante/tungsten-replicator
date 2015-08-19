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
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.replicator.plugin;

import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * This class defines a ReplicatorPlugin. Replicator plug-ins have the following
 * life cycle:
 * <p>
 * <ol>
 * <li>Instantiate plug-in from class name</li>
 * <li>Call setters on plug-in instance and load property names</li>
 * <li>Call configure() to signal configuration is complete</li>
 * <li>Call prepare() to create resources for operation</li>
 * <li>(Type-specific plug-in method calls)</li>
 * <li>Call release() to free resources</li>
 * </ol>
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public interface ReplicatorPlugin
{
    /**
     * Complete plug-in configuration. This is called after setters are invoked
     * at the time that the replicator goes through configuration.
     * 
     * @throws ReplicatorException Thrown if configuration is incomplete or
     *             fails
     */
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException;

    /**
     * Prepare plug-in for use. This method is assumed to allocate all required
     * resources. It is called before the plug-in performs any operations.
     * 
     * @throws ReplicatorException Thrown if resource allocation fails
     */
    public void prepare(PluginContext context) throws ReplicatorException,
            InterruptedException;

    /**
     * Release all resources used by plug-in. This is called before the plug-in
     * is deallocated.
     * 
     * @throws ReplicatorException Thrown if resource deallocation fails
     */
    public void release(PluginContext context) throws ReplicatorException,
            InterruptedException;
}
