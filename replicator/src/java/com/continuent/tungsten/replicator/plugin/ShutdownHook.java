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

import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * This interface denotes a replicator plugin that has a shutdown method that
 * should be called to ensure the plugin will stop. For instance, this interface
 * can be used to break out of a blocking read on a socket by closing the socket
 * object.
 */
public interface ShutdownHook
{
    /**
     * Shut down component. This is called after the task interrupt and should
     * ensure the component responds correctly to an interrupt.
     * 
     * @throws ReplicatorException Thrown if shutdown is unsuccessful
     */
    public void shutdown(PluginContext context) throws ReplicatorException,
            InterruptedException;
}