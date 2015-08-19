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
package com.continuent.tungsten.replicator.backup;

/**
 * 
 * This class defines a BackupPlugin.  BackupPlugin instances are lightweight
 * objects created for a single backup operation.  Here is the lifecycle: 
 * 
 * <li>Instantiate plug-in from class name</li>
 * <li>Call setters on plug-in instance and load property names</li>
 * <li>Call configure() to signal configuration is complete</li>
 * <li>Call backup operation</li>
 * <li>Call release() to free resources</li>
 * </ol>
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface BackupPlugin
{
    /**
     * Complete plug-in configuration.  This is called after setters are 
     * invoked. 
     * 
     * @throws ReplicatorException Thrown if configuration is incomplete
     * or fails
     */
    public void configure() throws BackupException;

    /**
     * 
     * Release all resources used by plug-in.  This is called before the
     * plug-in is deallocated.  
     * 
     * @throws ReplicatorException Thrown if resource deallocation fails
     */
    public void release() throws BackupException;
}