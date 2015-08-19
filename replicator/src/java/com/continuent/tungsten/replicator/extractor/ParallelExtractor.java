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

package com.continuent.tungsten.replicator.extractor;


/**
 * Denotes an extractor that extends normal Extractor capabilities to allow
 * parallel operation.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface ParallelExtractor extends Extractor
{
    /**
     * Sets the ID of the task using this extractor. This method is called prior
     * to invoking the configure() method.
     * 
     * @param id Task ID
     * @see #configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void setTaskId(int id);

    /**
     * Returns the store name on which this extractor operates. This is used to
     * implement orderly shutdown and synchronize waits.
     */
    public String getStoreName();
}