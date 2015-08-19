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

package com.continuent.tungsten.replicator.filter;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.plugin.ReplicatorPlugin;

/**
 * 
 * This class defines a Filter
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public interface Filter extends ReplicatorPlugin
{
    /**
     * Filter the event. Filters may transform the event or return null if the
     * event should be discarded. Filters must be prepared to be interrupted,
     * which mechanism is used to cancel processing.
     * <p>
     * 
     * @param event An event to be filtered
     * @return Filtered ReplDBMSEvent or null
     * @throws ReplicatorException Thrown if there is a processing error
     * @throws InterruptedException Must be thrown if the filter is interrupted
     *             or the replicator may hang
     */
    public ReplDBMSEvent filter(ReplDBMSEvent event)
            throws ReplicatorException, InterruptedException;
}
