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

package com.continuent.tungsten.replicator.filter;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Implements a simple harness to test filters that have minimal dependencies on
 * properties supplied the PluginContext class. Where such dependencies exist
 * users are responsible for supplying a plugin that has suitable properties.
 */
public class FilterVerificationHelper
{
    // Filter to be tested.
    private Filter        filter;

    // Plugin context, which is usually a ReplicatorRuntime.
    private PluginContext context;

    /**
     * Set the replication context, which will be used for life-cycle calls to
     * filter. This is optional. For filters that do not use PluginContext it is
     * fine for this to be null.
     */
    public void setContext(PluginContext context)
    {
        this.context = context;
    }

    /**
     * Assign a filter to be tested. Caller must instantiate and assign
     * properties, then call this method. The help calls configure() and
     * prepare() on the filter instance.
     * 
     * @throws InterruptedException
     * @throws ReplicatorException
     */
    public void setFilter(Filter filter) throws ReplicatorException,
            InterruptedException
    {
        this.filter = filter;
        filter.configure(context);
        filter.prepare(context);
    }

    /**
     * Deliver a transaction to the filter and return the filter output.
     * 
     * @throws InterruptedException
     * @throws ReplicatorException
     */
    public ReplDBMSEvent filter(ReplDBMSEvent event)
            throws ReplicatorException, InterruptedException
    {
        return filter.filter(event);
    }

    /**
     * Calls release() method on the filter.
     * 
     * @throws InterruptedException
     * @throws ReplicatorException
     */
    public void done() throws ReplicatorException, InterruptedException
    {
        filter.release(context);
    }
}