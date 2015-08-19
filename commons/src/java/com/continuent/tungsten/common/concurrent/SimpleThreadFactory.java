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

package com.continuent.tungsten.common.concurrent;

import java.util.concurrent.ThreadFactory;

/**
 * This class implements a thread factory used to create threads for the
 * SimpleThreadService class. The main thing the factory does is ensure threads
 * are properly named.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class SimpleThreadFactory implements ThreadFactory
{
    private final String  prefix;
    private volatile long threadCount = 0;

    /**
     * Creates a new thread factory with a prefix for thread names.
     * 
     * @param prefix Thread prefix
     */
    public SimpleThreadFactory(String prefix)
    {
        this.prefix = prefix;
    }

    /**
     * Creates a new thread with proper name. {@inheritDoc}
     * 
     * @see java.util.concurrent.ThreadFactory#newThread(java.lang.Runnable)
     */
    public Thread newThread(Runnable runnable)
    {
        String name = prefix + "-" + threadCount++;
        return new Thread(runnable, name);
    }
}