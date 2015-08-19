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

package com.continuent.tungsten.replicator.management;

import java.util.TimeZone;

import com.continuent.tungsten.common.jmx.JmxManager;
import com.continuent.tungsten.fsm.event.EventDispatcher;

/**
 * Dummy OpenReplicatorContext used for testing.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class MockOpenReplicatorContext implements OpenReplicatorContext
{
    private EventDispatcher eventDispatcher    = new MockEventDispatcher();
    private TimeZone        hostTimeZone       = TimeZone.getDefault();
    private TimeZone        replicatorTimeZone = TimeZone.getTimeZone("GMT");

    public EventDispatcher getEventDispatcher()
    {
        return eventDispatcher;
    }

    public void registerMBean(Object mbean, Class<?> mbeanClass, String name)
    {
        JmxManager.registerMBean(mbean, mbeanClass);
    }

    public TimeZone getHostTimeZone()
    {
        return hostTimeZone;
    }

    public TimeZone getReplicatorTimeZone()
    {
        return replicatorTimeZone;
    }
}
