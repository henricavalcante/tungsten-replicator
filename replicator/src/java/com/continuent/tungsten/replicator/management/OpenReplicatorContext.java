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

import com.continuent.tungsten.fsm.event.EventDispatcher;

/**
 * This class defines a context passed into replicator plugins that exposes
 * call-backs into the replicator itself to fetch configuration information and
 * invoke services.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */

public interface OpenReplicatorContext
{
    /** Returns the event dispatcher for reporting interesting events. */
    public EventDispatcher getEventDispatcher();

    /** Registers a JMX MBean from a lower-level service. */
    public void registerMBean(Object mbean, Class<?> mbeanClass, String name);

    /**
     * Returns the host time zone. Replicators override the host time zone, so
     * this is the only way for services to determine the time zone used by the
     * host itself.
     */
    public TimeZone getHostTimeZone();

    /**
     * Returns the replicator time zone, which defaults to GMT or can be
     * overridden from services.properties for testing. 
     */
    public TimeZone getReplicatorTimeZone();
}