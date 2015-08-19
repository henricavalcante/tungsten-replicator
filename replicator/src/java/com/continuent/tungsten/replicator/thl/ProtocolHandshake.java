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
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.thl;

import java.util.HashMap;
import java.util.Map;

/**
 * This class defines a ProtocolHandshake, which is sent from the master to a
 * slave as the slave connects. Masters offer capabilities to slaves.
 */
public class ProtocolHandshake extends ProtocolMessage
{
    static final long           serialVersionUID = 234524352L;

    private Map<String, String> capabilities     = new HashMap<String, String>();

    /**
     * Create a new instance.
     */
    public ProtocolHandshake()
    {
        super(null);
    }

    /** Sets a capability to a particular value. */
    public void setCapability(String name, String value)
    {
        getCapabilities().put(name, value);
    }

    /** Gets a capability value. */
    public String getCapability(String name)
    {
        return getCapabilities().get(name);
    }

    /**
     * Returns the current capability settings or null if no capabilities exist.
     * Older replicators do not return capabilities.
     */
    public Map<String, String> getCapabilities()
    {
        // Required for compatibility with older classes.
        if (capabilities == null)
            capabilities = new HashMap<String, String>();
        return capabilities;
    }
}