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
 * Initial developer(s): Csaba Endre Simon
 * Contributor(s): 
 */

package com.continuent.tungsten.common.network;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.network.Echo.EchoStatus;

/**
 * Tests for reachability using the echo server.
 * 
 * @author <a href="mailto:csimon@vmware.com">Csaba Endre Simon</a>
 */
public class EchoPing implements PingMethod
{
    private final static int DEFAULT_ECHO_PORT = 7;

    private String           notes             = "EchoPing";

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.network.PingMethod#ping(HostAddress, int)
     */
    @Override
    public boolean ping(HostAddress address, int timeout)
    {
        TungstenProperties response = Echo.isReachable(address.getHostName(),
                DEFAULT_ECHO_PORT, timeout);
        return response.getObject(Echo.STATUS_KEY) == EchoStatus.OK;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.network.PingMethod#getNotes()
     */
    @Override
    public String getNotes()
    {
        return notes;
    }
}