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

package com.continuent.tungsten.common.network;

import java.io.IOException;
import java.net.InetAddress;

/**
 * Tests for reachability using the Java InetAddress.isReachable() method.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class InetAddressPing implements PingMethod
{
    private String notes;

    /**
     * Tests a host for reachability.
     * 
     * @param address Host name
     * @param timeout Timeout in milliseconds
     * @return True if host is reachable, otherwise false.
     */
    public boolean ping(HostAddress address, int timeout) throws HostException
    {
        notes = "InetAddress.isReachable()";
        InetAddress inetAddress = address.getInetAddress();
        try
        {
            return inetAddress.isReachable(timeout);
        }
        catch (IOException e)
        {
            throw new HostException("Ping operation failed unexpectedly", e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.network.PingMethod#getNotes()
     */
    public String getNotes()
    {
        return notes;
    }
}