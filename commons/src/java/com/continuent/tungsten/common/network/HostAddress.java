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

import java.net.InetAddress;

/**
 * Provides a wrapper on the standard InetAddress that supports extended methods
 * for determining host reachability.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class HostAddress
{
    private final InetAddress address;

    /**
     * Create a new host address on a given InetAddress.
     * 
     * @param address InetAddress to use
     */
    public HostAddress(InetAddress address)
    {
        this.address = address;
    }

    /**
     * Returns the InetAddress corresponding to this host name.
     */
    public InetAddress getInetAddress()
    {
        return address;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    public boolean equals(Object arg0)
    {
        return address.equals(arg0);
    }

    /**
     * @see java.net.InetAddress#getAddress()
     */
    public byte[] getAddress()
    {
        return address.getAddress();
    }

    /**
     * @see java.net.InetAddress#getCanonicalHostName()
     */
    public String getCanonicalHostName()
    {
        return address.getCanonicalHostName();
    }

    /**
     * @see java.net.InetAddress#getCanonicalHostName()
     */
    public String getHostAddress()
    {
        return address.getHostAddress();
    }

    public String getHostName()
    {
        return address.getHostName();
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#hashCode()
     */
    public int hashCode()
    {
        return address.hashCode();
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        return address.toString();
    }
}