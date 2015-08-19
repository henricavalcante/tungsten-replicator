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

/**
 * Sample ping method to test exception handling.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class SamplePingMethod implements PingMethod
{
    // Can be set to tell the sample method to throw an exception.
    public static boolean exception = false;

    private String        notes;

    /**
     * Sample test for reachability.
     * 
     * @param address Host name
     * @param timeout Timeout in milliseconds
     * @return True if host is reachable, otherwise false.
     */
    public boolean ping(HostAddress address, int timeout) throws HostException
    {
        if (exception)
        {
            notes = "exception";
            throw new HostException("exception");
        }
        else
        {
            notes = "ok";
            return true;
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.network.PingMethod#getNotes()
     */
    public String getNotes()
    {
        return notes;
    }
}