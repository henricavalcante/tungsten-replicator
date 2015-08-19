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
import com.continuent.tungsten.common.sockets.EchoServer;

import junit.framework.TestCase;

public class EchoPingTest extends TestCase
{
    // An IP address we hope is unknown. This is the IP address test range
    // 198.51.100.0/24 (aka TEST-NET-2 in RFC5735, used for sample code and
    // documentation). It should not be allocated.
    private static String  UNKNOWN_IP   = "198.51.100.100";

    // IP address of localhost
    private static String  LOCALHOST_IP = "127.0.0.1";

    // Echo port for testing
    private static int     PORT         = 11233;

    // Do not use SSL
    private static boolean NO_SSL       = false;

    // Default timeout 1000 ms
    private static int     TIMEOUT      = 1000;

    // Echo server
    private EchoServer     server;

    protected void setUp() throws Exception
    {
        server = new EchoServer(LOCALHOST_IP, PORT, NO_SSL);
        server.start();
    }

    protected void tearDown()
    {
        server.shutdown();
    }

    public void testEchoToLocalHost() throws Exception
    {
        TungstenProperties result = Echo.isReachable(LOCALHOST_IP, PORT,
                TIMEOUT);
        assertEquals("Can ping localhost", EchoStatus.OK,
                result.getObject(Echo.STATUS_KEY));
    }

    public void testEchoToUnknown() throws Exception
    {
        TungstenProperties result = Echo.isReachable(UNKNOWN_IP, PORT, TIMEOUT);
        assertEquals("Cannot ping unknown address",
                EchoStatus.SOCKET_CONNECT_TIMEOUT,
                result.getObject(Echo.STATUS_KEY));
    }

    public void testEchotoNonExistentDomain() throws Exception
    {
        TungstenProperties result = Echo.isReachable("nonexistent.domain.com",
                PORT, TIMEOUT);
        assertEquals("Cannot ping unknown address", EchoStatus.UNKNOWN_HOST,
                result.getObject(Echo.STATUS_KEY));
    }

    // TODO implement a special echo server where the connect, send, receive
    // timeout, message received, message send is configurable and test
    // different scenarios
}
