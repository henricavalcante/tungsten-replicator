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
 * Initial developer(s): Ludovic Launer
 */

package com.continuent.tungsten.common.security;

import junit.framework.TestCase;

import com.continuent.tungsten.common.jmx.ServerRuntimeException;

/**
 * Implements a simple unit test for AuthenticationInfo
 * 
 * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
 * @version 1.0
 */
public class AuthenticationInfoTest extends TestCase
{
    /**
     * Client side: checks that if a trustorelocation is set, the
     * checkAuthenticationInfo verifies that the trustore existe If it doesn't,
     * it throws an exception with a non null cause.
     * 
     * @throws Exception
     */
    public void testCheckAuthenticationInfo() throws Exception
    {
        AuthenticationInfo authInfo = new AuthenticationInfo();
        boolean sreThrown = false;

        // If encryption required: trustore location exist
        try
        {
            authInfo.setTruststoreLocation("");
            authInfo.checkAndCleanAuthenticationInfo();
        }
        catch (ServerRuntimeException sre)
        {
            assertNotNull(sre.getCause());
            sreThrown = true;
        }

        assert (sreThrown);
    }


}
