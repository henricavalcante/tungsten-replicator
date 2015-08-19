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

package com.continuent.tungsten.common.tdf;

import javax.ws.rs.core.Response;

import junit.framework.TestCase;

/**
 * Implements a simple unit test for APIResponse
 * 
 * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
 * @version 1.0
 */
public class TdfApiResponseTest extends TestCase
{
    
    /**
     * Tests that the getOutputPayloadClass is truly returning the payload type
     * 
     * @throws Exception
     */
    public void testgetOutputPayloadClass()
    {
       TdfApiResponse apires = new TdfApiResponse();
       
       String dummyO = new String("dummy");
       
       apires.setOutputPayload(dummyO);
       
       Class<?> t = apires.getOutputPayloadClass();
       
       if (t.getName() == String.class.getName())
           assertTrue(true);
       else
           assertTrue("The returned Type is not the same as the original Object", false);
    }

    /**
     * Tests that the getReturnMessage returns a message when a valid http returnCode is set
     * 
     * @throws Exception
     */
    public void testgetReturnMessage() 
    {
        TdfApiResponse apiResponse = new TdfApiResponse.Builder()
        .returnCode(Response.Status.OK.getStatusCode())
        .outputPayload(new String("dummy"))
        .build(); 
       assertNotNull(apiResponse.getReturnMessage());           // OK:200 should deliver a "OK" message
       
       apiResponse = new TdfApiResponse.Builder()
       .returnCode(999)
       .outputPayload(new String("dummy"))
       .build(); 
       assertNull(apiResponse.getReturnMessage());              // 999 does not correspond to a known http error code: no returnMessage
       
       apiResponse = new TdfApiResponse.Builder()
       .returnCode(999)
       .returnMessage("User provided return message")
       .outputPayload(new String("dummy"))
       .build(); 
       assertNotNull(apiResponse.getReturnMessage());           // Return message provided by the user
    }
    

}
