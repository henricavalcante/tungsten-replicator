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
 * Contributor(s): Gilles Rayrat
 */

package com.continuent.tungsten.common.resource.test;

import junit.framework.TestCase;

import com.continuent.tungsten.common.cluster.resource.physical.ReplicatorCapabilities;
import com.continuent.tungsten.common.config.TungstenProperties;

/**
 * Implements a simple unit test for Tungsten properties.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ReplicatorCapabilitiesTest extends TestCase
{
    /**
     * Tests round trip storage in properties.
     */
    public void testProperties() throws Exception
    {
        // Check empty instance.
        ReplicatorCapabilities caps1 = new ReplicatorCapabilities();
        TungstenProperties cprops = caps1.asProperties();
        ReplicatorCapabilities caps2 = new ReplicatorCapabilities(cprops);
        testEquality(caps1, caps2);

        // Check instance with values. 
        caps1 = new ReplicatorCapabilities();
        caps1.addRole("master");
        caps1.addRole("slave");
        caps1.setConsistencyCheck(true);
        caps1.setFlush(true);
        caps1.setHeartbeat(true);
        caps1.setProvisionDriver(ReplicatorCapabilities.PROVISION_DONOR);
        caps1.setModel(ReplicatorCapabilities.MODEL_PEER);
        cprops = caps1.asProperties();
        caps2 = new ReplicatorCapabilities(cprops);
        testEquality(caps1, caps2);
    }

    // Test that two capabilities instances are equal. 
    private void testEquality(ReplicatorCapabilities rc1,
            ReplicatorCapabilities rc2)
    {
        assertEquals("model", rc1.getModel(), rc2.getModel());
        assertEquals("driver", rc1.getProvisionDriver(), rc2.getProvisionDriver());
        assertEquals("roles", rc1.getRoles().size(), rc2.getRoles().size());
        assertEquals("consistency check", rc1.isConsistencyCheck(), rc2.isConsistencyCheck());
        assertEquals("flush", rc1.isFlush(), rc2.isFlush());
        assertEquals("heartbeat", rc1.isHeartbeat(), rc2.isHeartbeat());
    }
}
