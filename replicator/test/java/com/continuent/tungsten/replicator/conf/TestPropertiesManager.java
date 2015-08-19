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

package com.continuent.tungsten.replicator.conf;

import java.io.File;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * This class tests the property manager. It assumes a valid
 * replicator.properties is available in the working directory and that the
 * working directory is readable.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class TestPropertiesManager extends TestCase
{
    File              replicatorProperties = new File("replicator.properties");
    File              dynamicProperties    = new File("dynamic.properties");
    File              dynamicRole          = new File("dynamic.role");
    PropertiesManager pm;

    /**
     * Validate property file locations and instantiate manager for testing.
     * 
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        if (!replicatorProperties.exists() || !replicatorProperties.canRead())
            throw new Exception(
                    "replicator.properties file not found or not readable: "
                            + replicatorProperties.getAbsolutePath());

        File workDir = new File(".");
        if (!workDir.canWrite())
            throw new Exception("Current working directory not writable: "
                    + workDir.getAbsolutePath());

        if (dynamicProperties.exists())
            dynamicProperties.delete();

        pm = new PropertiesManager(replicatorProperties, dynamicProperties,
                dynamicRole);
    }

    /**
     * Delete dynamic.properties file used for testing.
     * 
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        dynamicProperties.delete();
    }

    /**
     * Prove that basic property file loading works.
     */
    public void testPropertyLoading() throws Exception
    {
        pm.loadProperties();
        TungstenProperties tp = pm.getProperties();
        assertTrue("Property file must contain some properties", tp.size() > 0);
    }

    /**
     * Test the full life cycle of dynamic properties including loading,
     * updating, fetching, and clearing.
     */
    public void testDynamicPropertyUpdate() throws Exception
    {
        PropertiesManager pm = new PropertiesManager(replicatorProperties,
                dynamicProperties, dynamicRole);

        // Ensure no dynamic properties are present.
        assertFalse("dynamic.properties file exists",
                dynamicProperties.exists());

        // Get the original value of THL remote URI.
        pm.loadProperties();
        TungstenProperties tp1 = pm.getProperties();
        String thlRemoteUri = tp1.getString(ReplicatorConf.MASTER_CONNECT_URI);
        assertNotNull(ReplicatorConf.MASTER_CONNECT_URI, thlRemoteUri);

        TungstenProperties tp1Dynamic = pm.getDynamicProperties();
        assertTrue("Dynamic properties are empty", tp1Dynamic.size() > 0);
        assertEquals(ReplicatorConf.MASTER_CONNECT_URI + " before set",
                thlRemoteUri,
                tp1Dynamic.getString(ReplicatorConf.MASTER_CONNECT_URI));

        // Update and save.
        TungstenProperties dynaProps = new TungstenProperties();
        dynaProps.setString(ReplicatorConf.MASTER_CONNECT_URI, "hi!");
        pm.setDynamicProperties(dynaProps);
        assertTrue("dynamic.properties file exists", dynamicProperties.exists());

        // Read it back.
        TungstenProperties tp2 = pm.getProperties();
        assertEquals(ReplicatorConf.MASTER_CONNECT_URI + " after set", "hi!",
                tp2.getString(ReplicatorConf.MASTER_CONNECT_URI));
        TungstenProperties tp2Dynamic = pm.getDynamicProperties();
        assertTrue("Dynamic properties have at least 1 value",
                tp2Dynamic.size() >= 1);

        // Reload files and confirm.
        pm.loadProperties();
        TungstenProperties tp3 = pm.getProperties();
        assertEquals(ReplicatorConf.MASTER_CONNECT_URI + " after set", "hi!",
                tp3.getString(ReplicatorConf.MASTER_CONNECT_URI));

        // Wipe out dynamic properties and confirm they are gone. Old value
        // should be back and dynamic.properties should be gone.
        pm.clearDynamicProperties();
        assertFalse("dynamic.properties file exists",
                dynamicProperties.exists());

        pm.loadProperties();
        TungstenProperties tp4 = pm.getProperties();
        assertEquals(ReplicatorConf.MASTER_CONNECT_URI, thlRemoteUri,
                tp4.getString(ReplicatorConf.MASTER_CONNECT_URI));

        TungstenProperties tp4Dynamic = pm.getDynamicProperties();
        assertEquals(ReplicatorConf.MASTER_CONNECT_URI + " after clear",
                thlRemoteUri,
                tp4Dynamic.getString(ReplicatorConf.MASTER_CONNECT_URI));
    }

    /**
     * Prove that updating a non-dynamic property or one that does not exist
     * generates an exception.
     */
    public void testBadDynamicUpdate() throws Exception
    {
        pm.loadProperties();
        try
        {
            TungstenProperties dynaProps = new TungstenProperties();
            dynaProps.setString(ReplicatorConf.APPLIER, "bad value!");
            pm.setDynamicProperties(dynaProps);
            fail("Able to update non-dynamic property: "
                    + ReplicatorConf.APPLIER);
        }
        catch (ReplicatorException e)
        {
        }

        try
        {
            TungstenProperties dynaProps = new TungstenProperties();
            dynaProps.setString("foo", "non-existent property");
            pm.setDynamicProperties(dynaProps);
            fail("Able to update non-dynamic property: foo");
        }
        catch (ReplicatorException e)
        {
        }
    }

    /**
     * Prove that we can get the current values of all dynamic parameters.
     */
    public void testGetDynamicParameters() throws Exception
    {
        pm.loadProperties();
        TungstenProperties dynamic = pm.getDynamicProperties();
        TungstenProperties all = pm.getProperties();
        assertTrue("Must have at least one dynamic parameter",
                dynamic.size() > 0);
        for (String name : dynamic.keyNames())
        {
            assertEquals("Checking dynamic vs. static: " + name,
                    all.getString(name), dynamic.getString(name));
        }
    }

    /**
     * Prove that property loading fails in a reasonable way if
     * replicator.properties cannot be found.
     */
    public void testNoReplicatorProperties() throws Exception
    {
        PropertiesManager pm = new PropertiesManager(new File("foo"),
                this.dynamicProperties, dynamicRole);
        try
        {
            pm.loadProperties();
            throw new Exception(
                    "Did not fail when loading bad replicator.properties");
        }
        catch (ReplicatorException e)
        {
        }
    }
}