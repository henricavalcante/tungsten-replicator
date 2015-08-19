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

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.config.cluster.ConfigurationException;
import com.continuent.tungsten.common.jmx.ServerRuntimeException;
import com.continuent.tungsten.common.security.SecurityHelper.TUNGSTEN_APPLICATION_NAME;

/**
 * Implements a simple unit test for SecurityHelper
 * 
 * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
 * @version 1.0
 */
public class SecurityHelperTest extends TestCase
{
    /**
     * Test we can retrieve passwords from the passwords.store file
     * 
     * @throws Exception
     */
    public void testLoadPasswordsFromFile() throws Exception
    {
        AuthenticationInfo authenticationInfo = new AuthenticationInfo();
        authenticationInfo.setPasswordFileLocation("sample.passwords.store");

        // This should not be null if we retrieve passwords from the file
        TungstenProperties tungsteProperties = SecurityHelper
                .loadPasswordsFromAuthenticationInfo(authenticationInfo);
        assertNotNull(tungsteProperties);
    }

    /**
     * Test we can retrieve authentication information from the
     * security.properties file
     * 
     * @throws ConfigurationException
     */
    public void testloadAuthenticationInformation()
            throws ConfigurationException
    {
        // Get authInfo from the configuration file on the CLIENT_SIDE
        AuthenticationInfo authInfo = SecurityHelper
                .loadAuthenticationInformation("sample.security.properties");
        assertNotNull(authInfo);

        // Get authInfo from the configuration file on the SERVER_SIDE
        authInfo = SecurityHelper
                .loadAuthenticationInformation("sample.security.properties");
        assertNotNull(authInfo);

        // Check that an Exception is thrown when the configuration file is not
        // found
        ConfigurationException configurationException = null;
        try
        {
            authInfo = SecurityHelper.loadAuthenticationInformation(
                    "sample.security.properties_DOES_NOT_EXIST");
        }
        catch (ConfigurationException ce)
        {
            configurationException = ce;
        }
        assertNotNull(configurationException);

    }

    /**
     * Confirm that we can retrieve values from the security.properties file
     * 
     * @throws ConfigurationException
     */
    public void testRetrieveInformation() throws ConfigurationException
    {
        // Get authInfo from the configuration file on the SERVER_SIDE
        AuthenticationInfo authInfo = SecurityHelper
                .loadAuthenticationInformation("sample.security.properties");
        assertNotNull(authInfo);

        TungstenProperties securityProp = authInfo.getAsTungstenProperties();
        assertNotNull(securityProp);

        Boolean useJmxAuthentication = securityProp
                .getBoolean(SecurityConf.SECURITY_JMX_USE_AUTHENTICATION);
        assertNotNull(useJmxAuthentication);
    }

    /**
     * Reset system properties to null value
     */
    private void resetSecuritySystemProperties()
    {
        System.clearProperty("javax.net.ssl.keyStore");
        System.clearProperty("javax.net.ssl.keyStorePassword");
        System.clearProperty("javax.net.ssl.trustStore");
        System.clearProperty("javax.net.ssl.trustStorePassword");
    }

    /**
     * Confirm that once we have loaded the security information, it becomes
     * available in system properties
     * 
     * @throws ConfigurationException
     */
    public void testloadAuthenticationInformation_and_setSystemProperties()
            throws ConfigurationException
    {
        // Reset info
        resetSecuritySystemProperties();

        // Get authInfo from the configuration file on the SERVER_SIDE
        AuthenticationInfo authInfo = SecurityHelper
                .loadAuthenticationInformation("sample.security.properties");
        assertNotNull(authInfo);

        // Check it's available in system wide properties
        String systemProperty = null;
        systemProperty = System.getProperty("javax.net.ssl.keyStore", null);
        assertNotNull(systemProperty);

        systemProperty = System.getProperty("javax.net.ssl.keyStorePassword",
                null);
        assertNotNull(systemProperty);

        systemProperty = System.getProperty("javax.net.ssl.trustStore", null);
        assertNotNull(systemProperty);

        systemProperty = System.getProperty("javax.net.ssl.trustStorePassword",
                null);
        assertNotNull(systemProperty);
    }

    /**
     * Confirm that once we have loaded the security information, it becomes
     * available in system properties Confirm that application specific info is
     * used Note: Connector only info for now
     * 
     * @throws ConfigurationException
     */
    public void testloadAuthenticationInformation_and_setSystemProperties_4_Connector()
    {
        // Reset info
        resetSecuritySystemProperties();

        // Get authInfo from the configuration file on the SERVER_SIDE
        AuthenticationInfo authInfo = null;
        try
        {
            authInfo = SecurityHelper.loadAuthenticationInformation(
                    "sample.security.properties", false,
                    TUNGSTEN_APPLICATION_NAME.CONNECTOR);
        }
        catch (ConfigurationException e)
        {
            assertFalse("Could not load authentication and securiy information",
                    true);
        }
        assertNotNull(authInfo);

        // Check it's available in system wide properties
        String systemProperty = null;
        systemProperty = System.getProperty("javax.net.ssl.keyStore", null);
        assertNotNull(systemProperty);
        assertTrue(systemProperty.contains("connector"));

        systemProperty = System.getProperty("javax.net.ssl.keyStorePassword",
                null);
        assertNotNull(systemProperty);
        assertTrue(systemProperty.contains("connector"));

        systemProperty = System.getProperty("javax.net.ssl.trustStore", null);
        assertNotNull(systemProperty);
        assertTrue(systemProperty.contains("connector"));

        systemProperty = System.getProperty("javax.net.ssl.trustStorePassword",
                null);
        assertNotNull(systemProperty);
        assertTrue(systemProperty.contains("connector"));
    }

    /**
     * Confirm that when set to empty in the configuration file, important
     * properties are set to null and not ""
     * 
     * @throws ConfigurationException
     */
    public void testCrucialEmptyPropertiesAreNull()
    {
        // Reset info
        resetSecuritySystemProperties();

        // Get authInfo from the configuration file on the SERVER_SIDE
        AuthenticationInfo authInfo = null;
        try
        {
            authInfo = SecurityHelper.loadAuthenticationInformation(
                    "test.security.properties", true,
                    TUNGSTEN_APPLICATION_NAME.CONNECTOR);
        }
        catch (ConfigurationException e)
        {
            assertFalse("Could not load authentication and securiy information",
                    true);
        }
        assertNotNull(authInfo);

        assertTrue(authInfo.getKeystoreLocation() == null);
        assertTrue(authInfo.getTruststoreLocation() == null);
    }

    /**
     * Confirm behavior when connector.security.use.SSL=false
     * 
     * @throws ConfigurationException
     */
    public void testConnectorSecuritySettingsSSL_false()
    {
        // Reset info
        resetSecuritySystemProperties();

        // Confirm that by default connector.security.use.SSL=false
        AuthenticationInfo authInfo = null;
        try
        {
            authInfo = SecurityHelper.loadAuthenticationInformation(
                    "test.security.properties", true,
                    TUNGSTEN_APPLICATION_NAME.CONNECTOR);
        }
        catch (ConfigurationException e)
        {
            assertFalse("Could not load authentication and securiy information",
                    true);
        }
        assertFalse(authInfo.isConnectorUseSSL());

        // Confirm that unnecessary information has been deleted
        assertNull(authInfo.getKeystoreLocation());
        assertNull(authInfo.getKeystorePassword());
        assertNull(authInfo.getTruststoreLocation());
        assertNull(authInfo.getTruststorePassword());

        // Check it's NOT available in system wide properties
        String systemProperty = null;
        systemProperty = System.getProperty("javax.net.ssl.keyStore", null);
        assertNull(systemProperty);

        systemProperty = System.getProperty("javax.net.ssl.keyStorePassword",
                null);
        assertNull(systemProperty);

        systemProperty = System.getProperty("javax.net.ssl.trustStore", null);
        assertNull(systemProperty);

        systemProperty = System.getProperty("javax.net.ssl.trustStorePassword",
                null);
        assertNull(systemProperty);

    }

    /**
     * Confirm behavior when connector.security.use.SSL=true
     * 
     * @throws ConfigurationException
     */
    public void testConnectorSecuritySettingsSSL_true()
    {
        // Reset info
        resetSecuritySystemProperties();

        // Confirm that by default connector.security.use.SSL=false
        AuthenticationInfo authInfo = null;
        try
        {
            authInfo = SecurityHelper.loadAuthenticationInformation(
                    "sample.security.properties", true,
                    TUNGSTEN_APPLICATION_NAME.CONNECTOR);
        }
        catch (ConfigurationException e)
        {
            assertFalse("Could not load authentication and securiy information",
                    true);
        }
        assertTrue(authInfo.isConnectorUseSSL());
    }

    /**
     * Confirm behavior when connector.security.use.SSL=true
     * 
     * @throws ConfigurationException
     */
    public void testConnectorSecuritySettingsSSL_errors_true()
    {
        // Reset info
        resetSecuritySystemProperties();

        // Confirm that exception is thrown when keystore location is not
        // specified
        AuthenticationInfo authInfo = null;
        try
        {
            authInfo = SecurityHelper.loadAuthenticationInformation(
                    "test.ssl.security.properties", true,
                    TUNGSTEN_APPLICATION_NAME.CONNECTOR);
        }
        catch (ServerRuntimeException e)
        {
            assertTrue("An exception was thrown, that's expected !", true);
        }
        catch (ConfigurationException e)
        {
            assertFalse(
                    "That should not be this kind of Exception being thrown",
                    true);
        }
        assertEquals(null, authInfo);

        // Reset info
        resetSecuritySystemProperties();

        // Confirm that exception is thrown when trustore location is not
        // specified
        authInfo = null;
        try
        {
            authInfo = SecurityHelper.loadAuthenticationInformation(
                    "test.ssl2.security.properties", true,
                    TUNGSTEN_APPLICATION_NAME.CONNECTOR);
        }
        catch (ServerRuntimeException e)
        {
            assertTrue("An exception was thrown, that's expected !", true);
        }
        catch (ConfigurationException e)
        {
            assertFalse(
                    "That should not be this kind of Exception being thrown",
                    true);
        }
        assertEquals(null, authInfo);

    }

    /**
     * Confirm behavior when connector.security.use.SSL=true and alias are
     * defined
     * 
     * @throws ConfigurationException
     */
    public void testConnectorSecuritySettingsSSL_alias()
    {
        // Reset info
        resetSecuritySystemProperties();

        // Confirm that exception is thrown when keystore location is not
        // specified
        // AuthenticationInfo authInfo = null;
        try
        {
            // authInfo =
            SecurityHelper.loadAuthenticationInformation(
                    "test.ssl.alias.security.properties", true,
                    TUNGSTEN_APPLICATION_NAME.CONNECTOR);
        }
        catch (ServerRuntimeException e)
        {
            assertTrue("There should not be any exception thrown", false);
        }
        catch (ConfigurationException e)
        {
            assertFalse(
                    "That should not be this kind of Exception being thrown",
                    true);
        }

        // Reset info
        resetSecuritySystemProperties();
    }

    /**
     * Confirm behavior when connector.security.use.SSL=true and wrong alias is
     * specified
     * 
     * @throws ConfigurationException
     */
    public void testConnectorSecuritySettingsSSL_alias_error()
    {
        // Reset info
        resetSecuritySystemProperties();

        // Confirm that exception is thrown when keystore location is not
        // specified
        // AuthenticationInfo authInfo = null;
        try
        {
            // authInfo =
            SecurityHelper.loadAuthenticationInformation(
                    "test.ssl.alias.wrong.security.properties", true,
                    TUNGSTEN_APPLICATION_NAME.CONNECTOR);
        }
        catch (ServerRuntimeException e)
        {
            assertTrue("An exception was thrown, that's expected !", true);
        }
        catch (ConfigurationException e)
        {
            assertFalse(
                    "That should not be this kind of Exception being thrown",
                    true);
        }

        // Reset info
        resetSecuritySystemProperties();
    }

    /**
     * Confirm behavior when connector.security.use.SSL=true and alias is
     * defined + alias is not the first in the list inside the keystore. This
     * shows that we can select an alias inside the keystore
     * 
     * @throws ConfigurationException
     */
    public void testConnectorSecuritySettingsSSL_alias_2()
    {
        // Reset info
        resetSecuritySystemProperties();

        // Confirm that exception is thrown when keystore location is not
        // specified
        // AuthenticationInfo authInfo = null;
        try
        {
            // authInfo =
            SecurityHelper.loadAuthenticationInformation(
                    "test.ssl.alias.2.position.security.properties", true,
                    TUNGSTEN_APPLICATION_NAME.CONNECTOR);
        }
        catch (ServerRuntimeException e)
        {
            assertTrue("There should not be any exception thrown", false);
        }
        catch (ConfigurationException e)
        {
            assertFalse(
                    "That should not be this kind of Exception being thrown",
                    true);
        }

        // Reset info
        resetSecuritySystemProperties();
    }

    /**
     * Confirm behavior when connector.security.use.SSL=true and alias are not
     * defined This shows that it uses first alias it finds
     * 
     * @throws ConfigurationException
     */
    public void testConnectorSecuritySettingsSSL_alias_not_defined()
    {
        // Reset info
        resetSecuritySystemProperties();

        // Confirm that exception is thrown when keystore location is not
        // specified
        // AuthenticationInfo authInfo = null;
        try
        {
            // authInfo =
            SecurityHelper.loadAuthenticationInformation(
                    "test.ssl.alias.not.defined.security.properties", true,
                    TUNGSTEN_APPLICATION_NAME.CONNECTOR);
        }
        catch (ServerRuntimeException e)
        {
            assertTrue("There should not be any exception thrown", false);
        }
        catch (ConfigurationException e)
        {
            assertFalse(
                    "That should not be this kind of Exception being thrown",
                    true);
        }

        // Reset info
        resetSecuritySystemProperties();
    }

}
