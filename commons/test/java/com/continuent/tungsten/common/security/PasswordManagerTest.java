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
import com.continuent.tungsten.common.security.PasswordManager.ClientApplicationType;

/**
 * Implements a simple unit test for AuthenticationInfo
 * 
 * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
 * @version 1.0
 */
public class PasswordManagerTest extends TestCase
{

    /**
     * Confirm that we can create an instance of PasswordManager with a non
     * existing security.proprties.
     * The exception is raised once we do a check on the AuthenticationInfo
     * Create an instance of a PasswordManager reading from a
     * security.properties file
     * 
     * @throws ConfigurationException
     */
    public void testCreatePasswordManager() throws ConfigurationException
    {
        // This file should load just fine
        PasswordManager pwd = new PasswordManager("sample.security.properties");
        assertTrue(true);

        // This one should load fine too
        try
        {
            pwd = new PasswordManager(
                    "sample.security.properties_DOES_NOT_EXIST");
            AuthenticationInfo authenticationInfo = pwd.getAuthenticationInfo();
            authenticationInfo.checkAndCleanAuthenticationInfo();
        }
        catch (ConfigurationException e)
        {
            assertTrue(false);
        }
        catch (ServerRuntimeException sre)
        {
            // That's the expected exception from checkAuthenticationInfo();
            assertTrue(true);
        }

    }

    /**
     * Test loading passwords from a file
     * Should succeed with correct password file location
     * Should throw an Exception if the password file location is not correct
     * 
     * @throws ConfigurationException
     */
    public void testloadPasswordsAsTungstenProperties()
            throws ConfigurationException
    {
        // --- Load passwords from existing file ---
        // This file should load just fine
        PasswordManager pwd = new PasswordManager("sample.security.properties");

        // List of passwords is popualted once we have loaded it
        TungstenProperties passwdProps = pwd
                .loadPasswordsAsTungstenProperties();
        assertEquals(true, passwdProps.size() != 0);

        // And we can retrieve a password for an existing user
        String goodPassword = passwdProps.get("tungsten");
        assertNotNull(goodPassword);

        // --- Load passwords from a non existing file ---
        // We modify the password file location so that it does not exist
        AuthenticationInfo authInfo = pwd.getAuthenticationInfo();
        authInfo.setPasswordFileLocation(authInfo.getPasswordFileLocation()
                + "_DOES_NOT_EXIST");

        // We should now have an exception when trying to get passwords
        try
        {
            passwdProps = pwd.loadPasswordsAsTungstenProperties();
            assertTrue(false);
        }
        catch (ServerRuntimeException e)
        {
            assertTrue(true);
        }

    }

    /**
     * Test retrieving passwords from file.
     * Passwords are returned in clear text even if they were encoded
     * Decryption is done in the AuthenticationInfo class
     * 
     * @throws ConfigurationException
     */
    public void testgetPasswordForUser() throws ConfigurationException
    {
        PasswordManager pwd = null;
        String goodPassword = null;
        try
        {
            pwd = new PasswordManager("sample.security.properties");
        }
        catch (ConfigurationException e)
        {
            assertTrue(false);
        }

        // Try to get password without having loaded the passwords
        goodPassword = pwd.getClearTextPasswordForUser("tungsten");
        assertNotNull(goodPassword);

        // Get a password for a non existing user
        goodPassword = pwd.getClearTextPasswordForUser("non_existing_user");
        assertNull(goodPassword);
    }

    /**
     * Test retrieving passwords from file : Application Specific
     * Passwords are returned in clear text even if they were encoded
     * Decryption is done in the AuthenticationInfo class
     * Gets application specific user
     * 
     * @throws ConfigurationException
     */
    public void testgetPasswordForUser_by_Application()
            throws ConfigurationException
    {
        PasswordManager pwd = null;
        String goodPassword = null;
        String goodEncryptedPassword = null;
        try
        {
            pwd = new PasswordManager("sample.security.properties",
                    ClientApplicationType.RMI_JMX);
        }
        catch (ConfigurationException e)
        {
            assertTrue(false);
        }

        // Try to get password without having loaded the passwords
        goodEncryptedPassword = pwd.getEncryptedPasswordForUser("tungsten");
        goodPassword = pwd.getClearTextPasswordForUser("tungsten");

        assertNotNull(goodEncryptedPassword);                   // We should get something
        assertNotNull(goodPassword);

        assertTrue(goodEncryptedPassword != goodPassword);      // Clear text and
                                                           // encyprted password
                                                           // should be
                                                           // different

        assertEquals("secret", goodPassword);                   // The expected clear text
                                              // password = secret

        // Get a password for a non existing user
        goodPassword = pwd.getClearTextPasswordForUser("non_existing_user");
        assertNull(goodPassword);
    }

    /**
     * Confirm that we can authenticate (or not) a user.
     * 
     * @throws ConfigurationException
     */
    public void testAuthenticateUser() throws ConfigurationException
    {
        PasswordManager pwd = null;
        try
        {
            pwd = new PasswordManager("sample.security.properties",
                    ClientApplicationType.RMI_JMX);
        }
        catch (ConfigurationException e)
        {
            assertTrue(false);
        }

        // Try to authenticate the user
        boolean authOK = pwd.authenticateUser("tungsten", "secret");
        assertTrue(authOK);

        // Try with a wrong password
        authOK = pwd.authenticateUser("tungsten", "wrong_password");
        assertFalse(authOK);

        // Try with a non existing user
        try
        {
            authOK = pwd
                    .authenticateUser("non_existing_user", "wrong_password");
            assertTrue(
                    "Try with a non existing user: call should have raised an exception",
                    false);
        }
        catch (ServerRuntimeException sre)
        {
            assertTrue(true);
        }

    }

    /**
     * Test creating / updating / deleting passwords from file
     * 
     * @throws ConfigurationException
     */
    public void testSavePasswordForUser_by_Application()
            throws ConfigurationException
    {
        PasswordManager pwd = new PasswordManager("sample.security.properties",
                ClientApplicationType.RMI_JMX);

        // -- Try to create a new username=password
        String username = "ludovic";
        String password = "ludovic_password";
        String new_password = "my_new_password";

        try
        {
            pwd.setPasswordForUser(username, password);
        }
        catch (Exception e)
        {
            assertTrue(false);                      // Password saved without any Exception
        }

        // --- Try to retrieve what we've just stored ---
        pwd = new PasswordManager("sample.security.properties",
                ClientApplicationType.RMI_JMX);
        String retrievePassword = pwd.getClearTextPasswordForUser(username);
        assertEquals(password, retrievePassword);

        // --- Try to update exising password ---
        pwd.setPasswordForUser(username, new_password);

        // --- Again, try to retrieve the new password ---
        pwd = new PasswordManager("sample.security.properties",
                ClientApplicationType.RMI_JMX);
        retrievePassword = pwd.getClearTextPasswordForUser(username);
        assertEquals(new_password, retrievePassword);

        // ##### Backward compatibility ###
        // --- Retrieve the list of passwords using standard TungstenProperties
        // ---
        // This tests the backward compatibility with the TungstenProperties
        // reader. Apache adds a space after the key and before the value: key =
        // value
        pwd = new PasswordManager("sample.security.properties",
                ClientApplicationType.RMI_JMX);

        // List of passwords is popualted once we have loaded it
        TungstenProperties passwdProps = pwd
                .loadPasswordsAsTungstenProperties();
        assertEquals(true, passwdProps.size() != 0);

        // And we can retrieve a password for an existing user
        String goodPassword = passwdProps.get(pwd
                .getApplicationSpecificUsername(username));
        assertNotNull(goodPassword);
        // ################################

        // --- Try to delete a non existing user ---
        try
        {
            pwd.deleteUser("non_existing_user");
            assertTrue(false);                      // We should not get there as there should be an
                               // exception raised
        }
        catch (Exception e)
        {
            assertTrue(true);                      // An exception was raised: that's fine
        }

        // --- Try to delete the created user ---
        try
        {
            pwd.deleteUser("ludovic");
        }
        catch (Exception e)
        {
            assertTrue(false);                      // Password saved without any Exception
        }

        // --- Check that the user does not exist anymore ---
        pwd = new PasswordManager("sample.security.properties",
                ClientApplicationType.RMI_JMX);
        retrievePassword = pwd.getClearTextPasswordForUser("ludovic");
        assertNull(retrievePassword);               // The user should not be there anymore
    }

}
