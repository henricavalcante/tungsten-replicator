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
 * Contributor(s): 
 */

package com.continuent.tungsten.common.security;

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.config.cluster.ConfigurationException;
import com.continuent.tungsten.common.jmx.ServerRuntimeException;
import com.continuent.tungsten.common.security.SecurityHelper.TUNGSTEN_APPLICATION_NAME;

/**
 * Class managing passwords in a file. Retrieves, Creates, deletes, updates
 * 
 * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
 * @version 1.0
 */
public class PasswordManager
{
    private static Logger logger = Logger.getLogger(PasswordManager.class);

    /*
     * Type of Client application.
     * This allows for application specific management of passwords
     */
    public enum ClientApplicationType
    {
        UNKNOWN,
        RMI_JMX,                    // Any application: generic RMI+JMX
        CONNECTOR;                   // The Tungsten Connector

        public static ClientApplicationType fromString(String x)
                throws IllegalArgumentException
        {
            if (x == null)
                return null;

            for (ClientApplicationType currentType : ClientApplicationType
                    .values())
            {
                if (x.equalsIgnoreCase(currentType.toString()))
                {
                    return currentType;
                }
            }
            throw new IllegalArgumentException(
                    "Cannot cast to PasswordManager.ClientApplicationType: "
                            + x);
        }
    };

    // Authentication and Encryption information
    private AuthenticationInfo    authenticationInfo    = null;
    private TungstenProperties    passwordsProperties   = null;
    private ClientApplicationType clientApplicationType = null;

    /**
     * Creates a new <code>PasswordManager</code> object
     * 
     * @param securityPropertiesFileLocation location of the security.properties
     *            file. If set to null will look for the default file.
     * @throws ConfigurationException
     */
    public PasswordManager(String securityPropertiesFileLocation)
            throws ConfigurationException
    {
        this(securityPropertiesFileLocation, ClientApplicationType.UNKNOWN);
    }

    /**
     * Creates a new <code>PasswordManager</code> object Loads Security related
     * properties from a file. File location =
     * {clusterhome}/conf/security.properties
     * 
     * @param securityPropertiesFileLocation location of the security.properties
     *            file. If set to null will look for the default file.
     * @param clientApplicationType Type of client application. Used to retrieve
     *            application specific information (password, ...)
     * @throws ConfigurationException
     */
    public PasswordManager(String securityPropertiesFileLocation,
            ClientApplicationType clientApplicationType)
            throws ConfigurationException
    {
        this.setClientApplicationType(clientApplicationType);

        // --- Try to get Security information from properties file ---
        // If securityPropertiesFileLocation==null will try to locate
        // default file
        try
        {
            this.authenticationInfo = SecurityHelper
                    .loadAuthenticationInformation(
                            securityPropertiesFileLocation, false, TUNGSTEN_APPLICATION_NAME.ANY);
        }
        catch (ConfigurationException ce)
        {
            logger.debug(MessageFormat.format("Configuration error: {0}",
                    ce.getMessage()));
            throw ce;
        }
        catch (ServerRuntimeException sre)
        {
            logger.debug(MessageFormat.format(
                    "Could not get authentication information : {0}",
                    sre.getMessage()));
        }
    }

    /**
     * Creates a new <code>PasswordManager</code> object
     * 
     * @param authenticationInfo the <code>AuthenticationInfo</code> object from
     *            which to retrieve properties
     * @param clientApplicationType Type of client application. Used to retrieve
     *            application specific information (password, ...)
     */
    public PasswordManager(AuthenticationInfo authenticationInfo,
            ClientApplicationType clientApplicationType)
    {
        this.authenticationInfo = authenticationInfo;
        this.setClientApplicationType(clientApplicationType);
    }

    /**
     * Passwords loaded from file as TungstenProperties. Example:
     * getPasswordsAsTungstenProperties.get(username);
     * 
     * @return TungstenProperties class containing the passwords
     */
    public TungstenProperties loadPasswordsAsTungstenProperties()
            throws ServerRuntimeException
    {
        this.passwordsProperties = SecurityHelper
                .loadPasswordsFromAuthenticationInfo(this.authenticationInfo);
        return passwordsProperties;
    }

    /**
     * Get clear text password for a username: decrypts password if needed
     * 
     * @param username the username for which to get the password
     * @throws ConfigurationException
     */
    public String getClearTextPasswordForUser(String username)
            throws ConfigurationException
    {
        return this.getPasswordForUser(username, true);
    }

    /**
     * Get Encrypted (or "as it is") password for a user
     * 
     * @param username the username for which to get the password
     * @throws ConfigurationException
     */
    public String getEncryptedPasswordForUser(String username)
            throws ConfigurationException
    {
        return this.getPasswordForUser(username, false);
    }

    /**
     * Tries to authenticate a user with a given password
     * 
     * @param username
     * @param candidatePassword
     * @return true if the user was successfully authenticated, false otherwise.
     * @throws ConfigurationException
     */
    public boolean authenticateUser(String username, String candidatePassword)
            throws ConfigurationException
    {
        boolean authOK = false;

        String goodPassword = this.getClearTextPasswordForUser(username);
        if (goodPassword == null)
            throw new ServerRuntimeException(MessageFormat.format(
                    "Cannot find password for user= {0}", username));
        if (goodPassword.equals(candidatePassword))
            authOK = true;

        return authOK;
    }

    /**
     * Get clear text password for a username: decrypts password if needed
     * The list of passwords is loaded from the file if it hasn't been done
     * before
     * 
     * @param username the username for which to get the password
     * @param decryptPassword true of the password needs to be decrypted
     * @return String containing the password corresponding to the username
     * @throws ConfigurationException
     */
    private String getPasswordForUser(String username, boolean decryptPassword)
            throws ConfigurationException
    {
        String userPassword = null;
        String _username = this.getApplicationSpecificUsername(username);          // Take
                                                                          // application
                                                                          // specific
                                                                          // information
                                                                          // into
                                                                          // account

        // --- Load passwords from file if necessary
        if (this.passwordsProperties == null)
            this.loadPasswordsAsTungstenProperties();

        userPassword = this.passwordsProperties.get(_username);

        // --- Decrypt password if asked for ---
        if (decryptPassword)
        {
            this.authenticationInfo.setUsername(_username);
            this.authenticationInfo.setPassword(userPassword);

            userPassword = this.authenticationInfo.getDecryptedPassword();
        }

        return userPassword;
    }

    /**
     * Set and store the password for a given user.
     * The password is encryted if authenticationInfo requires so
     * 
     * @param username
     * @param password
     */
    public void setPasswordForUser(String username, String password)
            throws ServerRuntimeException
    {
        String _password = this.createPasswordForUser(password);
        String _username = this.getApplicationSpecificUsername(username);          // Take
                                                                          // application
                                                                          // specific
                                                                          // information
                                                                          // into
                                                                          // account

        this.authenticationInfo.setUsername(_username);
        this.authenticationInfo.setPassword(_password);

        // Load passwords and add / update the new or existing one
        SecurityHelper
                .saveCredentialsFromAuthenticationInfo(this.authenticationInfo);
    }

    /**
     * Delete a user from the password file
     * 
     * @param username the username to be deleted from the password file
     * @throws ServerRuntimeException
     */
    public void deleteUser(String username) throws ServerRuntimeException
    {
        String _username = this.getApplicationSpecificUsername(username);
        this.authenticationInfo.setUsername(_username);

        // Delete user from password file
        SecurityHelper
                .deleteUserFromAuthenticationInfo(this.authenticationInfo);

    }

    /**
     * Refactor a username prior to adding it into the list.
     * Takes into account application specific configuration and add prefix to
     * link username to an application
     * 
     * @param username the username to refactor for the current application
     * @return username with the application specific suffix
     */
    public String getApplicationSpecificUsername(String username)
    {
        String _username = username;
        // --- Take application specific information into account ---
        // Appends application name before the property
        if (this.clientApplicationType != null
                && this.clientApplicationType != ClientApplicationType.UNKNOWN)
            switch (this.clientApplicationType)
            {
                case RMI_JMX :
                    _username = SecurityConf.SECURITY_APPLICATION_RMI_JMX + "."
                            + username;
                    break;
                case CONNECTOR :
                    _username = SecurityConf.SECURITY_APPLICATION_CONNECTOR
                            + "." + username;
                    break;
            }

        return _username;
    }

    /**
     * create an encoded or clear text password (depending on settings) for the
     * given user.
     * 
     * @param clearTextPassword the clear text password to encrypt or not.
     * @return a clear text or encoder password
     */
    private String createPasswordForUser(String clearTextPassword)
    {
        String encryptedPassword = clearTextPassword;

        if (this.authenticationInfo.isUseEncryptedPasswords())
        {
            Encryptor encryptor = null;
            ;
            try
            {
                encryptor = new Encryptor(this.authenticationInfo);
            }
            catch (ConfigurationException e)
            {
                throw new ServerRuntimeException(e.getMessage());
            }
            if (encryptor != null)
                encryptedPassword = encryptor.encrypt(clearTextPassword);
        }

        return encryptedPassword;
    }

    /**
     * When possible, tries to create files needed by the Password Manager.
     * Best effort: not successs guaranted
     * 
     * @throws ConfigurationException
     */
    public void try_createAuthenticationInfoFiles()
            throws ConfigurationException
    {
        // --- Try to create password_file.location ---
        File f = null;
        try
        {
            f = new File(this.authenticationInfo.getPasswordFileLocation()); // Throws
                                                                             // a
                                                                             // NullPointerException
                                                                             // if
                                                                             // parameter
                                                                             // is
                                                                             // null
            if (!f.isFile() || !f.canRead())
            {
                logger.warn(MessageFormat.format(
                        "Creating non existing file: {0}", f.getAbsolutePath()));

                String parentPath = f.getParent();
                if (parentPath != null)
                {
                    File pathToTarget = new File(parentPath);    // Create parent
                                                              // directories
                    if (!pathToTarget.isDirectory())
                        pathToTarget.mkdirs();
                }
                f.createNewFile();                              // Create file
            }
        }
        catch (IOException e)
        {
            logger.warn(MessageFormat.format("Could not create file: {0}",
                    f.getAbsolutePath()));
        }
        catch (NullPointerException npe)
        {
            // This is thrown when
            // this.authenticationInfo.getPasswordFileLocation()==null
            throw new ConfigurationException("password_file.location is null");
        }
    }

    public AuthenticationInfo getAuthenticationInfo()
    {
        return authenticationInfo;
    }

    public ClientApplicationType getClientApplicationType()
    {
        return clientApplicationType;
    }

    public void setClientApplicationType(
            ClientApplicationType clientApplicationType)
    {
        if (clientApplicationType == null)
            this.clientApplicationType = ClientApplicationType.UNKNOWN;
        else
            this.clientApplicationType = clientApplicationType;
    }

}
