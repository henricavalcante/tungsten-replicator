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
 * Contributors: Robert Hodges
 */

package com.continuent.tungsten.common.security;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.MessageFormat;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.config.cluster.ClusterConfiguration;
import com.continuent.tungsten.common.config.cluster.ConfigurationException;
import com.continuent.tungsten.common.jmx.ServerRuntimeException;
import com.continuent.tungsten.common.utils.CLUtils;

/**
 * Helper class for security related topics
 * 
 * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
 * @version 1.0
 */
public class SecurityHelper
{
    private static final Logger logger = Logger.getLogger(SecurityHelper.class);

    /*
     * Defines the type of application requesting Security information. This
     * allows module specific configuration of security.
     */
    // TUC-1872
    public static enum TUNGSTEN_APPLICATION_NAME
    {
        CONNECTOR, REPLICATOR, ANY;
    }

    /**
     * Save passwords from a TungstenProperties into a file
     * 
     * @param authenticationInfo containing password file location
     */
    public static void saveCredentialsFromAuthenticationInfo(
            AuthenticationInfo authenticationInfo)
            throws ServerRuntimeException
    {
        String passwordFileLocation = authenticationInfo
                .getPasswordFileLocation();

        try
        {
            String username = authenticationInfo.getUsername();
            String password = authenticationInfo.getPassword();

            PropertiesConfiguration props = new PropertiesConfiguration(
                    passwordFileLocation); // Use Apache commons-configuration:
                                           // preserves comments in .properties
                                           // !
            props.setProperty(username, password);
            props.save();
        }
        catch (org.apache.commons.configuration.ConfigurationException ce)
        {
            logger.error("Error while saving properties for file:"
                    + authenticationInfo.getPasswordFileLocation(), ce);
            throw new ServerRuntimeException("Error while saving Credentials: "
                    + ce.getMessage());
        }
    }

    /**
     * Delete a user and password from a file
     * 
     * @param authenticationInfo containing password file location
     */
    public static void deleteUserFromAuthenticationInfo(
            AuthenticationInfo authenticationInfo)
            throws ServerRuntimeException
    {
        String username = authenticationInfo.getUsername();
        String passwordFileLocation = authenticationInfo
                .getPasswordFileLocation();

        try
        {
            PropertiesConfiguration props = new PropertiesConfiguration(
                    passwordFileLocation);

            // --- Check that the user exists ---
            String usernameInFile = props.getString(username);
            if (usernameInFile == null)
            {
                throw new ServerRuntimeException(MessageFormat.format(
                        "Username does not exist: {0}", username));
            }

            props.clearProperty(username);
            props.save();
        }
        catch (org.apache.commons.configuration.ConfigurationException ce)
        {
            logger.error("Error while saving properties for file:"
                    + authenticationInfo.getPasswordFileLocation(), ce);
            throw new ServerRuntimeException("Error while saving Credentials: "
                    + ce.getMessage());
        }
    }

    /**
     * Loads passwords from a TungstenProperties from a .properties file
     * 
     * @return TungstenProperties containing logins as key and passwords as
     *         values
     */
    public static TungstenProperties loadPasswordsFromAuthenticationInfo(
            AuthenticationInfo authenticationInfo)
            throws ServerRuntimeException
    {
        try
        {
            String passwordFileLocation = authenticationInfo
                    .getPasswordFileLocation();
            TungstenProperties newProps = new TungstenProperties();
            newProps.load(new FileInputStream(passwordFileLocation), false);
            newProps.trim();
            logger.debug(MessageFormat.format("Passwords loaded from: {0}",
                    passwordFileLocation));
            return newProps;
        }
        catch (FileNotFoundException e)
        {
            throw new ServerRuntimeException("Unable to find properties file: "
                    + authenticationInfo.getPasswordFileLocation(), e);

        }
        catch (IOException e)
        {
            throw new ServerRuntimeException("Unable to read properties file: "
                    + authenticationInfo.getPasswordFileLocation(), e);
        }
    }

    /**
     * Loads Authentication and Encryption parameters from default location for
     * service.properties file
     * 
     * @return AuthenticationInfo loaded from file
     * @throws ConfigurationException
     */
    public static AuthenticationInfo loadAuthenticationInformation()
            throws ConfigurationException
    {
        return loadAuthenticationInformation(null);
    }

    /**
     * Loads Authentication and Encryption parameters from service.properties
     * file
     * 
     * @param propertiesFileLocation Location of the security.properties file.
     *            If set to null, will try to locate default file.
     * @return AuthenticationInfo
     * @throws ConfigurationException
     * @throws ReplicatorException
     */
    public static AuthenticationInfo loadAuthenticationInformation(
            String propertiesFileLocation) throws ConfigurationException
    {
        return loadAuthenticationInformation(propertiesFileLocation, true,
                TUNGSTEN_APPLICATION_NAME.ANY);
    }

    public static AuthenticationInfo loadAuthenticationInformation(
            String propertiesFileLocation, boolean doConsistencyChecks,
            TUNGSTEN_APPLICATION_NAME tungstenApplicationName)
            throws ConfigurationException
    {
        // Load properties and perform substitution
        TungstenProperties securityProperties = null;
        try
        {
            securityProperties = loadSecurityPropertiesFromFile(propertiesFileLocation);
        }
        catch (ConfigurationException ce)
        {
            if (doConsistencyChecks)
                throw ce;
        }

        AuthenticationInfo authInfo = new AuthenticationInfo(
                propertiesFileLocation);

        // Authorisation and/or encryption
        if (securityProperties != null)
        {
            securityProperties.trim(); // Remove white spaces
            boolean useAuthentication = securityProperties.getBoolean(
                    SecurityConf.SECURITY_JMX_USE_AUTHENTICATION,
                    SecurityConf.SECURITY_USE_AUTHENTICATION_DEFAULT, false);
            boolean useEncryption = securityProperties.getBoolean(
                    SecurityConf.SECURITY_JMX_USE_ENCRYPTION,
                    SecurityConf.SECURITY_USE_ENCRYPTION_DEFAULT, false);
            boolean useTungstenAuthenticationRealm = securityProperties
                    .getBoolean(
                            SecurityConf.SECURITY_JMX_USE_TUNGSTEN_AUTHENTICATION_REALM,
                            SecurityConf.SECURITY_USE_TUNGSTEN_AUTHENTICATION_REALM_DEFAULT,
                            false);
            boolean useEncryptedPassword = securityProperties
                    .getBoolean(
                            SecurityConf.SECURITY_JMX_USE_TUNGSTEN_AUTHENTICATION_REALM_ENCRYPTED_PASSWORD,
                            SecurityConf.SECURITY_USE_TUNGSTEN_AUTHENTICATION_REALM_ENCRYPTED_PASSWORD_DEFAULT,
                            false);

            // Define application specific settings
            // Use default values by default
            String security_keystore_location = SecurityConf.SECURITY_KEYSTORE_LOCATION;
            String security_keystore_password = SecurityConf.SECURITY_KEYSTORE_PASSWORD;
            String security_truststore_location = SecurityConf.SECURITY_TRUSTSTORE_LOCATION;
            String security_truststore_password = SecurityConf.SECURITY_TRUSTSTORE_PASSWORD;
            // Use application specific settings if needed
            switch (tungstenApplicationName)
            {
                case CONNECTOR :
                    security_keystore_location = SecurityConf.CONNECTOR_SECURITY_KEYSTORE_LOCATION;
                    security_keystore_password = SecurityConf.CONNECTOR_SECURITY_KEYSTORE_PASSWORD;
                    security_truststore_location = SecurityConf.CONNECTOR_SECURITY_TRUSTSTORE_LOCATION;
                    security_truststore_password = SecurityConf.CONNECTOR_SECURITY_TRUSTSTORE_PASSWORD;
                    break;
                default :
                    // Keep default values
                    break;
            }

            // --- Retrieve properties ---
            boolean connectorUseSSL = securityProperties.getBoolean(
                    SecurityConf.CONNECTOR_USE_SSL, "false", false);

            String parentFileLocation = securityProperties
                    .getString(SecurityConf.SECURITY_PROPERTIES_PARENT_FILE_LOCATION);
            String passwordFileLocation = securityProperties
                    .getString(SecurityConf.SECURITY_PASSWORD_FILE_LOCATION);
            String accessFileLocation = securityProperties
                    .getString(SecurityConf.SECURITY_ACCESS_FILE_LOCATION);
            String keystoreLocation = securityProperties
                    .getString(security_keystore_location);
            keystoreLocation = (keystoreLocation != null && StringUtils
                    .isNotBlank(keystoreLocation)) ? keystoreLocation : null;
            String keystorePassword = securityProperties
                    .getString(security_keystore_password);
            String truststoreLocation = securityProperties
                    .getString(security_truststore_location);
            truststoreLocation = (truststoreLocation != null && StringUtils
                    .isNotBlank(truststoreLocation))
                    ? truststoreLocation
                    : null;
            String truststorePassword = securityProperties
                    .getString(security_truststore_password);
            String userName = securityProperties.getString(
                    SecurityConf.SECURITY_JMX_USERNAME, null, false);
            // Aliases for keystore
            String connector_alias_client_to_connector = securityProperties
                    .getString(
                            SecurityConf.KEYSTORE_ALIAS_CONNECTOR_CLIENT_TO_CONNECTOR,
                            SecurityConf.KEYSTORE_ALIAS_CONNECTOR_CLIENT_TO_CONNECTOR_DEFAULT,
                            false);
            String connector_alias_connector_to_db = securityProperties
                    .getString(
                            SecurityConf.KEYSTORE_ALIAS_CONNECTOR_CONNECTOR_TO_DB,
                            SecurityConf.KEYSTORE_ALIAS_CONNECTOR_CONNECTOR_TO_DB_DEFAULT,
                            false);

            // --- Populate return object ---
            authInfo.setConnectorUseSSL(connectorUseSSL);
            authInfo.setParentPropertiesFileLocation(parentFileLocation);
            authInfo.setAuthenticationNeeded(useAuthentication);
            authInfo.setUseTungstenAuthenticationRealm(useTungstenAuthenticationRealm);
            authInfo.setUseEncryptedPasswords(useEncryptedPassword);
            authInfo.setEncryptionNeeded(useEncryption);
            authInfo.setPasswordFileLocation(passwordFileLocation);
            authInfo.setAccessFileLocation(accessFileLocation);
            authInfo.setKeystoreLocation(keystoreLocation);
            authInfo.setKeystorePassword(keystorePassword);
            authInfo.setTruststoreLocation(truststoreLocation);
            authInfo.setTruststorePassword(truststorePassword);
            authInfo.setUsername(userName);
            authInfo.setParentProperties(securityProperties);
            // aliases
            if (connector_alias_client_to_connector != null)
                authInfo.getMapKeystoreAliasesForTungstenApplication()
                        .put(SecurityConf.KEYSTORE_ALIAS_CONNECTOR_CLIENT_TO_CONNECTOR,
                                connector_alias_client_to_connector);
            if (connector_alias_connector_to_db != null)
                authInfo.getMapKeystoreAliasesForTungstenApplication().put(
                        SecurityConf.KEYSTORE_ALIAS_CONNECTOR_CONNECTOR_TO_DB,
                        connector_alias_connector_to_db);

            // --- Check information is correct ---
            // Checks authentication and encryption parameters
            // file exists, aliases exists...
            if (doConsistencyChecks)
                authInfo.checkAndCleanAuthenticationInfo(tungstenApplicationName);

            // --- Set critical properties as System Properties ---
            SecurityHelper.setSecurityProperties(authInfo, false);
        }
        return authInfo;
    }

    /**
     * Set system properties required for SSL and password management. Since
     * these settings are critical to correct operation we optionally log them.
     * 
     * @param authInfo Populated authenticatino information
     * @param verbose If true, log information
     */
    private static void setSecurityProperties(AuthenticationInfo authInfo,
            boolean verbose)
    {
        if (verbose)
        {
            CLUtils.println("Setting security properties!");
        }
        setSystemProperty("javax.net.ssl.keyStore",
                authInfo.getKeystoreLocation(), verbose);
        setSystemProperty("javax.net.ssl.keyStorePassword",
                authInfo.getKeystorePassword(), verbose);
        setSystemProperty("javax.net.ssl.trustStore",
                authInfo.getTruststoreLocation(), verbose);
        setSystemProperty("javax.net.ssl.trustStorePassword",
                authInfo.getTruststorePassword(), verbose);
    }

    /**
     * Sets a system property with a log message. Java -Dxxx system property
     * 
     * @param name the name of the system property to set
     * @param value value of the system property
     * @param verbose log the property being set if true.
     */
    private static void setSystemProperty(String name, String value,
            boolean verbose)
    {
        if (verbose)
        {
            CLUtils.println("Setting system property: name=" + name + " value="
                    + value);
        }
        if (value != null)
            System.setProperty(name, value);
    }

    /**
     * Loads Security related properties from a file. File location =
     * {clusterhome}/conf/security.properties
     * 
     * @param propertiesFileLocation location of the security.properties file.
     *            If set to null will look for the default file.
     * @return TungstenProperties containing security parameters
     * @throws ConfigurationException
     */
    private static TungstenProperties loadSecurityPropertiesFromFile(
            String propertiesFileLocation) throws ConfigurationException
    {
        TungstenProperties securityProps = null;
        FileInputStream securityConfigurationFileInputStream = null;

        // --- Get Configuration file ---
        if (propertiesFileLocation == null
                && ClusterConfiguration.getClusterHome() == null)
        {
            throw new ConfigurationException(
                    "No cluster.home found from which to configure cluster resources.");
        }

        File securityPropertiesFile;
        if (propertiesFileLocation == null) // Get from default location
        {
            File clusterConfDirectory = ClusterConfiguration
                    .getDir(ClusterConfiguration
                            .getGlobalConfigDirName(ClusterConfiguration
                                    .getClusterHome()));
            securityPropertiesFile = new File(clusterConfDirectory.getPath(),
                    SecurityConf.SECURITY_PROPERTIES_FILE_NAME);
        }
        else
        // Get from supplied location
        {
            securityPropertiesFile = new File(propertiesFileLocation);
        }

        // --- Get properties ---
        try
        {
            securityProps = new TungstenProperties();
            securityConfigurationFileInputStream = new FileInputStream(
                    securityPropertiesFile);
            securityProps.load(securityConfigurationFileInputStream, true);
            closeSecurityConfigurationFileInputStream(securityConfigurationFileInputStream);
        }
        catch (FileNotFoundException e)
        {
            String msg = MessageFormat.format(
                    "Cannot find configuration file: {0}",
                    securityPropertiesFile.getPath());
            logger.debug(msg, e);
            throw new ConfigurationException(msg);
        }
        catch (IOException e)
        {
            String msg = MessageFormat.format(
                    "Cannot load configuration file: {0}.\n Reason: {1}",
                    securityPropertiesFile.getPath(), e.getMessage());
            logger.debug(msg, e);
            throw new ConfigurationException(msg);
        }
        finally
        {
            closeSecurityConfigurationFileInputStream(securityConfigurationFileInputStream);
        }

        if (logger.isDebugEnabled())
        {
            logger.debug(MessageFormat.format(": {0}",
                    securityPropertiesFile.getPath()));
        }

        // Update propertiesFileLocation with the location actualy used
        securityProps.put(
                SecurityConf.SECURITY_PROPERTIES_PARENT_FILE_LOCATION,
                securityPropertiesFile.getAbsolutePath());

        return securityProps;
    }

    /**
     * Close the security.properties input stream once it's been used. Best
     * effort
     * 
     * @param fis
     */
    private static void closeSecurityConfigurationFileInputStream(
            FileInputStream fis)
    {
        // TUC-2065 Close input stream once it's used
        if (fis != null)
        {
            try
            {
                fis.close();
            }
            catch (Exception ignoreMe)
            {
            }
        }
    }

    /**
     * Get the system keystore location
     * 
     * @return The keyStore location
     */
    public static String getKeyStoreLocation()
    {
        return System.getProperty("javax.net.ssl.keyStore");
    }

}
