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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.text.MessageFormat;
import java.util.Enumeration;
import java.util.HashMap;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.config.cluster.ClusterConfiguration;
import com.continuent.tungsten.common.config.cluster.ConfigurationException;
import com.continuent.tungsten.common.jmx.ServerRuntimeException;
import com.continuent.tungsten.common.security.SecurityHelper.TUNGSTEN_APPLICATION_NAME;
import com.continuent.tungsten.common.utils.CLLogLevel;
import com.continuent.tungsten.common.utils.CLUtils;

/**
 * Information class holding Authentication and Encryption parameters Some of
 * the properties may be left null depending on how and when this is used
 * 
 * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
 * @version 1.0
 */
public final class AuthenticationInfo
{
    private static final Logger     logger                                   = Logger.getLogger(AuthenticationInfo.class);
    /** Location of the file from which this was built **/
    private String                  parentPropertiesFileLocation             = null;
    /** Properties from the files from which this was built **/
    private TungstenProperties      parentProperties                         = null;

    private boolean                 authenticationNeeded                     = false;
    private boolean                 encryptionNeeded                         = false;
    private boolean                 useTungstenAuthenticationRealm           = true;
    private boolean                 useEncryptedPasswords                    = false;
    /** Set to true if the connector should be using SSL **/
    private boolean                 connectorUseSSL                          = false;

    // Authentication parameters
    private String                  username                                 = null;
    private String                  password                                 = null;
    private String                  passwordFileLocation                     = null;
    private String                  accessFileLocation                       = null;
    // Encryption parameters
    private String                  keystoreLocation                         = null;
    private String                  keystorePassword                         = null;
    private String                  truststoreLocation                       = null;
    private String                  truststorePassword                       = null;
    // Alias for entries in keystore
    // key=identifier as defined in SecurityConf value=alias for this
    // application
    private HashMap<String, String> mapKeystoreAliasesForTungstenApplication = new HashMap<String, String>();

    public final static String      AUTHENTICATION_INFO_PROPERTY             = "authenticationInfo";
    public final static String      TUNGSTEN_AUTHENTICATION_REALM            = "tungstenAutenthicationRealm";
    // Possible command line parameters
    public final static String      USERNAME                                 = "-username";
    public final static String      PASSWORD                                 = "-password";
    public final static String      KEYSTORE_LOCATION                        = "-keystoreLocation";
    public final static String      KEYSTORE_PASSWORD                        = "-keystorePassword";
    public final static String      TRUSTSTORE_LOCATION                      = "-truststoreLocation";
    public final static String      TRUSTSTORE_PASSWORD                      = "-truststorePassword";
    public final static String      SECURITY_CONFIG_FILE_LOCATION            = "-securityProperties";

    /**
     * Creates a new <code>AuthenticationInfo</code> object
     */
    public AuthenticationInfo(String parentPropertiesFileLocation)
    {
        this.parentPropertiesFileLocation = parentPropertiesFileLocation;
    }

    public AuthenticationInfo()
    {
        this(null);
    }

    /**
     * Check Authentication information consistency
     * 
     * @throws ConfigurationException
     */
    public void checkAndCleanAuthenticationInfo()
            throws ServerRuntimeException, ConfigurationException
    {
        checkAndCleanAuthenticationInfo(TUNGSTEN_APPLICATION_NAME.ANY);
    }

    public void checkAndCleanAuthenticationInfo(
            TUNGSTEN_APPLICATION_NAME tungstenApplicationName)
            throws ServerRuntimeException, ConfigurationException
    {
        // --- Check security.properties location ---
        if (this.parentPropertiesFileLocation != null)
        {
            File f = new File(this.parentPropertiesFileLocation);
            // --- Find absolute path if needed
            if (!f.isFile())
            {
                f = this.findAbsolutePath(f);
                this.parentPropertiesFileLocation = f.getAbsolutePath();
            }
            // --- Check file is readable
            if (!f.isFile() || !f.canRead())
            {
                String msg = MessageFormat.format(
                        "Cannot find or read {0} file: {1}",
                        SECURITY_CONFIG_FILE_LOCATION,
                        this.parentPropertiesFileLocation);
                CLUtils.println(msg, CLLogLevel.detailed);
                throw new ServerRuntimeException(msg, new AssertionError(
                        "File must exist"));
            }
        }
        // --- Clean up ---
        if (tungstenApplicationName == TUNGSTEN_APPLICATION_NAME.CONNECTOR
                && !this.isConnectorUseSSL())
        {
            // The Connector does not use SSL, delete unnecessary information.
            this.keystoreLocation = null;
            this.keystorePassword = null;
            this.truststoreLocation = null;
            this.truststorePassword = null;
        }

        // ---------------------- Check Keystore ----------------------------
        if ((this.isEncryptionNeeded() && this.keystoreLocation != null)
                || (tungstenApplicationName == TUNGSTEN_APPLICATION_NAME.CONNECTOR && this
                        .isConnectorUseSSL()))
        {
            // --- Check file location is specified ---
            if (this.keystoreLocation == null)
            {
                String msg = MessageFormat.format(
                        "Configuration error: {0}={1} but: {2}={3}",
                        SecurityConf.CONNECTOR_USE_SSL,
                        this.isConnectorUseSSL(),
                        SecurityConf.CONNECTOR_SECURITY_KEYSTORE_LOCATION,
                        this.keystoreLocation);
                CLUtils.println(msg, CLLogLevel.detailed);
                throw new ServerRuntimeException(msg, new AssertionError(
                        "File must exist"));
            }
            File f = new File(this.keystoreLocation);
            // --- Find absolute path if needed
            if (!f.isFile())
            {
                f = this.findAbsolutePath(f);
                this.keystoreLocation = f.getAbsolutePath();
            }
            // --- Check file is readable
            if (!f.isFile() || !f.canRead())
            {
                String msg = MessageFormat.format(
                        "Cannot find or read {0} file: {1}", KEYSTORE_LOCATION,
                        this.keystoreLocation);
                CLUtils.println(msg, CLLogLevel.detailed);
                throw new ServerRuntimeException(msg, new AssertionError(
                        "File must exist"));
            }

            // --- Check password is defined
            if (this.keystorePassword == null)
            {
                throw new ConfigurationException(
                        SecurityConf.CONNECTOR_SECURITY_KEYSTORE_PASSWORD);
            }
        }

        // --- Check Aliases are defined in the keystore ---
        if ((this.isEncryptionNeeded() && this.keystoreLocation != null)
                || (tungstenApplicationName == TUNGSTEN_APPLICATION_NAME.CONNECTOR && this
                        .isConnectorUseSSL()))
        {
            FileInputStream is = null;
            try
            {
                // Aliases to check
                HashMap<String, String> mapAliases = this
                        .getMapKeystoreAliasesForTungstenApplication();

                boolean connector_alias_client_to_connector_isFound = false;
                boolean connector_alias_connector_to_db_isFound = false;

                String connector_alias_client_to_connector = mapAliases
                        .get(SecurityConf.KEYSTORE_ALIAS_CONNECTOR_CLIENT_TO_CONNECTOR);
                String connector_alias_connector_to_db = mapAliases
                        .get(SecurityConf.KEYSTORE_ALIAS_CONNECTOR_CONNECTOR_TO_DB);

                // If an aliase is not defined, do not look for it...obviously
                connector_alias_client_to_connector_isFound = (connector_alias_client_to_connector == null)
                        ? true
                        : false;
                connector_alias_connector_to_db_isFound = (connector_alias_connector_to_db == null)
                        ? true
                        : false;

                // Load the keystore in the user's home directory
                // Check only if there are aliases to find
                if (!connector_alias_client_to_connector_isFound
                        || !connector_alias_connector_to_db_isFound)
                {
                    is = new FileInputStream(this.getKeystoreLocation());
                    KeyStore keystore = KeyStore.getInstance(KeyStore
                            .getDefaultType());
                    String password = this.getKeystorePassword();
                    keystore.load(is, password.toCharArray());

                    // List the aliases
                    Enumeration<String> enumAliases = keystore.aliases();
                    while (enumAliases.hasMoreElements())
                    {
                        String alias = enumAliases.nextElement();

                        // Does alias refer to a private key?
                        // boolean b = keystore.isKeyEntry(alias);

                        // Does alias refer to a trusted certificate?
                        // b = keystore.isCertificateEntry(alias);

                        connector_alias_client_to_connector_isFound = connector_alias_client_to_connector_isFound == true
                                || (connector_alias_client_to_connector != null && connector_alias_client_to_connector
                                        .equals(alias));

                        connector_alias_connector_to_db_isFound = connector_alias_connector_to_db_isFound == true
                                || (connector_alias_connector_to_db != null && connector_alias_connector_to_db
                                        .equals(alias));
                    }
                    // --- Throw Exception when an alias is defined but not
                    // found
                    String _aliasErrorMessage = "Keystore alias is defined as {0}={1} but cannot be found in {2}";
                    // client to connector
                    if (connector_alias_client_to_connector != null
                            && connector_alias_client_to_connector_isFound == false)
                    {
                        this.closeInputStream(is);

                        String aliasErrorMessage = MessageFormat
                                .format(_aliasErrorMessage,
                                        SecurityConf.KEYSTORE_ALIAS_CONNECTOR_CLIENT_TO_CONNECTOR,
                                        connector_alias_client_to_connector,
                                        this.getKeystoreLocation());
                        throw new ServerRuntimeException(aliasErrorMessage,
                                new AssertionError(
                                        "Alias must exist in keystore"));
                    }
                    // Connector to DB
                    if (connector_alias_connector_to_db != null
                            && connector_alias_connector_to_db_isFound == false)
                    {
                        this.closeInputStream(is);

                        String aliasErrorMessage = MessageFormat
                                .format(_aliasErrorMessage,
                                        SecurityConf.KEYSTORE_ALIAS_CONNECTOR_CONNECTOR_TO_DB,
                                        connector_alias_connector_to_db,
                                        this.getKeystoreLocation());
                        throw new ServerRuntimeException(aliasErrorMessage,
                                new AssertionError(
                                        "Alias must exist in keystore"));
                    }
                }

            }
            catch (java.security.cert.CertificateException e)
            {
                this.closeInputStream(is);
                throw new ConfigurationException(e.getMessage());
            }
            catch (NoSuchAlgorithmException e)
            {
                this.closeInputStream(is);
                throw new ConfigurationException(e.getMessage());
            }
            catch (FileNotFoundException e)
            {
                this.closeInputStream(is);
                // Noting to do: this has already been checked
            }
            catch (KeyStoreException e)
            {
                this.closeInputStream(is);
                throw new ConfigurationException(e.getMessage());
            }
            catch (IOException e)
            {
                this.closeInputStream(is);
                throw new ConfigurationException(e.getMessage());
            }
            this.closeInputStream(is); // Close inputStream if not already done
        }

        // --- Check Truststore location ---
        if ((this.isEncryptionNeeded() && this.truststoreLocation != null)
                || (tungstenApplicationName == TUNGSTEN_APPLICATION_NAME.CONNECTOR && this
                        .isConnectorUseSSL()))
        {
            // --- Check file location is specified ---
            if (this.truststoreLocation == null)
            {
                String msg = MessageFormat.format(
                        "Configuration error: {0}={1} but: {2}={3}",
                        SecurityConf.CONNECTOR_USE_SSL,
                        this.isConnectorUseSSL(),
                        SecurityConf.CONNECTOR_SECURITY_TRUSTSTORE_LOCATION,
                        this.truststoreLocation);
                CLUtils.println(msg, CLLogLevel.detailed);
                throw new ServerRuntimeException(msg, new AssertionError(
                        "File must exist"));
            }
            File f = new File(this.truststoreLocation);
            // --- Find absolute path if needed
            if (!f.isFile())
            {
                f = this.findAbsolutePath(f);
                this.truststoreLocation = f.getAbsolutePath();
            }
            // --- Check file is readable
            if (!f.isFile() || !f.canRead())
            {
                String msg = MessageFormat.format(
                        "Cannot find or read {0} file: {1}",
                        TRUSTSTORE_LOCATION, this.truststoreLocation);
                CLUtils.println(msg, CLLogLevel.detailed);
                throw new ServerRuntimeException(msg, new AssertionError(
                        "File must exist"));
            }
        }
        else if (this.isEncryptionNeeded() && this.truststoreLocation == null)
        {
            throw new ConfigurationException("truststore.location");
        }

        // --- Check password for Truststore ---
        if (this.isEncryptionNeeded() && this.truststorePassword == null)
        {
            throw new ConfigurationException("truststore.password");
        }

        // --- Check password file location ---
        if (this.isAuthenticationNeeded() && this.passwordFileLocation != null)
        {
            File f = new File(this.passwordFileLocation);
            // --- Find absolute path if needed
            if (!f.isFile())
            {
                f = this.findAbsolutePath(f);
                this.passwordFileLocation = f.getAbsolutePath();
            }
            // --- Check file is readable
            if (!f.isFile() || !f.canRead())
            {
                String msg = MessageFormat.format(
                        "Cannot find or read {0} file: {1}",
                        SecurityConf.SECURITY_PASSWORD_FILE_LOCATION,
                        this.passwordFileLocation);
                CLUtils.println(msg, CLLogLevel.detailed);
                throw new ServerRuntimeException(msg, new AssertionError(
                        "File must exist"));
            }
        }

        // --- Check access file location ---
        if (this.isAuthenticationNeeded() && this.accessFileLocation != null)
        {
            File f = new File(this.accessFileLocation);
            // --- Find absolute path if needed
            if (!f.isFile())
            {
                f = this.findAbsolutePath(f);
                this.accessFileLocation = f.getAbsolutePath();
            }
            // --- Check file is readable
            if (!f.isFile() || !f.canRead())
            {
                String msg = MessageFormat.format(
                        "Cannot find or read {0} file: {1}",
                        SecurityConf.SECURITY_ACCESS_FILE_LOCATION,
                        this.accessFileLocation);
                CLUtils.println(msg, CLLogLevel.detailed);
                throw new ServerRuntimeException(msg, new AssertionError(
                        "File must exist"));
            }
        }

    }

    /**
     * Get the AuthenticationInfo as a TungstenProperties
     * 
     * @return TungstenProperties
     */
    public TungstenProperties getAsTungstenProperties()
    {
        TungstenProperties jmxProperties = new TungstenProperties();
        jmxProperties.put(AUTHENTICATION_INFO_PROPERTY, this);

        return jmxProperties;
    }

    // /**
    // * Retrieve (encrypted) password from file
    // *
    // * @throws ConfigurationException
    // */
    // public void retrievePasswordFromFile() throws ConfigurationException
    // {
    // TungstenProperties passwordProps = SecurityHelper
    // .loadPasswordsFromAuthenticationInfo(this);
    // String username = this.getUsername();
    // String goodPassword = passwordProps.get(username);
    // this.password = goodPassword;
    //
    // if (goodPassword == null)
    // throw new ConfigurationException(
    // MessageFormat
    // .format("Cannot find password for username= {0} \n PasswordFile={1}",
    // username, this.getPasswordFileLocation()));
    // }

    /**
     * Returns the decrypted password
     * 
     * @return String containing the (if needed) decrypted password
     * @throws ConfigurationException
     */
    public String getDecryptedPassword() throws ConfigurationException
    {
        if (this.password == null)
            return null;

        String clearTextPassword = this.password;
        // --- Try to decrypt the password ---
        if (this.useEncryptedPasswords)
        {
            Encryptor encryptor = new Encryptor(this);
            clearTextPassword = encryptor.decrypt(this.password);
        }
        return clearTextPassword;
    }

    /**
     * @return the encrypted password if useEncryptedPasswords==true or the
     *         clear text password otherwise
     */
    public String getPassword()
    {
        return this.password;
    }

    public void setKeystore(String keyStoreLocation, String keystorePassword)
    {
        this.setKeystoreLocation(keyStoreLocation);
        this.setKeystorePassword(keystorePassword);
    }

    public void setTruststore(String truststoreLocation,
            String truststorePassword)
    {
        this.setTruststoreLocation(truststoreLocation);
        this.setTruststorePassword(truststorePassword);
    }

    public boolean isAuthenticationNeeded()
    {
        return authenticationNeeded;
    }

    public void setAuthenticationNeeded(boolean authenticationNeeded)
    {
        this.authenticationNeeded = authenticationNeeded;
    }

    public boolean isEncryptionNeeded()
    {
        return encryptionNeeded;
    }

    public void setEncryptionNeeded(boolean encryptionNeeded)
    {
        this.encryptionNeeded = encryptionNeeded;
    }

    public String getKeystoreLocation()
    {

        return keystoreLocation;
    }

    public void setKeystoreLocation(String keystoreLocation)
    {
        this.keystoreLocation = keystoreLocation;
    }

    public String getUsername()
    {
        return username;
    }

    public void setUsername(String username)
    {
        this.username = username;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    public String getPasswordFileLocation()
    {
        return passwordFileLocation;
    }

    public void setPasswordFileLocation(String passwordFileLocation)
    {
        this.passwordFileLocation = passwordFileLocation;
    }

    public String getAccessFileLocation()
    {
        return accessFileLocation;
    }

    public void setAccessFileLocation(String accessFileLocation)
    {
        this.accessFileLocation = accessFileLocation;
    }

    public String getKeystorePassword()
    {
        return keystorePassword;
    }

    public void setKeystorePassword(String keystorePassword)
    {
        this.keystorePassword = keystorePassword;
    }

    public String getTruststoreLocation()
    {
        return truststoreLocation;
    }

    public void setTruststoreLocation(String truststoreLocation)
    {
        this.truststoreLocation = truststoreLocation;
    }

    public String getTruststorePassword()
    {
        return truststorePassword;
    }

    public void setTruststorePassword(String truststorePassword)
    {
        this.truststorePassword = truststorePassword;
    }

    public boolean isUseTungstenAuthenticationRealm()
    {
        return useTungstenAuthenticationRealm;
    }

    public void setUseTungstenAuthenticationRealm(
            boolean useTungstenAuthenticationRealm)
    {
        this.useTungstenAuthenticationRealm = useTungstenAuthenticationRealm;
    }

    public boolean isUseEncryptedPasswords()
    {
        return useEncryptedPasswords;
    }

    public void setUseEncryptedPasswords(boolean useEncryptedPasswords)
    {
        this.useEncryptedPasswords = useEncryptedPasswords;
    }

    public String getParentPropertiesFileLocation()
    {
        return parentPropertiesFileLocation;
    }

    public void setParentPropertiesFileLocation(
            String parentPropertiesFileLocation)
    {
        this.parentPropertiesFileLocation = parentPropertiesFileLocation;
    }

    /**
     * Returns the connectorUseSSL value.
     * 
     * @return Returns the connectorUseSSL.
     */
    public boolean isConnectorUseSSL()
    {
        return connectorUseSSL;
    }

    /**
     * Sets the connectorUseSSL value.
     * 
     * @param connectorUseSSL The connectorUseSSL to set.
     */
    public void setConnectorUseSSL(boolean connectorUseSSL)
    {
        this.connectorUseSSL = connectorUseSSL;
    }

    /**
     * Returns the parentProperties value.
     * 
     * @return Returns the parentProperties.
     */
    public TungstenProperties getParentProperties()
    {
        return parentProperties;
    }

    /**
     * Sets the parentProperties value.
     * 
     * @param parentProperties The parentProperties to set.
     */
    public void setParentProperties(TungstenProperties parentProperties)
    {
        this.parentProperties = parentProperties;
    }

    /**
     * Returns the mapKeystoreAliasesForTungstenApplication value.
     * 
     * @return Returns the mapKeystoreAliasesForTungstenApplication.
     */
    public HashMap<String, String> getMapKeystoreAliasesForTungstenApplication()
    {
        return mapKeystoreAliasesForTungstenApplication;
    }

    /**
     * Sets the mapKeystoreAliasesForTungstenApplication value.
     * 
     * @param mapKeystoreAliasesForTungstenApplication The
     *            mapKeystoreAliasesForTungstenApplication to set.
     */
    public void setMapKeystoreAliasesForTungstenApplication(
            HashMap<String, String> mapKeystoreAliasesForTungstenApplication)
    {
        this.mapKeystoreAliasesForTungstenApplication = mapKeystoreAliasesForTungstenApplication;
    }

    /**
     * Get the alias defined for the corresponding Tungsten application
     * 
     * @param tungestenApplicationName
     * @return the alias defined in security.properties if it exists. null
     *         otherwise
     */
    public String getKeystoreAliasForTungstenApplication(
            TUNGSTEN_APPLICATION_NAME tungestenApplicationName)
    {
        this.mapKeystoreAliasesForTungstenApplication
                .get(tungestenApplicationName);

        return null;
    }

    /**
     * Try to find a file absolute path from a series of default location
     * 
     * @param fileToFind the file for which to look for an absolute path
     * @return the file with absolute path if found. returns the same unchanged
     *         object otherwise
     */
    private File findAbsolutePath(File fileToFind)
    {
        File foundFile = fileToFind;

        try
        {
            String clusterHome = ClusterConfiguration.getClusterHome();

            if (fileToFind.getPath() == fileToFind.getName()) // No absolute or
            // relative path
            // was given
            {
                // --- Try to find find in: cluster-home/conf
                File candidateFile = new File(clusterHome + File.separator
                        + "conf" + File.separator + fileToFind.getName());
                if (candidateFile.isFile())
                {
                    foundFile = candidateFile;
                    logger.debug(MessageFormat
                            .format("File was specified with name only, and found in default location: {0}",
                                    foundFile.getAbsoluteFile()));
                }
                else
                    throw new ConfigurationException(MessageFormat.format(
                            "File does not exist: {0}",
                            candidateFile.getAbsolutePath()));
            }
        }
        catch (ConfigurationException e)
        {
            logger.debug(MessageFormat.format(
                    "Cannot find absolute path for file: {0} \n{1}",
                    fileToFind.getName(), e.getMessage()));
            return fileToFind;
        }

        return foundFile;
    }

    /**
     * Silently tries to close an InputStream.
     * 
     * @param in
     */
    private void closeInputStream(InputStream in)
    {
        try
        {
            in.close();
        }
        catch (Exception e)
        {
            // Nothing to do, it's a last chance close
        }
    }

}
