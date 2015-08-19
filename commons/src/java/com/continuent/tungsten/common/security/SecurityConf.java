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

/**
 * This class defines a SecurityConf This matches parameters used for
 * Authentication and Encryption
 * 
 * @author Ludovic Launer
 * @version 1.0
 */
public class SecurityConf
{
    /** Location of the file where this is all coming from **/
    static public final String SECURITY_PROPERTIES_PARENT_FILE_LOCATION                              = "security.properties.parent.file.location";

    static public final String SECURITY_PROPERTIES_FILE_NAME                                         = "security.properties";

    /** Location of file used for security **/
    static public final String SECURITY_DIR                                                          = "security.dir";

    /** Authentication and Encryption */
    static public final String SECURITY_JMX_USE_AUTHENTICATION                                       = "security.rmi.authentication";
    static public final String SECURITY_JMX_USERNAME                                                 = "security.rmi.authentication.username";
    static public final String SECURITY_JMX_USE_TUNGSTEN_AUTHENTICATION_REALM                        = "security.rmi.tungsten.authenticationRealm";
    static public final String SECURITY_JMX_USE_TUNGSTEN_AUTHENTICATION_REALM_ENCRYPTED_PASSWORD     = "security.rmi.tungsten.authenticationRealm.encrypted.password";
    static public final String SECURITY_JMX_USE_ENCRYPTION                                           = "security.rmi.encryption";
    static public final String SECURITY_PASSWORD_FILE_LOCATION                                       = "security.password_file.location";
    static public final String SECURITY_ACCESS_FILE_LOCATION                                         = "security.rmi.jmxremote.access_file.location";
    static public final String SECURITY_KEYSTORE_LOCATION                                            = "security.keystore.location";
    static public final String SECURITY_KEYSTORE_PASSWORD                                            = "security.keystore.password";
    static public final String SECURITY_TRUSTSTORE_LOCATION                                          = "security.truststore.location";
    static public final String SECURITY_TRUSTSTORE_PASSWORD                                          = "security.truststore.password";
    static public final String CONNECTOR_USE_SSL                                                     = "connector.security.use.ssl";
    static public final String CONNECTOR_SECURITY_KEYSTORE_LOCATION                                  = "connector.security.keystore.location";
    static public final String CONNECTOR_SECURITY_KEYSTORE_PASSWORD                                  = "connector.security.keystore.password";
    static public final String CONNECTOR_SECURITY_TRUSTSTORE_LOCATION                                = "connector.security.truststore.location";
    static public final String CONNECTOR_SECURITY_TRUSTSTORE_PASSWORD                                = "connector.security.truststore.password";

    /** Alias for Tungsten applications */
    static public final String KEYSTORE_ALIAS_CONNECTOR_CLIENT_TO_CONNECTOR                          = "connector.security.keystore.alias.client.to.connector";
    static public final String KEYSTORE_ALIAS_CONNECTOR_CONNECTOR_TO_DB                              = "connector.security.keystore.alias.connector.to.db";

    /** Authentication and Encryption: DEFAULT values */
    static public final String SECURITY_USE_AUTHENTICATION_DEFAULT                                   = "false";
    static public final String SECURITY_USE_ENCRYPTION_DEFAULT                                       = "false";
    static public final String SECURITY_USE_TUNGSTEN_AUTHENTICATION_REALM_DEFAULT                    = "true";
    static public final String SECURITY_USE_TUNGSTEN_AUTHENTICATION_REALM_ENCRYPTED_PASSWORD_DEFAULT = "false";

    static public final String KEYSTORE_ALIAS_CONNECTOR_CLIENT_TO_CONNECTOR_DEFAULT                  = null;
    static public final String KEYSTORE_ALIAS_CONNECTOR_CONNECTOR_TO_DB_DEFAULT                      = null;

    /** Application specific information */
    static public final String SECURITY_APPLICATION_RMI_JMX                                          = "rmi_jmx";
    static public final String SECURITY_APPLICATION_CONNECTOR                                        = "connector";

}
