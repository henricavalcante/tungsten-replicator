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

import java.util.Collections;

import javax.management.remote.JMXAuthenticator;
import javax.management.remote.JMXPrincipal;
import javax.security.auth.Subject;

import com.continuent.tungsten.common.config.cluster.ConfigurationException;
import com.continuent.tungsten.common.security.PasswordManager.ClientApplicationType;

/**
 * Custom Authentication Realm
 * 
 * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
 * @version 1.0
 */
public class RealmJMXAuthenticator implements JMXAuthenticator
{
//    private static final Logger logger                 = Logger.getLogger(JmxManager.class);
//    private TungstenProperties  passwordProps          = null;

    private AuthenticationInfo  authenticationInfo     = null;

    private PasswordManager     passwordManager        = null;

    private static final String INVALID_CREDENTIALS    = "Invalid credentials";
    private static final String AUTHENTICATION_PROBLEM = "Error while trying to authenticate";

    public RealmJMXAuthenticator(AuthenticationInfo authenticationInfo) throws ConfigurationException
    {
        this.authenticationInfo = authenticationInfo;
        this.passwordManager    = new PasswordManager(authenticationInfo.getParentPropertiesFileLocation(), ClientApplicationType.RMI_JMX);

//        this.passwordProps = SecurityHelper
//                .loadPasswordsFromAuthenticationInfo(authenticationInfo);
    }

    /**
     * Authenticate {@inheritDoc}
     * 
     * @see javax.management.remote.JMXAuthenticator#authenticate(java.lang.Object)
     */
    public Subject authenticate(Object credentials)
    {
        boolean authenticationOK = false;

        String[] aCredentials = this.checkCredentials(credentials);

        // --- Get auth parameters ---
        String username = (String) aCredentials[0];
        String password = (String) aCredentials[1];
//        String realm = (String) aCredentials[2];

        // --- Perform authentication ---
        try
        {
            // Password file syntax:
            // username=password
//            String goodPassword = this.passwordProps.get(username);
            String goodPassword = this.passwordManager.getClearTextPasswordForUser(username);
//            this.authenticationInfo.setPassword(goodPassword);
//            // Decrypt password if needed
//            goodPassword = this.authenticationInfo.getPassword();

            if (goodPassword.equals(password))
                authenticationOK = true;

        }
        catch (Exception e)
        {
            // Throw same exception as authentication not OK :
            // Do not give any hint on failure reason
            throw new SecurityException(AUTHENTICATION_PROBLEM);
        }

        if (authenticationOK)
        {
            return new Subject(true, Collections.singleton(new JMXPrincipal(
                    username)), Collections.EMPTY_SET, Collections.EMPTY_SET);
        }
        else
        {
            throw new SecurityException(INVALID_CREDENTIALS);
        }
    }

    /**
     * Check credentials are OK
     * 
     * @param credentials
     * @return String[] containing {username, password, realm}
     */
    private String[] checkCredentials(Object credentials)
    {
        // Verify that credentials is of type String[].
        if (!(credentials instanceof String[]))
        {
            // Special case for null so we get a more informative message
            if (credentials == null)
            {
                throw new SecurityException("Credentials required");
            }
            throw new SecurityException("Credentials should be String[]");
        }

        // Verify that the array contains three elements
        // (username/password/realm).
        final String[] aCredentials = (String[]) credentials;
        if (aCredentials.length != 3)
        {
            throw new SecurityException("Credentials should have 3 elements");
        }

        return aCredentials;
    }
    
    /**
     * Returns the authenticationInfo value.
     * 
     * @return Returns the authenticationInfo.
     */
    public AuthenticationInfo getAuthenticationInfo()
    {
        return authenticationInfo;
    }

    /**
     * Sets the authenticationInfo value.
     * 
     * @param authenticationInfo The authenticationInfo to set.
     */
    public void setAuthenticationInfo(AuthenticationInfo authenticationInfo)
    {
        this.authenticationInfo = authenticationInfo;
    }

}
