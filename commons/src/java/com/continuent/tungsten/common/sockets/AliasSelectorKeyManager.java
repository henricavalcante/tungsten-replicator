/**
 * VMware Continuent Tungsten Replicator
 * Copyright (C) 2007-2015 VMware, Inc. All rights reserved.
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

package com.continuent.tungsten.common.sockets;

import java.net.Socket;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.text.MessageFormat;

import javax.net.ssl.X509KeyManager;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.security.SecurityHelper;

/**
 * Implements a class handle selecting multiple aliases from an X509KeyManager.
 * The author gratefully acknowledges the blog article by Alexandre Saudate for
 * providing guidance for the implementation.
 * 
 * @see <a href=
 *      "http://alesaudate.wordpress.com/2010/08/09/how-to-dynamically-select-a-certificate-alias-when-invoking-web-services/">
 *      How to dynamically select a certificate alias when invoking web
 *      services</a>
 */
public class AliasSelectorKeyManager implements X509KeyManager
{
    private static final Logger logger = Logger
            .getLogger(AliasSelectorKeyManager.class);

    private X509KeyManager sourceKeyManager = null;
    private String         alias;

    public AliasSelectorKeyManager(X509KeyManager keyManager, String alias)
    {
        this.sourceKeyManager = keyManager;
        this.alias = alias;

    }

    public String chooseClientAlias(String[] keyType, Principal[] issuers,
            Socket socket)
    {
        boolean aliasFound = false;

        // Get all aliases from the key manager. If any matches with the managed
        // alias, then return it. If the alias has not been found, return null
        // (and let the API handle it, causing the handshake to fail).
        for (int i = 0; i < keyType.length && !aliasFound; i++)
        {
            String[] validAliases = sourceKeyManager
                    .getClientAliases(keyType[i], issuers);
            if (validAliases != null)
            {
                for (int j = 0; j < validAliases.length && !aliasFound; j++)
                {
                    if (validAliases[j].equals(alias))
                        aliasFound = true;
                }
            }
        }

        if (aliasFound)
        {
            return alias;
        }
        else
            return null;
    }

    public String chooseServerAlias(String keyType, Principal[] issuers,
            Socket socket)
    {
        if (this.alias == null)
            return sourceKeyManager.chooseServerAlias(keyType, issuers, socket);
        else
        {
            boolean aliasFound = false;

            String[] validAliases = sourceKeyManager.getClientAliases(keyType,
                    issuers);
            if (validAliases != null)
            {
                for (int j = 0; j < validAliases.length && !aliasFound; j++)
                {
                    if (validAliases[j].equals(alias))
                        aliasFound = true;
                }
            }

            if (aliasFound)
                return alias;
            else
            {
                String keyStoreLocation = SecurityHelper.getKeyStoreLocation();
                logger.error(MessageFormat.format(
                        "Could not find alias: {0} in keystore: {1}",
                        this.alias, keyStoreLocation));
            }
        }

        return null;
    }

    public X509Certificate[] getCertificateChain(String alias)
    {
        return sourceKeyManager.getCertificateChain(alias);
    }

    public String[] getClientAliases(String keyType, Principal[] issuers)
    {
        return sourceKeyManager.getClientAliases(keyType, issuers);
    }

    public PrivateKey getPrivateKey(String alias)
    {

        return sourceKeyManager.getPrivateKey(alias);
    }

    public String[] getServerAliases(String keyType, Principal[] issuers)
    {
        return sourceKeyManager.getServerAliases(keyType, issuers);
    }

}