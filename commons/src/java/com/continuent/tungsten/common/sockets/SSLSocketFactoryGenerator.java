/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2013 Continuent Inc.
 * Contact: tungsten@continuent.org
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of version 2 of the GNU General Public License as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA
 *
 * Initial developer(s): Ludovic Launer
 * Contributor(s): 
 */

package com.continuent.tungsten.common.sockets;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509KeyManager;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.security.AuthenticationInfo;

/**
 * Implements an class to generate SSLSocketFactory instances. The author
 * gratefully acknowledges the blog article by Alexandre Saudate for providing
 * guidance in the implementation.
 * 
 * @see <a href=
 *      "http://alesaudate.wordpress.com/2010/08/09/how-to-dynamically-select-a-certificate-alias-when-invoking-web-services/">
 *      How to dynamically select a certificate alias when invoking web
 *      services</a>
 */
public class SSLSocketFactoryGenerator
{
    private static final Logger logger = Logger
            .getLogger(SSLSocketFactoryGenerator.class);

    private String     alias                                = null;
    private String     keystoreLocation                     = null;
    private String     trustStoreLocation                   = null;
    AuthenticationInfo securityPropertiesAuthenticationInfo = null;

    public SSLSocketFactoryGenerator(String alias,
            AuthenticationInfo securityPropertiesAuthenticationInfo)
    {
        this.alias = alias;
        this.securityPropertiesAuthenticationInfo = securityPropertiesAuthenticationInfo;
        this.keystoreLocation = securityPropertiesAuthenticationInfo
                .getKeystoreLocation();
        this.trustStoreLocation = securityPropertiesAuthenticationInfo
                .getTruststoreLocation();
    }

    public SSLSocketFactory getSSLSocketFactory()
            throws IOException, GeneralSecurityException
    {
        // --- No alias defined. Use default SSL socket factory ---
        if (this.alias == null)
        {
            logger.debug(
                    "No keystore alias entry defined. Will use default SSLSocketFactory selecting 1st entry in keystore !");
            return (SSLSocketFactory) SSLSocketFactory.getDefault();
        }
        // --- Alias defined. Use custom alias selector ---
        else
        {
            KeyManager[] keyManagers = getKeyManagers();
            TrustManager[] trustManagers = getTrustManagers();

            // For each key manager, check if it is a X509KeyManager (because we
            // will override its //functionality
            for (int i = 0; i < keyManagers.length; i++)
            {
                if (keyManagers[i] instanceof X509KeyManager)
                {
                    keyManagers[i] = new AliasSelectorKeyManager(
                            (X509KeyManager) keyManagers[i], alias);
                }
            }

            SSLContext context = SSLContext.getInstance("SSL");
            context.init(keyManagers, trustManagers, null);

            SSLSocketFactory ssf = context.getSocketFactory();
            return ssf;
        }

    }

    public String getKeyStorePassword()
    {
        return this.securityPropertiesAuthenticationInfo.getKeystorePassword();
    }

    public String getTrustStorePassword()
    {
        return this.securityPropertiesAuthenticationInfo
                .getTruststorePassword();
    }

    public String getKeyStore()
    {
        return keystoreLocation;
    }

    public String getTrustStore()
    {
        return trustStoreLocation;
    }

    private KeyManager[] getKeyManagers()
            throws IOException, GeneralSecurityException
    {
        // Init a key store with the given file.

        String alg = KeyManagerFactory.getDefaultAlgorithm();
        KeyManagerFactory kmFact = KeyManagerFactory.getInstance(alg);

        FileInputStream fis = new FileInputStream(getKeyStore());
        KeyStore ks = KeyStore.getInstance("jks");
        ks.load(fis, getKeyStorePassword().toCharArray());
        fis.close();

        // Init the key manager factory with the loaded key store
        kmFact.init(ks, getKeyStorePassword().toCharArray());

        KeyManager[] kms = kmFact.getKeyManagers();
        return kms;
    }

    protected TrustManager[] getTrustManagers()
            throws IOException, GeneralSecurityException
    {
        String alg = TrustManagerFactory.getDefaultAlgorithm();
        TrustManagerFactory tmFact = TrustManagerFactory.getInstance(alg);

        FileInputStream fis = new FileInputStream(getTrustStore());
        KeyStore ks = KeyStore.getInstance("jks");
        ks.load(fis, getTrustStorePassword().toCharArray());
        fis.close();

        tmFact.init(ks);

        TrustManager[] tms = tmFact.getTrustManagers();
        return tms;
    }
}