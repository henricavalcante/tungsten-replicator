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

import java.io.FileInputStream;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.Certificate;
import java.text.MessageFormat;
import java.util.Enumeration;

import javax.crypto.Cipher;
import javax.xml.bind.DatatypeConverter;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.cluster.ConfigurationException;
import com.continuent.tungsten.common.jmx.ServerRuntimeException;

/**
 * Utility class to cipher / uncipher critical information based on public /
 * private key encryption
 * 
 * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
 * @version 1.0
 */

public class Encryptor
{
    private static final Logger logger             = Logger.getLogger(Encryptor.class);

    private AuthenticationInfo  authenticationInfo = null;

    /**
     * Creates a new <code>Encryptor</code> object
     * 
     * @param authenticationInfo
     * @throws ConfigurationException 
     * @throws ServerRuntimeException 
     */
    public Encryptor(AuthenticationInfo authenticationInfo) throws ServerRuntimeException, ConfigurationException
    {
        this.authenticationInfo = authenticationInfo;

        // --- Check parameters ---
        this.authenticationInfo.checkAndCleanAuthenticationInfo();
    }

    /**
     * Retrieve public and/or private keys from Keystore/Strustore Uses the
     * first Alias found in the keystore
     * 
     * @return KeyPair
     */
    public KeyPair getKeys(String storeLocation, String storePassword)
    {
        FileInputStream storeFile;
        KeyPair keyPair = null;

        try
        {
            storeFile = new FileInputStream(storeLocation);
            KeyStore keystore = KeyStore.getInstance(KeyStore.getDefaultType());

            keystore.load(storeFile, storePassword.toCharArray());

            Enumeration<String> listAliases = keystore.aliases();

            // --- Get first alias ---
            String alias = null;
            if (listAliases.hasMoreElements())
                alias = listAliases.nextElement();

            // Get certificate of public key
            Certificate cert = keystore.getCertificate(alias);
            // Get public key
            PublicKey publicKey = cert.getPublicKey();

            Key key = keystore.getKey(alias, storePassword.toCharArray());
            if (key instanceof PrivateKey)
            {
                keyPair = new KeyPair(publicKey, (PrivateKey) key);
            }
            else
            {
                keyPair = new KeyPair(publicKey, null);
            }

        }
        catch (Exception e)
        {
            String msg = MessageFormat.format(
                    "Cannot retrieve key from: {0} Reason={1}", storeLocation,
                    e.getMessage());
            logger.error(msg);
            throw new ServerRuntimeException(msg, e);
        }

        return keyPair;
    }

    /**
     * Get the Public key from a TrustStore
     * 
     * @return Public key from the Truststore
     */
    public PublicKey getPublicKey_from_Truststore()
    {
        KeyPair keyPair = this.getKeys(
                this.authenticationInfo.getTruststoreLocation(),
                this.authenticationInfo.getTruststorePassword());
        return keyPair.getPublic();
    }

    /**
     * Get the Public and Private key from a KeyStore
     * 
     * @return PrivateKey extracted from the Keystore
     */
    public PrivateKey getPrivateKey_from_KeyStore()
    {
        KeyPair keyPair = this.getKeys(
                this.authenticationInfo.getKeystoreLocation(),
                this.authenticationInfo.getKeystorePassword());
        return keyPair.getPrivate();
    }

    /**
     * Encrypt a String using public key located in truststore.
     * 
     * @param message to be encrypted
     * @return Base64 encoded and encryoted message
     */
    public String encrypt(String message)
    {
        String base64 = null;
        PublicKey publicKey = this.getPublicKey_from_Truststore();

        try
        {
            Cipher cipher = Cipher.getInstance(publicKey.getAlgorithm());
            cipher.init(Cipher.ENCRYPT_MODE, publicKey);

            // Gets the raw bytes to encrypt, UTF8 is needed for
            // having a standard character set
            byte[] stringBytes = message.getBytes("UTF8");

            // Encrypt using the cypher
            byte[] raw = cipher.doFinal(stringBytes);

            // Converts to base64 for easier display.
            base64 = DatatypeConverter.printBase64Binary(raw);
        }
        catch (Exception e)
        {
            String msg = MessageFormat.format(
                    "Cannot encrypt message. Error= {0}", e.getMessage());
            logger.error(msg);
            throw new ServerRuntimeException(msg, e);
        }

        return base64;
    }

    /**
     * Decrypt a String using private key located in KeyStore.
     * 
     * @param encryptedMessage
     * @return Decrypted String
     * @throws ConfigurationException
     */
    public String decrypt(String encryptedMessage)
            throws ConfigurationException
    {
        if (encryptedMessage == null)
            return null;

        String clearMessage = null;
        PrivateKey privateKey = this.getPrivateKey_from_KeyStore();

        try
        {
            Cipher cipher = Cipher.getInstance(privateKey.getAlgorithm());
            cipher.init(Cipher.DECRYPT_MODE, privateKey);

            // Decode the BASE64 coded message
            byte[] raw = DatatypeConverter.parseBase64Binary(encryptedMessage);

            // Decode the message
            byte[] stringBytes = cipher.doFinal(raw);

            // converts the decoded message to a String
            clearMessage = new String(stringBytes, "UTF8");      
            
        }
        catch (Exception e)
        {
            String msg = MessageFormat.format(
                    "Cannot decrypt message. Error= {0}",e);
            logger.error(msg);
            throw new ConfigurationException(msg);
        }
        return clearMessage;
    }

}
