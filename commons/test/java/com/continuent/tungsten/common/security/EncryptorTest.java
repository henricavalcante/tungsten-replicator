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

import java.security.KeyPair;

import junit.framework.TestCase;

import com.continuent.tungsten.common.config.cluster.ConfigurationException;
import com.continuent.tungsten.common.jmx.ServerRuntimeException;

/**
 * Implements a simple unit test for AuthenticationInfo
 * 
 * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
 * @version 1.0
 */
public class EncryptorTest extends TestCase
{
    AuthenticationInfo authInfo = new AuthenticationInfo();
    Encryptor encryptor = null;
    
    public EncryptorTest()
    {
        this.authInfo.setKeystore("tungsten_sample_keystore.jks", "secret");
        this.authInfo.setTruststore("tungsten_sample_truststore.ts", "secret");
        
//        authInfo.setKeystore("tungsten_keystore.jks", "tungsten");
//        authInfo.setTruststore("tungsten_truststore.ts", "tungsten");
        
        try
        {
            this.encryptor = new Encryptor(authInfo);
        }
        catch (ServerRuntimeException e)
        {
            assertTrue(false);
        }
        catch (ConfigurationException e)
        {
            assertTrue(false);
        }
    }
    
    /**
     * Tests encryption / decryption
     */
    public void testEncrytion() throws Exception
    {
        // --- Test encryption / decryption ---
        String testString       = "secret";
        String someRandomString = "and now, for something completly different";
        
        String encryptedString = encryptor.encrypt(testString);
        String decryptedString = encryptor.decrypt(encryptedString);
        
        assertEquals(testString, decryptedString);
        assertFalse(testString.equals(someRandomString));
    }
    
    public void testGetKeysOnKeystore()
    {
        // -- Keystore--
        KeyPair keyPair = encryptor.getKeys(this.authInfo.getKeystoreLocation(), this.authInfo.getKeystorePassword());
        
        assertNotNull(keyPair.getPrivate());
        assertNotNull(keyPair.getPublic());
    }
    
    public void testGetKeysOnTruststore()
    {
        // -- Keystore--
        KeyPair keyPair = encryptor.getKeys(this.authInfo.getTruststoreLocation(), this.authInfo.getTruststorePassword());
        
        assertNull(keyPair.getPrivate());
        assertNotNull(keyPair.getPublic());
    }

   
    

}
