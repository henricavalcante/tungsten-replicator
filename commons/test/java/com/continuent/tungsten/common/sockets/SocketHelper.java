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
 * Initial developer(s): Robert Hodges
 * Contributor(s):
 */

package com.continuent.tungsten.common.sockets;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import com.continuent.tungsten.common.config.cluster.ConfigurationException;
import com.continuent.tungsten.common.jmx.ServerRuntimeException;
import com.continuent.tungsten.common.security.AuthenticationInfo;
import com.continuent.tungsten.common.security.SecurityHelper;

/**
 * Implements utility functions for testing.
 */
public class SocketHelper
{
    /** Validate and load security properties. */
    public void loadSecurityProperties() throws ConfigurationException
    {
        AuthenticationInfo authInfo = SecurityHelper
                .loadAuthenticationInformation("sample.security.properties");
        // Validate security settings.
        if (authInfo == null)
        {
            throw new ServerRuntimeException(
                    "Unable to locate security information; ensure security.properties file is configured");
        }
    }

    /** Sends a string and confirms it is echoed back. */
    public String echo(Socket sock, String message) throws IOException
    {
        InputStream is = sock.getInputStream();
        OutputStream os = sock.getOutputStream();
        byte[] buf1 = message.getBytes();
        os.write(buf1, 0, buf1.length);
        byte[] buf2 = new byte[buf1.length];
        int offset = 0;
        int length = 0;
        while (offset < buf2.length)
        {
            length = is.read(buf2, offset, buf2.length - offset);
            offset += length;
        }
        String echoMessage = new String(buf2);
        return echoMessage;
    }
}