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
 * Initial developer(s):
 * Contributor(s): 
 */

package com.continuent.tungsten.common.utils;

import java.io.*;
import java.net.*;

class TCPServer
{
    public static void main(String argv[])
    {

        if (argv.length != 1)
        {
            System.out.println("usage: TCPServer <port>");
            System.exit(1);
        }

        try
        {
            int port = Integer.parseInt(argv[0]);

            String clientSentence;
            String capitalizedSentence;
            ServerSocket welcomeSocket = new ServerSocket(port);

            while (true)
            {
                Socket connectionSocket = welcomeSocket.accept();
                BufferedReader inFromClient = new BufferedReader(
                        new InputStreamReader(connectionSocket.getInputStream()));
                DataOutputStream outToClient = new DataOutputStream(
                        connectionSocket.getOutputStream());
                clientSentence = inFromClient.readLine();
                System.out.println("Received: " + clientSentence);
                capitalizedSentence = clientSentence.toUpperCase() + '\n';
                outToClient.writeBytes(capitalizedSentence);
            }
        }
        catch (Exception e)
        {
            System.out.println(e);
            System.exit(1);
        }
    }
}
