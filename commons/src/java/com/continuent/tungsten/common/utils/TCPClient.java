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

class TCPClient
{
    public static void main(String argv[])
    {

        if (argv.length != 2)
        {
            System.out.println("usage: TCPClient <host> <port>");
            System.exit(1);
        }

        try
        {
            String host = argv[0];
            int port = Integer.parseInt(argv[1]);

            String sentence;
            String modifiedSentence;
            BufferedReader inFromUser = new BufferedReader(
                    new InputStreamReader(System.in));
            Socket clientSocket = new Socket(host, port);
            DataOutputStream outToServer = new DataOutputStream(
                    clientSocket.getOutputStream());
            BufferedReader inFromServer = new BufferedReader(
                    new InputStreamReader(clientSocket.getInputStream()));
            sentence = inFromUser.readLine();
            outToServer.writeBytes(sentence + '\n');
            modifiedSentence = inFromServer.readLine();
            System.out.println("FROM SERVER: " + modifiedSentence);
            clientSocket.close();
        }
        catch (Exception e)
        {
            System.out.println(e);
            System.exit(1);
        }
    }
}