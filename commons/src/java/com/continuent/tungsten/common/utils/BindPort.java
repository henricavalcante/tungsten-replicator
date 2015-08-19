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

import java.net.ServerSocket;

public class BindPort
{

    public static void main(String[] args)
    {

        if (args.length < 1)
        {
            System.err.println("usage: bindPort <port>");
            System.exit(1);
        }

        int iterations = 0;
        while (true)
        {
            int port = Integer.parseInt(args[0]);

            try
            {
                new ServerSocket(port);
                System.out.println("listening on port " + port);
                sleep(Long.MAX_VALUE);

            }
            catch (Exception ex)
            {
                System.out.print("*");

                if (++iterations % 40 == 0)
                    System.out.println("");

                sleep(1000);

            }
        }

    }

    public static void sleep(long milliseconds)
    {
        try
        {
            Thread.sleep(milliseconds);
        }
        catch (Exception ignored)
        {
        }

    }
}