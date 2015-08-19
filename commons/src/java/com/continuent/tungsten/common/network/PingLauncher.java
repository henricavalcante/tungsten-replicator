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

package com.continuent.tungsten.common.network;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.PropertyConfigurator;

/**
 * Provides a simple utility for checking host accessibility.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class PingLauncher
{
    private static Logger logger  = Logger.getLogger(PingLauncher.class);

    // Ping utility paramemters..
    boolean               verbose = false;
    int                   timeout = 10;
    String                method  = null;
    String                host    = null;

    /** Creates a new Launcher instance. */
    public PingLauncher()
    {
    }

    public void setVerbose(boolean verbose)
    {
        this.verbose = verbose;
    }

    public void setTimeout(int timeout)
    {
        this.timeout = timeout;
    }

    public void setMethod(String method)
    {
        this.method = method;
    }

    public void setHost(String host)
    {
        this.host = host;
    }

    /** Main method to permit external invocation. */
    public static void main(String argv[]) throws Exception
    {
        // Create and run launcher.
        PingLauncher launcher = new PingLauncher();
        try
        {
            launcher.process(argv);
        }
        catch (Throwable e)
        {
            logger.fatal("ERROR: " + e.getMessage(), e);
        }
        exitWithSuccess();
    }

    /**
     * Process command request.
     */
    public void process(String argv[])
    {
        boolean verbose = false;
        int timeout = 10;
        String method = null;
        String host = null;
        String log4j = null;

        // Parse arguments.
        int argc = 0;
        while (argc < argv.length)
        {
            String nextArg = argv[argc];
            argc++;

            if ("-timeout".equals(nextArg))
            {
                timeout = getIntArg(argv, argc++);
            }
            else if ("-method".equals(nextArg))
            {
                method = getStringArg(argv, argc++);
            }
            else if ("-verbose".equals(nextArg))
            {
                verbose = true;
            }
            else if ("-log4j".equals(nextArg))
            {
                log4j = getStringArg(argv, argc++);
            }
            else if ("-help".equals(nextArg))
            {
                usage();
                return;
            }
            else
            {
                if (host == null)
                    host = nextArg;
                else
                {
                    String msg = "Unrecognized option: " + nextArg;
                    exitWithFailure(msg);
                }
            }
        }

        // Configure log4j from properties if option is specified, otherwise
        // suppress anything below warning level.
        if (log4j == null)
        {
            // Configure log4j
            Logger rootLogger = Logger.getRootLogger();
            if (!rootLogger.getAllAppenders().hasMoreElements())
            {
                rootLogger.setLevel(Level.WARN);
                rootLogger.addAppender(new ConsoleAppender(new PatternLayout(
                        "%-5p [%t]: %m%n")));
            }
        }
        else
        {
            PropertyConfigurator.configure(log4j);
        }

        // Ensure we have a host.
        if (host == null)
        {
            String msg = "Missing host name";
            exitWithFailure(msg);
        }

        // Try to execute the test.
        try
        {
            HostAddressService service = new HostAddressService(true);
            service.setTimeout(timeout * 1000);
            HostAddress address = HostAddressService.getByName(host);
            List<String> methodNames;
            if (method == null)
                methodNames = service.getEnabledMethodNames();
            else
            {
                methodNames = new ArrayList<String>(1);
                methodNames.add(method);
            }

            try
            {
                for (String methodName : methodNames)
                {
                    PingResponse response = service.isReachableByMethod(
                            methodName, address);
                    if (response.isReachable())
                    {
                        println("(" + methodName + ") Host is reachable: "
                                + address.getHostAddress());
                        return;
                    }
                    else
                    {
                        // We did not find anything.
                        println("(" + methodName + ") No response: "
                                + address.getHostAddress());
                    }
                }
            }
            catch (HostException e)
            {
                println("Ping error: " + e.getMessage());
                if (verbose)
                    e.printStackTrace();
            }
        }
        catch (HostException e)
        {
            println("Ping configuration error: " + e.getMessage());
            if (verbose)
                e.printStackTrace();
        }
        catch (UnknownHostException e)
        {
            println("Unknown host: " + host);
            if (verbose)
                e.printStackTrace();
        }

        // We did not find the host, hence must exit with not found status.
        exitWithNotFound();
    }

    // Return a parsed integer value.
    private int getIntArg(String argv[], int argc)
    {
        String value = null;
        int intValue = 0;
        try
        {
            value = getStringArg(argv, argc);
            intValue = Integer.parseInt(argv[argc++]);
        }
        catch (NumberFormatException e)
        {
            String msg = "Invalid integer value: " + value;
            exitWithFailure(msg);
        }
        return intValue;
    }

    // Return next string argument value.
    private String getStringArg(String argv[], int argc)
    {
        String value = null;
        try
        {
            value = argv[argc];
        }
        catch (ArrayIndexOutOfBoundsException e)
        {
            String msg = "Missing argument at end of command";
            exitWithFailure(msg);
        }
        return value;
    }

    /** Print to standard out. */
    protected static void println(String message)
    {
        System.out.println(message);
    }

    /** Print usage. */
    protected static void usage()
    {
        println("HOST PING UTILITY (\"hostping\")");
        println("Usage: hostping [options] hostname");
        println("Options:");
        println("  -help          Print usage and exit");
        println("  -method name   Ping method to use (default=use all methods)");
        println("  -log4j file    Log4j properties file (default=suppress all below WARN)");
        println("  -timeout secs  Time out to wait for replication (default=60)");
        println("  -verbose       Print verbose error output");
    }

    // Fail gloriously.
    protected static void exitWithFailure(String message)
    {
        println(message);
        System.exit(2);
    }

    // Exit with a host not found code.
    protected static void exitWithNotFound()
    {
        System.exit(1);
    }

    // Exit with a success code.
    protected static void exitWithSuccess()
    {
        System.exit(0);
    }
}