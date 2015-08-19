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
 * Initial developer(s): Seppo Jaakola
 * Contributor(s): 
 */

package com.continuent.tungsten.replicator.management;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.rmi.ConnectException;
import java.rmi.RemoteException;

import javax.management.remote.JMXConnector;

import com.continuent.tungsten.common.exec.ArgvIterator;
import com.continuent.tungsten.common.jmx.JmxManager;
import com.continuent.tungsten.common.jmx.ServerRuntimeException;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;

/**
 * This class defines a OpenReplicatorSignaler that implements a simple utility
 * to access ReplicatorManager JMX interface and send signal for the replicator manager. 
 * See the printHelp() command for a description of current commands.
 * 
 * @author <a href="mailto:seppo.jaakola@continuent.com">Seppo Jaakola</a>
 * @version 1.0
 */
public class OpenReplicatorSignaler
{
    // Statics to read from stdin.
    static InputStreamReader converter            = new InputStreamReader(
                                                          System.in);
    static BufferedReader    stdin                = new BufferedReader(
                                                          converter);

    private static final String RMI_HOST = "localhost";
    private static final int    RMI_PORT = 10000;
    
    // Instance variables.
    private boolean          expectLostConnection = false;
    private ArgvIterator     argvIterator;

    OpenReplicatorSignaler(String[] argv)
    {
        argvIterator = new ArgvIterator(argv);
    }

    static void printHelp()
    {
        println("Replicator Manager Signaler Utility");
        println("Syntax:  [java " + OpenReplicatorSignaler.class.getName()
                + " \\");
        println("             [global-options] command [command-options]");
        println("Global Options:");
        println("\t-host name       - Host name of replicator [default: localhost]");
        println("\t-port number     - Port number of replicator [default: 10000]");
        println("Commands:");
        println("\thelp             - Print this help");
        println("\terror msg        - send error notification <msg>");
        println("\tsynced msg       - send error notification <msg>");
        println("\tonline msg       - send online reached notification <msg>");
        println("\toffline msg      - send offline reached notification <msg>");
        println("\tpaused msg       - send paused state reached notification <msg>");
        println("Omitting a command prints help text");
    }

    /**
     * Main method to run utility.
     * 
     * @param argv optional command string
     */
    public static void main(String argv[])
    {
        OpenReplicatorSignaler ctrl = new OpenReplicatorSignaler(argv);
        ctrl.go();
    }

    /**
     * Process replicator command.
     */
    public void go()
    {
        // Set defaults for properties.
        String rmiHost = RMI_HOST;
        int rmiPort = RMI_PORT;
        
        boolean verbose = false;
        String command = null;

        // Parse global options and command.
        String curArg = null;
        try
        {
            while (argvIterator.hasNext())
            {
                curArg = argvIterator.next();
                if ("-host".equals(curArg))
                    rmiHost = argvIterator.next();
                else if ("-port".equals(curArg))
                    rmiPort = Integer.parseInt(argvIterator.next());
                else if ("-verbose".equals(curArg))
                    verbose = true;
                else if (curArg.startsWith("-"))
                {
                    fatal("Unrecognized global option: " + curArg, null);
                }
                else
                {
                    command = curArg;
                    break;
                }
            }
        }
        catch (NumberFormatException e)
        {
            fatal("Bad numeric argument for " + curArg, null);
        }
        catch (ArrayIndexOutOfBoundsException e)
        {
            fatal("Missing value for " + curArg, null);
        }

        OpenReplicatorManagerMBean manager = null;
        try
        {
            // Connect with appropriate protection against a lost connection.
            try
            {
                // Connect.
                JMXConnector conn = JmxManager.getRMIConnector(rmiHost,
                        rmiPort, ReplicatorConf.RMI_DEFAULT_SERVICE_NAME);
                manager = (OpenReplicatorManagerMBean) JmxManager.getMBeanProxy(
                        conn, OpenReplicatorManager.class, false);
            }
            catch (ServerRuntimeException e)
            {
                fatal("Connection failed: " + e, e);
            }

            if (command != null)
            {
                if (command.equals(Commands.HELP))
                    manager.online();
                else if (command.equals(Commands.ERROR))
                {
                    // Check for error message
                    StringBuffer msg = new StringBuffer();
                    while (argvIterator.hasNext())
                    {
                        msg.append(argvIterator.next());
                    }
                    manager.signal(OpenReplicatorManagerMBean.signalError, msg.toString());
                }
                else if (command.equals(Commands.SYNCED))
                {
                    StringBuffer msg = new StringBuffer();
                    while (argvIterator.hasNext())
                    {
                        msg.append(argvIterator.next());
                    }
                    manager.signal(OpenReplicatorManagerMBean.signalSynced, msg.toString());
                }
                else if (command.equals(Commands.RESTORED))
                {
                    StringBuffer msg = new StringBuffer();
                    while (argvIterator.hasNext())
                    {
                        msg.append(argvIterator.next());
                    }
                    manager.signal(OpenReplicatorManagerMBean.signalRestored, msg.toString());
                }
                else if (command.equals(Commands.OFFLINE))
                {
                    StringBuffer msg = new StringBuffer();
                    while (argvIterator.hasNext())
                    {
                        msg.append(argvIterator.next());
                    }
                    manager.signal(OpenReplicatorManagerMBean.signalOfflineReached, msg.toString());
                }
                else if (command.equals(Commands.SHUTDOWN))
                {
                    StringBuffer msg = new StringBuffer();
                    while (argvIterator.hasNext())
                    {
                        msg.append(argvIterator.next());
                    }
                    manager.signal(OpenReplicatorManagerMBean.signalShutdown, msg.toString());
                }
                else if (command.equals(Commands.CONSISTENCY))
                {
                    StringBuffer msg = new StringBuffer();
                    while (argvIterator.hasNext())
                    {
                        msg.append(argvIterator.next());
                    }
                    manager.signal(OpenReplicatorManagerMBean.signalConsistencyFail, msg.toString());
                }
                else if (command.equals(Commands.HELP))
                {
                    printHelp();
                }
                else
                {
                    println("Unknown command: '" + command + "'");
                    printHelp();
                }
            }

            // Check status assuming we didn't expect to lose the connection.
            println("State: " + manager.getState());
            if (manager.getPendingError() != null)
            {
                println("Error: " + manager.getPendingError());
                println("Exception Message: "
                        + manager.getPendingExceptionMessage());
            }
        }
        catch (ConnectException e)
        {
            // This occurs if JMX fails to connect via RMI.
            if (expectLostConnection)
                println("RMI connection lost!");
            else
                fatal("RMI connection lost!", e);
        }
        catch (RemoteException e)
        {
            // Occurs if there is an MBean server error, for example because
            // the server exited.
            if (expectLostConnection)
                println("Replicator appears to be stopped");
            else
            {
                fatal("Fatal RMI communication error: " + e.getMessage(), e);
            }
        }
        catch (Exception e)
        {
            // Occurs when there is a server-side application exception.
            println("Operation failed: " + e.getMessage());
            if (verbose)
                e.printStackTrace();
            if (manager.getPendingError() != null)
            {
                println("Error: " + manager.getPendingError());
                println("Exception Message: "
                        + manager.getPendingExceptionMessage());
            }
        }
        catch (Throwable t)
        {
            // Occurs if there is a really bad problem.
            fatal("Fatal error: " + t.getMessage(), t);
        }
    }

    // Print a message to stdout.
    private static void println(String msg)
    {
        System.out.println(msg);
    }

    // Abort following a fatal error.
    private static void fatal(String msg, Throwable t)
    {
        System.out.println(msg);
        if (t != null)
            t.printStackTrace();
        System.exit(1);
    }

    // List of commands
    class Commands
    {
        public static final String ERROR       = "error";
        public static final String SYNCED      = "synced";
        public static final String SHUTDOWN    = "shutdown";
        public static final String OFFLINE     = "offline";
        public static final String RESTORED    = "restored";
        public static final String CONSISTENCY = "consistency";
        public static final String HELP        = "help";
    }
}
