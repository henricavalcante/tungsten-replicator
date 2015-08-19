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
 * Initial developer(s): Teemu Ollakka
 * Contributor(s): Robert Hodges, Stephane Giron, Linas Virbalas
 */

package com.continuent.tungsten.replicator.management;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.rmi.ConnectException;
import java.rmi.RemoteException;
import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import javax.management.remote.JMXConnector;

import org.apache.log4j.Logger;
import org.json.simple.JSONValue;

import com.continuent.tungsten.common.cluster.resource.OpenReplicatorParams;
import com.continuent.tungsten.common.cluster.resource.physical.Replicator;
import com.continuent.tungsten.common.cluster.resource.physical.ReplicatorCapabilities;
import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.config.cluster.ConfigurationException;
import com.continuent.tungsten.common.csv.CsvReader;
import com.continuent.tungsten.common.csv.CsvWriter;
import com.continuent.tungsten.common.exec.ArgvIterator;
import com.continuent.tungsten.common.jmx.JmxManager;
import com.continuent.tungsten.common.jmx.ServerRuntimeException;
import com.continuent.tungsten.common.security.AuthenticationInfo;
import com.continuent.tungsten.common.security.PasswordManager;
import com.continuent.tungsten.common.security.PasswordManager.ClientApplicationType;
import com.continuent.tungsten.common.security.SecurityHelper;
import com.continuent.tungsten.common.utils.ManifestParser;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.PropertiesManager;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntimeConf;
import com.continuent.tungsten.replicator.consistency.ConsistencyTable;
import com.continuent.tungsten.replicator.shard.ShardManager;
import com.continuent.tungsten.replicator.shard.ShardManagerMBean;
import com.continuent.tungsten.replicator.shard.ShardTable;
import com.continuent.tungsten.replicator.thl.ProtocolParams;

/**
 * This class defines a ReplicatorManagerCtrl that implements a simple utility
 * to access ReplicatorManager JMX interface. See the printHelp() command for a
 * description of current commands.
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class OpenReplicatorManagerCtrl
{
    private static Logger                  logger                         = Logger.getLogger(OpenReplicatorManagerCtrl.class);
    // Statics to read from stdin.
    private static InputStreamReader       converter                      = new InputStreamReader(
                                                                                  System.in);
    private static BufferedReader          stdin                          = new BufferedReader(
                                                                                  converter);

    // Instance variables.
    private boolean                        verbose                        = false;
    private boolean                        expectLostConnection           = false;
    private ArgvIterator                   argvIterator;
    private JMXConnector                   conn                           = null;
    private ReplicationServiceManagerMBean serviceManagerMBean            = null;
    private OpenReplicatorManagerMBean     openReplicatorMBean            = null;
    private ShardManagerMBean              shardMBean                     = null;

    // JMX connection parameters.
    private String                         rmiHost;
    private int                            rmiPort;
    private String                         service;
    private int                            connectDelay                   = 10;
    // Authentication and Encryption information
    private AuthenticationInfo             authenticationInfo             = null;
    private String                         securityPropertiesFileLocation = null;
    private TungstenProperties             serviceProps;

    OpenReplicatorManagerCtrl(String[] argv)
    {
        argvIterator = new ArgvIterator(argv);

        // Find and load the service.properties file.
        File confDir = ReplicatorRuntimeConf.locateReplicatorConfDir();
        File propsFile = new File(confDir, "services.properties");
        try
        {
            serviceProps = PropertiesManager.loadProperties(propsFile);
        }
        catch (ReplicatorException e)
        {
            logger.warn("Unable to load 'services.properties' file", e);
        }
    }

    private void printHelp()
    {
        println("Tungsten Replicator Control Utility");
        println("Syntax:  trepctl [global-options] command [command-options]");
        println("Global Options:");
        println("  -host name                   - Host name of replicator [default: localhost]");
        println("  -port number                 - Port number of replicator [default: 10000]");
        println("  -service name                - Name of replicator service [default: none]");
        println("                                 (Added implicitly if only one service was defined)");
        println("  -verbose                     - Print verbose messages");
        println("  -retry N                     - Retry connections up to N times [default: 10]");
        // TODO: uncomment when releasing security related features
        // println("Security Properties:");
        // println("  "
        // + AuthenticationInfo.SECURITY_CONFIG_FILE_LOCATION
        // +
        // " sl       - Location of the security properties file. By default file located in {clusterhome}/security.properties will be used.");
        println("Replicator-Wide Commands:");
        println("  version                      - Show replicator version and build");
        println("  services [-json] [-full]     - List replication services");
        println("  kill [-y]                    - Exit immediately without shutting down services");
        println("Service-Specific Commands (Require -service option)");
        println("  backup [-backup agent] [-storage agent] [-limit s]");
        println("                               - Backup database");
        println("                                 Warning! This is deprecated and will be removed in a future release!");
        println("  capabilities                 - List replicator capabilities");
        println("  check <table> [-limit offset,limit] [-method m]");
        println("                               - Generate consistency check for the given table");
        println("  clear                        - Clear one or all dynamic variables");
        println("  clients [-json]              - Clients (replicators) that have been connected during this ONLINE state");
        println("  configure [file]             - Reload replicator properties file");
        println("  flush [-limit s]             - Synchronize transaction history log to database");
        println("  heartbeat [-name name]       - Insert a heartbeat event with optional name");
        println("  offline [-immediate]         - Set replicator to OFFLINE state");
        println("  offline-deferred [-at-seqno seqno] [-at-event event] [-at-heartbeat [name]] [-at-time YYYY-MM-DD_hh:mm:ss]");
        println("                               - Set replicator OFFLINE at future point");
        println("  online [-force] [-from-event event] [-base-seqno x] [-skip-seqno x,y,z] [-until-seqno seqno] ");
        println("         [-until-event event] [-until-heartbeat [name]] [-until-time YYYY-MM-DD_hh:mm:ss]");
        println("         [-no-checksum]");
        println("         [-provision]");
        println("                               - Set Replicator to ONLINE with start and stop points");
        println("  properties [-filter name]    - Print all in-memory properties and their current values");
        println("             [-values]         - Print only the values in plain text");
        println("  purge [-y] [-limit s]        - Purge non-Tungsten logins on DBMS, waiting up to s seconds");
        println("  reset [-y] {-all | -thl | -relay | -db}");
        println("                               - Deletes the replicator service (-all or empty), thl directory,");
        println("                                 relay logs directory or tungsten database for the service");
        println("  restore [-uri u] [-limit s]  - Restore database");
        println("                                 Warning! This is deprecated and will be removed in a future release!");
        println("  setrole -role r [-uri u]     - Set replicator role");
        println("  load                         - Load and start replication service");
        println("  status [-name {channel-assignments|services|shards|stages|stores|tasks|watches}] [-json]");
        println("                               - Print replicator status information");
        println("  unload [-y]                  - Stop and unload replication service");
        println("  wait -state st [-limit s]    - Wait up to s seconds for replicator state st");
        println("  wait -applied x [-limit s]   - Wait up to s seconds for seqno x to be applied");
        println("Shard Commands:");
        println("  shard [-list ]                   - List shards installed in a given service");
        println("  shard [-insert shard_definition] - Add a new shard");
        println("  shard [-update shard_definition] - Update a shard");
        println("  shard [-delete shardId]          - Delete a shard");
    }

    /**
     * Prints release information from the manifest file.
     */
    static void printVersion()
    {
        println(ManifestParser.parseReleaseWithBuildNumber());
    }

    /**
     * Main method to run utility.
     * 
     * @param argv optional command string
     */
    public static void main(String argv[])
    {
        OpenReplicatorManagerCtrl ctrl = new OpenReplicatorManagerCtrl(argv);
        ctrl.go();
    }

    /**
     * Process replicator command.
     */
    public void go()
    {
        // Set defaults for properties.
        rmiHost = ReplicatorConf.RMI_DEFAULT_HOST;

        // Get the rmiPort from service.properties file
        rmiPort = serviceProps.getInt(ReplicatorConf.RMI_PORT,
                ReplicatorConf.RMI_DEFAULT_PORT, false);
        // Check whether rmi port value was overridden as a system property
        rmiPort = new Integer(System.getProperty(ReplicatorConf.RMI_PORT,
                String.valueOf(rmiPort))).intValue();
        service = null;
        String command = null;

        // Parse global options and command.
        String curArg = null;
        try
        {
            // --- Browse through command line parameters ---
            while (argvIterator.hasNext())
            {
                curArg = argvIterator.next();
                if ("-host".equals(curArg))
                    rmiHost = argvIterator.next();
                else if ("-port".equals(curArg))
                    // If an -port option was given, this overrides other
                    // settings
                    rmiPort = Integer.parseInt(argvIterator.next());
                else if ("-verbose".equals(curArg))
                    verbose = true;
                else if ("-service".equals(curArg))
                    service = argvIterator.next();
                else if ("-retry".equals(curArg))
                {
                    String nextVal = argvIterator.peek();
                    if (nextVal == null || (nextVal.length() == 0)
                            || !Character.isDigit(nextVal.charAt(0)))
                    {
                        // Take default.
                    }
                    else
                    {
                        try
                        {
                            connectDelay = Integer.parseInt(nextVal);
                            argvIterator.next();
                        }
                        catch (NumberFormatException e)
                        {
                            // Take default.
                        }
                    }
                }
                // Authentication and Encryption from a properties file
                else if (AuthenticationInfo.SECURITY_CONFIG_FILE_LOCATION
                        .equals(curArg))
                {
                    securityPropertiesFileLocation = argvIterator.next();
                }

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

            // --- Try to get Security information from properties file ---
            // If securityPropertiesFileLocation==null will try to locate
            // default file
            try
            {
                this.authenticationInfo = SecurityHelper
                        .loadAuthenticationInformation(securityPropertiesFileLocation);
                // Sets the username and password in the authenticationInfo.
                // This will be used as credentials when connecting
                // Password is provided "as is" (potentilaly encrypted) and will
                // be decrypted by the server if needed
                PasswordManager passwordManager = new PasswordManager(
                        this.authenticationInfo, ClientApplicationType.RMI_JMX);
                String goodPassword = passwordManager
                        .getEncryptedPasswordForUser(this.authenticationInfo
                                .getUsername());
                this.authenticationInfo.setPassword(goodPassword);
            }
            catch (ConfigurationException ce)
            {
                logger.debug(MessageFormat.format("Configuration error: {0}",
                        ce.getMessage()));
            }
            catch (ServerRuntimeException sre)
            {
                logger.debug(MessageFormat.format(
                        "Could not get authentication information : {0}",
                        sre.getMessage()));
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

        // Process help before trying to connect.
        if (command != null && command.equals(Commands.HELP))
        {
            printHelp();
            System.exit(0);
        }

        try
        {
            // Connect with appropriate protection against a lost connection.
            try
            {
                connect();
            }
            catch (ServerRuntimeException e)
            {
                fatal("Connection failed: " + e.getMessage(), e);
            }

            if (command == null)
                fatal("No command entered: try 'help' to get a list", null);
            else if (command.equals(Commands.SERVICES))
                doServices();
            else if (command.equals(Commands.START))
                fatal("This command has been renamed, use '" + Commands.LOAD
                        + "'", null);
            else if (command.equals(Commands.STOP))
                fatal("This command has been renamed, use '" + Commands.UNLOAD
                        + "'", null);
            else if (command.equals(Commands.LOAD))
                doLoadService();
            else if (command.equals(Commands.UNLOAD))
                doUnloadService();
            else if (command.equals(Commands.RESET))
                doResetService();
            else if (command.equals(Commands.SHUTDOWN))
                doShutdown();
            else if (command.equals(Commands.KILL))
                doKill();
            else if (command.equals(Commands.ONLINE))
                doOnline();
            else if (command.equals(Commands.OFFLINE))
                doOffline();
            else if (command.equals(Commands.OFFLINE_DEFERRED))
                doOfflineDeferred();
            else if (command.equals(Commands.WAIT))
                doWait();
            else if (command.equals(Commands.CHECK))
                doCheck();
            else if (command.equals(Commands.HEARTBEAT))
                doHeartbeat();
            else if (command.equals(Commands.FLUSH))
                doFlush();
            else if (command.equals(Commands.PURGE))
                doPurge();
            else if (command.equals(Commands.CONFIGURE))
                doConfigure();
            else if (command.equals(Commands.SETROLE))
                doSetRole();
            else if (command.equals(Commands.CLEAR))
                doClearDynamic();
            else if (command.equals(Commands.BACKUP))
                doBackup();
            else if (command.equals(Commands.RESTORE))
                doRestore();
            // Remove undocumented "provision" command
            // else if (command.equals(Commands.PROVISION))
            // doProvision();
            else if (command.equals(Commands.STATS))
                doStatus();
            else if (command.equals(Commands.PROPERTIES))
                doProperties();
            else if (command.equals(Commands.CLIENTS))
                doClients();
            else if (command.equals(Commands.HELP))
                printHelp();
            else if (command.equals(Commands.VERSION))
                printVersion();
            else if (command.equals(Commands.CAPABILITIES))
                doCapabilities();

            // Shard commands
            else if (command.equals(Commands.SHARD))
            {
                doShardCommand();
            }

            else
                fatal("Unknown command, try 'help' to get a list: '" + command,
                        null);
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
            fatal("Operation failed: " + e.getMessage(), e);
        }
        catch (Throwable t)
        {
            // Occurs if there is a really bad problem.
            fatal("Fatal error: " + t.getMessage(), t);
        }
    }

    // Perform initial JMX connection.
    private void connect() throws Exception
    {
        // Get JMX connection.
        int delay = 0;
        for (;;)
        {
            Exception failure = null;
            try
            {
                // Add Authentication and Encryption parameters if needed
                if (authenticationInfo != null)
                    authenticationInfo.checkAndCleanAuthenticationInfo();

                TungstenProperties securityProperties = (authenticationInfo != null)
                        ? authenticationInfo.getAsTungstenProperties()
                        : null;

                conn = JmxManager.getRMIConnector(rmiHost, rmiPort,
                        ReplicatorConf.RMI_DEFAULT_SERVICE_NAME,
                        securityProperties);
            }
            catch (Exception e)
            {
                failure = e;
            }
            if (failure == null)
            {
                if (verbose)
                    println("Connected to replicator process");
                break;
            }
            // Could not connect:
            // If an AssertionError was added: no need to try again : missing
            // Credentials, parameters, ...
            else if (connectDelay > delay
                    && !(failure.getCause() instanceof AssertionError))
            {
                sleep(1);
                print(".");
                delay++;
            }
            else
                throw failure;
        }
        // Get MBean connection.
        for (;;)
        {
            Exception failure = null;
            try
            {
                serviceManagerMBean = (ReplicationServiceManagerMBean) JmxManager
                        .getMBeanProxy(conn, ReplicationServiceManager.class,
                                false);
                serviceManagerMBean.isAlive();
            }
            catch (Exception e)
            {
                failure = e;
            }
            if (failure == null)
            {
                if (verbose)
                    println("Connected to ReplicationServiceManagerMBean");
                break;
            }
            else if (connectDelay > delay)
            {
                sleep(1);
                print(".");
                delay++;
            }
            else
                throw failure;
        }
        // If we delayed, need to have a carriage return.
        if (delay > 0)
            println("");
    }

    // Fetch the current replicator service MBean, if possible.
    private OpenReplicatorManagerMBean getOpenReplicator() throws Exception
    {
        if (this.openReplicatorMBean == null)
        {
            if (service == null)
            {
                // If there is just one available service, we will use that as
                // the default.
                List<Map<String, String>> services = serviceManagerMBean
                        .services();
                if (services.size() == 0)
                {
                    throw new Exception(
                            "Operation requires a service name, but the replicator does not have any services defined");
                }
                else if (services.size() == 1)
                {
                    service = services.get(0).get("name");
                    logger.debug("Inferring service name automatically: "
                            + service);
                }
                else
                {
                    throw new Exception(
                            "You must specify a service name with the -service flag");
                }
            }
            // Get MBean connection.
            openReplicatorMBean = getOpenReplicatorSafely(service);
        }
        return openReplicatorMBean;
    }

    // Fetch a specific replicator manager MBean by name, with optional delay.
    private OpenReplicatorManagerMBean getOpenReplicatorSafely(String name)
            throws Exception
    {
        // Get MBean connection.
        int delay = 0;
        for (;;)
        {
            Exception failure = null;
            try
            {
                // Fetch MBean with service name.
                openReplicatorMBean = (OpenReplicatorManagerMBean) JmxManager
                        .getMBeanProxy(conn, OpenReplicatorManager.class,
                                OpenReplicatorManagerMBean.class, name, false,
                                false);
                openReplicatorMBean.isAlive();
            }
            catch (Exception e)
            {
                failure = e;
            }
            if (failure == null)
            {
                if (verbose)
                    println("Connected to OpenReplicatorManagerMBean: " + name);
                break;
            }
            else if (connectDelay > delay)
            {
                sleep(1);
                print(".");
                delay++;
            }
            else
                throw failure;
        }
        // If we delayed, need to have a carriage return.
        if (delay > 0)
            println("");
        return openReplicatorMBean;
    }

    // Sleep for N seconds.
    private void sleep(int n)
    {
        try
        {
            Thread.sleep(1000 * n);
        }
        catch (InterruptedException e)
        {
        }
    }

    // REPLICATION SERVICES COMMANDS //

    // Handle a request for status.
    private void doServices() throws Exception
    {
        String curArg = null;
        boolean json = false;
        boolean full = false;
        while (argvIterator.hasNext())
        {
            curArg = argvIterator.next();
            if ("-json".equals(curArg))
                json = true;
            else if ("-full".equals(curArg))
                full = true;
        }

        if (!json)
            println("Processing services command...");
        List<Map<String, String>> serviceList = this.serviceManagerMBean
                .services();
        List<Map<String, String>> propList = new ArrayList<Map<String, String>>();

        for (Map<String, String> map : serviceList)
        {
            Map<String, String> props = new HashMap<String, String>();

            // Print the service information.
            String name = map.get("name");
            String started = map.get("started");
            props.put(Replicator.SERVICE_NAME, name);
            props.put("started", started);

            // Look up the state of the replication service if it is started.
            if (new Boolean(started).booleanValue())
            {
                OpenReplicatorManagerMBean mbean = getOpenReplicatorSafely(name);
                Map<String, String> liveProps = mbean.status();
                if (!full)
                {
                    props.put(Replicator.ROLE, liveProps.get(Replicator.ROLE));
                    props.put(Replicator.SERVICE_TYPE,
                            liveProps.get(Replicator.SERVICE_TYPE));
                    props.put(Replicator.STATE, liveProps.get(Replicator.STATE));
                    props.put(Replicator.APPLIED_LAST_SEQNO,
                            liveProps.get(Replicator.APPLIED_LAST_SEQNO));
                    props.put(Replicator.APPLIED_LATENCY,
                            liveProps.get(Replicator.APPLIED_LATENCY));
                }
                else
                    props.putAll(liveProps);
            }
            else
            {
                props.put(Replicator.ROLE, "Unknown");
                props.put(Replicator.SERVICE_TYPE, "Unknown");
                props.put(Replicator.STATE, "Unknown");
                props.put(Replicator.APPLIED_LAST_SEQNO, "Unknown");
                props.put(Replicator.APPLIED_LATENCY, "Unknown");
            }
            propList.add(props);
        }

        printlnPropList(propList, json);
        if (!json)
            println("Finished services command...");
    }

    /**
     * List in-memory property values.
     */
    private void doProperties() throws Exception
    {
        OpenReplicatorManagerMBean mbean = getOpenReplicator();

        String containing = null;
        boolean valuesOnly = false;
        String curArg = null;
        while (argvIterator.hasNext())
        {
            curArg = argvIterator.next();
            if ("-filter".equals(curArg))
                containing = argvIterator.next();
            else if ("-values".equals(curArg))
                valuesOnly = true;
            else
            {
                fatal("Unrecognized option: " + curArg, null);
            }
        }

        if (valuesOnly)
            printPropertiesValues(mbean.properties(containing));
        else
            printPropertiesJSON(mbean.properties(containing));
    }

    /**
     * List currently connected slave Replicator processes.
     */
    private void doClients() throws Exception
    {
        String curArg = null;
        boolean json = false;
        while (argvIterator.hasNext())
        {
            curArg = argvIterator.next();
            if ("-json".equals(curArg))
                json = true;
        }

        if (json)
            println("[");
        else
            println("Processing clients command...");

        OpenReplicatorManagerMBean mbean = getOpenReplicator();
        List<Map<String, String>> clients = mbean.getClients();
        if (clients != null)
        {
            int propIdx = 0;
            for (Map<String, String> client : clients)
            {
                if (json)
                {
                    if (propIdx > 0)
                        println(",");
                    printPropertiesJSON(client, propIdx);
                }
                else
                {

                    println(String.format("%s:%s",
                            client.get(ProtocolParams.RMI_HOST),
                            client.get(ProtocolParams.RMI_PORT)));
                }
                propIdx++;
            }
        }
        else
            fatal("No clients found. Maybe unsupported Replicator plugin?",
                    null);

        if (json)
            println("\n]");
        else
            println("Finished clients command...");
    }

    // Start a service.
    private void doLoadService() throws Exception
    {
        if (service == null)
            throw new Exception(
                    "You must specify a service name using -service");
        boolean ok = serviceManagerMBean.loadService(service);
        if (ok)
            println("Service started successfully: name=" + service);
        else
            println("Service appears to be started already: name=" + service);
    }

    // Stop a service.
    private void doUnloadService() throws Exception
    {
        // Make sure we have a service name.
        if (service == null)
            this.getOpenReplicator();

        boolean yes = confirm(String.format(
                "Do you really want to stop replication service %s?", service));
        if (yes)
        {
            boolean ok = serviceManagerMBean.unloadService(service);
            if (ok)
                println("Service stopped successfully: name=" + service);
            else
                println("Unable to stop service: name=" + service);
        }
    }

    // Reset (delete) a service.
    private void doResetService() throws Exception
    {
        TungstenProperties options = new TungstenProperties();
        boolean yes = false;

        if (service == null)
            throw new Exception(
                    "You must specify a service name using -service");

        if (!argvIterator.hasNext())
            yes = confirm(String
                    .format("Do you really want to delete replication service %s completely?",
                            service));
        else
        {
            while (argvIterator.hasNext())
            {
                String curOption = argvIterator.next();
                if ("-y".equals(curOption))
                {
                    yes = true;
                }
                else if ("-all".equalsIgnoreCase(curOption))
                {
                    yes = confirm(String
                            .format("Do you really want to delete replication service %s completely?",
                                    service));
                    options.put("option", curOption);
                    // For now, take only first option into account
                    break;
                }
                else if ("-thl".equalsIgnoreCase(curOption))
                {
                    yes = confirm(String
                            .format("Do you really want to delete THL for replication service %s completely?",
                                    service));
                    options.put("option", curOption);
                    // For now, take only first option into account
                    break;
                }
                else if ("-relay".equalsIgnoreCase(curOption))
                {
                    yes = confirm(String
                            .format("Do you really want to delete relay logs for replication service %s completely?",
                                    service));
                    options.put("option", curOption);
                    // For now, take only first option into account
                    break;
                }
                else if ("-db".equalsIgnoreCase(curOption))
                {
                    yes = confirm(String
                            .format("Do you really want to delete database for replication service %s completely?",
                                    service));
                    options.put("option", curOption);
                    // For now, take only first option into account
                    break;
                }
                else
                    fatal("Unrecognized option for reset command : "
                            + curOption, null);
            }
        }

        if (yes)
        {
            serviceManagerMBean.resetService(service, options.map());
        }
    }

    // Shuts down the replicator nicely.
    private void doShutdown() throws Exception
    {
        fatal("This command was DEPRECATED as of version 3.0.0. Use `replicator stop` instead.",
                null);
    }

    // Terminate the replicator process with prejudice.
    private void doKill() throws Exception
    {
        // If the user really wants to kill the replicator, do it.
        boolean yes = confirm("Do you really want to kill the replicator process?");
        if (yes)
        {
            println("Sending kill command to replicator");
            expectLostConnection = true;
            this.serviceManagerMBean.kill();
        }
    }

    // OPEN REPLICATOR COMMANDS //

    // Handle online operation.
    private void doOnline() throws Exception
    {
        // Checks for params option.
        String params = null;
        String fromEvent = null;
        long baseSeqno = -1;
        String toEvent = null;
        long toSeqno = -1;
        String heartbeat = null;
        long toTime = -1;
        long skip = 0;
        String seqnos = null;
        boolean force = false;
        boolean doChecksum = true;
        boolean doProvision = false;

        while (argvIterator.hasNext())
        {
            String curArg = argvIterator.next();
            try
            {
                if ("-skip".equals(curArg))
                {
                    fatal("The -skip flag is no longer supported; use -skip-seqno instead",
                            null);
                }
                else if ("-event".equals(curArg))
                {
                    fatal("The -event flag is no longer supported; use -until-event instead",
                            null);
                }
                else if ("-seqno".equals(curArg))
                {
                    fatal("The -seqno flag is no longer supported; use -until-seqno instead",
                            null);
                }
                else if ("-heartbeat".equals(curArg))
                {
                    fatal("The -heartbeat flag is no longer supported; use -until-heartbeat instead",
                            null);
                }
                else if ("-time".equals(curArg))
                {
                    fatal("The -time flag is no longer supported; use -until-time instead",
                            null);
                }
                else if ("-params".equals(curArg))
                    params = argvIterator.next();
                else if ("-from-event".equals(curArg))
                    fromEvent = argvIterator.next();
                else if ("-base-seqno".equals(curArg))
                    baseSeqno = Long.parseLong(argvIterator.next());
                else if ("-until-event".equals(curArg))
                    toEvent = argvIterator.next();
                else if ("-until-seqno".equals(curArg))
                    toSeqno = Long.parseLong(argvIterator.next());
                else if ("-until-heartbeat".equals(curArg))
                {
                    // Take the next non-option argument as the heartbeat name.
                    heartbeat = argvIterator.peek();
                    if (heartbeat == null || heartbeat.startsWith("-"))
                        heartbeat = "*";
                    else
                        argvIterator.next();
                }
                else if ("-until-time".equals(curArg))
                    toTime = getDatetimeMillis(argvIterator.next());
                else if ("-skip-seqno".equals(curArg))
                    seqnos = argvIterator.next();
                else if ("-force".equals(curArg))
                    force = true;
                else if ("-no-checksum".equals(curArg))
                    doChecksum = false;
                else if ("-provision".equals(curArg))
                {
                    // Take the next, if any, non-option argument as the
                    // provision position.
                    fromEvent = argvIterator.peek();
                    if (fromEvent == null || fromEvent.startsWith("-"))
                        fromEvent = null;
                    else
                        argvIterator.next();

                    doProvision = true;
                }
                else
                    fatal("Unrecognized option: " + curArg, null);
            }
            catch (Exception e)
            {
                fatal("Missing or invalid argument to flag: " + curArg, null);
            }
        }

        // Validation.
        if (fromEvent != null && baseSeqno != -1)
            fatal("Parameters -from-event and -base-seqno cannot be used together",
                    null);

        // Split params object into a Tungsten properties object.
        TungstenProperties paramProps = new TungstenProperties();
        if (params != null)
            paramProps.load(params, false);

        // Or just go for more user-friendly syntax.
        if (skip > 0)
            paramProps.setLong(OpenReplicatorParams.SKIP_APPLY_EVENTS, skip);
        if (fromEvent != null)
            paramProps.setString(OpenReplicatorParams.INIT_EVENT_ID, fromEvent);
        if (baseSeqno > -1)
        {
            // Validation.
            if (getOpenReplicator().getRole().equals("slave"))
                fatal("-base-seqno is not supported on a slave, use tungsten_set_position script instead",
                        null);
            paramProps.setLong(OpenReplicatorParams.BASE_SEQNO, baseSeqno);
        }
        if (toEvent != null)
            paramProps.setString(OpenReplicatorParams.ONLINE_TO_EVENT_ID,
                    toEvent);
        if (toSeqno >= 0)
            paramProps.setLong(OpenReplicatorParams.ONLINE_TO_SEQNO, toSeqno);
        if (heartbeat != null)
            paramProps.setString(OpenReplicatorParams.ONLINE_TO_HEARTBEAT,
                    heartbeat);
        if (toTime >= 0)
            paramProps
                    .setLong(OpenReplicatorParams.ONLINE_TO_TIMESTAMP, toTime);
        if (seqnos != null)
            paramProps
                    .setString(OpenReplicatorParams.SKIP_APPLY_SEQNOS, seqnos);
        if (force)
            paramProps.setBoolean(OpenReplicatorParams.FORCE, true);
        if (!doChecksum)
            paramProps.setBoolean(OpenReplicatorParams.DO_CHECKSUM, false);
        if (doProvision)
            paramProps.setBoolean(OpenReplicatorParams.DO_PROVISION, true);

        // Put replicator online.
        if (doProvision)
            getOpenReplicator().provisionOnline(paramProps.map());
        else
            getOpenReplicator().online2(paramProps.map());
    }

    // Convert a date-time string to Java time.
    private long getDatetimeMillis(String datetimeString) throws Exception
    {
        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
        long dateMillis = -1;
        try
        {
            Date date = formatter.parse(datetimeString);
            dateMillis = date.getTime();
        }
        catch (ParseException e)
        {
            fatal("Date format must be YYYY-MM-DD_hh:mm:ss: " + datetimeString,
                    null);
        }
        return dateMillis;
    }

    // Handle offline command.
    private void doOffline() throws Exception
    {
        // Checks for options.
        boolean immediate = false;

        while (argvIterator.hasNext())
        {
            String curArg = argvIterator.next();
            if ("-immediate".equals(curArg))
                immediate = true;
            else
                fatal("Unrecognized option: " + curArg, null);
        }

        // Use simple offline only if -immediate is present. Otherwise do
        // deferred offline.
        if (immediate)
            getOpenReplicator().offline();
        else
        {
            TungstenProperties paramProps = new TungstenProperties();
            paramProps.setString(OpenReplicatorParams.OFFLINE_TRANSACTIONAL,
                    "true");
            getOpenReplicator().offlineDeferred(paramProps.map());
        }
    }

    // Handle offline deferred command.
    private void doOfflineDeferred() throws Exception
    {
        // Checks for options.
        String atEvent = null;
        long atSeqno = -1;
        String heartbeat = null;
        long atTime = -1;

        while (argvIterator.hasNext())
        {
            String curArg = argvIterator.next();
            try
            {
                if ("-event".equals(curArg))
                {
                    fatal("The -event flag is no longer supported; use -at-event instead",
                            null);
                }
                else if ("-seqno".equals(curArg))
                {
                    fatal("The -seqno flag is no longer supported; use -at-seqno instead",
                            null);
                }
                else if ("-heartbeat".equals(curArg))
                {
                    fatal("The -heartbeat flag is no longer supported; use -at-heartbeat instead",
                            null);
                }
                else if ("-time".equals(curArg))
                {
                    fatal("The -time flag is no longer supported; use -at-time instead",
                            null);
                }
                if ("-at-event".equals(curArg))
                    atEvent = argvIterator.next();
                else if ("-at-seqno".equals(curArg))
                    atSeqno = Long.parseLong(argvIterator.next());
                else if ("-at-heartbeat".equals(curArg))
                {
                    // Take the next non-option argument as the heartbeat name.
                    heartbeat = argvIterator.peek();
                    if (heartbeat == null || heartbeat.startsWith("-"))
                        heartbeat = "*";
                    else
                        argvIterator.next();
                }
                else if ("-at-time".equals(curArg))
                    atTime = getDatetimeMillis(argvIterator.next());
                else
                    fatal("Unrecognized option: " + curArg, null);
            }
            catch (Exception e)
            {
                fatal("Missing or invalid argument to flag: " + curArg, null);
            }
        }

        // Load parameters, if any.
        TungstenProperties paramProps = new TungstenProperties();
        if (atEvent != null)
            paramProps.setString(OpenReplicatorParams.OFFLINE_AT_EVENT_ID,
                    atEvent);
        if (atSeqno >= 0)
            paramProps.setLong(OpenReplicatorParams.OFFLINE_AT_SEQNO, atSeqno);
        if (heartbeat != null)
            paramProps.setString(OpenReplicatorParams.OFFLINE_AT_HEARTBEAT,
                    heartbeat);
        if (atTime >= 0)
            paramProps.setLong(OpenReplicatorParams.OFFLINE_AT_TIMESTAMP,
                    atTime);

        getOpenReplicator().offlineDeferred(paramProps.map());
    }

    // Handle wait command.
    private void doWait() throws Exception
    {
        // Check for options.
        String state = null;
        String event = null;
        long seconds = 0;
        while (argvIterator.hasNext())
        {
            String curArg = argvIterator.next();
            try
            {
                if ("-state".equals(curArg))
                    state = argvIterator.next();
                else if ("-applied".equals(curArg))
                    event = argvIterator.next();
                else if ("-limit".equals(curArg))
                    seconds = Long.parseLong(argvIterator.next());
                else
                    fatal("Unrecognized option: " + curArg, null);
            }
            catch (Exception e)
            {
                fatal("Missing or invalid argument to flag: " + curArg, null);
            }
        }

        // Check to see whether we have a valid command.
        boolean succeeded = false;
        if (state != null && event != null)
        {
            fatal("You must specify -state or -applied, not both", null);
        }
        else if (state != null)
        {
            // Wait for the state.
            succeeded = getOpenReplicator().waitForState(state, seconds);
        }
        else if (event != null)
        {
            succeeded = getOpenReplicator().waitForAppliedSequenceNumber(event,
                    seconds);
        }
        else
        {
            fatal("You must specify what to wait for using -state or -applied",
                    null);
        }
        if (!succeeded)
        {
            fatal("Wait timed out!", null);
        }
    }

    // Handle a check command.
    private void doCheck() throws Exception
    {
        String schemaName = null;
        String tableName = null;
        int rowOffset = ConsistencyTable.ROW_UNSET;
        int rowLimit = ConsistencyTable.ROW_UNSET;
        String ccType = "md5";

        while (argvIterator.hasNext())
        {
            String curArg = argvIterator.next();
            if ("-limit".equals(curArg))
            {
                String[] limits = argvIterator.next().split(",");
                if (limits.length != 2)
                {
                    fatal("'-limit' option requires two comma-separated positive integer parameters: offset,range",
                            null);
                }
                rowOffset = Integer.parseInt(limits[0]);
                rowLimit = Integer.parseInt(limits[1]);
                if (rowOffset < 0 || rowLimit < 0)
                {
                    fatal("'-limit' option requires non-negative parameters",
                            null);
                }
            }
            else if ("-method".equals(curArg))
            {
                ccType = argvIterator.next();
            }
            else if (schemaName == null)
            {
                String[] names = curArg.split("\\.");
                if (names.length > 2 || names.length < 1)
                {
                    fatal("Schema/table name must be in the form schema[.table]. Found: "
                            + curArg, null);
                }
                schemaName = names[0];
                if (names.length == 2)
                {
                    tableName = names[1];
                }
            }
            else
            {
                fatal("Unrecognized argument: " + curArg, null);
            }
        }

        if (schemaName == null)
        {
            fatal("Schema/table name must be supplied for check comamnd", null);
        }

        if (tableName == null && rowOffset != ConsistencyTable.ROW_UNSET)
        {
            println("Only schema name supplied, row limits will be ignored.");
        }

        int id = getOpenReplicator().consistencyCheck(ccType, schemaName,
                tableName, rowOffset, rowLimit);
        println("Check (id=" + id + ") issued");

        if (getOpenReplicator().getRole().equals("slave"))
        {
            println("WARN: check should be issued from the master as opposed to the slave;"
                    + " to avoid duplicate key errors, remove it from the consistency table when done");
        }
    }

    // Perform a heartbeat operation.
    private void doHeartbeat() throws Exception
    {
        // Check for options.
        HashMap<String, String> params = new HashMap<String, String>();
        while (argvIterator.hasNext())
        {
            String curArg = argvIterator.next();
            try
            {
                if ("-name".equals(curArg))
                    params.put(OpenReplicatorParams.HEARTBEAT_NAME,
                            argvIterator.next());
                else
                    fatal("Unrecognized option: " + curArg, null);
            }
            catch (Exception e)
            {
                fatal("Missing or invalid argument to flag: " + curArg, null);
            }
        }
        getOpenReplicator().heartbeat(params);
    }

    // Handle a flush operation.
    private void doFlush() throws Exception
    {
        // Check for options.
        long seconds = 0;
        while (argvIterator.hasNext())
        {
            String curArg = argvIterator.next();
            try
            {
                if ("-limit".equals(curArg))
                    seconds = Long.parseLong(argvIterator.next());
                else
                    fatal("Unrecognized option: " + curArg, null);
            }
            catch (Exception e)
            {
                fatal("Missing or invalid argument to flag: " + curArg, null);
            }
        }

        String seqno = getOpenReplicator().flush(seconds);
        println("Master log is synchronized with database at log sequence number: "
                + seqno);
    }

    // Handle a purge operation, which kills non-Tungsten DBMS sessions.
    private void doPurge() throws Exception
    {
        // Check for options.
        boolean yes = false;
        long seconds = 0;
        while (argvIterator.hasNext())
        {
            String curArg = argvIterator.next();
            if ("-y".equals(curArg))
                yes = true;
            else if ("-limit".equals(curArg))
                seconds = Long.parseLong(argvIterator.next());
            else
                fatal("Unrecognized option: " + curArg, null);
        }

        // Prompt if user did not enter -yes.
        if (!yes)
        {
            String answer = readline("Do you really want to purge non-Tungsten DBMS sessions? [yes/NO] ");
            yes = "yes".equals(answer);
        }

        // If the user really wants to purge user sessions, do it.
        if (yes)
        {
            println("Directing replicator to purge non-Tungsten sessions");
            TungstenProperties paramProps = new TungstenProperties();
            if (seconds > 0)
                paramProps.setLong(OpenReplicatorParams.TIMEOUT, seconds);
            int count = getOpenReplicator().purge(paramProps.map());
            println("Number of sessions purged: " + count);
        }
    }

    // Handle a configure operation.
    private void doConfigure() throws Exception
    {
        // Load properties file if specified.
        TungstenProperties conf = null;
        if (argvIterator.hasNext())
        {
            File propsFile = new File(argvIterator.next());
            if (!propsFile.exists() || !propsFile.canRead())
            {
                fatal("Properties file not found: "
                        + propsFile.getAbsolutePath(), null);
            }
            conf = new TungstenProperties();
            try
            {
                conf.load(new FileInputStream(propsFile));
            }
            catch (IOException e)
            {
                fatal("Unable to read properties file: "
                        + propsFile.getAbsolutePath() + " (" + e.getMessage()
                        + ")", null);
            }
        }
        Map<String, String> confMap;
        if (conf == null)
            confMap = null;
        else
            confMap = conf.map();
        getOpenReplicator().configure(confMap);
    }

    // Handle a setrole command.
    private void doSetRole() throws Exception
    {
        String role = null;
        String uri = null;
        String curArg = null;
        try
        {
            while (argvIterator.hasNext())
            {
                curArg = argvIterator.next();
                if ("-role".equals(curArg))
                    role = argvIterator.next();
                else if ("-uri".equals(curArg))
                    uri = argvIterator.next();
                else
                {
                    fatal("Unrecognized option: " + curArg, null);
                }
            }
        }
        catch (ArrayIndexOutOfBoundsException e)
        {
            fatal("Missing value for " + curArg, null);
        }

        getOpenReplicator().setRole(role, uri);
    }

    // Handle a request to clear dynamic properties.
    private void doClearDynamic() throws Exception
    {
        getOpenReplicator().clearDynamicProperties();
    }

    // Handle a backup operation.
    private void doBackup() throws Exception
    {
        String backupAgent = null;
        String storageAgent = null;
        long seconds = 0;
        String curArg = null;
        try
        {
            while (argvIterator.hasNext())
            {
                curArg = argvIterator.next();
                if ("-backup".equals(curArg))
                    backupAgent = argvIterator.next();
                else if ("-storage".equals(curArg))
                    storageAgent = argvIterator.next();
                else if ("-limit".equals(curArg))
                    seconds = Long.parseLong(argvIterator.next());
                else
                {
                    fatal("Unrecognized option: " + curArg, null);
                }
            }
        }
        catch (ArrayIndexOutOfBoundsException e)
        {
            fatal("Missing value for " + curArg, null);
        }
        println("Warning! This is deprecated and will be removed in a future release!");
        String uri = getOpenReplicator().backup(backupAgent, storageAgent,
                seconds);
        if (uri == null)
            println("Backup is pending; check log for status");
        else
            println("Backup completed successfully; URI=" + uri);
    }

    // Handle a restore operation.
    private void doRestore() throws Exception
    {
        String uri = null;
        long seconds = 0;
        String curArg = null;
        try
        {
            while (argvIterator.hasNext())
            {
                curArg = argvIterator.next();
                if ("-uri".equals(curArg))
                    uri = argvIterator.next();
                else if ("-limit".equals(curArg))
                    seconds = Long.parseLong(argvIterator.next());
                else
                {
                    fatal("Unrecognized option: " + curArg, null);
                }
            }
        }
        catch (ArrayIndexOutOfBoundsException e)
        {
            fatal("Missing value for " + curArg, null);
        }

        println("Warning! This is deprecated and will be removed in a future release!");
        String result = getOpenReplicator().restore(uri, seconds);
        if (result != null)
            println("Restore completed successfully; URI=" + result);
        else
            println("Restore is pending; check log for status");
    }

    // Handle a request for status.
    private void doStatus() throws Exception
    {
        String name = null;
        String curArg = null;
        boolean json = false;
        try
        {
            while (argvIterator.hasNext())
            {
                curArg = argvIterator.next();
                if ("-name".equals(curArg))
                    name = argvIterator.next();
                else if ("-json".equals(curArg))
                    json = true;
                else
                {
                    fatal("Unrecognized option: " + curArg, null);
                }
            }
        }
        catch (ArrayIndexOutOfBoundsException e)
        {
            fatal("Missing value for " + curArg, null);
        }

        if (name == null)
        {
            if (!json)
            {
                println("Processing status command...");
                List<Map<String, String>> propList = new ArrayList<Map<String, String>>();
                propList.add(getOpenReplicator().status());
                printlnPropList(propList, json);
                println("Finished status command...");
            }
            else
                printPropertiesJSON(getOpenReplicator().status());
        }
        else
        {
            if (!json)
                println("Processing status command (" + name + ")...");
            List<Map<String, String>> propList = getOpenReplicator()
                    .statusList(name);
            printlnPropList(propList, json);
            if (!json)
                println("Finished status command (" + name + ")...");
        }
    }

    // Handle a request to show plugin capabilities.
    private void doCapabilities() throws Exception
    {
        Map<String, String> caps = getOpenReplicator().capabilities();
        ReplicatorCapabilities capabilities = new ReplicatorCapabilities(
                new TungstenProperties(caps));
        println("Replicator Capabilities");
        println("  Roles:             " + capabilities.getRoles());
        println("  Replication Model: " + capabilities.getModel());
        // println("  Provision Driver:  " + capabilities.getProvisionDriver());
        println("  Consistency Check: " + capabilities.isConsistencyCheck());
        println("  Heartbeat:         " + capabilities.isHeartbeat());
        println("  Flush:             " + capabilities.isFlush());
    }

    /**
     * Print properties output.
     * 
     * @param json If true, print in JSON format.
     */
    private static void printlnPropList(List<Map<String, String>> propList,
            boolean json)
    {
        // Scan for maximum property name and value lengths.
        int maxName = 4;
        int maxValue = 5;
        for (Map<String, String> props : propList)
        {
            for (String key : props.keySet())
            {
                if (key.length() > maxName)
                    maxName = key.length();
            }
        }

        // Construct formating strings.
        String headerFormat = "%-" + maxName + "s  %-" + maxValue + "s\n";
        String valueFormat = "%-" + maxName + "s: %s\n";
        String nextValFormat = "%-" + maxName + "s  %s\n";
        // Print values.
        int propIdx = 0;
        if (json)
            println("[");
        for (Map<String, String> props : propList)
        {
            if (json)
            {
                if (propIdx > 0)
                    println(",");
                printPropertiesJSON(props, propIdx);
            }
            else
            {
                printf(headerFormat, "NAME", "VALUE");
                printf(headerFormat, "----", "-----");

                TreeSet<String> treeSet = new TreeSet<String>(props.keySet());
                for (String key : treeSet)
                {
                    String value = props.get(key);
                    if (value != null)
                    {
                        String[] split = value.split("\n");
                        boolean first = true;
                        for (String string : split)
                        {
                            if (first)
                            {
                                printf(valueFormat, key, string);
                                first = false;
                            }
                            else
                                printf(nextValFormat, "", string);
                        }
                    }
                    else
                        printf(valueFormat, key, value);
                }
            }
            propIdx++;
        }
        if (json)
            println("\n]");
    }

    /**
     * Prints properties and values in JSON. One level of nesting.
     */
    private static void printPropertiesJSON(Map<String, String> props)
    {
        printPropertiesJSON(props, -1);
    }

    /**
     * Prints properties and values in JSON.
     * 
     * @param propIdx Index of the given property map. -1, if it's a single one
     *            only.
     */
    private static void printPropertiesJSON(Map<String, String> props,
            int propIdx)
    {
        // Using a JSON library to escape various special characters.
        String jsonText = JSONValue.toJSONString(props);
        println(jsonText);
    }

    /**
     * Prints property values only. Useful for feeding into another process
     * parameters.
     */
    private static void printPropertiesValues(Map<String, String> props)
    {
        Object[] keys = props.keySet().toArray();
        for (int i = 0; i < keys.length; i++)
        {
            String key = (String) keys[i];
            String value = props.get(key);
            if (value != null)
                println(value);
            else
                println("");
        }
    }

    // Print a message to stdout.
    private static void println(String msg)
    {
        System.out.println(msg);
    }

    // Print a formatted message to stdout.
    private static void printf(String format, Object... args)
    {
        System.out.printf(format, args);
    }

    // Print a message to stdout without a carriage return.
    private static void print(String msg)
    {
        System.out.print(msg);
    }

    // Read a line from standard out with optional prompt.
    private String readline(String prompt)
    {
        if (prompt != null)
            print(prompt);
        try
        {
            return stdin.readLine();
        }
        catch (IOException e)
        {
            return null;
        }
    }

    // Abort following a fatal error.
    private void fatal(String msg, Throwable t)
    {
        System.out.println(msg);
        if (verbose && t != null)
            t.printStackTrace();
        System.exit(1);
    }

    // Issue a yes/no prompt.
    private boolean confirm(String prompt)
    {
        // Check for a -yes option.
        boolean yes = false;
        while (argvIterator.hasNext())
        {
            String curArg = argvIterator.next();
            if ("-y".equals(curArg))
                yes = true;
            else
                fatal("Unrecognized option: " + curArg, null);
        }

        // Prompt if user did not enter -y.
        if (!yes)
        {
            String answer = readline(prompt + " [yes/NO] ");
            yes = "yes".equals(answer);
        }

        return yes;
    }

    // SHARD COMMANDS //

    private ShardManagerMBean getShardManager() throws Exception
    {
        if (this.shardMBean == null)
        {
            if (service == null)
            {
                // If there is just one available service, we will use that as
                // the default.
                List<Map<String, String>> services = serviceManagerMBean
                        .services();
                if (services.size() == 0)
                {
                    throw new Exception(
                            "Operation requires a service name, but the replicator does not have any services defined");
                }
                else if (services.size() == 1)
                {
                    service = services.get(0).get("name");
                    logger.debug("Inferring service name automatically: "
                            + service);
                }
                else
                {
                    throw new Exception(
                            "You must specify a service name with the -service flag");
                }
            }
            // Get MBean connection.
            // shardMBean = (ShardManagerMBean)
            // getOpenReplicator().getExtensionMBean("shard-manager");
            // shardMBean = (ShardManagerMBean) JmxManager.getMBeanProxy(conn,
            // ShardManager.class, false);
            shardMBean = getShardManagerSafely(service);

        }
        return shardMBean;
    }

    // Fetch a specific replicator manager MBean by name, with optional delay.
    private ShardManagerMBean getShardManagerSafely(String name)
            throws Exception
    {
        // Get MBean connection.
        int delay = 0;
        for (;;)
        {
            Exception failure = null;
            try
            {
                // Fetch MBean with service name.
                shardMBean = (ShardManagerMBean) JmxManager.getMBeanProxy(conn,
                        ShardManager.class, ShardManagerMBean.class, name,
                        false, false);
                shardMBean.isAlive();
            }
            catch (Exception e)
            {
                failure = e;
            }
            if (failure == null)
            {
                if (verbose)
                    println("Connected to ShardManagerMBean: " + name);
                break;
            }
            else if (connectDelay > delay)
            {
                sleep(1);
                print(".");
                delay++;
            }
            else
                throw failure;
        }
        // If we delayed, need to have a carriage return.
        if (delay > 0)
            println("");
        return shardMBean;
    }

    private void doShardCommand() throws Exception
    {
        // Checks for options.
        int operation = -1;
        String params = null;
        String separator = "";

        boolean readStdInput = false;

        while (argvIterator.hasNext())
        {
            String curArg = argvIterator.next();
            try
            {
                if ("-insert".equals(curArg))
                {
                    separator = "\\],\\[";
                    operation = ShardCommand.ADD;
                    if (argvIterator.hasNext())
                    {
                        params = argvIterator.next();
                    }
                    else
                    {
                        println("Reading from standard input");
                        readStdInput = true;
                    }
                }
                else if ("-update".equals(curArg))
                {
                    separator = "\\],\\[";
                    operation = ShardCommand.UPD;
                    if (argvIterator.hasNext())
                    {
                        params = argvIterator.next();
                    }
                    else
                    {
                        println("Reading from standard input");
                        readStdInput = true;
                    }
                }
                else if ("-delete".equals(curArg))
                {
                    separator = ",";
                    operation = ShardCommand.DEL;
                    params = argvIterator.next();
                }
                else if ("-deleteAll".equals(curArg))
                {
                    operation = ShardCommand.DELALL;
                }
                else if ("-list".equals(curArg))
                {
                    operation = ShardCommand.LIST;
                }
                else
                    fatal("Unrecognized option: " + curArg, null);
            }
            catch (Exception e)
            {
                fatal("Missing or invalid argument to flag: " + curArg, null);
            }
        }

        List<String[]> values = new ArrayList<String[]>();

        // // Load parameters, if any.
        TungstenProperties paramProps = new TungstenProperties();

        if (params != null)
        {
            String[] param = params.split(separator);
            for (int i = 0; i < param.length; i++)
            {
                param[i] = param[i].replace("[", "");
                param[i] = param[i].replace("]", "");
                values.add(param[i].split(","));
            }
        }
        else if (readStdInput)
        {
            CsvReader csvReader = new CsvReader(
                    new InputStreamReader(System.in));
            csvReader.setCollapseSeparators(true);
            while (csvReader.next())
            {
                String[] row = new String[3];
                row[0] = csvReader.getString(ShardTable.SHARD_ID_COL);
                row[1] = csvReader.getString(ShardTable.SHARD_MASTER_COL);
                row[2] = csvReader.getString(ShardTable.SHARD_CRIT_COL);
                values.add(row);
            }
        }

        ShardManagerMBean shardManager = getShardManager();
        if (shardManager == null)
        {
            throw new Exception("Unable to connect to shard manager");
        }

        int shardsCount = 0;
        List<Map<String, String>> shardParams = new ArrayList<Map<String, String>>();
        switch (operation)
        {
            case ShardCommand.ADD :
                if (values.isEmpty())
                    return;
                for (Iterator<String[]> iterator = values.iterator(); iterator
                        .hasNext();)
                {
                    String[] val = iterator.next();

                    if (val.length < 3)
                        println("Missing parameter for shard creation : " + val
                                + "\nSkipping");
                    else
                    {
                        paramProps.put(ShardTable.SHARD_ID_COL, val[0]);
                        paramProps.put(ShardTable.SHARD_MASTER_COL, val[1]);
                        paramProps.put(ShardTable.SHARD_CRIT_COL, val[2]);

                        shardParams.add(paramProps.map());
                    }
                }

                shardsCount = shardManager.insert(shardParams);
                println(shardsCount
                        + " new shard"
                        + (shardsCount > 1 ? "s" : "")
                        + " inserted."
                        + (shardsCount == values.size()
                                ? ""
                                : " Some shards were not inserted due to errors. Please check the log."));
                break;

            case ShardCommand.UPD :
                if (values.isEmpty())
                    return;

                for (Iterator<String[]> iterator = values.iterator(); iterator
                        .hasNext();)
                {
                    String[] val = iterator.next();

                    if (val.length < 4)
                        println("Missing parameter for shard creation : " + val
                                + "\nSkipping");
                    else
                    {
                        paramProps.put(ShardTable.SHARD_ID_COL, val[0]);
                        paramProps.put(ShardTable.SHARD_MASTER_COL, val[1]);
                        paramProps.put(ShardTable.SHARD_CRIT_COL, val[2]);

                        shardParams.add(paramProps.map());
                    }
                }
                shardsCount = shardManager.update(shardParams);
                println(shardsCount + " shard" + (shardsCount > 1 ? "s" : "")
                        + " updated.");

                break;

            case ShardCommand.DEL :
                if (values.isEmpty())
                    return;

                for (Iterator<String[]> iterator = values.iterator(); iterator
                        .hasNext();)
                {
                    String[] val = iterator.next();
                    paramProps.put(ShardTable.SHARD_ID_COL, val[0]);
                    shardParams.add(paramProps.map());
                }
                shardsCount = shardManager.delete(shardParams);
                println(shardsCount + " shard" + (shardsCount > 1 ? "s" : "")
                        + " deleted.");
                break;

            case ShardCommand.DELALL :
                shardsCount = shardManager.deleteAll();
                println(shardsCount + " shard" + (shardsCount > 1 ? "s" : "")
                        + " deleted.");
                break;

            case ShardCommand.LIST :
                List<Map<String, String>> list = shardManager.list();
                if (list == null)
                {
                    println("Empty shard list");
                }
                else
                {
                    CsvWriter csvWriter = new CsvWriter(new OutputStreamWriter(
                            System.out));
                    csvWriter.setFieldSeparator("\t");
                    csvWriter.addColumnName(ShardTable.SHARD_ID_COL);
                    csvWriter.addColumnName(ShardTable.SHARD_MASTER_COL);
                    csvWriter.addColumnName(ShardTable.SHARD_CRIT_COL);
                    for (Iterator<Map<String, String>> iterator = list
                            .iterator(); iterator.hasNext();)
                    {
                        Map<String, String> map = iterator.next();
                        csvWriter.put(ShardTable.SHARD_ID_COL,
                                map.get(ShardTable.SHARD_ID_COL));
                        csvWriter.put(ShardTable.SHARD_MASTER_COL,
                                map.get(ShardTable.SHARD_MASTER_COL));
                        csvWriter.put(ShardTable.SHARD_CRIT_COL,
                                map.get(ShardTable.SHARD_CRIT_COL));
                        csvWriter.write();
                    }
                    csvWriter.flush();
                }

                break;
            default :
                break;
        }
    }

    // List of commands. Originally a separate class in commons.
    class Commands
    {
        // Replicator-wide commands.
        public static final String SERVICES         = "services";
        public static final String START            = "start";
        public static final String STOP             = "stop";
        public static final String LOAD             = "load";
        public static final String UNLOAD           = "unload";
        public static final String SHUTDOWN         = "shutdown";
        public static final String KILL             = "kill";

        // Service-specific replicator commands.
        public static final String ONLINE           = "online";
        public static final String OFFLINE          = "offline";
        public static final String OFFLINE_DEFERRED = "offline-deferred";
        public static final String FLUSH            = "flush";
        public static final String PURGE            = "purge";
        public static final String HEARTBEAT        = "heartbeat";
        public static final String CONFIGURE        = "configure";
        public static final String SETROLE          = "setrole";
        public static final String CLEAR            = "clear";
        public static final String STATS            = "status";
        public static final String CLIENTS          = "clients";
        public static final String PROPERTIES       = "properties";
        public static final String HELP             = "help";
        public static final String VERSION          = "version";
        public static final String WAIT             = "wait";
        public static final String CHECK            = "check";
        public static final String BACKUP           = "backup";
        public static final String RESTORE          = "restore";
        public static final String PROVISION        = "provision";
        public static final String CAPABILITIES     = "capabilities";
        public static final String RESET            = "reset";

        // Shard commands (service-specific).
        public static final String SHARD            = "shard";
    }

    class ShardCommand
    {
        public static final int ADD    = 0;
        public static final int UPD    = 1;
        public static final int DEL    = 2;
        public static final int LIST   = 3;
        public static final int DELALL = 4;
    }
}
