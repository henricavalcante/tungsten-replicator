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
 * Contributor(s): Linas Virbalas, Stephane Giron
 */

package com.continuent.tungsten.replicator.management;

import java.io.File;
import java.io.FilenameFilter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.Vector;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.cluster.resource.ResourceState;
import com.continuent.tungsten.common.cluster.resource.physical.Replicator;
import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.config.cluster.ConfigurationException;
import com.continuent.tungsten.common.jmx.DynamicMBeanHelper;
import com.continuent.tungsten.common.jmx.JmxManager;
import com.continuent.tungsten.common.jmx.MethodDesc;
import com.continuent.tungsten.common.jmx.ParamDesc;
import com.continuent.tungsten.common.security.AuthenticationInfo;
import com.continuent.tungsten.common.security.SecurityHelper;
import com.continuent.tungsten.common.utils.CLUtils;
import com.continuent.tungsten.common.utils.ManifestParser;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.PropertiesManager;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntimeConf;
import com.continuent.tungsten.replicator.datasource.DataSourceAdministrator;

/**
 * This class implements the main() method for launching replicator process and
 * starting all services.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ReplicationServiceManager implements ReplicationServiceManagerMBean
{
    public static final String CONFIG_SERVICES    = "services.properties";
    public static final String CONFIG_FILE_PREFIX = "static-";
    public static final String CONFIG_FILE_SUFFIX = ".properties";

    private static Logger                               logger                = Logger
            .getLogger(ReplicationServiceManager.class);
    private TungstenProperties                          serviceProps          = null;
    private TreeMap<String, OpenReplicatorManagerMBean> replicators           = new TreeMap<String, OpenReplicatorManagerMBean>();
    private Map<String, TungstenProperties>             serviceConfigurations = new TreeMap<String, TungstenProperties>();

    private int masterListenPortStart = 2111;
    private int masterListenPortMax   = masterListenPortStart;

    private String   managerRMIHost     = null;
    private int      managerRMIPort     = -1;
    private TimeZone hostTimeZone       = null;
    private TimeZone replicatorTimeZone = null;

    /**
     * Creates a new <code>ReplicatorManager</code> object
     * 
     * @throws Exception
     */
    public ReplicationServiceManager() throws Exception
    {
    }

    /**
     * Start replicator services.
     * 
     * @param forceOffline Forces the replicator to start every services offline
     */
    public void go(boolean forceOffline) throws ReplicatorException
    {
        // Find and load the service.properties file.
        File confDir = ReplicatorRuntimeConf.locateReplicatorConfDir();
        File propsFile = new File(confDir, CONFIG_SERVICES);
        serviceProps = PropertiesManager.loadProperties(propsFile);

        // Get Authentication and encryption parameters for JMX and set SSL
        // parameters required for secure operation.
        TungstenProperties jmxProperties = null;
        try
        {
            logger.info("Loading security information");
            AuthenticationInfo authenticationInfo = SecurityHelper
                    .loadAuthenticationInformation(); // Load security
                                                      // information and set
                                                      // critical properties as
                                                      // system properties
            jmxProperties = authenticationInfo.getAsTungstenProperties();
        }
        catch (ConfigurationException ce)
        {
            // Log a warning and continue without security properties
            String msg = MessageFormat.format(
                    "Error while loading Security properties from file: will continue with default values.\n Caused by: {0}",
                    ce.getMessage());
            logger.warn(msg);
            // throw new ReplicatorException(ce.getMessage(), ce);
        }

        // Initialize time zone data.
        initializeTimeZoneInfo(serviceProps);

        // --- Start JMX registry ----
        managerRMIPort = serviceProps.getInt(ReplicatorConf.RMI_PORT,
                ReplicatorConf.RMI_DEFAULT_PORT, false);
        managerRMIHost = getHostName(serviceProps);

        JmxManager jmxManager = new JmxManager(managerRMIHost, managerRMIPort,
                ReplicatorConf.RMI_DEFAULT_SERVICE_NAME, jmxProperties);
        jmxManager.start();

        // Make sure we have configurations for the replicators to work with.
        loadServiceConfigurations();
        Vector<TungstenProperties> remoteServices = new Vector<TungstenProperties>();

        // We will start the local services first, and only then will we start
        // remote services.
        for (String serviceName : serviceConfigurations.keySet())
        {
            TungstenProperties replProps = serviceConfigurations
                    .get(serviceName);
            String serviceType = replProps
                    .getString(ReplicatorConf.SERVICE_TYPE);
            boolean isDetached = replProps.getBoolean(ReplicatorConf.DETACHED);

            replProps.setBoolean(ReplicatorConf.FORCE_OFFLINE, forceOffline);
            if (serviceType.equals("local"))
            {
                // Get properties file name if specified or generate default.
                try
                {
                    logger.info(String.format(
                            "Starting the %s/%s replication service '%s'",
                            (isDetached ? "detached" : "internal"), serviceType,
                            serviceName));
                    startReplicationService(replProps);
                }
                catch (Exception e)
                {
                    logger.error(
                            "Unable to instantiate replication service: name="
                                    + serviceName,
                            e);
                }
            }
            else if (serviceType.equals("remote"))
            {
                remoteServices.add(replProps);
            }
            else
            {
                logger.warn(String.format(
                        "The replication service '%s' has an urecognized type '%s'",
                        serviceName, serviceType));
            }
        }

        for (TungstenProperties replProps : remoteServices)
        {
            String serviceName = replProps
                    .getString(ReplicatorConf.SERVICE_NAME);
            String serviceType = replProps
                    .getString(ReplicatorConf.SERVICE_TYPE);

            // Get properties file name if specified or generate default.
            try
            {
                logger.info(String.format(
                        "Starting the %s replication service '%s'", serviceType,
                        serviceName));
                startReplicationService(replProps);
            }
            catch (Exception e)
            {
                logger.error("Unable to instantiate replication service: name="
                        + serviceName, e);
            }
        }

        // Register ourselves as the master service manager bean.
        // JmxManager.registerMBean(this, ReplicationServiceManagerMBean.class);
        JmxManager.registerMBean(this, ReplicationServiceManager.class);
    }

    /**
     * Initializes the JVM time zone after capturing the host time zone
     * information. The time zone if other than GMT must be set in
     * services.properties.
     * 
     * @throws ReplicatorException Thrown if time zone safety conditions are
     *             violated
     */
    private void initializeTimeZoneInfo(TungstenProperties serviceProps)
            throws ReplicatorException
    {
        // Time zones are important so we need to print a good message at
        // start-up.
        logger.info(
                "Compatibility note: Replicator time zone is set from services.properties and defaults to GMT");
        logger.info(
                "Setting time zones via wrapper.conf -Duser.timezone option is deprecated");
        logger.info(
                "Consult system documentation before making any changes to time zone-related settings");

        // Store the default time zone from the host.
        hostTimeZone = TimeZone.getDefault();
        logger.info("Storing host time zone: id=" + hostTimeZone.getID()
                + " display name=" + hostTimeZone.getDisplayName());

        // Fetch the global time zone from the services.properties file.
        String replicatorTimeZoneID = serviceProps.getString(
                ReplicatorConf.TIME_ZONE, ReplicatorConf.TIME_ZONE_DEFAULT,
                true);

        if (!"GMT".equals(replicatorTimeZoneID))
        {
            logger.warn(
                    "Overriding replicator default GMT time zone using services.properties; this is not recommended except for test/emergency purposes: time zone id="
                            + replicatorTimeZoneID);
        }
        replicatorTimeZone = TimeZone.getTimeZone(replicatorTimeZoneID);
        TimeZone.setDefault(replicatorTimeZone);
        logger.info("Setting replicator JVM time zone: id="
                + replicatorTimeZone.getID() + " display name="
                + replicatorTimeZone.getDisplayName());
    }

    /**
     * Main method for ReplicatorManager.
     * 
     * @param argv
     */
    public static void main(String argv[])
    {
        // Check that log4j.rootAppender is set. Otherwise, default to file
        // so that users can see a log. This must be done before the first
        // call to log4j.
        String rootAppender = System.getProperty("log4j.rootAppender");
        if (rootAppender == null)
            System.setProperty("log4j.rootAppender", "file");

        // Mark release.
        ManifestParser.logReleaseWithBuildNumber(logger);
        logger.info("Starting replication service manager");

        boolean forceOffline = false;

        // Parse global options and command.
        for (int i = 0; i < argv.length; i++)
        {
            String curArg = argv[i++];
            if ("-clear".equals(curArg))
            {
                System.setProperty(
                        ReplicatorRuntimeConf.CLEAR_DYNAMIC_PROPERTIES, "true");
            }
            else if ("-help".equals(curArg))
            {
                printHelp();
                System.exit(0);
            }
            else
                if ("-offline".equalsIgnoreCase(curArg)
                        || "offline".equalsIgnoreCase(curArg))
            {
                forceOffline = true;
            }
            else
            {
                System.err.println("Unrecognized option: " + curArg);
                System.exit(1);
            }
        }

        try
        {
            ReplicationServiceManager rmgr = new ReplicationServiceManager();
            rmgr.go(forceOffline);
            try
            {
                Thread.sleep(Long.MAX_VALUE);
            }
            catch (InterruptedException ie)
            {
                System.err.println("Interrupted");
            }
            logger.info("Stopping replication service manager");
        }
        catch (Throwable e)
        {
            logger.fatal("Unable to start replicator", e);
        }
    }

    // START OF MBEAN API

    /**
     * Returns true so that clients can confirm connection liveness.
     */
    @MethodDesc(description = "Confirm service liveness", usage = "isAlive")
    public boolean isAlive()
    {
        return true;
    }

    /**
     * Returns a list of replicators, started or not. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.ReplicationServiceManagerMBean#services()
     */
    @MethodDesc(description = "List known replication services", usage = "services")
    public List<Map<String, String>> services() throws Exception
    {
        List<Map<String, String>> services = new ArrayList<Map<String, String>>();

        for (String name : serviceConfigurations.keySet())
        {
            Map<String, String> info = new TreeMap<String, String>();

            info.put("name", name);

            if (replicators.get(name) != null)
            {
                info.put("started", "true");
            }
            else
            {
                info.put("started", "false");
            }
            services.add(info);
        }
        return services;
    }

    /**
     * Starts a service if it is defined. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.ReplicationServiceManagerMBean#loadService(String)
     */
    @MethodDesc(description = "Start individual replication service", usage = "startService name")
    public boolean loadService(
            @ParamDesc(name = "name", description = "service name") String name)
                    throws Exception
    {
        loadServiceConfigurations();

        if (!serviceConfigurations.keySet().contains(name))
        {
            throw new Exception("Unknown replication service name: " + name);
        }
        else if (replicators.get(name) == null)
        {
            startReplicationService(serviceConfigurations.get(name));
            return true;
        }
        else
        {
            return false;
        }
    }

    /**
     * Stops a service if it is started and defined. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.ReplicationServiceManagerMBean#unloadService(String)
     */
    @MethodDesc(description = "Stop individual replication service", usage = "stopService name")
    public boolean unloadService(
            @ParamDesc(name = "name", description = "service name") String name)
                    throws Exception
    {
        loadServiceConfigurations();

        if (!serviceConfigurations.keySet().contains(name))
        {
            throw new Exception("Unknown replication service name: " + name);
        }
        else if (replicators.get(name) == null)
        {
            return false;
        }
        else
        {
            stopReplicationService(name);
            return true;
        }
    }

    /**
     * Resets a replication service. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.ReplicationServiceManagerMBean#resetService(java.lang.String)
     */
    @MethodDesc(description = "Reset individual replication service", usage = "resetService name")
    public Map<String, String> resetService(
            @ParamDesc(name = "name", description = "service name") String name)
                    throws Exception
    {
        return resetService(name, null);
    }

    /**
     * Resets a replication service. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.ReplicationServiceManagerMBean#resetService(java.lang.String)
     */
    @MethodDesc(description = "Reset individual replication service", usage = "resetService name")
    public Map<String, String> resetService(
            @ParamDesc(name = "name", description = "service name") String name,
            @ParamDesc(name = "controlParams", description = "0 or more control parameters expressed as name-value pairs") Map<String, String> controlParams)
                    throws Exception
    {
        loadServiceConfigurations();

        // Make sure we have heard of this replicator.
        if (!serviceConfigurations.keySet().contains(name))
        {
            throw new Exception("Unknown replication service name: " + name);
        }

        // If the replicator is started, we cannot delete it.
        OpenReplicatorManagerMBean orm = replicators.get(name);
        if (orm != null && !orm.getState().startsWith("OFFLINE"))
        {
            throw new Exception("Replication service " + name
                    + " must be offline before resetting.\n"
                    + "(Use 'trepctl -service " + name + " offline' first)");
        }

        // Get service information.
        TungstenProperties serviceProps = serviceConfigurations.get(name);
        TungstenProperties.substituteSystemValues(serviceProps.getProperties());

        Map<String, String> progress = new LinkedHashMap<String, String>();

        String option = null;
        if (controlParams != null)
            option = controlParams.get("option");

        if (option == null || option.equalsIgnoreCase("-all"))
        {
            logger.info("Resetting replication service: name=" + name);

            resetDatabase(serviceProps, progress);

            resetTHL(serviceProps, progress);

            resetRelay(serviceProps, progress);
        }
        else if (option.equalsIgnoreCase("-thl"))
        {
            logger.info("Resetting THL for service " + name);
            resetTHL(serviceProps, progress);

        }
        else if (option.equalsIgnoreCase("-relay"))
        {
            logger.info("Resetting relay logs for service " + name);
            resetRelay(serviceProps, progress);
        }
        else if (option.equalsIgnoreCase("-db"))
        {
            logger.info("Resetting database for service " + name);
            resetDatabase(serviceProps, progress);
        }
        else
        {
            logger.info("Unimplemented reset option : " + option);
        }

        logger.info("\n" + CLUtils.formatMap("progress", progress, "", false)
                + "\n");
        return progress;
    }

    /**
     * Resets all catalog data for data sources associated with current
     * replicator.
     */
    private void resetDatabase(TungstenProperties serviceProps,
            Map<String, String> progress) throws InterruptedException
    {
        // Clear data source state.
        DataSourceAdministrator admin = null;
        String serviceName = serviceProps
                .getString(ReplicatorConf.SERVICE_NAME);
        try
        {
            admin = new DataSourceAdministrator(serviceProps);
            admin.prepare();
            progress.put("clear data source catalogs", serviceName);
            boolean cleared = admin.resetAll();
            if (cleared)
            {
                logger.info("Data source catalog information cleared");
            }
            else
            {
                logger.info("Unable to clear data source information: service="
                        + serviceName);
            }
        }
        catch (ReplicatorException e)
        {
            logger.error(String.format(
                    "Error while clearing data source information %s: %s",
                    serviceName, e.getMessage()), e);
        }
        finally
        {
            if (admin != null)
            {
                admin.release();
            }
        }
    }

    /**
     * Resets relay log information if it exists.
     */
    private void resetRelay(TungstenProperties serviceProps,
            Map<String, String> progress)
    {
        // Remove relay logs, if present.
        String relayLogDirName = serviceProps
                .getString("replicator.extractor.dbms.relayLogDir");

        if (relayLogDirName != null)
        {
            File relayLogDir = new File(relayLogDirName);

            if (!removeDirectory(relayLogDir, progress))
            {
                logger.error(String.format(
                        "Could not remove the relay log directory %s",
                        relayLogDirName));
            }
        }
    }

    /**
     * Clears THL if it exists.
     */
    private void resetTHL(TungstenProperties serviceProps,
            Map<String, String> progress)
    {
        // Remove THL files.
        String logDirName = serviceProps
                .getString("replicator.store.thl.log_dir");

        File logDir = new File(logDirName);

        if (!removeDirectory(logDir, progress))
        {
            logger.error(String.format("Could not remove the log directory %s",
                    logDirName));
        }
    }

    /**
     * Stops all services and terminates the replicator process. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.ReplicationServiceManagerMBean#stop()
     */
    @MethodDesc(description = "Stop replication services cleanly and exit process", usage = "stop")
    public void stop() throws Exception
    {
        for (String name : replicators.keySet())
        {
            stopReplicationService(name);
        }
        exitProcess(true, "Shutting down process after stopping services");
    }

    /**
     * Returns a list of properties that have the status for each of the current
     * services.
     * 
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorManagerMBean#status()
     */
    @MethodDesc(description = "Return the status for one or more replicators", usage = "status(name)")
    public Map<String, String> replicatorStatus(
            @ParamDesc(name = "name", description = "optional name of replicator") String name)
                    throws Exception
    {

        OpenReplicatorManagerMBean mgr = replicators.get(name);

        if (mgr == null)
        {
            throw new Exception(String.format(
                    "There is no replication service with the name '%s'",
                    name));
        }

        return mgr.status();
    }

    /**
     * Returns a map of status properties for all current replicators
     * 
     * @throws Exception
     */
    @MethodDesc(description = "Return the status for all current replicators", usage = "status()")
    public Map<String, String> getStatus() throws Exception
    {
        Map<String, String> managerProps = new HashMap<String, String>();

        managerProps.put(Replicator.SOURCEID,
                serviceProps.getString(ReplicatorConf.SOURCE_ID));
        managerProps.put(Replicator.STATE, ResourceState.ONLINE.toString());
        managerProps.put(Replicator.CLUSTERNAME,
                serviceProps.getString(ReplicatorConf.CLUSTER_NAME));
        managerProps.put(Replicator.HOST,
                serviceProps.getString(ReplicatorConf.REPLICATOR_HOST));
        managerProps.put(Replicator.RESOURCE_JDBC_URL,
                serviceProps.getString(ReplicatorConf.RESOURCE_JDBC_URL));
        managerProps.put(Replicator.RESOURCE_JDBC_DRIVER,
                serviceProps.getString(ReplicatorConf.RESOURCE_JDBC_DRIVER));
        managerProps.put(Replicator.RESOURCE_VENDOR,
                serviceProps.getString(ReplicatorConf.RESOURCE_VENDOR));
        managerProps.put(Replicator.RESOURCE_LOGDIR,
                serviceProps.getString(ReplicatorConf.RESOURCE_LOGDIR));
        managerProps.put(Replicator.RESOURCE_LOGPATTERN,
                serviceProps.getString(ReplicatorConf.RESOURCE_LOGPATTERN));
        managerProps.put(ReplicatorConf.RESOURCE_DISKLOGDIR,
                serviceProps.getString(ReplicatorConf.RESOURCE_DISKLOGDIR));
        managerProps.put(Replicator.RESOURCE_PORT, Integer
                .toString(serviceProps.getInt(ReplicatorConf.RESOURCE_PORT)));
        managerProps.put(Replicator.DATASERVER_HOST, serviceProps
                .getString(ReplicatorConf.RESOURCE_DATASERVER_HOST));
        managerProps.put(Replicator.USER,
                serviceProps.getString(ReplicatorConf.GLOBAL_DB_USER));
        managerProps.put(Replicator.PASSWORD,
                serviceProps.getString(ReplicatorConf.GLOBAL_DB_PASSWORD));
        managerProps.put(Replicator.MAX_PORT, Integer.toString(getMaxPort()));

        Map<String, Map<String, String>> statusProps = new TreeMap<String, Map<String, String>>();

        for (String name : replicators.keySet())
        {
            statusProps.put(name, replicatorStatus(name));
        }

        managerProps.put("serviceProperties", statusProps.toString());

        return managerProps;
    }

    /**
     * Convenience method that can be visible in manager.
     * 
     * @throws Exception
     */
    @MethodDesc(description = "Return the status for all current replicators", usage = "status()")
    public Map<String, String> status() throws Exception
    {
        return getStatus();
    }

    /**
     * Terminates the replicator process immediately. Only the PID file is
     * cleaned up.
     */
    @MethodDesc(description = "Exit replicator immediately without cleanup", usage = "kill")
    public void kill() throws Exception
    {
        exitProcess(true,
                "Shutting down process immediately without stopping services");
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorManager#createHelper()
     */
    @MethodDesc(description = "Returns a DynamicMBeanHelper to facilitate dynamic JMX calls", usage = "createHelper")
    public DynamicMBeanHelper createHelper() throws Exception
    {
        return JmxManager.createHelper(getClass());
    }

    // END OF MBEAN API

    /**
     * Utility routine to start a replication service. It will be started either
     * as an internal thread or as a detached process, depending on it's
     * configuration
     */
    private void startReplicationService(TungstenProperties replProps)
            throws ReplicatorException
    {
        String serviceName = replProps.getString(ReplicatorConf.SERVICE_NAME);
        String serviceType = replProps.getString(ReplicatorConf.SERVICE_TYPE);
        boolean isDetached = replProps.getBoolean(ReplicatorConf.DETACHED);

        OpenReplicatorManagerMBean orm = null;

        try
        {
            if (isDetached)
            {
                throw new ReplicatorException(
                        "Creating of detached service is no longer supported");
            }
            else
            {
                orm = createInternalService(serviceName);
            }

            // Put the service in the list of replicators now, as the start
            // might fail.
            replicators.put(serviceName, orm);
            orm.start(replProps.getBoolean(ReplicatorConf.FORCE_OFFLINE));

            int listenPort = orm.getMasterListenPort();
            if (listenPort > masterListenPortMax)
                masterListenPortMax = listenPort;

            logger.info(String.format(
                    "%s/%s replication service '%s' started successfully",
                    (isDetached ? "detached" : "internal"), serviceType,
                    serviceName));
        }
        catch (Exception e)
        {
            logger.error(
                    String.format("Unable to start replication service '%s'",
                            serviceName),
                    e);
        }

    }

    /**
     * Creates a replication service that will run as a thread internal to the
     * ReplicationServiceManager.
     * 
     * @param serviceName
     * @throws ReplicatorException
     */
    private OpenReplicatorManagerMBean createInternalService(String serviceName)
            throws ReplicatorException
    {
        logger.info("Starting replication service: name=" + serviceName);
        try
        {
            OpenReplicatorManager orm = new OpenReplicatorManager(serviceName);
            orm.setRmiHost(managerRMIHost);
            orm.setRmiPort(managerRMIPort);
            orm.setHostTimeZone(hostTimeZone);
            orm.setReplicatorTimeZone(replicatorTimeZone);
            orm.advertiseInternal();
            return (OpenReplicatorManagerMBean) orm;
        }
        catch (Exception e)
        {
            throw new ReplicatorException(String.format(
                    "Unable to instantiate replication service '%s'",
                    serviceName), e);
        }
    }

    // Utility routine to start a replication service.
    private void stopReplicationService(String name) throws Exception
    {
        logger.info("Stopping replication service: name=" + name);
        OpenReplicatorManagerMBean orm = replicators.get(name);
        try
        {
            orm.offline();
        }
        catch (Exception e)
        {
            logger.warn("Could not place service in offline state: "
                    + e.getMessage());
        }
        orm.stop();
        replicators.remove(name);
        logger.info("Replication service stopped successfully: name=" + name);
    }

    /**
     * Returns the hostname to be used to bind ports for RMI use.
     */
    public static String getHostName(TungstenProperties properties)
    {
        String defaultHost = properties.getString(ReplicatorConf.RMI_HOST);
        String hostName = System.getProperty(ReplicatorConf.RMI_HOST,
                defaultHost);
        // No value provided, retrieve from environment.
        if (hostName == null)
        {
            try
            {
                // Get hostname.
                InetAddress addr = InetAddress.getLocalHost();
                hostName = addr.getHostName();
            }
            catch (UnknownHostException e)
            {
                logger.info(
                        "Exception when trying to get the host name from the environment, reason="
                                + e);
            }
        }
        return hostName;
    }

    static void printHelp()
    {
        println("Tungsten Replicator Manager");
        println("Syntax:  [java " + ReplicationServiceManager.class.getName()
                + " \\");
        println("             [global-options]");
        println("Global Options:");
        println("\t-clear      Clear dynamic properties and start from defaults only");
        println("\t-offline    Start replicator and leave all services offline");
        println("\t-help       Print help");
    }

    // Print a message to stdout.
    private static void println(String msg)
    {
        System.out.println(msg);
    }

    /**
     * Exit the process with as much clean-up as we can manage.
     * 
     * @param message
     */
    private void exitProcess(boolean ok, String message)
    {
        // Remove the PID file if it exists. Failures in this code may not
        // block the final exit call.
        logger.info(message);
        try
        {
            // Beware--no [back-]slashes allowed. Otherwise we'll fail on
            // some platforms. Use 'new File()' to concatenate file names.
            File replicatorHome = ReplicatorRuntimeConf
                    .locateReplicatorHomeDir();
            File varDir = new File(replicatorHome, "var");
            File pidFile = new File(varDir, "trep.pid");
            if (pidFile.exists())
            {
                logger.info("Removing PID file");
                pidFile.delete();
            }
        }
        catch (Throwable t)
        {
            logger.warn("Unable to complete logic to remove PID file", t);
        }

        // Exit the process.
        logger.info("Exiting process");
        if (ok)
            System.exit(0);
        else
            System.exit(1);
    }

    private void loadServiceConfigurations() throws ReplicatorException
    {
        File confDir = ReplicatorRuntimeConf.locateReplicatorConfDir();

        FilenameFilter serviceConfigFilter = new FilenameFilter()
        {
            public boolean accept(File dir, String name)
            {
                return name.startsWith(CONFIG_FILE_PREFIX)
                        && name.endsWith(CONFIG_FILE_SUFFIX);
            }
        };

        serviceConfigurations.clear();

        // Make sure we have a list of files, sorted by name
        Map<String, File> serviceConfFiles = new TreeMap<String, File>();
        File[] fileArray = confDir.listFiles(serviceConfigFilter);
        for (File configFile : fileArray)
        {
            serviceConfFiles.put(configFile.getName(), configFile);
        }

        for (File serviceConf : serviceConfFiles.values())
        {
            // Name starts in the form static-<host>.<service>.properties
            String serviceConfName = serviceConf.getName();

            // get <host>.<service>.properties
            serviceConfName = serviceConfName
                    .substring(serviceConfName.indexOf(CONFIG_FILE_PREFIX)
                            + CONFIG_FILE_PREFIX.length());

            // get <host>.<service>
            String baseFileName = serviceConfName.substring(0,
                    serviceConfName.indexOf(CONFIG_FILE_SUFFIX));

            // This should just be the service name.
            String serviceName = baseFileName
                    .substring(baseFileName.lastIndexOf(".") + 1);

            TungstenProperties replProps = OpenReplicatorManager
                    .getConfigurationProperties(serviceName);

            serviceConfigurations.put(serviceName, replProps);
        }

    }

    /**
     * Returns the maxPort value.
     * 
     * @return Returns the maxPort.
     */
    public int getMaxPort()
    {
        return masterListenPortMax;
    }

    /**
     * Sets the maximum listen port value for the master.
     * 
     * @param maxPort maximum port allowed
     */
    public void setMaxPort(int maxPort)
    {
        this.masterListenPortMax = maxPort;
    }

    /**
     * Utility function to recursively remove a directory hierarchy and all
     * files in it. This function tracks what it does by putting entries in the
     * 'progress' map passed in.
     * 
     * @param directory - directory to start at
     * @param progress - initialized map to be used to track progress.
     */
    private boolean removeDirectory(File directory,
            Map<String, String> progress)
    {
        if (directory == null)
            return false;
        if (!directory.exists())
            return true;
        if (!directory.isDirectory())
            return false;

        String[] list = directory.list();

        if (list != null)
        {
            for (int i = 0; i < list.length; i++)
            {
                File entry = new File(directory, list[i]);

                if (entry.isDirectory())
                {
                    if (!removeDirectory(entry, progress))
                        return false;
                }
                else
                {
                    progress.put("delete file", entry.getName());
                    if (!entry.delete())
                        return false;
                }
            }
        }

        progress.put("delete directory", directory.getName());
        return directory.delete();
    }
}
