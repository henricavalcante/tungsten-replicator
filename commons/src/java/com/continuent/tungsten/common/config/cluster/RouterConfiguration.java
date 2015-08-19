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
 * Contributor(s): Robert Hodges, Edward Archibald, Gilles Rayrat, Ludovic Launer
 */

package com.continuent.tungsten.common.config.cluster;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.cluster.resource.ResourceType;
import com.continuent.tungsten.common.config.TungstenProperties;

/**
 * The SQL-SQLRouter can be configured either by properties stored in a set of
 * properties files, appropriately organized in a directory tree, or by a single
 * XML-based configuration file of the type managed by the Tungsten Manager. In
 * any case, either set of configuration information reduces to a set of
 * properties. Routers manage connections to a <cluster> and a <cluster> is
 * composed of a set of <datasource> which, in turn, have a set of properties of
 * their own. The directory structure is: resourceHome
 * <dataSourcename>.properties <dataSourcename>.properties
 */
public class RouterConfiguration extends ClusterConfiguration
        implements
            Cloneable
{
    private static Logger     logger                                         = Logger.getLogger(RouterConfiguration.class);
    /**
     *
     */
    @SuppressWarnings("unused")
    private static final long serialVersionUID                               = 1L;

    /**
     * RMI service name
     */
    private String            serviceName                                    = ConfigurationConstants.TR_SERVICE_NAME;
    /**
     * RMI port
     */
    private int               port                                           = new Integer(
                                                                                     ConfigurationConstants.TR_RMI_PORT_DEFAULT)
                                                                                     .intValue();
    /**
     * RMI host
     */
    private String            host                                           = ConfigurationConstants.TR_RMI_DEFAULT_HOST;

    /**
     * Indicates whether or not to enable the router on startup or to startup in
     * the disabled state.
     */
    private boolean           autoEnable                                     = true;
    /**
     * Indicates whether or not to wait for active connections to disconnect
     * before disabling.
     */
    private boolean           waitForDisconnect                              = true;
    /**
     * Indicates the amount of time to wait for all connections to finish before
     * going out and forcibly closing them.
     */
    private int               waitForDisconnectTimeout                       = 0;

    /**
     * Indicates whether or not we'll wait for a particular type of resource to
     * become available if it is not already.
     */
    private boolean           waitIfUnavailable                              = true;

    /**
     * The amount of time to wait, if any, for a particular type of resource to
     * become available before throwing an exception.
     */
    private int               waitIfUnavailableTimeout                       = 0;

    private boolean           waitIfDisabled                                 = true;

    private int               waitIfDisabledTimeout                          = 0;

    /**
     * The cluster member where the router is running.
     */
    private String            clusterMemberName;
//    /**
//     * If the property is non-null, this is a class that will be loaded tonull;
//     * listen for router notifications
//     */
//    private String            routerListenerClass                            = "com.continuent.tungsten.common.patterns.notification.adaptor.ResourceNotificationListenerStub";

    private int               notifyPort                                     = 10121;
    private String            notifierMonitorClass                           = "com.continuent.tungsten.common.patterns.notification.adaptor.ResourceNotifierStub";
    private String            dataSourceLoadBalancer_RO_RELAXED              = "com.continuent.tungsten.router.resource.loadbalancer.RoundRobinSlaveLoadBalancer";

    private boolean           rrIncludeMaster                                = false;

    /** Router Gateway manager list */
    private List<String>      managerList                                    = Arrays.asList("localhost");
    /** Router gateway listen port */
    private int               routerGatewayPort                              = Integer
                                                                                     .parseInt(ConfigurationConstants.TR_GW_PORT_DEFAULT);
    private String            c3p0JMXUrl                                     = "service:jmx:rmi:///jndi/rmi://localhost:3100/jmxrmi";
    /** When disconnected from managers, time to wait before going offline */
    private int               delayBeforeOfflineIfNoManager                  = ConfigurationConstants.DELAY_BEFORE_OFFLINE_IF_NO_MANAGER_DEFAULT;
    /**
     * When disconnected from managers AND in maintenance mode, time to wait
     * before going offline
     */
    private int               delayBeforeOfflineInMaintenanceModeIfNoManager = ConfigurationConstants.DELAY_BEFORE_OFFLINE_IN_MAINTENANCE_MODE_IF_NO_MANAGER_DEFAULT;
    /**
     * Delay after which a manager connection is considered broken if no
     * keep-alive command was received. Make sure manager has
     * "manager.notifications.send" set to true and frequency is higher than
     * this value
     */
    private int               keepAliveTimeout                               = ConfigurationConstants.KEEP_ALIVE_TIMEOUT_DEFAULT;

    /**
     * When connecting to a manager, how long to wait for the connection to
     * succeed before trying the next manager in line. Default 5s, must be
     * positive and max 30s
     */
    private int               gatewayConnectTimeoutMs                        = ConfigurationConstants.GATEWAY_CONNECT_TIMEOUT_MS_DEFAULT;

    private boolean           showRelativeLatency                            = false;

    private int               routerClientThreadsPerService                  = 1;

    private int               gatewayLocalBindStartingPort                   = 45847;

    /**
     * When reading manager commands in maintenance mode, the router will retry
     * a few times upon failure. This controls when to give up
     */
    private long              readCommandRetryTimeoutMs                      = ConfigurationConstants.READ_COMMAND_RETRY_TIMEOUT_MS_DEFAULT;

    public RouterConfiguration(String clusterName)
            throws ConfigurationException
    {
        super(clusterName);

        // set up the default service values
        setPort(new Integer(ConfigurationConstants.TR_RMI_PORT_DEFAULT));
        setHost("localhost");
        setServiceName(ConfigurationConstants.TR_SERVICE_NAME);
    }

    /**
     * Loads a router configuration from disk.
     * 
     * @return a fully initialized router configuration
     * @throws ConfigurationException
     */
    public RouterConfiguration load() throws ConfigurationException
    {
        load(ConfigurationConstants.TR_PROPERTIES);
        
        // TUC-1750 : managerList router properties is no longer in router.properties
        // Get the value from dataservices.properties
        DataServicesConfiguration d = DataServicesConfiguration.getInstance();
        String managerList = d.getProps().get(props.get(ConfigurationConstants.CLUSTER_CLUSTERNAME));
        props.put(ConfigurationConstants.CLUSTER_MANAGER_LIST, managerList);
        if (managerList==null)
            logger.warn((MessageFormat.format("Could not retrieve a value for {0} by reading {1}", ConfigurationConstants.CLUSTER_MANAGER_LIST, ConfigurationConstants.TR_PROPERTIES)));
        
        props.applyProperties(this, true);
        loadClusterDataSourceMap();
        return this;
    }

    /**
     * Loads data cluster configurations from disk.
     * 
     * @throws ConfigurationException
     */
    public synchronized Map<String, Map<String, TungstenProperties>> loadClusterDataSourceMap()
            throws ConfigurationException
    {
        return loadClusterConfiguration(ResourceType.DATASOURCE);
    }

    /**
     * Returns all of the datasource configurations for a given cluster.
     * 
     * @param clusterName
     * @return a map of TungstenProperties representing data sources for the
     *         cluster
     */
    public Map<String, TungstenProperties> getDataSourceMap(String clusterName)
    {
        Map<String, TungstenProperties> dsMap = new TreeMap<String, TungstenProperties>();

        Map<String, Map<String, TungstenProperties>> clusterDataSourceMap;

        try
        {
            if ((clusterDataSourceMap = loadClusterDataSourceMap()) != null)
            {
                Map<String, TungstenProperties> foundMap = clusterDataSourceMap
                        .get(clusterName);

                if (foundMap != null)
                {
                    // Do some quick validation to make sure that
                    // the datasource is well formed.
                    for (TungstenProperties dsProps : foundMap.values())
                    {
                        if (isValidDs(dsProps))
                        {
                            dsMap.put(dsProps.getString("name"), dsProps);
                        }
                    }

                    dsMap.putAll(foundMap);
                }
            }
        }
        catch (Exception e)
        {
            logger.error("Problem loading the datasource configuration", e);
        }

        return dsMap;
    }

    private boolean isValidDs(TungstenProperties dsToCheck)
    {
        String[] requiredProps = {"name", "vendor", "clusterName", "host",
                "driver", "url", "role", "precedence"};

        for (String prop : requiredProps)
        {
            if (dsToCheck.getString(prop) == null)
                return false;
        }

        return true;
    }

    /**
     * Returns an existing datasource configuration, if there is one, for the
     * named dataService.
     * 
     * @param clusterName
     * @param dsName
     * @return a TungstenProperties instances representing a data source
     * @throws ConfigurationException
     */
    public TungstenProperties getDataSource(String clusterName, String dsName)
            throws ConfigurationException
    {
        TungstenProperties foundDs = getDataSourceMap(clusterName).get(dsName);

        if (foundDs == null)
        {
            throw new ConfigurationException(String.format(
                    "datasource '%s' was not found in cluster '%s'", dsName,
                    clusterName));
        }

        return foundDs;
    }

    /**
     * Writes out the configuration for all datasources in the map.
     * 
     * @param clusterName
     * @param dataSourceMap
     * @throws ConfigurationException
     */
    public synchronized void storeDataSourceConfig(String clusterName,
            Map<String, TungstenProperties> dataSourceMap)
            throws ConfigurationException
    {

        if (logger.isDebugEnabled())
        {
            logger.debug(String.format(
                    "Storing the data source configuration for service='%s'",
                    clusterName));
        }
        for (TungstenProperties ds : dataSourceMap.values())
        {
            if (logger.isDebugEnabled())
            {
                logger.debug(String.format("Storing the data source '%s@%s'",
                        ds.getString("name"), clusterName));
            }
            storeDataSourceConfig(clusterName, ds);
        }
    }

    /**
     * Writes out a single datasource configuration
     * 
     * @param clusterName
     * @param ds
     * @throws ConfigurationException
     */
    public synchronized void storeDataSourceConfig(String clusterName,
            TungstenProperties ds) throws ConfigurationException
    {
        storeResourceConfig(clusterName, ResourceType.DATASOURCE, ds);
    }

    /**
     * @return the autoEnable
     */
    public boolean isAutoEnable()
    {
        return autoEnable;
    }

    /**
     * @param autoEnable the autoEnable to set
     */
    public void setAutoEnable(boolean autoEnable)
    {
        this.autoEnable = autoEnable;
        this.props.setBoolean("autoEnable", autoEnable);
    }

    /**
     * @return the waitForDisconnect
     */
    public boolean isWaitForDisconnect()
    {
        return waitForDisconnect;
    }

    /**
     * @param waitForDisconnect the waitForDisconnect to set
     */
    public void setWaitForDisconnect(boolean waitForDisconnect)
    {
        this.waitForDisconnect = waitForDisconnect;
        this.props.setBoolean("waitForDisconnect", waitForDisconnect);
    }

    /**
     * @return the waitForDisconnectTimeout
     */
    public int getWaitForDisconnectTimeout()
    {
        return waitForDisconnectTimeout;
    }

    /**
     * @param waitForDisconnectTimeout the waitForDisconnectTimeout to set
     */
    public void setWaitForDisconnectTimeout(int waitForDisconnectTimeout)
    {
        this.waitForDisconnectTimeout = waitForDisconnectTimeout;
        this.props.setInt("waitForDisconnectTimeout", waitForDisconnectTimeout);
    }

    /**
     * @return the dataSourceMap
     */
    public Map<String, Map<String, TungstenProperties>> getDataServicesMap()
            throws ConfigurationException
    {
        return loadClusterDataSourceMap();
    }

    /**
     * Returns the waitIfUnavailable value.
     * 
     * @return Returns the waitIfUnavailable.
     */
    public boolean getWaitIfUnavailable()
    {
        return waitIfUnavailable;
    }

    /**
     * Sets the waitIfUnavailable value.
     * 
     * @param waitIfUnavailable The waitIfUnavailable to set.
     */
    public void setWaitIfUnavailable(boolean waitIfUnavailable)
    {
        this.waitIfUnavailable = waitIfUnavailable;
        this.props.setBoolean("waitIfUnavailable", waitIfUnavailable);
    }

    /**
     * Returns the waitIfUnavailableTimeout value.
     * 
     * @return Returns the waitIfUnavailableTimeout.
     */
    public int getWaitIfUnavailableTimeout()
    {
        return waitIfUnavailableTimeout;
    }

    public boolean getShowRelativeLatency()
    {
        return showRelativeLatency;
    }

    /**
     * Sets the waitIfUnavailableTimeout value.
     * 
     * @param waitIfUnavailableTimeout The waitIfUnavailableTimeout to set.
     */
    public void setWaitIfUnavailableTimeout(int waitIfUnavailableTimeout)
    {
        this.waitIfUnavailableTimeout = waitIfUnavailableTimeout;
        this.props.setInt("waitIfUnavailableTimeout", waitIfUnavailableTimeout);
    }

//    /**
//     * Returns the routerListenerClass value.
//     * 
//     * @return Returns the routerListenerClass.
//     */
//    public String getRouterListenerClass()
//    {
//        return routerListenerClass;
//    }

//    /**
//     * Sets the routerListenerClass value.
//     * 
//     * @param routerListenerClass The routerListenerClass to set.
//     */
//    public void setRouterListenerClass(String routerListenerClass)
//    {
//        this.routerListenerClass = routerListenerClass;
//    }

    public boolean isWaitIfDisabled()
    {
        return waitIfDisabled;
    }

    public void setWaitIfDisabled(boolean waitIfDisabled)
    {
        this.waitIfDisabled = waitIfDisabled;
        this.props.setBoolean("waitIfDisabled", waitIfDisabled);
    }

    public int getWaitIfDisabledTimeout()
    {
        return waitIfDisabledTimeout;
    }

    public void setWaitIfDisabledTimeout(int waitIfDisabledTimeout)
    {
        this.waitIfDisabledTimeout = waitIfDisabledTimeout;
        this.props.setInt("waitIfDisabledTimeout", waitIfDisabledTimeout);
    }

    public void setShowRelativeLatency(boolean showRelativeLatency)
    {
        this.showRelativeLatency = showRelativeLatency;
        this.props.setBoolean("showRelativeLatency", showRelativeLatency);
    }

    public String getServiceName()
    {
        return serviceName;
    }

    public void setServiceName(String serviceName)
    {
        this.serviceName = serviceName;
    }

    public int getPort()
    {
        return port;
    }

    public void setPort(int port)
    {
        this.port = port;
    }

    public String getHost()
    {
        return host;
    }

    public void setHost(String host)
    {
        this.host = host;
    }

    public int getNotifyPort()
    {
        return notifyPort;
    }

    public void setNotifyPort(int notifyPort)
    {
        this.notifyPort = notifyPort;
    }

    public String getNotifierMonitorClass()
    {
        return notifierMonitorClass;
    }

    public void setNotifierMonitorClass(String notifierMonitorClass)
    {
        this.notifierMonitorClass = notifierMonitorClass;
    }

    public String getDataSourceLoadBalancer_RO_RELAXED()
    {
        return dataSourceLoadBalancer_RO_RELAXED;
    }

    public void setDataSourceLoadBalancer_RO_RELAXED(
            String dataSourceLoadBalancer_RO_RELAXED)
    {
        this.dataSourceLoadBalancer_RO_RELAXED = dataSourceLoadBalancer_RO_RELAXED;
    }

    public String getClusterMemberName()
    {
        return clusterMemberName;
    }

    public void setClusterMemberName(String clusterMemberName)
    {
        this.clusterMemberName = clusterMemberName;
    }

    public boolean isRrIncludeMaster()
    {
        return rrIncludeMaster;
    }

    public void setRrIncludeMaster(boolean rrIncludeMaster)
    {
        this.rrIncludeMaster = rrIncludeMaster;
    }

    public void setManagerList(List<String> list)
    {
        this.managerList = list;
    }

    public List<String> getManagerList()
    {
        return managerList;
    }

    public int getRouterGatewayPort()
    {
        return routerGatewayPort;
    }

    public void setRouterGatewayPort(int port)
    {
        this.routerGatewayPort = port;
    }

    public String getC3p0JmxUrl()
    {
        return c3p0JMXUrl;
    }

    public void setC3p0JMXUrl(String url)
    {
        c3p0JMXUrl = url;
    }

    public int getDelayBeforeOfflineIfNoManager()
    {
        return delayBeforeOfflineIfNoManager;
    }

    public void setDelayBeforeOfflineIfNoManager(
            int delayBeforeOfflineIfNoManager)
    {
        this.delayBeforeOfflineIfNoManager = delayBeforeOfflineIfNoManager;
    }

    public int getDelayBeforeOfflineInMaintenanceModeIfNoManager()
    {
        return delayBeforeOfflineInMaintenanceModeIfNoManager;
    }

    public void setDelayBeforeOfflineInMaintenanceModeIfNoManager(int delayInS)
    {
        delayBeforeOfflineInMaintenanceModeIfNoManager = delayInS;
    }

    public int getKeepAliveTimeout()
    {
        return keepAliveTimeout;
    }

    public void setKeepAliveTimeout(int timeoutInMs)
    {
        keepAliveTimeout = timeoutInMs;
    }

    public void setGatewayConnectTimeoutMs(int timeoutMs)
    {
        gatewayConnectTimeoutMs = timeoutMs;
    }

    public int getGatewayConnectTimeoutMs()
    {
        return gatewayConnectTimeoutMs;
    }

    public void setGatewayLocalBindStartingPort(int port)
    {
        if (port < 1024)
        {
            port = 1024;
        }
        gatewayLocalBindStartingPort = port;
    }

    public int getGatewayLocalBindStartingPort()
    {
        return gatewayLocalBindStartingPort;
    }

    @Override
    public Object clone()
    {
        Object o = null;
        try
        {
            o = super.clone();
        }
        catch (CloneNotSupportedException cnse)
        {
            // Should never happen
            logger.fatal("Unable to clone this RouterConfiguration", cnse);
        }
        return o;
    }

    /**
     * Returns the routerClientThreadsPerService value.
     * 
     * @return Returns the routerClientThreadsPerService.
     */
    public int getRouterClientThreadsPerService()
    {
        return routerClientThreadsPerService;
    }

    /**
     * Sets the routerClientThreadsPerService value.
     * 
     * @param routerClientThreadsPerService The routerClientThreadsPerService to
     *            set.
     */
    public void setRouterClientThreadsPerService(
            int routerClientThreadsPerService)
    {
        this.routerClientThreadsPerService = routerClientThreadsPerService;
    }

    /**
     * Checks critical configuration values and throws and exception if invalid
     * settings are found. TUC-1738.
     * 
     * @throws ConfigurationException upon first invalid configuration value
     */
    public void validateConfigurationValues() throws ConfigurationException
    {
        if (getKeepAliveTimeout() <= 0
                || getKeepAliveTimeout() > ConfigurationConstants.KEEP_ALIVE_TIMEOUT_MAX)
        {
            throw new ConfigurationException(
                    "Detected invalid keepAliveTimeout of "
                            + getKeepAliveTimeout()
                            + "ms in router.properties. keepAliveTimeout must be positive and lower than "
                            + ConfigurationConstants.KEEP_ALIVE_TIMEOUT_MAX
                            + " ("
                            + ConfigurationConstants.KEEP_ALIVE_TIMEOUT_MAX
                            / 60000 + "min).");

        }
        if (getDelayBeforeOfflineIfNoManager() <= 0
                || getDelayBeforeOfflineIfNoManager() > ConfigurationConstants.DELAY_BEFORE_OFFLINE_IF_NO_MANAGER_MAX)
        {
            throw new ConfigurationException(
                    "Detected invalid delayBeforeOfflineIfNoManager of "
                            + getDelayBeforeOfflineIfNoManager()
                            + "ms in router.properties. It must be positive and lower than "
                            + ConfigurationConstants.DELAY_BEFORE_OFFLINE_IF_NO_MANAGER_MAX
                            + " ("
                            + ConfigurationConstants.DELAY_BEFORE_OFFLINE_IF_NO_MANAGER_MAX
                            / 60 + "min).");
        }
        if (getGatewayConnectTimeoutMs() <= 0
                || getGatewayConnectTimeoutMs() >= ConfigurationConstants.GATEWAY_CONNECT_TIMEOUT_MS_MAX)
        {
            throw new ConfigurationException(
                    "Detected invalid gatewayConnectTimeout of "
                            + getGatewayConnectTimeoutMs()
                            + "ms in router.properties. It must be positive and lower than "
                            + ConfigurationConstants.GATEWAY_CONNECT_TIMEOUT_MS_MAX
                            + "ms.");
        }

    }

    public void setReadCommandRetryTimeoutMs(long timeout)
    {
        readCommandRetryTimeoutMs = timeout;
    }

    public long getReadCommandRetryTimeoutMs()
    {
        return readCommandRetryTimeoutMs;
    }
}
