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
 * Initial developer(s): Edward Archibald
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.common.config.cluster;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.cluster.resource.ResourceType;
import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.manager.router.gateway.RouterGatewayConstants;

public class ClusterConfiguration
{

    /**
     * Logger
     */
    private static Logger     logger               = Logger.getLogger(ClusterConfiguration.class);

    public static String      clusterHomeName      = null;

    private String            clusterName;

    /**
     * The source of the properties for this configuration. getClusterHome
     */
    public TungstenProperties props                = null;

    private File              clusterConfigDir     = null;
    private File              clusterConfigRootDir = null;

    private static String     configFileNameInUse  = null;

    public ClusterConfiguration(String clusterName)
    {
        this.clusterName = clusterName;
    }

    /**
     * Creates a new <code>ClusterConfiguration</code> object
     * 
     * @param configFileName
     * @throws ConfigurationException
     */
    public ClusterConfiguration(String clusterName, String configFileName)
            throws ConfigurationException
    {
        this(clusterName);
        load(configFileName);
    }

    /**
     * Loads a set of resource configurations from the appropriate directory
     * according to Tungsten resource conventions. The cluster directory
     * hierarchy looks like this:
     * {clusterHome}/cluster/{clusterName}/{resourceType}
     * 
     * @param resourceType
     * @throws ConfigurationException
     */
    public synchronized Map<String, Map<String, TungstenProperties>> loadClusterConfiguration(
            ResourceType resourceType) throws ConfigurationException
    {
        if (getClusterHome() == null)
        {
            throw new ConfigurationException(
                    "No cluster.home found from which to configure cluster resources.");
        }

        Map<String, Map<String, TungstenProperties>> clusterConfigurations = new HashMap<String, Map<String, TungstenProperties>>();

        File cluster = getDir(getClusterConfigRootDirName(getClusterHome()));

        for (File foundFile : cluster.listFiles())
        {
            if (foundFile.isDirectory())
            {
                Map<String, TungstenProperties> clusterConfig = loadConfiguration(
                        foundFile.getName(), resourceType);
                clusterConfigurations.put(foundFile.getName(), clusterConfig);
            }
        }

        return clusterConfigurations;

    }

    /**
     * Returns configurations for a set of resources of a given resourceType for
     * a given clusterName.
     * 
     * @param clusterName
     * @param resourceType
     * @throws ConfigurationException
     */
    public synchronized Map<String, TungstenProperties> loadConfiguration(
            String clusterName, ResourceType resourceType)
            throws ConfigurationException
    {

        if (getClusterHome() == null)
        {
            throw new ConfigurationException(
                    "No cluster.home found from which to configure cluster resources.");
        }

        File resources = getDir(getResourceConfigDirName(getClusterHome(),
                clusterName, resourceType));

        FilenameFilter propFilter = new FilenameFilter()
        {
            public boolean accept(File dir, String name)
            {
                return name.endsWith(".properties");
            }
        };

        Map<String, TungstenProperties> resourceMap = new HashMap<String, TungstenProperties>();

        // Load resource information
        try
        {

            for (File resourceConf : resources.listFiles(propFilter))
            {
                TungstenProperties resourceProps = null;
                RandomAccessFile resourceFile = null;
                InputStream byteStream = null;

                try
                {
                    resourceFile = new RandomAccessFile(
                            resourceConf.getAbsolutePath(), "r");

                    resourceFile.seek(0);

                    byte[] bytes = new byte[(int) resourceConf.length()];

                    resourceFile.read(bytes);
                    byteStream = new ByteArrayInputStream(bytes);
                    resourceFile.close();
                    resourceFile = null;

                    resourceProps = new TungstenProperties();

                    resourceProps.load(byteStream);
                    byteStream.close();
                    byteStream = null;

                }
                catch (IOException i)
                {
                    throw new ConfigurationException(String.format(
                            "Unable to load resource %s\n%s",
                            resourceConf.getAbsolutePath(), i.getMessage()));
                }
                finally
                {
                    if (resourceFile != null)
                    {
                        resourceFile.close();
                        resourceFile = null;
                    }

                    if (byteStream != null)
                    {
                        byteStream.close();
                        byteStream = null;
                    }

                }

                if (resourceProps.getString("name") == null)
                {
                    throw new ConfigurationException(String.format(
                            "The file %s appears to be corrupt or empty",
                            resourceConf.getPath()));
                }
                resourceMap.put(resourceProps.getString("name"), resourceProps);
            }
        }
        catch (FileNotFoundException f)
        {
            throw new ConfigurationException(
                    "Unable to process a file when configuring resources:" + f);
        }
        catch (IOException i)
        {
            throw new ConfigurationException(
                    "Error while loading datastore properties:" + i);
        }

        return resourceMap;

    }

    /**
     * Store a properties file as a resource configuration using the resource
     * configuration standards for Tungsten
     * 
     * @param resourceType
     * @param resourceProps
     * @throws ConfigurationException
     */
    public synchronized void storeResourceConfig(String clusterName,
            ResourceType resourceType, TungstenProperties resourceProps)
            throws ConfigurationException
    {
        if (getClusterHome() == null)
        {
            throw new ConfigurationException(
                    "The 'clusterHome' property was not found in the configuration file:"
                            + getModulePropertiesFileName(
                                    ConfigurationConstants.TR_PROPERTIES,
                                    getClusterHome()));
        }

        String resourceDir = getResourceConfigDirName(getClusterHome(),
                clusterName, resourceType);

        File resources = new File(resourceDir);

        if (!resources.isDirectory())
        {
            if (resources.mkdirs())
            {
                logger.info(String
                        .format("Created directory '%s'", resourceDir));
            }
            else
            {
                String msg = String
                        .format("The path indicated by the name %s must be a directory.",
                                getResourceConfigDirName(getClusterHome(),
                                        clusterName, resourceType));
                logger.error(msg);

                throw new ConfigurationException(msg);
            }
        }

        String outFileName = resources.getAbsolutePath() + File.separator
                + resourceProps.getString("name") + ".properties";

        store(resourceProps, outFileName);

    }

    /**
     * Store a list of resources in individual properties files
     * 
     * @param resourceType
     * @param resourceList
     * @throws ConfigurationException
     */
    public synchronized void storeResourcesConfig(String clusterName,
            ResourceType resourceType,
            Map<String, TungstenProperties> resourceList)
            throws ConfigurationException
    {
        if (getClusterHome() == null)
        {
            throw new ConfigurationException(
                    "No cluster.home found from which to configure cluster resources.");
        }

        for (TungstenProperties props : resourceList.values())
        {
            storeResourceConfig(clusterName, resourceType, props);
        }
    }

    /**
     * Delete a specific resource configuration.
     * 
     * @param clusterName
     * @param resourceType
     * @param dsName
     * @throws ConfigurationException
     */
    public void deleteResourceConfig(String clusterName,
            ResourceType resourceType, String dsName)
            throws ConfigurationException
    {
        if (getClusterHome() == null)
        {
            throw new ConfigurationException(
                    "No home directory found from which to configure resources.");
        }

        File resources = getDir(getResourceConfigDirName(getClusterHome(),
                clusterName, resourceType));

        String delFileName = resources.getAbsolutePath() + File.separator
                + dsName + ".properties";

        delFile(delFileName);

    }

    /**
     * Return the full pathname of a resource directory for a given cluster.
     * 
     * @param clusterName
     * @param resourceType
     */
    public static String getResourceConfigDirName(String clusterHome,
            String clusterName, ResourceType resourceType)
    {

        return getClusterConfigRootDirName(clusterHome) + File.separator
                + clusterName + File.separator
                + resourceType.toString().toLowerCase();
    }

    public static String getClusterConfigDirName(String clusterHome,
            String clusterName)
    {
        return getGlobalConfigDirName(clusterHome) + File.separator
                + ConfigurationConstants.CLUSTER_DIR + File.separator
                + clusterName;
    }

    public static String getGlobalConfigDirName(String clusterHome)
    {
        return clusterHome + File.separator
                + ConfigurationConstants.CLUSTER_CONF_DIR;
    }

    public static String getClusterConfigRootDirName(String clusterHome)
    {
        return clusterHome + File.separator
                + ConfigurationConstants.CLUSTER_CONF_DIR + File.separator
                + ConfigurationConstants.CLUSTER_DIR;
    }

    /**
     * Determines the filename of a given module properties file by either
     * getting the path from the system variable named with the same label as
     * the properties file (eg. $router.properties) or by getting it from the
     * default location in cluster-home/conf
     * 
     * @param moduleProps the module properties file name
     * @param clusterHome location of cluster home
     */
    public static String getModulePropertiesFileName(String moduleProps,
            String clusterHome)
    {
        String moduleProperties = System.getProperty(moduleProps);
        if (moduleProperties == null)
        {
            logger.debug("Seeking " + moduleProps + " using cluster.home");
            return getGlobalConfigDirName(clusterHome) + File.separator
                    + moduleProps;
        }
        else
        {
            logger.debug("Seeking " + moduleProps + " using " + moduleProps);
            return moduleProperties;
        }
    }

    public void createDefaultConfiguration(String clusterName)
            throws ConfigurationException
    {
        if (getClusterHome() == null)
        {
            throw new ConfigurationException(
                    "No cluster.home found from which to configure cluster resources.");
        }

        createClusterConfigRootDirs();
        createConfigDirs(clusterName);
        createRouterConfiguration(null);
        createPolicyManagerConfiguration(clusterName);
    }

    /**
     * Creates the directory hierarchy for the root of a cluster configuration
     * 
     * @throws ConfigurationException
     */
    public void createClusterConfigRootDirs() throws ConfigurationException
    {

        clusterConfigDir = new File(
                ClusterConfiguration
                        .getClusterConfigRootDirName(clusterHomeName));
        clusterConfigRootDir = new File(
                getClusterConfigRootDirName(clusterHomeName));

        // Ensure the generic 'cluster' exists.
        if (!clusterConfigDir.exists())
        {
            logger.debug("Creating new 'cluster' directory: "
                    + clusterConfigDir.getAbsolutePath());
            clusterConfigDir.mkdirs();
        }

        if (!clusterConfigDir.isDirectory() || !clusterConfigDir.canWrite())
        {
            throw new ConfigurationException(
                    "'cluster' directory invalid or unreadable: "
                            + clusterConfigDir.getAbsolutePath());
        }

        // Ensure the root level 'cluster' directory exists.
        if (!clusterConfigRootDir.exists())
        {
            logger.debug("Creating new cluster configuration directory: "
                    + clusterConfigRootDir.getAbsolutePath());
            clusterConfigRootDir.mkdirs();
        }

        if (!clusterConfigRootDir.isDirectory()
                || !clusterConfigRootDir.canWrite())
        {
            throw new ConfigurationException(
                    "cluster configuration directory invalid or unreadable: "
                            + clusterConfigRootDir.getAbsolutePath());
        }

    }

    public void createConfigDirs(String clusterName)
            throws ConfigurationException
    {
        createClusterConfigRootDirs();

        File clusterConfigDir = new File(clusterConfigRootDir, clusterName);
        File dataSourceConfigDir = new File(clusterConfigDir,
                ResourceType.DATASOURCE.toString().toLowerCase());
        File serviceConfigDir = new File(clusterConfigDir, "service");
        File extensionConfigDir = new File(clusterConfigDir, "extension");

        // Ensure the datasource directory exists.
        if (!dataSourceConfigDir.exists())
        {
            logger.debug("Creating new datasource directory: "
                    + dataSourceConfigDir.getAbsolutePath());
            dataSourceConfigDir.mkdirs();
        }
        if (!dataSourceConfigDir.isDirectory()
                || !dataSourceConfigDir.canWrite())
        {
            throw new ConfigurationException(
                    "DataSource config directory invalid or unreadable: "
                            + dataSourceConfigDir.getAbsolutePath());
        }
        // Ensure the datasource directory exists.
        if (!serviceConfigDir.exists())
        {
            logger.debug("Creating new service directory: "
                    + serviceConfigDir.getAbsolutePath());
            serviceConfigDir.mkdirs();
        }
        // Ensure the datasource directory exists.
        if (!extensionConfigDir.exists())
        {
            logger.debug("Creating new extension directory: "
                    + extensionConfigDir.getAbsolutePath());
            extensionConfigDir.mkdirs();
        }
    }

    /**
     * Creates a new router configuration file in the correct location.
     * 
     * @param clusterName
     * @throws ConfigurationException
     */
    public void createRouterConfiguration(String clusterName)
            throws ConfigurationException
    {
        String routerConfigFileName = getModulePropertiesFileName(
                ConfigurationConstants.TR_PROPERTIES, getClusterHome());
        File routerConfigFile = new File(routerConfigFileName);

        // Only create the file if it doesn't already exist.
        if (routerConfigFile.exists())
        {
            logger.debug(String.format(
                    "SQLRouter configuration already exists at '%s'",
                    routerConfigFileName));
            return;
        }

        RouterConfiguration config = new RouterConfiguration(null);
        config.setClusterHome(getClusterHome());
        config.setHost(ConfigurationConstants.TR_RMI_DEFAULT_HOST);

        ArrayList<String> al = new ArrayList<String>();
        al.add("localhost:9998");
        config.setManagerList(al);

        TungstenProperties configProps = new TungstenProperties();
        configProps.extractProperties(config, true);

        logger.debug("Writing out a router configuration to '"
                + routerConfigFileName + "'");
        logger.debug("router.properties contains:" + configProps);
        config.store(configProps, routerConfigFileName);

    }

    /**
     * Creates a default policy manager configuration in the correct location
     * 
     * @param clusterName
     * @throws ConfigurationException
     */
    public void createPolicyManagerConfiguration(String clusterName)
            throws ConfigurationException
    {

        String policyMgrConfigFileName = getModulePropertiesFileName(
                ConfigurationConstants.PM_PROPERTIES, getClusterHome());
        File policyMgrConfigFile = new File(policyMgrConfigFileName);

        // Only create the file if it doesn't already exist.
        if (policyMgrConfigFile.exists())
        {
            logger.debug(String.format(
                    "Policy manager configuration already exists at '%s'",
                    policyMgrConfigFileName));
            return;
        }

        ClusterPolicyManagerConfiguration config = new ClusterPolicyManagerConfiguration(
                clusterName);
        config.setClusterHome(getClusterHome());
        config.setHost(ConfigurationConstants.PM_RMI_DEFAULT_HOST);
        TungstenProperties configProps = new TungstenProperties();
        configProps.extractProperties(config, true);

        logger.debug("Writing out a policy manager configuration to '"
                + policyMgrConfigFileName + "'");
        logger.debug("policymgr.properties contains:" + configProps);
        config.store(configProps, policyMgrConfigFileName);

    }

    /**
     * Creates a default data services configuration in the correct location
     * 
     * @param clusterName
     * @throws ConfigurationException
     */
    public void createDataServicesConfiguration(String clusterName)
            throws ConfigurationException
    {
        String dataServicesConfigFileName = getModulePropertiesFileName(
                ConfigurationConstants.TR_SERVICES_PROPS, getClusterHome());
        File dataServicesConfigFile = new File(dataServicesConfigFileName);

        TungstenProperties configProps = new TungstenProperties();
        // Only create the file if it doesn't already exist.
        if (dataServicesConfigFile.exists())
        {
            logger.debug(String.format(
                    "DataServices configuration already exists at '%s'",
                    dataServicesConfigFileName));
            InputStream is = null;
            try
            {
                is = new FileInputStream(dataServicesConfigFile);
                configProps.load(is);
            }
            catch (FileNotFoundException e)
            {
                // not likely to happen, we just checked .exists() above
                throw new ConfigurationException(e.getLocalizedMessage());
            }
            catch (IOException e)
            {
                throw new ConfigurationException("Error while loading "
                        + dataServicesConfigFileName + ": "
                        + e.getLocalizedMessage());
            }
            finally
            {
                if (is != null)
                {
                    try
                    {
                        is.close();
                    }
                    catch (Exception ignored)
                    {
                    }
                }
            }
        }
        if (configProps.getString(clusterName) == null
                || configProps.getString(clusterName).isEmpty())
        {
            configProps.setString(clusterName, "localhost:"
                    + RouterGatewayConstants.DEFAULT_GATEWAY_PORT);
        }

        logger.info("Writing out data services configuration to '"
                + dataServicesConfigFileName + "'");
        logger.info("dataservices.properties contains:" + configProps);
        OutputStream os = null;
        try
        {
            os = new FileOutputStream(dataServicesConfigFile);
            configProps.store(os);
        }
        catch (IOException i)
        {
            throw new ConfigurationException("Error while storing properties:"
                    + i);
        }

    }

    /**
     * Gets a cluster configuration from a file located on the classpath and
     * returns it.
     * 
     * @param configFileName
     * @throws ConfigurationException
     */
    public static TungstenProperties getConfiguration(String configFileName)
            throws ConfigurationException
    {
        TungstenProperties props = new TungstenProperties();
        InputStream is = null;
        File configFile = null;

        // Ensure cluster home name is properly set.
        clusterHomeName = System
                .getProperty(ConfigurationConstants.CLUSTER_HOME);

        if (clusterHomeName == null)
        {
            throw new ConfigurationException(
                    "You must set the system property cluster.home");
        }

        // See if we have a system property corresponding to this name. If so,
        // we use that.
        String configFileNameFromProperty = System.getProperty(configFileName);
        if (configFileNameFromProperty == null)
        {
            logger.debug("Creating configuration file path from cluster.home: "
                    + getGlobalConfigDirName(clusterHomeName));
            configFile = new File(getGlobalConfigDirName(clusterHomeName),
                    configFileName);
        }
        else
        {
            logger.debug("Reading configuration file path from property: "
                    + configFileName);
            configFile = new File(configFileNameFromProperty);
        }
        // Record the full path
        configFileNameInUse = configFile.getAbsolutePath();

        // Ensure file exists and is readable.
        if (logger.isDebugEnabled())
            logger.debug("Loading config file from file: "
                    + configFileNameInUse);
        if (!configFile.exists() || !configFile.canRead())
        {
            throw new ConfigurationException(
                    "Configuration file does not exist or is not readable: "
                            + configFileNameInUse);
        }

        // Read the properties.
        try
        {
            is = new FileInputStream(configFile);
        }
        catch (FileNotFoundException f)
        {
            throw new ConfigurationException(String.format(
                    "Cannot create an input stream for file '%s', reason=%s",
                    configFileNameInUse, f));
        }

        try
        {
            props.load(is);
        }
        catch (IOException e)
        {
            throw new ConfigurationException(
                    "Unable to load configuration file:" + configFileName
                            + ", reason=" + e);
        }
        finally
        {
            try
            {
                is.close();
            }
            catch (IOException e)
            {
            }
        }

        return props;
    }

    /**
     * Loads a cluster configuration from a file located on the classpath and
     * applies the properties found to the current instance.
     * 
     * @param configFileName
     * @throws ConfigurationException
     */
    public void load(String configFileName) throws ConfigurationException
    {
        props = getConfiguration(configFileName);

        if (props != null)
        {
            props.applyProperties(this, true);
        }
    }

    static public void store(String configFileName, TungstenProperties props)
            throws ConfigurationException
    {
        // Ensure cluster home name is properly set.
        clusterHomeName = System
                .getProperty(ConfigurationConstants.CLUSTER_HOME);
        String configFilePath = null;

        if (clusterHomeName == null)
        {
            throw new ConfigurationException(
                    "You must set the system property cluster.home");
        }

        // See if we have a system property corresponding to this name. If so,
        // we use that.

        String configFileNameFromProperty = System.getProperty(configFileName);
        if (configFileNameFromProperty == null)
        {
            logger.info("Creating configuration file path from cluster.home: "
                    + getGlobalConfigDirName(clusterHomeName));
            configFilePath = getGlobalConfigDirName(clusterHomeName)
                    + File.pathSeparator + configFileName;
        }
        else
        {
            logger.info("Getting configuration file path from property: "
                    + configFileName);
            configFilePath = configFileNameFromProperty;
        }

        try
        {
            File checkFile = new File(configFilePath);
            File backupFile = new File(configFilePath + ".bak");

            if (checkFile.exists())
            {
                if (backupFile.exists())
                {
                    backupFile.delete();
                }

                checkFile.renameTo(new File(configFilePath + ".bak"));
            }
            FileOutputStream fout = new FileOutputStream(configFilePath);
            props.store(fout);
            fout.flush();
            fout.getFD().sync();
            fout.close();

        }
        catch (FileNotFoundException f)
        {
            throw new ConfigurationException(String.format(
                    "Unable to process a file when writing %s: %s",
                    configFilePath, f));
        }
        catch (IOException i)
        {
            throw new ConfigurationException(String.format(
                    "Error while storing properties for %s: %s",
                    configFilePath, i));
        }
    }

    static public void delete(String configFileName)
            throws ConfigurationException
    {
        // Ensure cluster home name is properly set.
        clusterHomeName = System
                .getProperty(ConfigurationConstants.CLUSTER_HOME);
        String configFilePath = null;

        if (clusterHomeName == null)
        {
            throw new ConfigurationException(
                    "You must set the system property cluster.home");
        }

        // See if we have a system property corresponding to this name. If so,
        // we use that.

        String configFileNameFromProperty = System.getProperty(configFileName);
        if (configFileNameFromProperty == null)
        {
            logger.info("Creating configuration file path from cluster.home: "
                    + getGlobalConfigDirName(clusterHomeName));
            configFilePath = getGlobalConfigDirName(clusterHomeName)
                    + File.pathSeparator + configFileName;
        }
        else
        {
            logger.info("Getting configuration file path from property: "
                    + configFileName);
            configFilePath = configFileNameFromProperty;
        }

        File checkFile = new File(configFilePath);
        File backupFile = new File(configFilePath + ".bak");

        if (checkFile.exists())
        {
            if (backupFile.exists())
            {
                backupFile.delete();
            }

            checkFile.renameTo(new File(configFilePath + ".bak"));
        }

        delFile(configFilePath);

    }

    /**
     * deletes a specific file
     * 
     * @param delFileName
     * @throws ConfigurationException
     */
    public static void delFile(String delFileName)
            throws ConfigurationException
    {
        File delFile = new File(delFileName);
        if (delFile.exists() && delFile.canWrite())
        {
            delFile.delete();
        }
        else
        {
            throw new ConfigurationException(
                    "Can't delete file because it is not writeable. File="
                            + delFileName);
        }
    }

    /**
     * Validates that a directory exists.
     * 
     * @param dirName
     * @throws ConfigurationException
     */
    public static File getDir(String dirName) throws ConfigurationException
    {
        File dir = new File(dirName);
        if (!dir.isDirectory())
        {
            throw new ConfigurationException(String.format(
                    "The path indicated by %s must be a directory.",
                    dir.getAbsolutePath()));
        }

        return dir;
    }

    /**
     * Stores a configuration file in a specific output file.
     * 
     * @param props
     * @param outFileName
     * @throws ConfigurationException
     */
    public void store(TungstenProperties props, String outFileName)
            throws ConfigurationException
    {
        try
        {
            File checkFile = new File(outFileName);
            File backupFile = new File(outFileName + ".bak");

            if (checkFile.exists())
            {
                if (backupFile.exists())
                {
                    backupFile.delete();
                }

                checkFile.renameTo(new File(outFileName + ".bak"));
            }
            FileOutputStream fout = new FileOutputStream(outFileName);
            props.store(fout);
            fout.flush();
            fout.getFD().sync();
            fout.close();

        }
        catch (FileNotFoundException f)
        {
            throw new ConfigurationException(
                    "Unable to process a file when configuring resources:" + f);
        }
        catch (IOException i)
        {
            throw new ConfigurationException("Error while storing properties:"
                    + i);
        }
    }

    /**
     * Apply the properties from this configuration to another instance.
     * 
     * @param o
     */
    public void applyProperties(Object o)
    {
        this.props.applyProperties(o);
    }

    public static String getClusterHome() throws ConfigurationException
    {

        if (clusterHomeName == null)
        {
            // Try to resolve it from a system property
            clusterHomeName = System
                    .getProperty(ConfigurationConstants.CLUSTER_HOME);
        }

        if (clusterHomeName == null)
        {
            throw new ConfigurationException(
                    "You must have the system property cluster.home set.");
        }

        return clusterHomeName;
    }

    public void setClusterHome(String chome)
    {
        clusterHomeName = chome;
    }

    /**
     * Returns the cluster properties
     * 
     * @return Returns the cluster properties.
     */
    public TungstenProperties getProps()
    {
        return props;
    }

    public String getClusterName()
    {
        return clusterName;
    }

    public void setClusterName(String clusterName)
    {
        this.clusterName = clusterName;
    }

    public String getConfigFileNameInUse()
    {
        return (configFileNameInUse == null) ? "<unset>" : configFileNameInUse;
    }
}
