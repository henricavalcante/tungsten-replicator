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

package com.continuent.tungsten.replicator.datasource;

import java.util.List;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.service.PipelineService;

/**
 * Provides a service interface interface to data source implementations. This
 * service is responsible for initializing implementations as well as underlying
 * catalog data at pipeline start-up time. It includes the following important
 * responsibilities:
 * <ul>
 * <li>Set channel count based on the pipeline configuration, which may be
 * greater than one for appliers but are in other cases always 1.</li>
 * <li>Implement aliasing, which allows multiple names for the same data source
 * and allows components to refer to data sources based on role rather than a
 * changeable name</li>
 * </ul>
 */
public class DataSourceService implements PipelineService
{
    private static Logger     logger    = Logger.getLogger(DataSourceService.class);

    // Recognized data source names.

    /** The default data source. */
    public static String      GLOBAL    = "global";

    /** The standard name for an extract-only data source. */
    public static String      EXTRACTOR = "extractor";

    /** The standard name for an apply-only data source. */
    public static String      APPLIER   = "applier";

    // Properties.
    private String            name;

    // Operational variables.
    private DataSourceManager manager   = new DataSourceManager();

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.service.PipelineService#getName()
     */
    public String getName()
    {
        return name;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.service.PipelineService#setName(java.lang.String)
     */
    public void setName(String name)
    {
        this.name = name;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        // Find names of data sources.
        TungstenProperties replicatorProps = context.getReplicatorProperties();
        List<String> datasourceNames = replicatorProps
                .getStringList("replicator.datasources");

        // Instantiate and configure each data source.
        for (String name : datasourceNames)
        {
            // Instantiate and add the data source.
            logger.info("Configuring data source: name=" + name);
            String dsPrefix = "replicator.datasource." + name;
            String className = replicatorProps.get(dsPrefix);
            TungstenProperties attributes = replicatorProps.subset(dsPrefix
                    + ".", true);
            attributes.setBeanSupportEnabled(true);
            UniversalDataSource ds = manager.add(name, className, attributes);

            // Configure it.
            ds.configure();
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        // Instantiate and configure each data source.
        for (String name : manager.names())
        {
            // Fetch the data source.
            UniversalDataSource ds = manager.find(name);

            // If it's an alias, just skip as the underlying source will be
            // prepared later.
            if (ds instanceof AliasDataSource)
            {
                continue;
            }
            logger.info("Preparing and initializing data source: name=" + name);

            // Set the channels based on the name of the data source.
            if (GLOBAL.equals(name))
            {
                // Could be apply or extract--the pipeline knows!
                ds.setChannels(context.getChannels());
            }
            else if (EXTRACTOR.equals(name))
            {
                // Must have a single channel only.
                ds.setChannels(1);
            }
            else if (APPLIER.equals(name))
            {
                // Let the pipeline decide.
                ds.setChannels(context.getChannels());
            }
            else
            {
                // Default to whatever is in the data source.
            }

            // If we have a SqlDataSource, apply rules for logging/not logging
            // catalog operations.
            if (ds instanceof SqlDataSource)
            {
                SqlDataSource sqlDs = (SqlDataSource) ds;
                String role = context.getRoleName();
                if ("master".equals(role))
                {
                    if (context.isPrivilegedMaster())
                    {
                        logger.info("Suppressing logging for privileged master: name="
                                + name);
                        sqlDs.setPrivileged(true);
                        sqlDs.setLogOperations(false);
                    }
                }
                else if ("slave".equals(role) || "relay".equals(role))
                {
                    if (context.isPrivilegedSlave())
                    {
                        logger.info("Setting catalog handling for privileged slave/relay: name="
                                + name);
                        sqlDs.setPrivileged(true);
                    }
                    if (context.logReplicatorUpdates())
                    {
                        logger.info("Enabling logging of updates for slave/relay: name="
                                + name);
                        sqlDs.setLogOperations(true);
                    }
                    else
                    {
                        logger.info("Disabling logging of updates for slave/relay: name="
                                + name);
                        sqlDs.setLogOperations(false);
                    }
                }
                else
                {
                    logger.info("Accepting defaults for SqlDataSource: name="
                            + name + " privileged=" + sqlDs.isPrivileged()
                            + " logging=" + sqlDs.isLogOperations());
                }
            }

            // Prepare data source and initialize catalog tables.
            ds.prepare();
            ds.initialize();
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void release(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        // Reduce catalog state to ensure clean offline.
        manager.removeAndReleaseAll(true);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.service.PipelineService#status()
     */
    public TungstenProperties status()
    {
        TungstenProperties props = new TungstenProperties();
        for (String name : manager.names())
        {
            UniversalDataSource ds = manager.find(name);
            props.setString(name, ds.toString());
        }
        return props;
    }

    /**
     * Looks up and returns a named data source. This automatically looks up
     * aliases and returns the aliased data source.
     * 
     * @return Data source corresponding to name or null if not found
     * @throws ReplicatorException Thrown if data source cannot be obtained due
     *             to an error, such as an alias data source that has no
     *             corresponding source
     */
    public UniversalDataSource find(String name) throws ReplicatorException
    {
        UniversalDataSource ds = manager.find(name);
        if (ds == null)
        {
            // If the data source does not exist, return nothing. 
            return null;
        }
        else if (ds instanceof AliasDataSource)
        {
            // For alias data source find the source. 
            String source = ((AliasDataSource) ds).getDataSource();
            if (source == null)
            {
                throw new ReplicatorException(
                        "Alias data source does not have source specified: name="
                                + name);
            }
            else if (source.equals(name))
            {
                throw new ReplicatorException(
                        "Alias data source specifies itself as a source: name="
                                + name);
            }
            else
            {
                ds = manager.find(source);
                if (ds == null)
                {
                    throw new ReplicatorException(
                            "Unable to find source of aliased data source: alias name="
                                    + name + " source name=" + source);
                }
                else if (ds instanceof AliasDataSource)
                {
                    throw new ReplicatorException(
                            "Aliased data source points to another alias as source: alias name="
                                    + name + " source name=" + source);
                }
                else
                {
                    return ds;
                }
            }
        }
        else
        {
            // Anything else we return as is. 
            return ds;
        }
    }
}