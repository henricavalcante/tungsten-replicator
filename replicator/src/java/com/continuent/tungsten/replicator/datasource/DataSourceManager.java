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
 * Contributor(s): Stephane Giron
 */

package com.continuent.tungsten.replicator.datasource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * Manages data sources. A single manager can have any number of uniquely named
 * data sources.
 */
public class DataSourceManager
{
    private static Logger                    logger      = Logger.getLogger(DataSourceManager.class);

    // Table of currently known data sources.
    private Map<String, UniversalDataSource> datasources = new TreeMap<String, UniversalDataSource>();

    /**
     * Creates a new instance.
     */
    public DataSourceManager()
    {
    }

    /**
     * Returns the number of data sources currently under management.
     */
    public int count()
    {
        return datasources.size();
    }

    /**
     * Adds a new data source. Clients <em>must</em> configure and prepare the
     * data source through the corresponding methods to make use of it.
     * 
     * @param name Name of the data source
     * @param className Name of the implementing class.
     * @param attributes TungstenProperties instance containing values to assign
     *            to data source instance. If the instance uses embedded beans,
     *            the properties should have bean property support enabled
     * @return Returns instantiated data source
     * @see TungstenProperties#setBeanSupportEnabled(boolean)
     * @see #addAndPrepare(String, String, TungstenProperties)
     */
    public UniversalDataSource add(String name, String className,
            TungstenProperties attributes) throws ReplicatorException,
            InterruptedException
    {
        // Check for a duplicate data source, then find the class name.
        if (datasources.get(name) != null)
        {
            throw new ReplicatorException(
                    "Foiled attempt to load duplicate data source: name="
                            + name);
        }

        // Instantiate the data source class and apply properties. If successful
        // add result to the data source table.
        try
        {
            logger.info("Loading data source: name=" + name + " className="
                    + className);
            UniversalDataSource datasource = (UniversalDataSource) Class
                    .forName(className).newInstance();
            attributes.applyProperties(datasource);
            datasource.setName(name);
            datasources.put(name, datasource);
            return datasource;
        }
        catch (Exception e)
        {
            throw new ReplicatorException(
                    "Unable to instantiate data source: name=" + name
                            + " className=" + className + " message="
                            + e.getMessage(), e);
        }
    }

    /**
     * Configures a data source.
     * 
     * @param name Name of the data source.
     */
    public void configure(String name) throws ReplicatorException,
            InterruptedException
    {
        UniversalDataSource ds = find(name);
        ds.configure();
    }

    /**
     * Prepares a data source.
     * 
     * @param name Name of the data source.
     */
    public void prepare(String name) throws ReplicatorException,
            InterruptedException
    {
        UniversalDataSource ds = find(name);
        ds.prepare();
    }

    /**
     * Adds configures, and prepares a data source in a single step.
     * 
     * @param name Name of the data source
     * @param className Name of the implementing class.
     * @param attributes TungstenProperties instance containing values to assign
     *            to data source instance. If the instance uses embedded beans,
     *            the properties should have bean property support enabled
     * @return Returns instantiated data source
     */
    public UniversalDataSource addAndPrepare(String name, String className,
            TungstenProperties attributes) throws InterruptedException,
            ReplicatorException
    {
        UniversalDataSource ds = this.add(name, className, attributes);
        ds.configure();
        ds.prepare();
        return ds;
    }

    /**
     * Releases a data source.
     * 
     * @param name Name of the data source.
     */
    public void release(String name) throws ReplicatorException,
            InterruptedException
    {
        UniversalDataSource ds = find(name);
        ds.release();
    }

    /**
     * Returns the names of currently stored data sources.
     */
    public List<String> names()
    {
        List<String> names = new ArrayList<String>(datasources.keySet());
        return names;
    }

    /**
     * Returns the named data source or null if it does not exist.
     */
    public UniversalDataSource find(String name)
    {
        return datasources.get(name);
    }

    /**
     * Removes a data source.
     * 
     * @param name Name of the data source to remove
     * @return Return data source or null if not found
     */
    public UniversalDataSource remove(String name) throws InterruptedException
    {
        return datasources.remove(name);
    }

    /**
     * Removes and deallocates a data source.
     * 
     * @param name Name of the data source to remove
     * @param reduce If true, reduce catalog data to ensure clean offline state
     *            that allows reconfiguration
     * @return Return data source or null if not found
     */
    public UniversalDataSource removeAndRelease(String name, boolean reduce)
            throws InterruptedException, ReplicatorException
    {
        UniversalDataSource ds = remove(name);
        if (name != null)
        {
            if (reduce)
            {
                ds.reduce();
            }
            ds.release();
        }
        return ds;
    }

    /**
     * Removes and deallocates all data sources. This should be called to ensure
     * data source resources are properly freed.
     * 
     * @param reduce If true, reduce catalog data to ensure clean offline state
     *            that allows reconfiguration
     */
    public void removeAndReleaseAll(boolean reduce) throws InterruptedException
    {
        for (String name : names())
        {
            UniversalDataSource ds = remove(name);
            try
            {
                try
                {
                    if (reduce)
                    {
                        ds.reduce();
                    }
                }
                finally
                {
                    ds.release();
                }
            }
            catch (ReplicatorException e)
            {
                logger.warn("Error while releasing data source: name=" + name, e);
            }
        }
    }
}