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
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;

/**
 * Handles stand-alone administration of data sources defined for a single
 * replication service. Clients can instantiate this class and perform
 * operations on data sources outside of the replicator pipeline.
 */
public class DataSourceAdministrator
{
    private static Logger     logger  = Logger.getLogger(DataSourceAdministrator.class);

    // Replicator properties.
    TungstenProperties        replicatorProps;

    // Operational variables.
    private DataSourceManager manager = new DataSourceManager();

    /**
     * Instantiates administrator for a particular replicator service.
     * 
     * @param replicatorProps Properties for a replication service
     */
    public DataSourceAdministrator(TungstenProperties replicatorProps)
    {
        this.replicatorProps = replicatorProps;
    }

    /**
     * Initializes the administrator. This must be called before performing
     * operations.
     */
    public void prepare() throws ReplicatorException, InterruptedException
    {
        // Find names of data sources.
        List<String> datasourceNames = replicatorProps
                .getStringList("replicator.datasources");

        // Instantiate and configure each data source.
        for (String name : datasourceNames)
        {
            // Instantiate and add the data source.
            logger.debug("Configuring data source: name=" + name);
            String dsPrefix = "replicator.datasource." + name;
            String className = replicatorProps.get(dsPrefix);
            TungstenProperties attributes = replicatorProps.subset(dsPrefix
                    + ".", true);
            attributes.setBeanSupportEnabled(true);
            UniversalDataSource ds = manager.add(name, className, attributes);

            // Configure and prepare the data source.
            ds.configure();
            ds.prepare();
        }
    }

    /**
     * Releases the administrator and frees all resources. This must be called
     * to ensure proper cleanup.
     */
    public void release() throws InterruptedException
    {
        // Do not reduce catalog data as this may change state unexpectedly or
        // trigger errors.
        manager.removeAndReleaseAll(false);
    }

    /** Returns a list of data sources. */
    public TungstenProperties list()
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
     * Removes all catalog data for a named data source.
     */
    public boolean reset(String name) throws ReplicatorException,
            InterruptedException
    {
        UniversalDataSource ds = manager.find(name);
        if (ds == null)
        {
            return false;
        }
        else
        {
            ds.clear();
            return true;
        }
    }
    
    /**
     * Gets position information from catalog data for a named data source.
     */
    public List<ReplDBMSHeader> get(String name) throws ReplicatorException,
            InterruptedException
    {
        UniversalDataSource ds = manager.find(name);
        if (ds == null)
        {
            return null;
        }
        else
        {
            return ds.getCommitSeqno().getHeaders();
        }
    }
    
    /**
     * Sets position information for catalog data for a named data source.
     * 
     * @return true, if successfully set, false, otherwise.
     */
    public boolean set(String name, long seqno, long epoch, String eventId,
            String sourceId) throws ReplicatorException, InterruptedException
    {
        UniversalDataSource ds = manager.find(name);
        if (ds == null)
        {
            return false;
        }
        else
        {
            ds.getCommitSeqno().initPosition(seqno, sourceId, epoch, eventId);
            return true;
        }
    }

    /**
     * Removes all catalog data for all data sources.
     */
    public boolean resetAll() throws ReplicatorException, InterruptedException
    {
        for (String name : manager.names())
        {
            reset(name);
        }
        return true;
    }
}