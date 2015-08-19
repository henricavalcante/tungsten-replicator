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
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.manager.resource.logical;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import com.continuent.tungsten.common.cluster.resource.DataSource;
import com.continuent.tungsten.common.cluster.resource.DataSourceRole;
import com.continuent.tungsten.common.cluster.resource.ResourceState;
import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.exception.ResourceException;

public class DataService
{
    @SuppressWarnings("unused")
    private static final long                  serialVersionUID = 1L;

    private String                             dataServiceName  = null;

    private Map<String, DataSource>            resources        = new TreeMap<String, DataSource>();
    private Map<String, ArrayList<DataSource>> resourcesByRole  = new HashMap<String, ArrayList<DataSource>>();

    public DataService(String dataServiceName,
            Map<String, TungstenProperties> dataSourceMap)
            throws ResourceException
    {
        this.dataServiceName = dataServiceName;

        for (TungstenProperties ds : dataSourceMap.values())
        {
            DataSource newDs = new DataSource(ds);
            newDs.setDataServiceName(this.dataServiceName);
            resources.put(newDs.getName(), newDs);
            addByRole(newDs);
        }

    }

    public boolean isActiveMaster(String dsName)
    {
        DataSource currentMaster = null;

        try
        {
            currentMaster = getCurrentMaster();
            if (currentMaster.getName().equals(dsName))
                return true;
        }
        catch (ResourceException ignored)
        {

        }

        return false;
    }

    public boolean isActiveSlave(String dsName)
    {
        DataSource foundDs = null;

        try
        {
            foundDs = getDataSource(dsName);

            if (foundDs.getRole().equals(DataSourceRole.slave.toString())
                    && !(foundDs.getState() == ResourceState.FAILED || foundDs
                            .getState() == ResourceState.SHUNNED))
                return true;

        }
        catch (ResourceException ignored)
        {

        }

        return false;
    }

    public DataSource getCurrentMaster() throws ResourceException
    {
        ArrayList<DataSource> dsByRole = getRoleArray(DataSourceRole.master
                .toString());
        DataSource foundDs = null;

        for (DataSource ds : dsByRole)
        {
            if (ds.getRole().equals(DataSourceRole.master.toString())
                    && ds.getState() != ResourceState.SHUNNED
                    && ds.getState() != ResourceState.FAILED)
            {
                foundDs = ds;
            }
        }

        if (foundDs == null)
        {
            throw new ResourceException("No master is currently available");
        }

        return foundDs;
    }

    public void removeDataSource(DataSource dsToRemove)
            throws ResourceException
    {

        DataSource foundDs = resources.get(dsToRemove.getName());

        if (foundDs == null)
        {
            throw new ResourceException(String.format(
                    "Did not find a datasource named '%s' in service '%s'",
                    dsToRemove.getName(), getDataServiceName()));
        }

        resources.remove(dsToRemove.getName());
        removeByRole(dsToRemove);

    }

    public DataSource getDataSource(String dsName) throws ResourceException
    {
        DataSource ds = null;

        synchronized (resources)
        {
            ds = resources.get(dsName);
        }

        if (ds == null)
        {
            throw new ResourceException(String.format(
                    "Datasource %s not found for service %s", dsName,
                    dataServiceName));
        }

        return ds;
    }

    public Map<String, DataSource> getAllDataSources()
    {
        synchronized (this)
        {
            return resources;
        }

    }

    /**
     * Returns the dataServiceName value.
     * 
     * @return Returns the dataServiceName.
     */
    public String getDataServiceName()
    {
        return dataServiceName;
    }

    /**
     * Sets the dataServiceName value.
     * 
     * @param dataServiceName The dataServiceName to set.
     */
    public void setDataServiceName(String dataServiceName)
    {
        this.dataServiceName = dataServiceName;
    }

    private void addByRole(DataSource ds) throws ResourceException
    {
        validate(ds);
        synchronized (this)
        {
            ArrayList<DataSource> dsByRole = getRoleArray(ds.getRole());
            dsByRole.add(ds);
        }
    }

    private void removeByRole(DataSource ds)
    {
        ArrayList<DataSource> dsByRole = getRoleArray(ds.getRole());

        int objIndex = -1;

        if ((objIndex = dsByRole.indexOf(ds)) >= 0)
        {
            dsByRole.remove(objIndex);
        }

    }

    private ArrayList<DataSource> getRoleArray(String role)
    {

        ArrayList<DataSource> dsByRole = resourcesByRole.get(role);

        if (dsByRole == null)
        {
            dsByRole = new ArrayList<DataSource>();
            resourcesByRole.put(role, dsByRole);
        }

        return dsByRole;
    }

    private void validate(DataSource ds) throws ResourceException
    {
        if ((ds.getName() == null || ds.getName().length() == 0)
                || (ds.getDriver() == null || ds.getDriver().length() == 0)
                || (ds.getUrl() == null || ds.getUrl().length() == 0)
                || ((ds.getRole().equals(DataSourceRole.undefined.toString()) && !ds
                        .isComposite())))
        {
            throw new ResourceException(String.format(
                    "Malformed datasource encountered: %s", ds.toString()));
        }
    }
}
