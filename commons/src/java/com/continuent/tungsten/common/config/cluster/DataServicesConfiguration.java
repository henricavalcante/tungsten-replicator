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
 * Initial developer(s): Gilles Rayrat
 * Contributor(s):
 */

package com.continuent.tungsten.common.config.cluster;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.continuent.tungsten.common.config.TungstenProperties;

/**
 * Provides the location as an IP addresses or host names of managers in charge
 * of each data services available in the cluster
 * 
 * @author <a href="mailto:gilles.rayrat@continuent.com">Gilles Rayrat</a>
 * @version 1.0
 */
public class DataServicesConfiguration extends ClusterConfiguration
{
    /** Composite Data Source name <> list of managers */
    private static Map<String, List<String>> physicalDataServiceManagers = new HashMap<String, List<String>>();
    private static DataServicesConfiguration instance                    = null;

    private DataServicesConfiguration() throws ConfigurationException
    {
        // no cluster name here
        super(null);
        load(ConfigurationConstants.TR_SERVICES_PROPS);
        for (String cds : props.keyNames())
        {
            physicalDataServiceManagers.put(cds, props.getStringList(cds));
        }
    }

    public static synchronized DataServicesConfiguration getInstance()
            throws ConfigurationException
    {
        if (instance == null)
        {
            reload();
        }
        return instance;
    }

    public static synchronized DataServicesConfiguration reload()
            throws ConfigurationException
    {
        physicalDataServiceManagers.clear();
        instance = new DataServicesConfiguration();
        return instance;
    }

    public void addDataService(String dataServiceName, List<String> managerList)
            throws ConfigurationException
    {
        if (physicalDataServiceManagers.get(dataServiceName) != null)
        {
            throw new ConfigurationException(String.format(
                    "Data service '%s' already exists.", dataServiceName));
        }

        physicalDataServiceManagers.put(dataServiceName, managerList);
    }

    public void store() throws ConfigurationException
    {
        TungstenProperties propsToStore = new TungstenProperties();
        for (String key : physicalDataServiceManagers.keySet())
        {
            String value = TungstenProperties
                    .listToString(physicalDataServiceManagers.get(key));
            propsToStore.setString(key, value);

        }

        store(ConfigurationConstants.TR_SERVICES_PROPS, propsToStore);
    }

    static public Map<String, List<String>> getPhyicalDataServiceManagersList()
    {
        return physicalDataServiceManagers;
    }

    /**
     * If the given data source name appears in our list of composite data
     * sources, it means that it is a composite data source
     * 
     * @param ds name of the data source for which to determine origin
     * @return true if the given name is a composite data source
     */
    public static boolean isPhysicalDataService(String ds)
    {
        return physicalDataServiceManagers.containsKey(ds);
    }

    /**
     * Returns the full path of the data services configuration file.
     */
    public String getConfigFileNameInUse()
    {
        return System.getProperty(ConfigurationConstants.CLUSTER_HOME)
                + File.separator + ConfigurationConstants.TR_SERVICES_PROPS;
    }
}
