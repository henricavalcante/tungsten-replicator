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
 * Initial developer(s): Ludovic Launer
 * Contributor(s):
 */

package com.continuent.tungsten.common.tdf.dto;

import java.util.HashMap;

import javax.xml.bind.annotation.XmlRootElement;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonPropertyOrder;
import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 * Data Transfer Model for: Service. The Service_Definition class is tha root
 * parent class for all services : DataService_Definition,
 * CompositeDataService_Definition, ...
 * 
 * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
 * @version 1.0
 */

@XmlRootElement
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder({"name", "compositeService"})
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class Service_Definition extends Service
{
    // --- Service Definition ---
    /** List of Sites = Data Services in the Service */
    private HashMap<String, DataService_Definition> listDataServiceDefinition = new HashMap<String, DataService_Definition>();

    public Service_Definition()
    {
    }

    /**
     * Creates a new <code>Service_Definition</code> object
     * 
     * @param name
     */
    public Service_Definition(String name)
    {
        super(name, null);
    }

    /**
     * Multi site = Composite Data Service. Single site = Data Service
     * 
     * @return True if the service is a multi site service. False if it's a
     *         single site service.
     */
    public boolean isCompositeService()
    {
        boolean isComposite = false;

        // Get info from reported list of Service
        if (this.listDataServiceDefinition != null
                && this.listDataServiceDefinition.size() > 1)
            isComposite = true;
        else
            isComposite = false;

        return isComposite;
    }

    /**
     * Add a DataService to the Service
     * 
     * @param dataServiceDefinition
     * @return HashMap<String, DataSource_Definition> the current list of
     *         DataSerivce_Definition
     */
    public HashMap<String, DataService_Definition> addDataService(
            DataService_Definition dataServiceDefinition)
    {
        this.listDataServiceDefinition.put(dataServiceDefinition.getName(),
                dataServiceDefinition); // Add a DataService

        return this.listDataServiceDefinition;
    }

    // --------------------------------- Getters and Setters ------------------
    @Override
    public String getName()
    {
        // By default Single site service have the name of their unique
        // DataService
        if (this.listDataServiceDefinition.size() == 1)
        {
            String dataServiceName = this.listDataServiceDefinition.keySet().iterator().next();
            this.name = dataServiceName;
        }
        return this.name;
    }
    
    /**
     * Sets the name of the Service.
     * Used for Composite DataService only
     */
    public HashMap<String, DataService_Definition> getListDataServiceDefinition()
    {
        return this.listDataServiceDefinition;
    }

    public void setListDataServiceDefinition(
            HashMap<String, DataService_Definition> listDataServiceDefinition)
    {
        this.listDataServiceDefinition = listDataServiceDefinition;
    }

    // ########################################################################

}
