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
 * Data Transfer Object for: Data Service. Definition used when creating a new
 * resource
 * 
 * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
 * @version 1.0
 */
@XmlRootElement
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder({"name"})
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class DataService_Definition extends Service
{
    // --- Data Service definition ---
    private HashMap<String, DataSource_Definition> listDataSourceDefinition = new HashMap<String, DataSource_Definition>();

    public DataService_Definition()
    {
    }

    public DataService_Definition(Service parentService, String name)
    {
        super(parentService);
        this.name = name;

    }
    
    /**
     * Add a DataSource to the DataService
     * 
     * @param dataSourceDefinition
     */
    public HashMap<String, DataSource_Definition> addDataSource(DataSource_Definition dataSourceDefinition)
    {
        this.listDataSourceDefinition.put(dataSourceDefinition.getName(), dataSourceDefinition);
        return this.listDataSourceDefinition;
    }

    // --------------------------------- Getters and Setters ------------------
    // @formatter:off
	public HashMap<String, DataSource_Definition> getListDataSourceDefinition() 								{return this.listDataSourceDefinition;}
	public void setListDataSourceDefinition(HashMap<String, DataSource_Definition> listDataSourceDefinition) 	{this.listDataSourceDefinition = listDataSourceDefinition;}
	// @formatter:on

}
