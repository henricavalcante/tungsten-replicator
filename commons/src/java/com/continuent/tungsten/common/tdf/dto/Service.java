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

import javax.xml.bind.annotation.XmlRootElement;

import org.apache.log4j.Logger;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonPropertyOrder;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 * Data Transfer Model for: Service.
 * 
 * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
 * @version 1.0
 */

@XmlRootElement
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder(alphabetic = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class Service
{
    private static final Logger   log                 = Logger.getLogger(Service.class);

    // ########## Common properties for all services ##########
    protected String              name                = null;
    /** URIs to various operations for this resource */
    protected TdfResourceLocation tdfResourceLocation = null;
    private Service               parentService       = null;

    public Service()
    {
    }

    /**
     * Creates a new <code>Service</code> object
     * 
     * @param name
     * @param parentService
     */
    public Service(String name, Service parentService)
    {
        this.name = name;
        this.parentService = parentService;
    }

    public Service(Service parentService)
    {
        this.parentService = parentService;
    }

    // ---------------- Getters and Setters -----------------------------------
    /**
     * Sets the name of the Service.
     * 
     * @param name the name of the Service
     */
    public void setName(String name)
    {
        this.name = name;
    }

    public String getName()
    {
        return this.name;
    }

    @JsonIgnore
    public Service getParentService()
    {
        return parentService;
    }

    @JsonIgnore
    public void setParentService(Service parentService)
    {
        this.parentService = parentService;
    }

    public TdfResourceLocation getTdfResourceLocation()
    {
        return tdfResourceLocation;
    }

    public void setTdfResourceLocation(TdfResourceLocation tdfResourceLocation)
    {
        this.tdfResourceLocation = tdfResourceLocation;
    }

    // ########################################################################

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        String json = null;

        try
        {
            ObjectMapper mapper = new ObjectMapper();
            json = mapper.writeValueAsString(this);

        }
        catch (Exception e)
        {
            log.warn(String.format("Error while trying to serialise %s\n%s",
                    this.getClass().getName(), e), e);
        }
        return json;
    }

}
