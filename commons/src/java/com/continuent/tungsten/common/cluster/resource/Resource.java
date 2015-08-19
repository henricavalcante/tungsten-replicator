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
 * Initial developer(s):
 * Contributor(s):
 */

package com.continuent.tungsten.common.cluster.resource;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlRootElement;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonPropertyOrder;
import org.codehaus.jackson.map.ObjectMapper;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.exception.ResourceException;
import com.continuent.tungsten.common.utils.ReflectUtils;
import com.continuent.tungsten.common.utils.ResultFormatter;

/**
 * @author edward
 */
@XmlRootElement
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder(alphabetic = true)
public class Resource implements Serializable
{
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    protected String          name             = null;
    protected ResourceType    type             = ResourceType.UNDEFINED;
    protected String          description      = "";
    protected boolean         isContainer      = false;
    protected boolean         isExecutable     = false;
    protected Boolean         compositeMember  = null;                      // True if part of a higher level Composite DataService
    protected ResourceType    childType        = ResourceType.UNDEFINED;

    public Resource()
    {
        this.name = "UNKNOWN";
    }

    public Resource(ResourceType type, String name)
    {
        this.name = name;
        this.type = type;

    }

    public TungstenProperties toProperties()
    {
        TungstenProperties props = new TungstenProperties();
        props.setString("type", this.getType().toString());
        props.extractProperties(this, true);

        return props;
    }

    /**
     * Describe this instance, in detail if necessary.
     * 
     * @param detailed
     * @return string description of this resource
     */
    public String describe(boolean detailed)
    {
        TungstenProperties props = this.toProperties();
        String formattedProperties = (new ResultFormatter(props.map(), false,
                ResultFormatter.DEFAULT_INDENT)).format();
        return (String.format("%s\n{\n%s\n}", name, formattedProperties));
    }

    public String toString()
    {
        return describe(false);
    }

    // ---------- Getters and Setters ----------
    /**
     * 
     */
    public String getKey()
    {
        return getName();
    }

    /**
     * @return the name
     */
    public String getName()
    {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name)
    {
        this.name = name;
    }

    /**
     * @return the type
     */
    public ResourceType getType()
    {
        return type;
    }

    /**
     * @param type the type to set
     */
    public void setType(ResourceType type)
    {
        this.type = type;
    }

    /**
     * @return the isContainerResource
     */
    public boolean isContainer()
    {
        return isContainer;
    }

    /**
     * @param isContainer the isContainer to set
     */
    public void setContainer(boolean isContainer)
    {
        this.isContainer = isContainer;
    }

    /**
     * @return the isExecutable
     */
    public boolean isExecutable()
    {
        return isExecutable;
    }

    /**
     * @param isExecutable the isExecutable to set
     */
    public void setExecutable(boolean isExecutable)
    {
        this.isExecutable = isExecutable;
    }

    /**
     * @return the childType
     */
    public ResourceType getChildType()
    {
        return childType;
    }

    /**
     * @param childType the childType to set
     */
    public void setChildType(ResourceType childType)
    {
        this.childType = childType;
    }

    /**
     * Returns the compositeMember value.
     * 
     * @return Returns the compositeMember. True if the resource is part of a higher level Composite DataService
     */
    public Boolean getCompositeMember()
    {
        return compositeMember;
    }

    /**
     * Sets the compositeMember value.
     * 
     * @param compositeMember The compositeMember to set.
     */
    public void setCompositeMember(Boolean compositeMember)
    {
        this.compositeMember = compositeMember;
    }

    // --------------------------------- Utility functions ----------------------------------
    /**
     * Copies values from fields of this instance to another instance
     * 
     * @param destination
     * @return the copied resource
     */
    public Resource copyTo(Resource destination)
    {
        ReflectUtils.copy(this, destination);
        destination.setName(this.getName());
        return destination;
    }

    public String toJSON() throws ResourceException
    {
        ObjectMapper mapper = new ObjectMapper();

        try
        {
            String jsonString = null;

            jsonString = mapper.writeValueAsString(this);

            // Sanity check that we can convert the string back to an object
            mapper.readValue(jsonString, this.getClass());

            return (jsonString);
        }
        catch (Exception e)
        {
            String message = "Possible serialization/deserialization issue";
            throw new ResourceException(message, e);
        }
    }
}
