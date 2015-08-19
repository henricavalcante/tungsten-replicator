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

import java.net.URI;

import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import javax.xml.bind.annotation.XmlRootElement;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonPropertyOrder;
import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 * Class describing a metadata embeded in a Response
 * 
 * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
 * @version 1.0
 */

@XmlRootElement
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder(alphabetic = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class TdfResourceLocation
{
    public URI statusUrl                 = null;
    public URI location                  = null;
    public URI commandUrl                = null;
    public URI listDescriptionServiceUrl = null;

    public TdfResourceLocation()
    {

    }

    /**
     * Creates a new <code>TdfResponseMetadata</code> object
     * 
     * @param uriInfo
     * @param service
     */
    @SuppressWarnings("rawtypes")
    public TdfResourceLocation(UriInfo uriInfo, Service service,
            Class statusResource, Class serviceResource, Class commandResource)
    {
        String parentPath = this.getParentPath(service).toString();
        parentPath = (parentPath == null) ? "." : parentPath;

        // --- Build absolute URLs ---
        this.statusUrl = UriBuilder.fromUri(uriInfo.getBaseUri())
                .path(statusResource).path(parentPath).path(service.getName())
                .build();
        this.location = UriBuilder.fromUri(uriInfo.getBaseUri())
                .path(serviceResource).path(parentPath).path(service.getName())
                .build();
        this.commandUrl = UriBuilder.fromUri(uriInfo.getBaseUri())
                .path(commandResource).path(parentPath).path(service.getName())
                .build();

        // --- Turn then into relative URLs ---
        this.statusUrl = UriBuilder.fromPath(this.statusUrl.getPath()).build();
        this.location = UriBuilder.fromPath(this.location.getPath()).build();
        this.commandUrl = UriBuilder.fromPath(this.commandUrl.getPath())
                .build();
    }

    /**
     * Generate path corresponding to the suffix for a Service
     * 
     * @param service
     * @return path to be prepended to the current service path
     */
    private URI getParentPath(Service service)
    {
        Service parent = service.getParentService();
        URI parentPath = UriBuilder.fromPath("").build();

        while (parent != null)
        {
            // If it's a single site service, do not preprend the name of the
            // service to the name of the DataService = it's the same
            if (parent instanceof Service_Definition
                    && !((Service_Definition) parent).isCompositeService())
                break;
            parentPath = UriBuilder.fromPath(parent.getName())
                    .path(parentPath.toString()).build();
            parent = parent.getParentService();
        }

        return parentPath;

    }

}
