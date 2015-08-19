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

public class RemoteDataService extends Resource implements Serializable
{

    /**
     * 
     */
    private static final long serialVersionUID = -4287212613595722268L;

    /*
     * State transitions for RemoteDataService:
     */
    // UNKNOWN->ONLINE->CONSISTENT
    // UNKNOWN->UNREACHABLE->OFFLINE

    private ResourceState     resourceState    = ResourceState.UNKNOWN;

    public RemoteDataService()
    {
       
    }

    public RemoteDataService(ResourceType type, String name, ResourceState state)
    {
        super(type, name);
        setResourceState(state);

    }

    /**
     * Returns the resourceState value.
     * 
     * @return Returns the resourceState.
     */
    public ResourceState getResourceState()
    {
        return resourceState;
    }

    /**
     * Sets the resourceState value.
     * 
     * @param resourceState The resourceState to set.
     */
    public void setResourceState(ResourceState resourceState)
    {
        this.resourceState = resourceState;
    }

    public String toString()
    {
        return String.format("%s: name=%s, resourceState=%s", getType(),
                getName(), getResourceState());
    }
}
