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

import java.text.MessageFormat;

import com.continuent.tungsten.common.exception.ResourceException;

public enum ResourceState
{
    ONLINE, OFFLINE, SYNCHRONIZING, JOINING, SUSPECT, STOPPED, UNKNOWN, TIMEOUT, DEGRADED, SHUNNED, CONSISTENT, MODIFIED, FAILED, BACKUP, UNREACHABLE, EXTENSION, RESTORING, RUNNING;

    /**
     * Create a ResourceState from a non case sensitive String
     * 
     * @param x the string to be converted
     * @return the converted ResourceState
     * @throws ResourceException if could not cast string to a ResourceState
     */
    public static ResourceState fromString(String x) throws ResourceException
    {
        for (ResourceState currentType : ResourceState.values())
        {
            if (x.equalsIgnoreCase(currentType.toString()))
                return currentType;
        }
        throw new ResourceException(MessageFormat.format(
                "Cannot cast into a known ResourceState: {0}", x));
    }
}
