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
 * Initial developer(s): Edward Archibald
 * Contributor(s):
 */

package com.continuent.tungsten.common.cluster.resource.notification;

import com.continuent.tungsten.common.cluster.resource.ResourceState;
import com.continuent.tungsten.common.cluster.resource.ResourceType;
import com.continuent.tungsten.common.cluster.resource.physical.Replicator;
import com.continuent.tungsten.common.config.TungstenProperties;

public class ReplicationServiceNotification extends ClusterResourceNotification
{

    /**
     * Used to determine if a given de-serialized object is compatible with this
     * class version.<br>
     * This value must be changed if and only if the new version of this class
     * is not compatible with old versions. See <a
     * href=http://java.sun.com/products/jdk/1.1/docs/guide
     * /serialization/spec/version.doc.html/> for a list of compatible changes.
     */
    private static final long   serialVersionUID            = -2097111546144612171L;

    private static final String SERVICE_STATE_ONLINE        = "ONLINE";
    private static final String SERVICE_STATE_GOING_ONLINE  = "GOING-ONLINE";
    private static final String SERVICE_STATE_OFFLINE       = "OFFLINE";
    private static final String SERVICE_STATE_ERROR         = "OFFLINE:ERROR";
    private static final String SERVICE_STATE_BACKUP        = "OFFLINE:BACKUP";
    private static final String SERVICE_STATE_SYNCHRONIZING = "SYNCHRONIZING";
    private static final String SERVICE_STATE_RESTORING     = "RESTORING";

    /**
     * @param clusterName
     * @param memberName
     * @param notificationSource
     * @param resourceName
     * @param resourceState
     * @param resourceProps
     */
    public ReplicationServiceNotification(String clusterName,
            String memberName, String notificationSource, String resourceName,
            ResourceState resourceState, TungstenProperties resourceProps)
    {
        super(NotificationStreamID.MONITORING, clusterName, memberName,
                notificationSource, ResourceType.REPLICATION_SERVICE,
                resourceName, resourceState, resourceProps);
    }

    /**
     * Parses the given state provided by the replicator and guess a generic
     * resourceState from it
     * 
     * @param state the state of the connected replicator, as provided by its
     *            JMX state attribute
     * @return a resource state, one of {@link ResourceState}
     */
    static public ResourceState replicatorStateToResourceState(String state)
    {
        if (state.startsWith(SERVICE_STATE_ONLINE))
            return ResourceState.ONLINE;
        else if (state.startsWith(SERVICE_STATE_OFFLINE))
        {
            if (state.equals(SERVICE_STATE_ERROR))
            {
                return ResourceState.SUSPECT;
            }
            else if (state.equals(SERVICE_STATE_BACKUP))
            {
                return ResourceState.BACKUP;
            }
            else
            {
                return ResourceState.OFFLINE;
            }
        }
        else if (state.contains(SERVICE_STATE_SYNCHRONIZING))
        {
            return ResourceState.SYNCHRONIZING;
        }
        else if (state.startsWith(SERVICE_STATE_GOING_ONLINE))
        {
            if (state.contains(SERVICE_STATE_RESTORING))
            {
                return ResourceState.RESTORING;
            }
            else
            {
                return ResourceState.UNKNOWN;
            }
        }
        else
            return ResourceState.UNKNOWN;
    }

    public String getRole()
    {
        return resourceProps.getString(Replicator.ROLE);
    }

    public String getHost()
    {
        return resourceProps.getString(Replicator.HOST);
    }

    public String getDataServerHost()
    {
        return resourceProps.getString(Replicator.DATASERVER_HOST);
    }

    public String getMasterReplicator()
    {
        String masterUri = resourceProps
                .getString(Replicator.MASTER_CONNECT_URI);
        return masterUri.substring(masterUri.indexOf("//") + 2,
                masterUri.lastIndexOf("/"));
    }

}
