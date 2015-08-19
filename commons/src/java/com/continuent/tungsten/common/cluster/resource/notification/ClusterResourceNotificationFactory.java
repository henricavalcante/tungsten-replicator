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

import java.util.Map;

import com.continuent.tungsten.common.cluster.resource.ResourceState;
import com.continuent.tungsten.common.cluster.resource.ResourceType;
import com.continuent.tungsten.common.config.TungstenProperties;

/**
 * The function of this class is to convert notifications from various
 * components in the system into notifications that can be more profitably used
 * in the complex event processing required by the cluster policy manager.
 * 
 * @author edward
 */
public class ClusterResourceNotificationFactory
{
    public static ClusterResourceNotification createInstance(
            String clusterName, String memberName,
            Map<String, Object> monitorNotification)
    {
        ResourceType resourceType = (ResourceType) monitorNotification
                .get(NotificationPropertyKey.KEY_RESOURCE_TYPE);
        String notificationSource = (String) monitorNotification
                .get(NotificationPropertyKey.KEY_NOTIFICATION_SOURCE);
        ResourceState resourceState = (ResourceState) monitorNotification
                .get(NotificationPropertyKey.KEY_RESOURCE_STATE);
        String resourceName = (String) monitorNotification
                .get(NotificationPropertyKey.KEY_RESOURCE_NAME);
        TungstenProperties resourceProperties = (TungstenProperties) monitorNotification
                .get(NotificationPropertyKey.KEY_RESOURCE_PROPERTIES);

        if (resourceType == ResourceType.REPLICATOR)
        {
            return (ClusterResourceNotification) new ReplicatorNotification(
                    clusterName, memberName, notificationSource, resourceName,
                    resourceState, resourceProperties);
        }
        else if (resourceType == ResourceType.DATASOURCE)
        {
            return (ClusterResourceNotification) new DataSourceNotification(
                    clusterName, memberName, resourceName, resourceState,
                    notificationSource, resourceProperties);
        }
        else if (resourceType == ResourceType.DATASERVER)
        {
            return (ClusterResourceNotification) new DataServerNotification(
                    clusterName, memberName, resourceName, resourceState,
                    notificationSource, null, null);
        }
        else
        {
            return null;
        }
    }

}
