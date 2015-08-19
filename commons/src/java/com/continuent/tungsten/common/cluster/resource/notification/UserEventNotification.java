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

import java.util.Properties;

import com.continuent.tungsten.common.cluster.resource.ResourceState;
import com.continuent.tungsten.common.cluster.resource.ResourceType;
import com.continuent.tungsten.common.config.TungstenProperties;

public class UserEventNotification extends ClusterResourceNotification
{

    /**
     * Used to determine if a given de-serialized object is compatible with this
     * class version.<br>
     * This value must be changed if and only if the new version of this class
     * is not compatible with old versions. See <a
     * href=http://java.sun.com/products/jdk/1.1/docs/guide
     * /serialization/spec/version.doc.html/> for a list of compatible changes.
     */
    private static final long serialVersionUID = -2097111546144612171L;

    /**
     * @param clusterName
     * @param memberName
     * @param notificationSource
     * @param resourceName
     * @param resourceState
     * @param resourceProps
     */
    public UserEventNotification(String clusterName, String memberName,
            String notificationSource, String resourceName,
            String resourceState, Properties resourceProps)
    {
        super(NotificationStreamID.MONITORING, clusterName, memberName,
                notificationSource, ResourceType.EVENT, resourceName,
                ResourceState.EXTENSION, null);

        TungstenProperties props = new TungstenProperties();
        props.load(resourceProps);
        setResourceProps(props);

    }

}
