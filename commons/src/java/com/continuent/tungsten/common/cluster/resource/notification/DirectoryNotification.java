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
import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.directory.Directory;

public class DirectoryNotification extends ClusterResourceNotification
{

    /**
     * 
     */
    private static final long serialVersionUID = 6671252824107054673L;

    public DirectoryNotification(String clusterName, String memberName,
            String notificationSource, String resourceName,
            ResourceState resourceState, TungstenProperties resourceProps)
    {
        super(NotificationStreamID.MONITORING, clusterName, memberName,
                notificationSource, ResourceType.DIRECTORY, resourceName,
                resourceState, resourceProps);
        setResource((Directory) resourceProps.getObject("directory"));
    }

    public Directory getDirectory()
    {
        return (Directory) getResource();
    }

}
