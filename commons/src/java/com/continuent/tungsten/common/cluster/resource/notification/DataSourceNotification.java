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

import com.continuent.tungsten.common.cluster.resource.DataSource;
import com.continuent.tungsten.common.cluster.resource.DataSourceRole;
import com.continuent.tungsten.common.cluster.resource.ResourceState;
import com.continuent.tungsten.common.cluster.resource.ResourceType;
import com.continuent.tungsten.common.config.TungstenProperties;

public class DataSourceNotification extends ClusterResourceNotification
{
    protected DataSourceRole  role             = DataSourceRole.undefined;
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public DataSourceNotification(String clusterName, String memberName,
            String resourceName, ResourceState state, String source,
            TungstenProperties resourceProps)
    {
        super(NotificationStreamID.MONITORING, clusterName, memberName, source,
                ResourceType.DATASOURCE, resourceName, state, resourceProps);

    }

    public DataSourceNotification(NotificationStreamID streamID,
            String clusterName, String memberName, String resourceName,
            ResourceState state, String source, TungstenProperties resourceProps)
    {
        super(streamID, clusterName, memberName, source,
                ResourceType.DATASOURCE, resourceName, state, resourceProps);
        if (resourceProps != null)
        {
            String dsRole = resourceProps.getString(DataSource.ROLE);
            if (dsRole != null)
            {
                this.role = DataSourceRole.valueOf(dsRole);
            }
        }
    }

    public DataSourceNotification(NotificationStreamID streamID, String source,
            DataSource ds)
    {
        super(streamID, ds.getDataServiceName(), ds.getName(), source,
                ResourceType.DATASOURCE, ds.getName(), ds.getState(), ds
                        .toProperties());
        this.role = ds.getDataSourceRole();
    }

    public DataSource getDataSource()
    {
        return (DataSource) new DataSource(resourceProps);
    }

    public DataSourceRole getRole()
    {
        return role;
    }
}
