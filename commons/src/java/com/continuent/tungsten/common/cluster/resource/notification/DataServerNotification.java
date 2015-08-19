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

import com.continuent.tungsten.common.cluster.resource.DataServer;
import com.continuent.tungsten.common.cluster.resource.ResourceState;
import com.continuent.tungsten.common.cluster.resource.ResourceType;
import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.config.cluster.DataServerConditionMapping;
import com.continuent.tungsten.common.config.cluster.MappedAction;

public class DataServerNotification extends ClusterResourceNotification
{
    /**
     * 
     */
    private static final long          serialVersionUID = 1L;

    private Exception                  lastException;

    private boolean                    readOnly         = false;

    private DataServerConditionMapping conditionMapping = null;

    public DataServerNotification(String clusterName, String memberName,
            String resourceName, ResourceState resourceState, String source,
            DataServer dataServer, TungstenProperties dsQueryResultProps)
    {
        super(NotificationStreamID.MONITORING, clusterName, memberName, source,
                ResourceType.DATASERVER, resourceName, resourceState, null);
        setResourceProps(dsQueryResultProps);
        setResource(dataServer);
    }

    public void setResourceProps(TungstenProperties resourceProps)
    {
        super.setResourceProps(resourceProps);
        if (resourceProps != null)
        {
            if (resourceProps.getInt("is_read_only", "0", false) == 1)
            {
                setReadOnly(true);
            }
            else
            {
                setReadOnly(false);
            }
        }

    }

    public DataServer getDataServer()
    {
        return (DataServer) getResource();
    }

    public Exception getLastException()
    {
        return lastException;
    }

    public void setLastException(Exception lastException)
    {
        this.lastException = lastException;
    }

    public String display()
    {
        TungstenProperties dsQueryProps = getResourceProps();

        if (dsQueryProps == null)
        {
            return super.toString();
        }

        StringBuilder builder = new StringBuilder();
        builder.append(super.toString()).append("\n");
        builder.append("Query results:\n");
        builder.append(String.format("%-30s %-30s\n", "COLUMN", "VALUE"));
        builder.append(String.format("%-30s %-30s\n",
                "------------------------------",
                "------------------------------"));

        for (Object key : dsQueryProps.getProperties().keySet())
        {
            builder.append(String.format("%-30s %-30s\n", key.toString(),
                    dsQueryProps.getString(key.toString())));
        }

        return builder.toString();

    }

    public void setReadOnly(boolean readOnly)
    {
        this.readOnly = readOnly;
    }

    public boolean isReadOnly()
    {
        return readOnly;
    }

    public DataServerConditionMapping getConditionMapping()
    {
        return conditionMapping;
    }

    public void setConditionMapping(DataServerConditionMapping conditionMapping)
    {
        this.conditionMapping = conditionMapping;
    }

    public MappedAction getAction()
    {
        if (conditionMapping != null)
        {
            return conditionMapping.getAction();
        }

        return MappedAction.NONE;
    }

    public ResourceState getResourceState()
    {
        if (conditionMapping != null)
        {
            return conditionMapping.getState();
        }

        return ResourceState.UNKNOWN;
    }
}
