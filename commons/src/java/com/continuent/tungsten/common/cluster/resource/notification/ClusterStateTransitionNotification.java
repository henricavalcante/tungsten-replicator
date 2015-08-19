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
import com.continuent.tungsten.common.patterns.order.Sequence;

public class ClusterStateTransitionNotification
        extends ClusterResourceNotification
{

    private Sequence                    sequence;
    private ClusterStateTransitionPhase phase            = ClusterStateTransitionPhase.undefined;

    /**
     * 
     */
    private static final long           serialVersionUID = 1L;

    /**
     * @param clusterName
     * @param memberName
     * @param notificationSource
     * @param resourceName
     * @param resourceState
     * @param resourceProps
     */
    public ClusterStateTransitionNotification(String clusterName,
            String memberName, String notificationSource, String resourceName,
            ResourceState resourceState, TungstenProperties resourceProps,
            ClusterStateTransitionPhase phase, Sequence sequence)
    {
        super(NotificationStreamID.MONITORING, clusterName, memberName,
                notificationSource, ResourceType.CONFIGURATION, resourceName,
                resourceState, resourceProps);

        this.phase = phase;
        this.sequence = sequence;
    }

    /**
     * Returns the sequence value.
     * 
     * @return Returns the sequence.
     */
    public Sequence getSequence()
    {
        return sequence;
    }

    /**
     * Sets the sequence value.
     * 
     * @param sequence The sequence to set.
     */
    public void setSequence(Sequence sequence)
    {
        this.sequence = sequence;
    }

    /**
     * Returns the phase value.
     * 
     * @return Returns the phase.
     */
    public ClusterStateTransitionPhase getPhase()
    {
        return phase;
    }

    /**
     * Sets the phase value.
     * 
     * @param phase The phase to set.
     */
    public void setPhase(ClusterStateTransitionPhase phase)
    {
        this.phase = phase;
    }

}
