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
 * Initial developer(s): Robert Hodges
 * Contributor(s): 
 */

package com.continuent.tungsten.common.network;

import java.util.LinkedList;
import java.util.List;

/**
 * Contains the response from pinging a host.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class PingResponse
{
    private boolean                reachable;
    private List<PingNotification> notifications = new LinkedList<PingNotification>();

    /** Returns true if the method determined the host was reachable. */
    public boolean isReachable()
    {
        return reachable;
    }

    public void setReachable(boolean reachable)
    {
        this.reachable = reachable;
    }

    /** Returns notifications from ping methods. */
    public List<PingNotification> getNotifications()
    {
        return notifications;
    }

    /**
     * Adds a ping notification to the current list.
     */
    public void addNotification(PingNotification notification)
    {
        notifications.add(notification);
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        StringBuffer sb = new StringBuffer("Ping Response: ");
        sb.append(" reachable=").append(reachable);
        return sb.toString();
    }
}