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
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.common.patterns.notification.adaptor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.cluster.resource.notification.ClusterResourceNotification;
import com.continuent.tungsten.common.patterns.notification.NotificationGroupMember;
import com.continuent.tungsten.common.patterns.notification.ResourceNotificationException;
import com.continuent.tungsten.common.patterns.notification.ResourceNotificationListener;
import com.continuent.tungsten.common.patterns.notification.ResourceNotifier;

/**
 * This class represents a means to receive monitoring information about
 * datasources that are pushed from a manager directly to the process that wants
 * to receive the notifications. This class, in effect, can act as a relay
 * between the manager and the host process.
 * 
 * @author <a href="mailto:edward.archibald@continuent.com">Ed Archibald</a>
 * @version 1.0
 */
public class ManagerNotificationAdaptor implements ResourceNotifier

{
    private static Logger                              logger        = Logger.getLogger(ManagerNotificationAdaptor.class);
    private Collection<ResourceNotificationListener>   listeners     = new ArrayList<ResourceNotificationListener>();

    private BlockingQueue<ClusterResourceNotification> notifications = new LinkedBlockingQueue<ClusterResourceNotification>();

    private static final int                           MAX_WARNINGS  = 100;

    /**
     * Add a listener that will be informed in the event of a resource
     * notification.
     * 
     * @param listener
     */
    public void addListener(ResourceNotificationListener listener)
    {
        if (listener == null)
        {
            logger.error("Attempting to add null listener");
        }
        listeners.add(listener);
    }

    /**
     * Deliver notification to listeners.
     * 
     * @param notification
     */
    public void notifyListeners(ClusterResourceNotification notification)
            throws ResourceNotificationException
    {
        if (notification == null)
            return;

        for (ResourceNotificationListener listener : listeners)
        {
            listener.notify(notification);
        }
    }

    /**
     * Run start the listener thread. It turns out that Group communications
     * don't really need a separate thread since the group comm adapters have
     * their own threads. So we start group comm instead.
     */
    public void run()
    {
        ClusterResourceNotification notification;
        int warningCount = 0;

        do
        {
            try
            {
                notification = notifications.take();

                notifyListeners(notification);
            }
            catch (InterruptedException i)
            {
                logger.warn("Interrupted while waiting for a notification");
                warningCount++;
            }
            catch (ResourceNotificationException r)
            {
                logger.warn("Exception while processing notifications" + r);
                warningCount++;
            }
        }
        while (warningCount < MAX_WARNINGS);

        logger.fatal("The maximum number of warnings has been exceeded. Exiting...");
        System.exit(1);
    }

    public void deliver(ClusterResourceNotification notification)
            throws ResourceNotificationException
    {
        try
        {
            notifications.put(notification);
        }
        catch (InterruptedException i)
        {
            logger.warn(String.format("Interupted while delivering %s",
                    notification));
        }
    }

    public Map<String, NotificationGroupMember> getNotificationGroupMembers()
    {
        return null;
    }

    public void prepare() throws Exception
    {
        

    }

}
