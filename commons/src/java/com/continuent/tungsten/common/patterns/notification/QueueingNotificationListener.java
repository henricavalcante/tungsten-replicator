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

package com.continuent.tungsten.common.patterns.notification;

import java.util.*;
import java.util.concurrent.*;

import org.apache.log4j.*;

import com.continuent.tungsten.common.cluster.resource.notification.ClusterResourceNotification;

public class QueueingNotificationListener
        implements
            ResourceNotificationListener,
            Runnable
{
    private Thread                             runner        = null;
    private static Logger                      logger        = Logger.getLogger(QueueingNotificationListener.class);

    private BlockingQueue<Map<String, Object>> notifications = new SynchronousQueue<Map<String, Object>>();
    private String                             type          = null;

    public void init(String type)
    {
        // STUB
    }

    public void notify(ClusterResourceNotification notification)
            throws ResourceNotificationException
    {
        notify(notification);
    }

    /**
     * Put a notification on the appropriate queue
     */
    public void putNotification(Map<String, Object> notification)
            throws ResourceNotificationException
    {
        synchronized (notifications)
        {

            try
            {
                notifications.put(notification);
            }
            catch (InterruptedException i)
            {
                throw new ResourceNotificationException(
                        "Interrupted while trying to put notification of type="
                                + type);
            }
        }
    }

    /**
     * Gets the next notification in the queue
     */
    public Map<String, Object> getNotification()
            throws ResourceNotificationException
    {
        Map<String, Object> notification = null;

        synchronized (notifications)
        {

            try
            {
                notification = notifications.take();
            }
            catch (InterruptedException i)
            {
                throw new ResourceNotificationException(
                        "Interrupted while waiting for notification of type="
                                + type);
            }

            return notification;
        }
    }

    private void processNotifications() throws ResourceNotificationException
    {

        Map<String, Object> notification = null;

        /*
         * Because notifications are posted on a BlockingQueue, the
         * getNotification() method call will block, waiting for new
         * notifications and will then process them synchronously.
         */
        while ((notification = getNotification()) != null)
        {

            logger.info("PROCESSING NOTIFICATION=" + notification);

        }
    }

    public void run()
    {
        try
        {
            processNotifications();
        }
        catch (ResourceNotificationException r)
        {
            logger.error("Exception while processing notifications:" + r);
            return;
        }
    }

    public void start()
    {
        runner = new Thread(this);
        runner.start();

    }

}
