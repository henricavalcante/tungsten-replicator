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

package com.continuent.tungsten.common.patterns.notification.adaptor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.cluster.resource.notification.ClusterResourceNotification;
import com.continuent.tungsten.common.patterns.notification.NotificationGroupMember;
import com.continuent.tungsten.common.patterns.notification.ResourceNotificationException;
import com.continuent.tungsten.common.patterns.notification.ResourceNotificationListener;
import com.continuent.tungsten.common.patterns.notification.ResourceNotifier;

/**
 * This is a stub that demonstrates the components of a ResourceNotifier in
 * action.
 * 
 * @author <a href="mailto:edward.archibald@continuent.com">Ed Archibald</a>
 * @version 1.0
 */
public class ResourceNotifierStub implements ResourceNotifier
{

    private static Logger                            logger    = Logger.getLogger(ResourceNotifierStub.class);

    private volatile Boolean                         shutdown  = new Boolean(
                                                                       false);
    static private Thread                            monThread = null;
    private Collection<ResourceNotificationListener> listeners = new ArrayList<ResourceNotificationListener>();

    /**
     * @param argv
     */
    public static void main(String argv[])
    {

        ResourceNotifierStub adaptor = null;

        try
        {
            adaptor = new ResourceNotifierStub();
            monThread = new Thread(adaptor, adaptor.getClass().getSimpleName());
            monThread.setDaemon(true);
            monThread.start();
            // Wait for the monitor thread to exit....
            monThread.wait();
        }
        catch (InterruptedException i)
        {
            logger.info("Exiting after interruption....");
            System.exit(0);
        }

    }

    /**
     * @param listener
     */
    public void addListener(ResourceNotificationListener listener)
    {
        listeners.add(listener);
    }

    /**
     * @param notification
     */
    public void notifyListeners(ClusterResourceNotification notification)
            throws ResourceNotificationException
    {
        for (ResourceNotificationListener listener : listeners)
        {
            listener.notify(notification);
        }
    }

    /**
     ** Simulates the reception of a notification every 20 seconds.
     */
    public void run()
    {

        logger.debug("ResourceNotifierStub MONITOR: STARTED");

        while (!shutdown)
        {
            try
            {
                Thread.sleep(20000);

                // Do a notification here if you want to....
            }
            catch (Exception e)
            {
                System.err.println(e);
            }
        }
    }

    /**
     * 
     */
    public void shutdown()
    {
        synchronized (shutdown)
        {
            shutdown = false;
            shutdown.notify();
        }
    }

    public Map<String, NotificationGroupMember> getNotificationGroupMembers()
    {
        return new HashMap<String, NotificationGroupMember>();
    }

    public void prepare() throws Exception
    {
        

    }

}
