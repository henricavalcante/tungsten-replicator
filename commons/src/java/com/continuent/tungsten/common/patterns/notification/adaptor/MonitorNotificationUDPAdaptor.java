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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.cluster.resource.notification.ClusterResourceNotification;
import com.continuent.tungsten.common.config.cluster.ClusterPolicyManagerConfiguration;
import com.continuent.tungsten.common.config.cluster.ConfigurationConstants;
import com.continuent.tungsten.common.config.cluster.ConfigurationException;
import com.continuent.tungsten.common.patterns.notification.NotificationGroupMember;
import com.continuent.tungsten.common.patterns.notification.ResourceNotificationException;
import com.continuent.tungsten.common.patterns.notification.ResourceNotificationListener;
import com.continuent.tungsten.common.patterns.notification.ResourceNotifier;

/**
 * This class represents a means to receive monitoring information about
 * datasources, in the form of TungstenProperties instances, via UDP.
 * 
 * @author <a href="mailto:edward.archibald@continuent.com">Ed Archibald</a>
 * @version 1.0
 */
public class MonitorNotificationUDPAdaptor implements ResourceNotifier
{

    private static Logger                            logger    = Logger.getLogger(MonitorNotificationUDPAdaptor.class);

    private DatagramSocket                           socket    = null;
    private ClusterPolicyManagerConfiguration        config    = null;
    private byte[]                                   buffer    = new byte[2048];
    private volatile Boolean                         shutdown  = new Boolean(
                                                                       false);
    private DatagramPacket                           packet    = null;
    static private Thread                            monThread = null;
    private Collection<ResourceNotificationListener> listeners = new ArrayList<ResourceNotificationListener>();

    /**
     * @param argv
     */
    public static void main(String argv[])
    {

        MonitorNotificationUDPAdaptor adaptor = null;

        try
        {
            adaptor = new MonitorNotificationUDPAdaptor(
                    ConfigurationConstants.CLUSTER_DEFAULT_NAME);
            monThread = new Thread(adaptor, adaptor.getClass().getSimpleName());
            monThread.setDaemon(true);
            monThread.start();
            // Wait for the monitor thread to exit....
            monThread.wait();
        }
        catch (NotificationAdaptorException r)
        {
            logger.error("Exception while initializing adaptor:" + r);
            System.exit(1);
        }
        catch (InterruptedException i)
        {
            logger.info("Exiting after interruption....");
            System.exit(0);
        }

    }

    /**
     * @param clusterName
     * @throws NotificationAdaptorException
     */
    public MonitorNotificationUDPAdaptor(String clusterName)
            throws NotificationAdaptorException
    {
        try
        {
            config = new ClusterPolicyManagerConfiguration(clusterName);
            config.load();
        }
        catch (ConfigurationException c)
        {
            logger.error("Unable to get configuration:" + c);
            throw new NotificationAdaptorException("Aborting adaptor startup");
        }

        try
        {
            // Create a socket to listen on the port.
            socket = new DatagramSocket(config.getNotifyPort());

            // Create a packet to receive data into the buffer
            packet = new DatagramPacket(buffer, buffer.length);
        }
        catch (SocketException s)
        {
            throw new NotificationAdaptorException(
                    "Unable to create a socket for datasource monitoring" + s);
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
     * Collect datagrams, each of which represents a specific datasource,
     * status, etc. and communicate this to the router manager.
     */
    public void run()
    {

        logger.debug("MonitorNotificationUDPAdaptor MONITOR: STARTED, listening on port="
                + config.getNotifyPort());

        while (!shutdown)
        {
            try
            {
                // Wait to receive a datagram
                socket.receive(packet);
                ClusterResourceNotification notification = getNotification();
                notifyListeners(notification);
                logger.debug("NOTIFICATION:"
                        + packet.getAddress().getHostName() + ": "
                        + notification);

                // Reset the length of the packet before reusing it.
                packet.setLength(buffer.length);
            }
            catch (Exception e)
            {
                System.err.println(e);
            }
        }
    }

    private ClusterResourceNotification getNotification()
    {

        try
        {
            ObjectInputStream in = new ObjectInputStream(
                    new ByteArrayInputStream(packet.getData()));

            ClusterResourceNotification notification = (ClusterResourceNotification) in
                    .readObject();
            in.close();
            return notification;

        }
        catch (ClassNotFoundException e)
        {
        }
        catch (IOException e)
        {
        }

        return null;
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
