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

import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.utils.CLUtils;

/**
 * Implements a service for performing operations on Internet addresses, such as
 * testing liveness. This class is designed to make calls to ping hosts as
 * robust and as simple as possible, at the cost of a little more up-front
 * configuration in some cases, for example to set timeouts.
 * <p/>
 * This class is thread-safe through the use of synchronized methods to access
 * the method table and enabled names list. The timeout is volatile, which
 * obviates the need for synchronization.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class HostAddressService
{
    /** Logger for this class */
    private static final Logger           logger        = Logger.getLogger(HostAddressService.class);

    /** Default Java ping method using InetAddress.isReachable(). */
    public static String                  DEFAULT       = "default";

    /** Ping method using operating system ping command. */
    public static String                  PING          = "ping";

    /** Ping method using the echo server. */
    public static String                  ECHO          = "echo";

    // Ping methods are stored in a list as well as a hash index. The names list
    // contains only enabled methods. Access to these *must* be synchronized to
    // preserve thread safety.
    private List<String>                  names         = new LinkedList<String>();
    private ConcurrentMap<String, String> methods       = new ConcurrentHashMap<String, String>();

    // Timeout for ping operations in milliseconds.
    private volatile int                  timeoutMillis = 5000;

    /**
     * Creates a new service.
     * 
     * @param autoEnable If true, enable ping methods automatically.
     * @throws HostException Thrown if there is a problem enabling a method
     */
    public HostAddressService(boolean autoEnable) throws HostException
    {
        // Add known ping methods.
        addMethod(DEFAULT, InetAddressPing.class.getName(), autoEnable);
        addMethod(PING, OsUtilityPing.class.getName(), autoEnable);
        addMethod(ECHO, EchoPing.class.getName(), autoEnable);
    }

    /**
     * Sets the timeout for ping methods. Methods will try for up to this time
     * before giving up.
     * 
     * @param timeoutMillis Timeout in milliseconds
     */
    public void setTimeout(int timeoutMillis)
    {
        this.timeoutMillis = timeoutMillis;
    }

    /**
     * Returns current timeout in milliseconds.
     */
    public int getTimeout()
    {
        return timeoutMillis;
    }

    /**
     * Adds a ping method to the service.
     * 
     * @param name Logical name of the method
     * @param methodClass Method class name
     * @param enable If true, enable the method for use
     * @throws HostException Thrown if there is a problem enabling a method.
     */
    public synchronized void addMethod(String name, String methodClass,
            boolean enable) throws HostException
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("Adding ping method: name=" + name + " class="
                    + methodClass);
        }

        // Ensure that we can instantiate a ping method. This is a minimal
        // check to ensure the ping method will succeed.
        instantiatePingMethod(methodClass);

        // Add to table of available methods and optionally enable.
        methods.put(name, methodClass);
        if (enable)
            enableMethod(name);
    }

    /**
     * Enables a ping method.
     * 
     * @param name of method to enable
     * @throws HostException Thrown if method name does not exist
     */
    public synchronized void enableMethod(String name) throws HostException
    {
        String methodClass = methods.get(name);
        if (methodClass == null)
        {
            StringBuffer sb = new StringBuffer();
            for (String legalName : this.getAvailableMethodNames())
            {
                if (sb.length() > 0)
                    sb.append(",");
                sb.append(legalName);
            }
            throw new HostException(String.format(
                    "Unknown ping method name; legal values are (%s): %s",
                    sb.toString(), name));
        }
        else
        {
            if (!names.contains(name))
                names.add(name);
        }
    }

    /**
     * Returns names of available ping methods, whether enabled or not.
     */
    public synchronized List<String> getAvailableMethodNames()
    {
        Set<String> allNames = this.methods.keySet();
        return new ArrayList<String>(allNames);
    }

    /**
     * Returns names of available ping methods.
     */
    public synchronized List<String> getEnabledMethodNames()
    {
        return names;
    }

    /**
     * Returns a ping method by name or null if no such method exists.
     */
    public synchronized String getMethodName(String name)
    {
        return methods.get(name);
    }

    /** Returns a host address instance. */
    public static HostAddress getByName(String host)
            throws UnknownHostException
    {
        InetAddress inetAddress = InetAddress.getByName(host);
        HostAddress address = new HostAddress(inetAddress);
        return address;
    }

    /**
     * This method returns the host address, in string format, or UNKNOWN if
     * there's a problem resolving the address.
     * 
     * @param host
     * @return host address in string form or UNKNOWN
     */
    public static String getCanonicalAddress(String host)
    {
        try
        {
            return (getByName(host).getInetAddress().getHostAddress());
        }
        catch (Exception e)
        {
            return "UNKNOWN";
        }
    }

    /**
     * Given a pair of addresses and a single network prefix, determines if
     * hosts are on the same subnet.
     */
    public static boolean addressesAreInSameSubnet(String host1, String host2,
            short prefix) throws Exception
    {
        if (prefix <= 0)
        {
            throw new Exception("Invalid prefix " + prefix);
        }

        HostAddress host1Address = getByName(host1);
        HostAddress host2Address = getByName(host2);

        byte[] netMask = netMaskFromPrefixLength(prefix);

        byte[] host1Raw = host1Address.getAddress();
        byte[] host2Raw = host2Address.getAddress();

        for (int octet = 0; octet < 3; octet++)
        {
            if ((host1Raw[octet] & netMask[octet]) != (host2Raw[octet] & netMask[octet]))
                return false;
        }

        return true;
    }

    /**
     * This method returns true if the host addresses are equal to each other,
     * otherwise false.
     * 
     * @param host1
     * @param host2
     * @return true if host addresses match, otherwise false.
     */
    public static boolean addressesAreEqual(String host1, String host2)
    {
        try
        {

            HostAddress host1Address = getByName(host1);
            HostAddress host2Address = getByName(host2);

            byte[] host1Raw = host1Address.getAddress();
            byte[] host2Raw = host2Address.getAddress();

            for (int octet = 0; octet < 4; octet++)
            {
                if (host1Raw[octet] != host2Raw[octet])
                    return false;
            }

            return true;
        }
        catch (Exception e)
        {
            CLUtils.println(String.format(
                    "addressesAreEqual(%s, %s) returns FALSE, Exception=%s",
                    host1, host2, e));

            return false;
        }
    }

    /**
     * Returns true if the host is reachable by an available ping method. This
     * method clears previous notifications.
     * 
     * @param host Name of host for which we want to test reachability
     * @return True if host is reachable, otherwise false
     * @throws HostException Thrown if a ping method fails
     */
    public PingResponse isReachable(HostAddress host) throws HostException
    {

        // Compose a response.
        PingResponse response = new PingResponse();
        response.setReachable(false);

        // Try all methods.
        for (String name : this.getEnabledMethodNames())
        {
            PingNotification notification = _isReachableByMethod(name, host);
            response.addNotification(notification);
            response.setReachable(notification.isReachable());

            if (response.isReachable())
            {
                break;
            }
        }

        // Return the response;
        return response;
    }

    /**
     * Returns true if the host is reachable by an available ping method. This
     * method clears previous notifications.
     * 
     * @param name Name of ping method to use
     * @param host Name of host for which we want to test reachability
     * @return True if host is reachable, otherwise false
     * @throws HostException Thrown if a ping method fails
     */
    public PingResponse isReachableByMethod(String name, HostAddress host)
            throws HostException
    {
        PingResponse response = new PingResponse();
        PingNotification notification = _isReachableByMethod(name, host);
        response.addNotification(notification);
        response.setReachable(notification.isReachable());
        return response;
    }

    // Private method to check reachability without clearning notifications.
    public PingNotification _isReachableByMethod(String name, HostAddress host)
            throws HostException
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("Testing host reachability: method=" + name + " host="
                    + host.toString() + " timeout=" + timeoutMillis);
        }
        String methodClass = getMethodName(name);
        if (name == null)
        {
            throw new HostException("Unknown ping method: " + name);
        }
        else
        {
            // Set up the notification to be used in the response.
            PingNotification notification = new PingNotification();
            notification.setHostName(host.getCanonicalHostName());
            notification.setMethodName(name);
            notification.setTimeout(timeoutMillis);

            long startMillis = System.currentTimeMillis();
            PingMethod method = null;
            try
            {
                // Instantiate the ping method and prepare it for use.
                method = instantiatePingMethod(methodClass);

                // Make the call.
                boolean status = method.ping(host, timeoutMillis);

                // Fill in missing ping information.
                notification.setReachable(status);
            }
            catch (Exception e)
            {
                // Fill in notification information for an exception.
                notification.setReachable(false);
                notification.setException(e);
            }
            finally
            {
                long duration = System.currentTimeMillis() - startMillis;
                notification.setDuration(duration);
                notification.setNotes(method.getNotes());
            }

            // Return the completed notification.
            return notification;
        }
    }

    // Instantiates and returns a ping method instance.
    private PingMethod instantiatePingMethod(String methodClass)
            throws HostException
    {
        try
        {
            PingMethod method = (PingMethod) Class.forName(methodClass)
                    .newInstance();
            return method;
        }
        catch (Throwable e)
        {
            String msg = String
                    .format("Unexpected failure while instantiating ping method: name=%s class=%s",
                            methodClass, methodClass);
            throw new HostException(msg, e);
        }

    }

    /**
     * This method returns a prefix for a given internet address. It will only
     * work on the host for which the address is bound.
     */
    public static short getLocalNetworkPrefix(String hostName) throws Exception
    {
        InetAddress memberAddr = getByName(hostName).getInetAddress();
        try
        {
            Enumeration<NetworkInterface> nets = NetworkInterface
                    .getNetworkInterfaces();

            for (NetworkInterface netint : Collections.list(nets))
            {
                for (InetAddress inetAddress : Collections.list(netint
                        .getInetAddresses()))
                {
                    if (inetAddress.equals(memberAddr))
                    {
                        List<InterfaceAddress> addresses = netint
                                .getInterfaceAddresses();

                        if (addresses == null || addresses.size() == 0)
                        {
                            return -1;
                        }

                        if (addresses.size() == 1)
                        {
                            return addresses.get(0).getNetworkPrefixLength();
                        }
                        else
                        {
                            return netint.getInterfaceAddresses().get(1)
                                    .getNetworkPrefixLength();
                        }

                    }

                }
            }
        }
        catch (Exception e)
        {
            logger.error(String.format(
                    "Unable to determine network interface for address %s",
                    memberAddr), e);

        }

        return -1;
    }

    public static byte[] netMaskFromPrefixLength(short prefix)
    {
        if (prefix < 0)
        {
            return null;
        }

        int mask = 0xffffffff << (32 - prefix);
        int value = mask;
        byte[] maskBytes = new byte[]{(byte) (value >>> 24),
                (byte) (value >> 16 & 0xff), (byte) (value >> 8 & 0xff),
                (byte) (value & 0xff)};

        return maskBytes;

    }
}