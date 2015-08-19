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
import java.util.List;

import junit.framework.TestCase;

import org.apache.log4j.Logger;

/**
 * Implements a unit test for time intervals, which are a Tungsten property
 * type.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class HostAddressServiceTest extends TestCase
{
    private static Logger logger     = Logger.getLogger(HostAddressServiceTest.class);

    // An IP address we hope is unknown. This is the IP address test range
    // 198.51.100.0/24 (aka TEST-NET-2 in RFC5735, used for sample code and
    // documentation). It should not be allocated.
    private static String UNKNOWN_IP = "198.51.100.100";

    /**
     * Verify ability to instantiate a host address service and find all ping
     * methods.
     */
    public void testAddressServiceInstantiation() throws Exception
    {
        HostAddressService has = new HostAddressService(true);
        List<String> methodNames = has.getEnabledMethodNames();
        assertTrue("Have at least one method", methodNames.size() > 0);
        for (String methodName : methodNames)
        {
            String pingMethod = has.getMethodName(methodName);
            assertNotNull("Checking method availability: " + methodName,
                    pingMethod);
        }
    }

    /**
     * Verify ability to ping local host using the default method.
     */
    public void testDefaultToLocalHost() throws Exception
    {
        HostAddressService has = new HostAddressService(true);
        has.setTimeout(3000);
        HostAddress address = HostAddressService.getByName(InetAddress
                .getLocalHost().getHostName());
        PingResponse response = has.isReachableByMethod(
                HostAddressService.DEFAULT, address);
        assertTrue("Can ping localhost", response.isReachable());
    }

    /**
     * Verify ability to ping local host using the OS ping method.
     */
    public void testOsPingToLocalHost() throws Exception
    {
        HostAddressService has = new HostAddressService(true);
        has.setTimeout(3000);
        HostAddress address = HostAddressService.getByName(InetAddress
                .getLocalHost().getHostName());
        PingResponse response = has.isReachableByMethod(
                HostAddressService.PING, address);
        assertTrue("Can ping localhost", response.isReachable());
    }

    /**
     * Verify that we cannot ping a non-existent Internet address.
     */
    public void testDefaultToUnknown() throws Exception
    {
        HostAddressService has = new HostAddressService(true);
        has.setTimeout(1500);
        HostAddress address = HostAddressService.getByName(UNKNOWN_IP);
        PingResponse response = has.isReachableByMethod(
                HostAddressService.DEFAULT, address);
        assertFalse("Cannot ping unknown address: " + address.toString(),
                response.isReachable());
    }

    /**
     * Verify that we cannot ping a non-existent Internet address.
     */
    public void testOsPingToUnknown() throws Exception
    {
        HostAddressService has = new HostAddressService(true);
        has.setTimeout(2000);
        HostAddress address = HostAddressService.getByName(UNKNOWN_IP);
        PingResponse response = has.isReachableByMethod(
                HostAddressService.PING, address);
        assertFalse("Cannot ping unknown address: " + address.toString(),
                response.isReachable());
    }

    /**
     * Verify that pinging a known host results in a notification for each
     * method used, which should be a single method in each case.
     */
    public void testSuccessNotifications() throws Exception
    {
        HostAddressService has = new HostAddressService(true);
        has.setTimeout(3000);
        HostAddress address = HostAddressService.getByName(InetAddress
                .getLocalHost().getHostName());

        // Ping with a single method.
        PingResponse response = has.isReachableByMethod(
                HostAddressService.PING, address);
        assertEquals("Direct method invocation", 1, response.getNotifications()
                .size());
        for (PingNotification notification : response.getNotifications())
        {
            logger.info("Notification: " + notification.toString());
        }

        // Ping with all methods. Only one should be needed.
        response = has.isReachable(address);
        assertEquals("General invocation", 1, response.getNotifications()
                .size());
        for (PingNotification notification : response.getNotifications())
        {
            logger.info("Notification: " + notification.toString());
        }
    }

    /**
     * Verify that pinging an unknown host results in a notification for each
     * available method, as all will be used.
     */
    public void testFailureNotifications() throws Exception
    {
        HostAddressService has = new HostAddressService(true);
        has.setTimeout(1000);
        HostAddress address = HostAddressService.getByName(UNKNOWN_IP);

        // Ping with all methods. All should be used.
        PingResponse response = has.isReachable(address);
        assertEquals("General invocation", has.getEnabledMethodNames().size(),
                response.getNotifications().size());
        for (PingNotification notification : response.getNotifications())
        {
            logger.info("Notification: " + notification.toString());
        }
    }

    /**
     * Verify that when a single ping method fails we get false as the response
     * with an exception in the notification. This also tests enabling of
     * non-default methods.
     */
    public void testMethodException() throws Exception
    {
        HostAddressService has = new HostAddressService(false);
        has.setTimeout(1000);
        HostAddress address = HostAddressService.getByName(InetAddress
                .getLocalHost().getHostName());
        has.addMethod("sample", SamplePingMethod.class.getName(), true);
        has.addMethod(HostAddressService.DEFAULT,
                InetAddressPing.class.getName(), true);

        // Show that a successful ping does not result in an exception.
        SamplePingMethod.exception = false;
        PingResponse response = has.isReachableByMethod("sample", address);
        assertTrue("Success invocation", response.isReachable());
        assertNull("Expect null exception on success", response
                .getNotifications().get(0).getException());

        // Show that a ping that blows up does result in an exception as well
        // as a failure overall.
        SamplePingMethod.exception = true;
        PingResponse response2 = has.isReachableByMethod("sample", address);
        assertFalse("Exception on invocation", response2.isReachable());
        assertNotNull("Expect exception on failures", response2
                .getNotifications().get(0).getException());

        // For good measure show that a method can blow up but a later method
        // can prove availability.
        SamplePingMethod.exception = true;
        PingResponse response3 = has.isReachable(address);
        assertTrue("Exception on invocation", response3.isReachable());
        assertNotNull("Expect exception on first method notification",
                response3.getNotifications().get(0).getException());
    }

    /**
     * Verify ability to perform ping requests in short order. Use all methods.
     */
    public void testPingPerformance() throws Exception
    {
        // Set up service.
        HostAddressService has = new HostAddressService(true);
        has.setTimeout(3000);
        HostAddress address = HostAddressService.getByName(InetAddress
                .getLocalHost().getHostName());
        String[] methods = {HostAddressService.DEFAULT, HostAddressService.PING};
        int count = 100;

        // Do successive pings using each method.
        for (int i = 0; i < methods.length; i++)
        {
            String method = methods[i];
            long startMillis = System.currentTimeMillis();
            for (int j = 0; j < count; j++)
            {
                PingResponse response = has
                        .isReachableByMethod(method, address);

                assertTrue("Can ping localhost: method=" + method
                        + " iteration=" + j, response.isReachable());
            }
            double duration = (System.currentTimeMillis() - startMillis) / 1000.0;
            logger.info("Completed ping using method: name=" + method
                    + " pings=" + count + " duration=" + duration);
        }
    }

    /**
     * Verify ability of multiple threads to issue localhost ping calls without
     * serializing.
     */
    public void testConcurrentLocalHostPing() throws Exception
    {
        // Set up service, an address to ping, and a number of iterations to
        // try.
        HostAddressService has = new HostAddressService(true);
        has.setTimeout(2000);
        HostAddress address = HostAddressService.getByName(InetAddress
                .getLocalHost().getHostName());

        // Test with one task and 200 pings.
        doConcurrentPing(has, address, 1, 200, true);

        // Test with 10 tasks with 20 pings each. This fails on Mac OS X, so
        // skip it on that platform.
        String os = System.getProperty("os.name");
        if ("Mac OS X".equals(os))
            logger.info("Skipping concurrent ping on Mac OS X platform...");
        else
            this.doConcurrentPing(has, address, 10, 20, true);
    }

    /**
     * Verify ability of multiple threads to issue localhost ping calls without
     * serializing even when the ping call is to an unknown host.
     */
    public void testConcurrentUnknownHostPing() throws Exception
    {
        // Set up service, an address to ping, and a number of iterations to
        // try.
        HostAddressService has = new HostAddressService(true);
        has.setTimeout(2000);
        HostAddress address = HostAddressService.getByName(UNKNOWN_IP);

        // Test 10 tasks with 2 pings each. This should take about 4
        // seconds per thread with full concurrency.
        this.doConcurrentPing(has, address, 10, 2, false);
    }

    /**
     * Positive and negative test of method to determine if a pair of host
     * addresses are equal.
     * 
     * @throws Exception
     */
    public void testHostAddressesAreEqual() throws Exception
    {
        String myHostName = InetAddress.getLocalHost().getHostName();
        String google = "www.google.com";

        assertTrue("Same host is equal",
                HostAddressService.addressesAreEqual(myHostName, myHostName));

        assertFalse("My host compared to google.com is not equal",
                HostAddressService.addressesAreEqual(google, myHostName));

    }

    /**
     * Tests for hosts on the same network and not on the same network.
     * 
     * @throws Exception
     */
    public void testHostAddressesAreInTheSameNetwork() throws Exception
    {
        String myHostName = InetAddress.getLocalHost().getHostName();
        String google = "www.google.com";
        short prefix = HostAddressService.getLocalNetworkPrefix(myHostName);

        assertTrue("Same host is on the same network",
                HostAddressService.addressesAreInSameSubnet(myHostName,
                        myHostName, prefix));

        assertFalse(
                "My host compared to google.com is not on the same network",
                HostAddressService.addressesAreInSameSubnet(google, myHostName,
                        prefix));

        try
        {
            HostAddressService.addressesAreInSameSubnet(myHostName, myHostName,
                    (short) 0);
            assertFalse("Should not get here...", true);
        }
        catch (Exception expected)
        {
            assertTrue("0 for prefix causes an exception", expected
                    .getLocalizedMessage().contains("Invalid prefix"));
        }

        try
        {
            assertFalse("Pass -1 for the prefix",
                    HostAddressService.addressesAreInSameSubnet(myHostName,
                            myHostName, (short) 0));
            assertFalse("Should not get here...", true);
        }
        catch (Exception expected)
        {
            assertTrue("1 for prefix causes an exception", expected
                    .getLocalizedMessage().contains("Invalid prefix"));
        }

    }

    // Private utility method to run concurrent ping test.
    private void doConcurrentPing(HostAddressService has, HostAddress address,
            int taskCount, int pingCount, boolean succeed)
            throws InterruptedException
    {
        // Start a group of threads
        long startMillis = System.currentTimeMillis();
        Thread[] threads = new Thread[taskCount];
        HostPingTask[] tasks = new HostPingTask[taskCount];
        for (int i = 0; i < threads.length; i++)
        {
            tasks[i] = new HostPingTask(has, address, pingCount);
            threads[i] = new Thread(tasks[i], "task-" + i);
            threads[i].start();
        }

        // Compute expected failures.
        int failures;
        if (succeed)
            failures = 0;
        else
            failures = pingCount;

        // Wait for threads to finish.
        int iterations = 0;
        for (int i = 0; i < threads.length; i++)
        {
            threads[i].join(60000);
            assertEquals(
                    "Checking task completed iterations: "
                            + threads[i].getName(), pingCount,
                    tasks[i].iterations);
            assertEquals("Checking task failures: " + threads[i].getName(),
                    failures, tasks[i].failures);
            assertTrue("Checking done-ness of task: " + threads[i].getName(),
                    tasks[i].done);
            iterations += tasks[i].iterations;
        }

        // Show duration and number of pings.
        double duration = (System.currentTimeMillis() - startMillis) / 1000.0;
        logger.info("Completed parallel ping: taskCount=" + tasks.length
                + " pingCount=" + pingCount + " actual pings=" + iterations
                + " succeed=" + succeed + " duration=" + duration);

    }
}

// Host ping task for concurrent testing.
class HostPingTask implements Runnable
{
    private static Logger    logger   = Logger.getLogger(HostPingTask.class);
    final HostAddressService has;
    final HostAddress        address;
    final int                count;

    volatile int             iterations;
    volatile boolean         done;
    volatile int             failures = 0;

    HostPingTask(HostAddressService has, HostAddress address, int count)
    {
        this.has = has;
        this.address = address;
        this.count = count;
    }

    // Execute ping operations count times, then exit.
    public synchronized void run()
    {
        iterations = 0;
        for (int i = 0; i < count; i++)
        {
            try
            {
                // PingResponse response = has.isReachable(address);
                PingResponse response = has.isReachableByMethod(
                        HostAddressService.DEFAULT, address);
                iterations++;
                if (!response.isReachable())
                    failures++;
            }
            catch (Throwable t)
            {
                logger.fatal(
                        "Host ping task failed: address=" + address.toString(),
                        t);
                break;
            }
        }
        done = true;
    }
}