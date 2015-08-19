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

package com.continuent.tungsten.common.server.test;

import javax.management.remote.JMXConnector;

import junit.framework.TestCase;

import com.continuent.tungsten.common.jmx.JmxManager;

/**
 * A simple unit test of the JMX manager class. This also demonstrates how to
 * set up JMX, register MBeans, etc.
 *
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class JmxManagerTDisabledBecauseItFailsEveryNowAndThen extends TestCase
{
    private String host        = "localhost";
    private int    port        = 1199;
    private String serviceName = "testBasic";

    /**
     * Demonstrate how to start and stop a JMX manager. We do it several times
     * to provide that JMX clean-up works.
     *
     * @throws Exception
     */
    public void testStartStop() throws Exception
    {
        for (int i = 0; i < 3; i++)
        {
            System.out.println(String.format(
                    "Creating service, host=%s, port=%d, serviceName=%s", host,
                    port, serviceName));
            JmxManager jmx = new JmxManager(host, port, serviceName);
            System.out.println(String.format(
                    "Starting service, host=%s, port=%d, serviceName=%s", host,
                    port, serviceName));
            jmx.start();
            jmx.stop();
        }
    }

    /**
     * Demonstrate that we can connect to the JMX manager with a client and
     * invoke an operation on an MBean.
     */
    public void testConnection()
    {
        // Start JMX.
        System.out.println(String.format(
                "Creating service, host=%s, port=%d, serviceName=%s", host,
                port, serviceName));
        JmxManager jmx = new JmxManager(host, port, serviceName);
        jmx.start();

        // Create an mbean implementation.
        Trial mbean = new Trial();

        // Register a bean.
        JmxManager.registerMBean(mbean, Trial.class);

        System.out.println(String.format(
                "Connecting to service, host=%s, port=%d, serviceName=%s", host,
                port, serviceName));
        // Connect to the JMX server and get a stub for this MBean.
        JMXConnector connector = JmxManager.getRMIConnector(host, port,
                serviceName);
        assertNotNull("Checking that connector is not null", connector);

        System.out.println(String.format(
                "Getting the mbean for class=%s", Trial.class.getName()));
        TrialMBean mbeanProxy = (TrialMBean) JmxManager.getMBeanProxy(
                connector, Trial.class, true);
        assertNotNull("Checking that proxy is not null", mbeanProxy);

        // Set and check count values via the MBean.
        mbeanProxy.setBeanCounter(14);
        assertEquals("Bean count set to 14 via proxy", 14, mbean.beanCounter);
        mbeanProxy.setBeanCounter(22);
        assertEquals("Bean count set to 22 via proxy", 22, mbean.beanCounter);

        // Stop the server.
        jmx.stop();
    }
}
