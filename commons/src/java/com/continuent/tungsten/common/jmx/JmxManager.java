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
 * Contributor(s): Edward Archibald, Stephane Giron
 */

package com.continuent.tungsten.common.jmx;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.NoSuchObjectException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.text.MessageFormat;
import java.util.HashMap;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MBeanServerNotification;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.rmi.RMIConnectorServer;
import javax.net.ssl.SSLHandshakeException;
import javax.rmi.ssl.SslRMIClientSocketFactory;
import javax.rmi.ssl.SslRMIServerSocketFactory;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.config.cluster.ConfigurationException;
import com.continuent.tungsten.common.security.AuthenticationInfo;
import com.continuent.tungsten.common.security.RealmJMXAuthenticator;

/**
 * Encapsulates JMX server start/stop and provides static utility methods to
 * register MBeans on the server side as well as get proxies for them on the
 * client side.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class JmxManager implements NotificationListener
{
    private static final Logger       logger              = Logger.getLogger(JmxManager.class);

    // RMI registry and connector server we are managing.
    protected Registry                rmiRegistry;
    protected JMXConnectorServer      jmxConnectorServer;
  
    // JMX server parameters.
    private final String              host;
    private final int                 registryPort;
    private static int                beanPort;

    private final String              serviceName;

    public final static String        CREATE_MBEAN_HELPER = "createHelper";

    // Authentication and Encryption parameters
    private static AuthenticationInfo authenticationInfo  = null;

    /**
     * Creates an instance to manage a JMX service
     * 
     * @param host The host name or IP to use
     * @param beanPort
     * @param registryPort The JMX server RMI registryPort
     * @param serviceName The JMX service name
     */
    public JmxManager(String host, int beanPort, int registryPort,
            String serviceName)
    {
        this.host = host;
        this.registryPort = registryPort;
        this.serviceName = serviceName;
        JmxManager.beanPort = beanPort;
    }

    /**
     * Creates an instance to manage a JMX service
     * 
     * @param host The host name or IP to use
     * @param registryPort The JMX server RMI registryPort
     * @param serviceName The JMX service name
     */
    public JmxManager(String host, int registryPort, String serviceName)
    {
        this(host, registryPort + 1, registryPort, serviceName);
    }

    /**
     * Creates an instance to manage a JMX service Called when using
     * authentication (and) encryption
     * 
     * @see <a
     *      href="http://download.java.net/jdk8/docs/technotes/guides/jmx/tutorial/security.html">JMX
     *      Tutorial on Security</a>
     * @param host
     * @param registryPort
     * @param serviceName
     * @param tungstenProperty
     */
    public JmxManager(String host, int registryPort, String serviceName,
            TungstenProperties tungstenProperty)
    {
        this(host, registryPort, serviceName);

        // Authentication and encryption parameters
        if (tungstenProperty != null)
            authenticationInfo = (AuthenticationInfo) tungstenProperty
                    .getObject(AuthenticationInfo.AUTHENTICATION_INFO_PROPERTY,
                            null, false);
    }

    /**
     * Starts the JXM server.
     */
    public synchronized void start()
    {
        createRegistry(registryPort);
        startJmxConnector();
    }

    /**
     * Stops the JXM server.
     */
    public synchronized void stop()
    {
        stopRMI();
        stopJmxConnector();
    }

    protected Registry locateDefaultRegistry()
    {

        Registry registry = null;

        try
        {
            registry = LocateRegistry.getRegistry();
        }
        catch (Exception r)
        {
            throw new ServerRuntimeException(
                    String.format(
                            "Unable to locate the default registry on registryPort 1099, reason='%s'",
                            r.getMessage()));
        }

        return registry;
    }

    /**
     * Starts the rmi registry.
     */
    protected void createRegistry(int port)
    {
        // Create a registry if we don't already have one.
        if (rmiRegistry == null)
        {
            try
            {
                if (logger.isDebugEnabled())
                {
                    logger.debug("Starting RMI registry on registryPort: "
                            + port);
                }
                rmiRegistry = LocateRegistry.createRegistry(port);
            }
            catch (Throwable e)
            {
                throw new ServerRuntimeException(
                        "Unable to start rmi registry on registryPort: " + port,
                        e);
            }

        }
    }

    /**
     * Deallocates the RMI registry.
     */
    protected void stopRMI()
    {
        if (rmiRegistry != null)
        {
            try
            {
                UnicastRemoteObject.unexportObject(rmiRegistry, true);
            }
            catch (NoSuchObjectException e)
            {
                logger.warn(
                        "Unexpected error while shutting down RMI registry", e);
            }
            rmiRegistry = null;
        }
    }

    /**
     * Starts the JMX connector for the server.
     */
    protected void startJmxConnector()
    {
        String serviceAddress = null;
        try
        {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

            serviceAddress = generateServiceAddress(host, beanPort,
                    registryPort, serviceName);
            JMXServiceURL address = new JMXServiceURL(serviceAddress);

            // --- Define security attributes ---
            HashMap<String, Object> env = new HashMap<String, Object>();

            // --- Authentication based on password and access files---
            if (authenticationInfo != null
                    && authenticationInfo.isAuthenticationNeeded())
            {

                if (authenticationInfo.isUseTungstenAuthenticationRealm())
                    env.put(JMXConnectorServer.AUTHENTICATOR,
                            new RealmJMXAuthenticator(authenticationInfo));
                else
                    env.put("jmx.remote.x.password.file",
                            authenticationInfo.getPasswordFileLocation());

                env.put("jmx.remote.x.access.file",
                        authenticationInfo.getAccessFileLocation());
            }

            // --- SSL encryption ---
            if (authenticationInfo != null
                    && authenticationInfo.isEncryptionNeeded())
            {
                // Keystore
                System.setProperty("javax.net.ssl.keyStore",
                        authenticationInfo.getKeystoreLocation());
                System.setProperty("javax.net.ssl.keyStorePassword",
                        authenticationInfo.getKeystorePassword());
                // Configure SSL
                SslRMIClientSocketFactory csf = new SslRMIClientSocketFactory();
                SslRMIServerSocketFactory ssf = new SslRMIServerSocketFactory();
                env.put(RMIConnectorServer.RMI_CLIENT_SOCKET_FACTORY_ATTRIBUTE,
                        csf);
                env.put(RMIConnectorServer.RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE,
                        ssf);
            }

            env.put(RMIConnectorServer.JNDI_REBIND_ATTRIBUTE, "true");
            JMXConnectorServer connector = JMXConnectorServerFactory
                    .newJMXConnectorServer(address, env, mbs);
            connector.start();

            logger.info(MessageFormat
                    .format("JMXConnector: security.propoerties={0} \n\t use.authentication={1} \n\t use.tungsten.authenticationRealm.encrypted.password={2} \n\t use.encryption={3}",
                            (authenticationInfo != null) ? authenticationInfo.getParentPropertiesFileLocation() : "No security.propoerties file found !...",
                            (authenticationInfo != null) ? authenticationInfo.isAuthenticationNeeded() : false,
                            (authenticationInfo != null) ? authenticationInfo.isUseEncryptedPasswords(): false,
                            (authenticationInfo != null) ? authenticationInfo.isEncryptionNeeded() : false));
            logger.info(String.format("JMXConnector started at address %s",
                    serviceAddress));

            jmxConnectorServer = connector;
        }
        catch (Throwable e)
        {
            throw new ServerRuntimeException("Unable to create RMI listener:"
                    + getServiceProps(), e);
        }
    }

    private String getServiceProps()
    {
        return ("RMI {host=" + host + ", registryPort=" + registryPort
                + ", service=" + serviceName + "}");
    }

    /**
     * Stops the JMX connector if it is running.
     */
    protected void stopJmxConnector()
    {
        // Shut down the JMX server.
        try
        {
            if (jmxConnectorServer != null)
                jmxConnectorServer.stop();
        }
        catch (IOException e)
        {
            logger.warn("Unexpected error while shutting down JMX server", e);
        }
    }

    /**
     * Server helper method to register a JMX MBean. MBeans are registered by a
     * combination of their MBean interface and the custom mbeanName argument.
     * The mbeanName permits multiple mBeans to be registered under the same
     * name.
     * 
     * @param mbean The MBean instance that should be registered
     * @param mbeanInterface The MBean interface this instance implements
     * @param mbeanName A custom name for this MBean
     * @throws ServerRuntimeException
     */
    public static void registerMBean(Object mbean, Class<?> mbeanInterface,
            String mbeanName, boolean ignored)
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            if (logger.isDebugEnabled())
                logger.debug("Registering mbean: " + mbean.getClass());

            ObjectName name = generateMBeanObjectName(mbeanInterface.getName(),
                    mbeanName);

            if (mbs.isRegistered(name))
                mbs.unregisterMBean(name);
            mbs.registerMBean(mbean, name);
        }
        catch (Exception e)
        {
            throw new ServerRuntimeException("Unable to register mbean: class="
                    + mbean.getClass() + " interface=" + mbeanInterface
                    + " name=" + mbeanName, e);

        }
    }

    /**
     * Server helper method to register a JMX MBean. MBeans are registered by a
     * combination of their MBean interface and the custom mbeanName argument.
     * The mbeanName permits multiple mBeans to be registered under the same
     * name.
     * 
     * @param mbean The MBean instance that should be registered
     * @param mbeanClass The base class for the mbean
     * @throws ServerRuntimeException
     */
    public static void registerMBean(Object mbean, Class<?> mbeanClass)
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            if (logger.isDebugEnabled())
                logger.debug("Registering mbean: " + mbean.getClass());

            ObjectName name = generateMBeanObjectName(mbeanClass);

            if (mbs.isRegistered(name))
                mbs.unregisterMBean(name);
            mbs.registerMBean(mbean, name);
        }
        catch (Exception e)
        {
            throw new ServerRuntimeException(String.format(
                    "Unable to register mbean for class %s because '%s'",
                    mbeanClass.getName(), e), e);

        }
    }

    /**
     * Server helper method to register a JMX MBean. MBeans are registered by a
     * combination of their MBean interface and the custom mbeanName argument.
     * The mbeanName permits multiple mBeans to be registered under the same
     * name.
     * 
     * @param mbeanInterface The MBean interface this instance implements
     * @param mbeanName A custom name for this MBean
     * @throws ServerRuntimeException
     */
    public static void unregisterMBean(Class<?> mbeanInterface, String mbeanName)
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            ObjectName name = generateMBeanObjectName(mbeanInterface.getName(),
                    mbeanName);
            if (mbs.isRegistered(name))
            {
                logger.info("Unregistering mbean: " + name.toString());
                mbs.unregisterMBean(name);
            }
            else
            {
                logger.warn("Ignoring attempt to unregister unknown mbean: "
                        + name.toString());
            }
        }
        catch (Exception e)
        {
            throw new ServerRuntimeException(
                    "Unable to unregister mbean: interface=" + mbeanInterface
                            + " name=" + mbeanName, e);

        }
    }

    /**
     * Server helper method to register a JMX MBean. MBeans are registered by a
     * combination of their MBean interface and the custom mbeanName argument.
     * The mbeanName permits multiple mBeans to be registered under the same
     * name.
     * 
     * @param mbeanInterface The MBean interface this instance implements
     * @throws ServerRuntimeException
     */
    public static void unregisterMBean(Class<?> mbeanInterface)
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            ObjectName name = generateMBeanObjectName(mbeanInterface);
            if (mbs.isRegistered(name))
            {
                logger.info("Unregistering mbean: " + name.toString());
                mbs.unregisterMBean(name);
            }
            else
            {
                logger.warn("Ignoring attempt to unregister unknown mbean: "
                        + name.toString());
            }
        }
        catch (Exception e)
        {
            throw new ServerRuntimeException(
                    "Unable to unregister mbean: interface=" + mbeanInterface,
                    e);

        }
    }

    /**
     * Client helper method to return an RMI connection. The arguments match
     * those used when instantiating the JmxManager class itself.
     * 
     * @param host the hostname to bind to in the jmx url
     * @param registryPort the registryPort number to bind to in the jmx url
     * @param serviceName the JMX service name
     * @return a connection to the server
     */
    public static JMXConnector getRMIConnector(String host, int registryPort,
            String serviceName)
    {
        return getRMIConnector(host, registryPort, serviceName, null);
    }

    /**
     * Client helper method to return an RMI connection. The arguments match
     * those used when instantiating the JmxManager class itself.
     * 
     * @param host the hostname to bind to in the jmx url
     * @param registryPort the registryPort number to bind to in the jmx url
     * @param serviceName the JMX service name
     * @param jmxProperties TungstenProperties holding the AuthenticationInfo
     *            instance.
     * @return a connection to the server
     */
    public static JMXConnector getRMIConnector(String host, int registryPort,
            String serviceName, TungstenProperties jmxProperties)
    {
        String serviceAddress = null;
        try
        {
            // --- Retrieve jmx Properties for Authentication ---
            AuthenticationInfo authInfo = null;
            if (jmxProperties != null)
                authInfo = (AuthenticationInfo) jmxProperties.getObject(
                        AuthenticationInfo.AUTHENTICATION_INFO_PROPERTY, null,
                        false);
            if (authInfo == null) // Last chance: try the static member
                authInfo = authenticationInfo;

            // --- Define security attributes ---
            HashMap<String, Object> env = new HashMap<String, Object>();

            // --- Authentication based on password and access files---
            if (authInfo != null && authInfo.isAuthenticationNeeded())
            {
                // Build credentials
                String[] credentials;
                if (authInfo.isUseTungstenAuthenticationRealm())
                    credentials = new String[]{authInfo.getUsername(),
                            authInfo.getDecryptedPassword(),
                            AuthenticationInfo.TUNGSTEN_AUTHENTICATION_REALM};
                else
                    credentials = new String[]{authInfo.getUsername(),
                            authInfo.getDecryptedPassword()};

                env.put("jmx.remote.credentials", credentials);
            }
            // --- SSL ---
            if (authInfo != null && authInfo.isEncryptionNeeded())
            {
                // Truststore
                System.setProperty("javax.net.ssl.trustStore",
                        authInfo.getTruststoreLocation());
                System.setProperty("javax.net.ssl.trustStorePassword",
                        authInfo.getTruststorePassword());
            }

            serviceAddress = generateServiceAddress(host, beanPort,
                    registryPort, serviceName);
            JMXServiceURL address = new JMXServiceURL(serviceAddress);
            JMXConnector connector = JMXConnectorFactory.connect(address, env);
            return connector;
        }
        catch (Exception e)
        {
            // --- Try to get more details on the connection problem
            String errorMessage = String
                    .format("Cannot establish a connection with component '%s' at address %s:%d\n",
                            serviceName, host, registryPort);
            String errorReason = null;
            AssertionError assertionError = null;

            // Authentication required by server
            if (e instanceof SecurityException)
            {
                errorReason = String.format("Reason=%s\n", e.toString());
                assertionError = new AssertionError(
                        "Authentication required by server");
            }
            // Encryption required by server
            else if (e.getCause() instanceof SSLHandshakeException)
            {
                errorReason = String
                        .format("Reason="
                                + "javax.net.ssl.SSLHandshakeException: Server requires SSL.\n");
                assertionError = new AssertionError(
                        "Encryption required by server");
            }
            else if (e instanceof ConfigurationException)
            {
                errorMessage = e.getMessage();
                assertionError = new AssertionError("Configuration error");
            }
            // Other IOException
            else if (e instanceof IOException)
            {
                errorMessage = String
                        .format("A component of type '%s' at address %s:%d is not available.\n %s\n",
                                serviceName, host, registryPort, e);
                errorReason = "Check to be sure that the service is running.\n";
            }

            if (logger.isDebugEnabled())
            {
                logger.debug(String.format(errorMessage + errorReason), e);
            }

            throw new ServerRuntimeException(String.format(errorMessage
                    + errorReason), (assertionError != null)
                    ? assertionError
                    : e);
        }
    }

    /**
     * Client helper method to obtain a proxy that implements the given
     * interface by forwarding its methods through the given MBean server to the
     * named MBean.
     * 
     * @param clientConnection the MBean server to forward to
     * @param mbeanClass The MBean interface this instance implements
     * @param mbeanName A custom name for this MBean
     * @param notificationBroadcaster If true make the returned proxy implement
     *            NotificationEmitter by forwarding its methods via connection
     * @return An MBean proxy
     */
    public static Object getMBeanProxy(JMXConnector clientConnection,
            Class<?> mbeanClass, String mbeanName,
            boolean notificationBroadcaster, boolean ignored)
    {
        try
        {

            ObjectName objectName = generateMBeanObjectName(
                    mbeanClass.getName(), mbeanName);

            return MBeanServerInvocationHandler.newProxyInstance(
                    clientConnection.getMBeanServerConnection(), objectName,
                    mbeanClass, notificationBroadcaster);
        }
        catch (Exception e)
        {
            throw new ServerRuntimeException(
                    "Unable to get proxy connection to bean", e);
        }
    }

    /**
     * Client helper method to obtain a proxy that implements the given
     * interface by forwarding its methods through the given MBean server to the
     * named MBean.
     * 
     * @param clientConnection the MBean server to forward to
     * @param mbeanClass The class for which an MBean exists
     * @param notificationBroadcaster If true make the returned proxy implement
     *            NotificationEmitter by forwarding its methods via connection
     * @return An MBean proxy
     */
    public static Object getMBeanProxy(JMXConnector clientConnection,
            Class<?> mbeanClass, boolean notificationBroadcaster)
    {
        String mbeanInterfaceClassName = mbeanClass.getName() + "MBean";
        Class<?> mbeanInterfaceClass = null;

        try
        {
            mbeanInterfaceClass = Class.forName(mbeanInterfaceClassName);
        }
        catch (ClassNotFoundException c)
        {
            throw new ServerRuntimeException(
                    String.format(
                            "Cannot get an RMI proxy for class %s because the interface class %s was not found",
                            mbeanClass.getName(), mbeanInterfaceClassName));
        }

        try
        {
            ObjectName objectName = generateMBeanObjectName(mbeanClass);

            return MBeanServerInvocationHandler.newProxyInstance(
                    clientConnection.getMBeanServerConnection(), objectName,
                    mbeanInterfaceClass, notificationBroadcaster);
        }
        catch (Exception e)
        {
            throw new ServerRuntimeException(
                    String.format(
                            "Cannot get an RMI proxy for class %s because of this exception: %s",
                            mbeanClass.getName(), e), e);
        }
    }

    /**
     * Client helper method to obtain a proxy that implements the given
     * interface by forwarding its methods through the given MBean server to the
     * named MBean.
     * 
     * @param clientConnection the MBean server to forward to
     * @param mbeanClass The MBean interface this instance implements
     * @param mbeanName A custom name for this MBean
     * @param notificationBroadcaster If true make the returned proxy implement
     *            NotificationEmitter by forwarding its methods via connection
     * @return An MBean proxy
     */
    public static Object getMBeanProxy(JMXConnector clientConnection,
            Class<?> mbeanClass, Class<?> mbeanInterface, String mbeanName,
            boolean notificationBroadcaster, boolean ignored)
    {
        try
        {

            ObjectName objectName = generateMBeanObjectName(
                    mbeanClass.getName(), mbeanName);

            return MBeanServerInvocationHandler.newProxyInstance(
                    clientConnection.getMBeanServerConnection(), objectName,
                    mbeanInterface, notificationBroadcaster);
        }
        catch (Exception e)
        {
            throw new ServerRuntimeException(
                    "Unable to get proxy connection to bean", e);
        }
    }

    /**
     * Attach NotificationListener that can be used to listen notifications
     * emitted by MBean server.
     * 
     * @param jmxConnector The MBean server connector.
     * @param mbeanInterface The MBean interface this instance implements.
     * @param mbeanName A custom name for the MBean.
     * @param notificationListener User provided NotificationListener instance.
     * @throws InstanceNotFoundException
     * @throws Exception
     */
    static public void addNotificationListener(JMXConnector jmxConnector,
            Class<?> mbeanInterface, String mbeanName,
            NotificationListener notificationListener, boolean ignored)
            throws InstanceNotFoundException, Exception
    {
        MBeanServerConnection mbsc = jmxConnector.getMBeanServerConnection();
        ObjectName objectName = generateMBeanObjectName(
                mbeanInterface.getName(), mbeanName);
        mbsc.addNotificationListener(objectName, notificationListener, null,
                null);
    }

    public static MBeanServerConnection getServerConnection(
            JMXConnector jmxConnector) throws Exception
    {
        return jmxConnector.getMBeanServerConnection();
    }

    /**
     * Attach NotificationListener that can be used to listen notifications
     * emitted by MBean server.
     * 
     * @param jmxConnector The MBean server connector.
     * @param mbeanClass The class for which an MBean exists.
     * @param notificationListener User provided NotificationListener instance.
     * @throws InstanceNotFoundException
     * @throws Exception
     */
    static public void addNotificationListener(JMXConnector jmxConnector,
            Class<?> mbeanClass, NotificationListener notificationListener)
            throws InstanceNotFoundException, Exception
    {
        MBeanServerConnection mbsc = jmxConnector.getMBeanServerConnection();
        ObjectName objectName = generateMBeanObjectName(mbeanClass);
        mbsc.addNotificationListener(objectName, notificationListener, null,
                null);
    }

    /**
     * Remove NotificationListener from this MBean.
     * 
     * @param jmxConnector The MBean server connector.
     * @param mbeanInterface The MBean interface this instance implements.
     * @param mbeanName A custom name for the MBean.
     * @param notificationListener Previously added NotificationListener
     *            instance.
     * @throws Exception
     */
    static public void removeNotificationListener(JMXConnector jmxConnector,
            Class<?> mbeanInterface, String mbeanName,
            NotificationListener notificationListener, boolean ignored)
            throws Exception
    {
        MBeanServerConnection mbsc = jmxConnector.getMBeanServerConnection();
        ObjectName objectName = generateMBeanObjectName(
                mbeanInterface.getName(), mbeanName);
        mbsc.removeNotificationListener(objectName, notificationListener);
    }

    /**
     * Remove NotificationListener from this MBean.
     * 
     * @param jmxConnector The MBean server connector.
     * @param mbeanClass The class for which an MBean exists.
     * @param notificationListener Previously added NotificationListener
     *            instance.
     * @throws Exception
     */
    static public void removeNotificationListener(JMXConnector jmxConnector,
            Class<?> mbeanClass, NotificationListener notificationListener)
            throws Exception
    {
        MBeanServerConnection mbsc = jmxConnector.getMBeanServerConnection();
        ObjectName objectName = generateMBeanObjectName(mbeanClass);
        mbsc.removeNotificationListener(objectName, notificationListener);
    }

    // Create a service address.addNotificationListener
    private static String generateServiceAddress(String host, int beanPort,
            int registryPort, String serviceName)
    {

        String serviceAddress = null;

        if (beanPort == -1)
        {

            serviceAddress = String.format(
                    "service:jmx:rmi:///jndi/rmi://%s:%d/%s", host,
                    registryPort, serviceName);
        }
        else
        {
            serviceAddress = String.format(
                    "service:jmx:rmi://%s:%d/jndi/rmi://%s:%d/%s", host,
                    beanPort, host, registryPort, serviceName);
        }

        if (logger.isDebugEnabled())
        {
            logger.debug("Service address for mbean is: " + serviceAddress);
        }

        return serviceAddress;
    }

    // Create an MBean name.
    public static ObjectName generateMBeanObjectName(Class<?> mbeanClass)
            throws Exception
    {
        String className = mbeanClass.getName();
        String type = className;
        String domain = "default";

        int lastPeriod = className.lastIndexOf('.');
        if (lastPeriod != -1)
        {
            domain = className.substring(0, lastPeriod);
            type = className.substring(className.lastIndexOf('.') + 1);
        }

        String name = String.format("%s:type=%s", domain, type);
        ObjectName objName = new ObjectName(name);

        if (logger.isDebugEnabled())
        {
            logger.debug("ObjectName is: " + objName.toString());
        }
        return objName;
    }

    // Create an MBean name.
    public static ObjectName generateMBeanObjectName(String mbeanName,
            String typeName) throws Exception
    {
        ObjectName name = new ObjectName(mbeanName + ":type=" + typeName);
        if (logger.isDebugEnabled())
        {
            logger.debug("ObjectName is: " + name.toString());
        }
        return name;
    }

    public void handleNotification(Notification notification, Object handback)
    {

        ObjectName objectName = ((MBeanServerNotification) notification)
                .getMBeanName();

        if (logger.isDebugEnabled())
        {
            logger.debug(String.format(
                    "MBean Added to the MBean server at %s:%d, ObjectName=%s",
                    host, registryPort, objectName));
        }

    }

    public static DynamicMBeanHelper createHelper(Class<?> mbeanClass)
            throws Exception
    {
        ObjectName mbeanName = generateMBeanObjectName(mbeanClass);

        MBeanInfo info = ManagementFactory.getPlatformMBeanServer()
                .getMBeanInfo(mbeanName);

        DynamicMBeanHelper helper = new DynamicMBeanHelper(mbeanClass,
                mbeanName, info);

        return helper;

    }

    /**
     * Get the hostname from the local host. Returns the IP address, in textual
     * form, if no hostname can be found.
     * 
     * @return the hostname for the local host
     */
    public static String getHostName()
    {
        String hostName = null;

        try
        {
            hostName = InetAddress.getLocalHost().getHostName();

        }
        catch (UnknownHostException e)
        {
            // Intentionally blank
        }

        return hostName;
    }

    public Registry getRegistry()
    {
        if (rmiRegistry == null)
            rmiRegistry = locateDefaultRegistry();
        return rmiRegistry;
    }

    public int getBeanPort()
    {
        return beanPort;
    }

    public static DynamicMBeanHelper createHelper(Class<?> mbeanClass,
            String alias) throws Exception
    {
        ObjectName mbeanName = generateMBeanObjectName(mbeanClass.getName(),
                alias);

        // ObjectName mbeanName = generateMBeanObjectName(mbeanClass);

        MBeanInfo info = ManagementFactory.getPlatformMBeanServer()
                .getMBeanInfo(mbeanName);

        DynamicMBeanHelper helper = new DynamicMBeanHelper(mbeanClass,
                mbeanName, info);

        return helper;

    }
}
