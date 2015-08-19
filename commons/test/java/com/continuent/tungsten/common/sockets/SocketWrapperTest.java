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

package com.continuent.tungsten.common.sockets;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Test;

/**
 * Implements a test of client and server socket wrappers using SSL and non-SSL
 * connections.
 * <p/>
 * IMPORTANT NOTE! To run this test in Eclipse set the working directory to
 * commons/build/work. Otherwise you won't be able to find the
 * sample.security.properties file.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class SocketWrapperTest
{
    private static Logger logger = Logger.getLogger(SocketWrapperTest.class);
    private EchoServer    server;
    private SocketHelper  helper = new SocketHelper();

    /** Terminate echo server if still running. */
    @After
    public void teardown()
    {
        if (server != null)
        {
            logger.info("Shutting down echo server...");
            server.shutdown();
        }
    }

    /**
     * Verify that we can connect using a non-SSL socket and get a value back
     * from a server.
     */
    @Test
    public void testNonSSLConnection() throws Exception
    {
        logger.info("### testNonSSLConnection");
        verifyConnection(2113, false);
    }

    /**
     * Verify that we can connect using an SSL socket and get a value back from
     * a server that also speaks SSL.
     */
    @Test
    public void testSSLConnection() throws Exception
    {
        logger.info("### testSSLConnection");
        helper.loadSecurityProperties();
        verifyConnection(2114, true);
    }

    /**
     * Verify that a non-SSL connection fails to connect to an SSL server.
     */
    @Test
    public void testSSLConnectionIncompatibility() throws Exception
    {
        // Start an SSL server.
        logger.info("### testSSLConnectionIncompatibility");
        helper.loadSecurityProperties();
        server = new EchoServer("127.0.0.1", 2115, true);
        server.start();

        // Connect with non-SSL socket.
        ClientSocketWrapper clientWrapper = new ClientSocketWrapper();
        clientWrapper.setAddress(new InetSocketAddress("127.0.0.1", 2115));
        clientWrapper.connect();

        // Write data to the server.
        Socket sock = clientWrapper.getSocket();
        OutputStream os = sock.getOutputStream();
        byte[] buf1 = "This is data".getBytes();
        os.write(buf1, 0, buf1.length);

        // Inquire as to the echo server's health. It should not be good.
        Throwable serverError = null;
        for (int i = 0; i < 10; i++)
        {
            serverError = server.getThrowable();
            if (serverError != null)
                break;
            else
                Thread.sleep(500);
        }
        Assert.assertNotNull("Server should have error", serverError);
        logger.info("Found expected server error: " + serverError.toString());

        clientWrapper.close();
    }

    /**
     * Verify that multiple non-SSL clients can connect to the server and that
     * the server can stop when the clients are idle.
     */
    @Test
    public void testNonSSLClientsBasic() throws Exception
    {
        logger.info("### testNonSSLClientsBasic");
        verifyClients(2116, 5, false);
    }

    /**
     * Verify that multiple SSL clients can connect to the server and that the
     * server can stop when the clients are idle.
     */
    @Test
    public void testSSLClientsBasic() throws Exception
    {
        logger.info("### testSSLClientsBasic");
        helper.loadSecurityProperties();
        verifyClients(2117, 5, true);
    }

    /**
     * Implement verification using a simple echo server.
     */
    private void verifyConnection(int port, boolean useSSL) throws Exception
    {
        server = new EchoServer("127.0.0.1", port, useSSL);
        server.start();

        ClientSocketWrapper clientWrapper = new ClientSocketWrapper();
        clientWrapper.setUseSSL(useSSL);
        clientWrapper.setAddress(new InetSocketAddress("127.0.0.1", port));
        clientWrapper.connect();

        Socket sock = clientWrapper.getSocket();
        Assert.assertNotNull("Expected to get a client socket", sock);

        String echoValue = helper.echo(sock, "hello");
        logger.info("Echoed value: " + echoValue);
        Assert.assertEquals("Expect echo to match", "hello", echoValue);
        clientWrapper.close();
    }

    /**
     * Verify that multiple clients can connect to the server and that the
     * server can stop when the clients are idle.
     */
    public void verifyClients(int port, int numberOfClients, boolean useSSL)
            throws Exception
    {
        // Start a server.
        server = new EchoServer("127.0.0.1", port, useSSL);
        server.start();

        // Launch echo clients with 100ms think time between
        // requests.
        EchoClient[] clients = new EchoClient[numberOfClients];
        for (int i = 0; i < clients.length; i++)
        {
            EchoClient client = new EchoClient("127.0.0.1", port, useSSL, 100);
            client.start();
            clients[i] = client;
        }

        // Bide a wee.
        Thread.sleep(5000);

        // Stop the threads and confirm that each has processed at least 10 echo
        // requests.
        try
        {
            for (EchoClient client : clients)
            {
                // Ensure we can shut down the client.
                Assert.assertTrue("Shut down client: " + client.getName(),
                        client.shutdown());
                Assert.assertTrue("Expect least 10 operations per client: "
                        + client.getName(), client.getEchoCount() >= 10);
                Assert.assertNull(
                        "Do not expect errors for client: " + client.getName(),
                        client.getThrowable());
            }
        }
        finally
        {
            // Shut down all clients.
            for (EchoClient client : clients)
                client.shutdown();
        }

        // Ensure the echo server is OK.
        Assert.assertNull("Echo server does not have any errors",
                server.getThrowable());
    }
}