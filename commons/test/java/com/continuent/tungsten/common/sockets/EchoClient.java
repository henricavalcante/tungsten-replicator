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

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.cluster.ConfigurationException;

/**
 * Implements a simple client that connects to server and sends a string to the
 * server at regular intervals.
 */
public class EchoClient implements Runnable
{
    private static Logger       logger            = Logger.getLogger(EchoClient.class);

    // Client properties.
    private final String        host;
    private final int           port;
    private final boolean       useSSL;
    private final long          sleepMillis;

    // Operational variables.
    private ClientSocketWrapper socket;
    private volatile boolean    shutdownRequested = false;
    private boolean             isShutdown        = false;
    private Throwable           throwable;
    private Thread              clientThread;
    private volatile String     clientName;
    private int                 echoCount         = 0;

    /**
     * Create a new echo server instance.
     */
    public EchoClient(String host, int port, boolean useSSL, long sleepMillis)
    {
        this.host = host;
        this.port = port;
        this.useSSL = useSSL;
        this.sleepMillis = sleepMillis;
    }

    public Throwable getThrowable()
    {
        return throwable;
    }

    public int getEchoCount()
    {
        return echoCount;
    }

    public String getName()
    {
        return clientName;
    }

    /**
     * Starts the server.
     */
    public synchronized void start() throws IOException, ConfigurationException
    {
        // Configure and connect.
        logger.info("Connecting client to server: host=" + host + " port="
                + port + " useSSL=" + useSSL + " sleepMillis=" + sleepMillis);
        socket = new ClientSocketWrapper();
        socket.setAddress(new InetSocketAddress(host, port));
        socket.setUseSSL(useSSL);
        socket.connect();

        // Spawn ourselves in a separate server.
        clientThread = new Thread(this);
        clientName = clientThread.getName();
        clientThread.start();
        logger.info("Spawned client thread: " + clientName);
    }

    /**
     * Loop through answering all incoming requests.
     */
    @Override
    public void run()
    {
        try
        {
            doRun();
        }
        catch (SocketTerminationException e)
        {
            logger.info("Client stopped by close on socket");
        }
        catch (InterruptedException e)
        {
            logger.info("Client stopped by interrupt on thread");
        }
        catch (Throwable t)
        {
            if (!this.shutdownRequested)
            {
                throwable = t;
                logger.info("Echo client failed: name=" + clientName
                        + " throwable=" + throwable.getMessage(), t);
            }
        }
        finally
        {
            socket.close();
        }
    }

    /**
     * Implements basic server processing, which continues until a call to
     * shutdown or the thread is interrupted.
     */
    private void doRun() throws IOException, InterruptedException
    {
        SocketHelper helper = new SocketHelper();
        while (shutdownRequested == false)
        {
            synchronized (this)
            {
                String echoValue = helper.echo(socket.getSocket(), clientName);
                if (!clientName.equals(echoValue))
                    throw new RuntimeException(
                            "Echo returned unexpected value: client="
                                    + clientName + " echoValue=" + echoValue);
                echoCount++;
            }
            Thread.sleep(sleepMillis);
        }
    }

    /**
     * Shut down a running client nicely, returning true if the thread is
     * finished.
     */
    public synchronized boolean shutdown()
    {
        if (isShutdown)
            return !clientThread.isAlive();

        logger.info("Shutting down echo client: " + clientName + " echoCount="
                + echoCount);
        shutdownRequested = true;
        socket.close();
        clientThread.interrupt();
        try
        {
            clientThread.join(5000);
        }
        catch (InterruptedException e)
        {
            logger.warn("Unable to shut down echo client: " + clientName);
        }
        finally
        {
            isShutdown = true;
        }
        return !clientThread.isAlive();
    }
}