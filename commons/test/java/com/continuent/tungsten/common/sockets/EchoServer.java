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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.cluster.ConfigurationException;

/**
 * Implements a simple echo server that echoes back bytes sent to it. The server
 * creates a new thread for every incoming request.
 */
public class EchoServer implements Runnable
{
    private static Logger                logger            = Logger.getLogger(EchoServer.class);
    private final String                 host;
    private final int                    port;
    private final boolean                useSSL;

    // Latch to signal server is ready. This prevents race conditions around
    // start-up so that clients do not connect too quickly.
    private volatile boolean             ready             = false;

    // Operational variables. These are volatile to permit concurrent access.
    private final ExecutorService        pool              = Executors
                                                                   .newFixedThreadPool(5);
    private volatile ServerSocketService socketService;
    private volatile boolean             shutdownRequested = false;
    private volatile Throwable           throwable;
    private volatile Thread              serverThread;

    /**
     * Create a new echo server instance.
     */
    public EchoServer(String host, int port, boolean useSSL)
    {
        this.host = host;
        this.port = port;
        this.useSSL = useSSL;
    }

    public Throwable getThrowable()
    {
        return throwable;
    }

    /**
     * Starts the server, returning when it is ready to receive clients.
     * 
     * @throws InterruptedException Thrown if wait is interrupted.
     */
    public void start() throws IOException, ConfigurationException,
            InterruptedException
    {
        // Configure and connect.
        logger.info("Binding server: host=" + host + " port=" + port
                + " useSSL=" + useSSL);
        socketService = new ServerSocketService();
        socketService.setAddress(new InetSocketAddress(host, port));
        socketService.setUseSSL(useSSL);
        socketService.bind();

        // Spawn ourselves in a separate server.
        logger.info("Spawning server thread");
        serverThread = new Thread(this);
        serverThread.start();

        // Wait until we are ready to handle connections, failing if we don't
        // ready that point in a reasonable interval.
        boolean ready = awaitReady(3000);
        if (!ready)
        {
            throw new IOException("Server did not become ready!");
        }
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
            logger.info("Server stopped by close on socket");
        }
        catch (InterruptedException e)
        {
            logger.info("Server stopped by interrupt on thread");
        }
        catch (Throwable t)
        {
            throwable = t;
            logger.info("Echo server failed: " + throwable.getMessage(), t);
        }
        finally
        {
            pool.shutdown();
            socketService.close();
        }
    }

    /**
     * Implements basic server processing, which continues until a call to
     * shutdown or the thread is interrupted.
     */
    private void doRun() throws IOException, InterruptedException
    {
        SocketWrapper client;
        ready = true;
        while ((shutdownRequested == false)
                && (client = socketService.accept()) != null)
        {
            EchoSocketHandler handler = new EchoSocketHandler(this, client);
            pool.execute(handler);
        }
    }

    /**
     * Wait for the server to enter the ready state.
     * 
     * @param howLongMillis How many milliseconds to wait for the server to
     *            become ready.
     * @throws InterruptedException Thrown if thread is interrupted
     */
    private boolean awaitReady(long howLongMillis) throws InterruptedException
    {
        long untilMillis = System.currentTimeMillis() + howLongMillis;
        while (!ready && System.currentTimeMillis() < untilMillis)
        {
            Thread.sleep(1);
        }
        return ready;
    }

    /** Shut down a running server nicely. */
    public void shutdown()
    {
        logger.info("Shutting down echo server");
        shutdownRequested = true;
        socketService.close();
        serverThread.interrupt();
        try
        {
            serverThread.join(5000);
        }
        catch (InterruptedException e)
        {
            logger.warn("Unable to shut down echo server");
        }
    }

    /** Shut down a running server after an error. */
    public void shutdownWithError(Throwable t)
    {
        this.throwable = t;
        shutdown();
    }
}

// Local class to implement a simple client handler.
class EchoSocketHandler implements Runnable
{
    private static Logger logger = Logger.getLogger(EchoSocketHandler.class);
    EchoServer            server;
    SocketWrapper         socketWrapper;

    EchoSocketHandler(EchoServer server, SocketWrapper socketWrapper)
    {
        this.server = server;
        this.socketWrapper = socketWrapper;
    }

    @Override
    public void run()
    {
        try
        {
            InputStream in = socketWrapper.getInputStream();
            OutputStream os = socketWrapper.getOutputStream();
            byte[] buffer = new byte[1024];
            int len;
            while ((len = in.read(buffer)) > 0 && !Thread.interrupted())
            {
                os.write(buffer, 0, len);
            }
        }
        catch (Throwable t)
        {
            logger.error("Socket handler failed: " + t.getMessage(), t);
            server.shutdownWithError(t);
        }
        finally
        {
            socketWrapper.close();
        }
    }
}