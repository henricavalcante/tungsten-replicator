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
import java.net.InetAddress;
import java.net.Socket;

import org.apache.log4j.Logger;

/**
 * Implements methods common to both client and server sockets. It provides
 * common methods for obtaining input and output streams on both socket types as
 * well as closing the socket. This class works around the fact that the Java
 * NIO Channel provides an incomplete abstraction for networking that does not
 * support SSL operation.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class SocketWrapper
{
    private static Logger logger = Logger.getLogger(SocketWrapper.class);

    protected Socket      socket = null;

    /**
     * Creates a new socket wrapper.
     * 
     * @param socket The socket to wrap or null if the socket has not yet been
     *            connected
     */
    SocketWrapper(Socket socket)
    {
        this.socket = socket;
    }

    /**
     * Sets the socket. This is used by clients sockets, which do not know the
     * socket type until they connect.
     */
    public void setSocket(Socket socket)
    {
        this.socket = socket;
    }

    /** Returns the socket. */
    public Socket getSocket()
    {
        return this.socket;
    }

    /**
     * Returns an input stream that can read data from the socket.
     */
    public InputStream getInputStream() throws IOException
    {
        return socket.getInputStream();
    }

    /**
     * Returns an output stream that can write data to the socket.
     */
    public OutputStream getOutputStream() throws IOException
    {
        return socket.getOutputStream();
    }

    /**
     * Close socket. This is synchronized to prevent accidental double calls.
     */
    public synchronized void close()
    {
        if (socket != null && !socket.isClosed())
        {
            try
            {
                socket.close();
            }
            catch (IOException e)
            {
                logger.warn(e.getMessage());
            }
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        sb.append(this.getClass().getSimpleName());
        if (socket == null)
        {
            sb.append(" [unbound]");
        }
        else
        {
            sb.append(" impl=").append(socket.getClass().getSimpleName());
            sb.append(" closed=").append(socket.isClosed());
            InetAddress address = socket.getInetAddress();
            sb.append(" local port=").append(socket.getLocalPort());
            if (address != null)
            {
                sb.append(" remote address=").append(address.getHostAddress());
            }
        }
        return sb.toString();
    }
}