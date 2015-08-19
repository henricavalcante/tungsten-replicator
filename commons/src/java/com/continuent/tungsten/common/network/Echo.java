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
 * Initial developer(s): Csaba Endre Simon
 * Contributor(s): 
 */

package com.continuent.tungsten.common.network;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;

/**
 * Tests for reachability using the TCP echo protocol.
 * 
 * @author <a href="mailto:csimon@vmware.com">Csaba Endre Simon</a>
 */
public class Echo
{
    private static Logger       logger               = Logger.getLogger(Echo.class);
    private final static String message              = "Hello";

    public static final String  TIME_TO_CONNECT_MS   = "TimeToConnectMs";
    public static final String  TIME_TO_SEND_MS      = "TimeToSendMs";
    public static final String  TIME_TO_RECEIVE_MS   = "TimeToReceiveMs";
    public static final String  STATUS_KEY           = "Status";
    public static final String  STATUS_MESSAGE_KEY   = "StatusMsg";
    public static final String  STATUS_EXCEPTION     = "Exception";

    public static final String  SOCKET_PHASE_CONNECT = "connecting to";
    public static final String  SOCKET_PHASE_RECEIVE = "reading from";
    public static final String  SOCKET_PHASE_WRITE   = "writing to";

    // Modifying the order here and in cluster-home/bin/tping bash script should
    // be synchronous
    public enum EchoStatus
    {
        OK, OPEN_FILE_LIMIT_ERROR, SOCKET_NO_IO, SOCKET_CONNECT_TIMEOUT, SEND_MESSAGE_TIMEOUT, RECEIVE_MESSAGE_TIMEOUT, MESSAGE_CORRUPT, SOCKET_IO_ERROR, HOST_IS_DOWN, NO_ROUTE_TO_HOST, UNKNOWN_HOST
    }

    /**
     * Tests a host for reachability.
     * 
     * @param hostName The host name of the echo server
     * @param portNumber The port number of the echo server
     * @param timeout Timeout in milliseconds
     * @return the result wrapped inside tungsten properties.
     */
    public static TungstenProperties isReachable(String hostName,
            int portNumber, int timeout)
    {
        TungstenProperties statusAndResult = new TungstenProperties();
        String statusMessage = null;
        Socket socket = null;
        InputStream socketInput = null;
        OutputStream socketOutput = null;
        String socketPhase = SOCKET_PHASE_CONNECT;
        EchoStatus timeoutPhase = EchoStatus.SOCKET_CONNECT_TIMEOUT;

        try
        {
            SocketAddress sockaddr = new InetSocketAddress(hostName, portNumber);
            socket = new Socket();
            socket.setSoTimeout(timeout);
            socket.setReuseAddress(true);

            long beforeConnect = System.currentTimeMillis();
            socket.connect(sockaddr, timeout);
            long timeToConnectMs = System.currentTimeMillis() - beforeConnect;
            statusAndResult.setLong(TIME_TO_CONNECT_MS, timeToConnectMs);

            socketInput = socket.getInputStream();
            socketOutput = socket.getOutputStream();

            if (socketInput == null || socketOutput == null)
            {
                statusMessage = String
                        .format("Socket connect error: InputStream=%s, OutputStream=%s after connect to %s:%s",
                                socketInput, socketOutput, hostName, portNumber);

                statusAndResult.setObject(STATUS_KEY, EchoStatus.SOCKET_NO_IO);
                statusAndResult.setString(STATUS_MESSAGE_KEY, statusMessage);
                logger.warn(formatExecStatus(statusAndResult));
                return logAndReturnProperties(statusAndResult);
            }

            int timeLeft = (int) (timeout - timeToConnectMs);
            if (timeLeft <= 0)
            {
                statusMessage = String
                        .format("Timeout while connecting: %d ms exceeds allowed timeout of %d ms.",
                                timeToConnectMs, timeout);

                statusAndResult.setObject(STATUS_KEY,
                        EchoStatus.SOCKET_CONNECT_TIMEOUT);
                statusAndResult.setString(STATUS_MESSAGE_KEY, statusMessage);
                logger.warn(formatExecStatus(statusAndResult));
                return logAndReturnProperties(statusAndResult);
            }

            socket.setSoTimeout(timeLeft);
            long beforeSend = System.currentTimeMillis();
            socketPhase = SOCKET_PHASE_WRITE;
            timeoutPhase = EchoStatus.SEND_MESSAGE_TIMEOUT;

            byte[] outBuff = message.getBytes();
            socketOutput.write(outBuff, 0, outBuff.length);
            long timeToSendMs = System.currentTimeMillis() - beforeSend;
            statusAndResult.setLong(TIME_TO_SEND_MS, timeToSendMs);

            timeLeft = (int) (timeLeft - timeToSendMs);
            if (timeLeft <= 0)
            {
                statusMessage = String
                        .format("Timeout while sending: %d ms exceeds allowed timeout of %d ms.",
                                timeToSendMs, timeLeft);

                statusAndResult.setObject(STATUS_KEY,
                        EchoStatus.SEND_MESSAGE_TIMEOUT);
                statusAndResult.setString(STATUS_MESSAGE_KEY, statusMessage);
                logger.warn(formatExecStatus(statusAndResult));
                return logAndReturnProperties(statusAndResult);
            }

            socket.setSoTimeout(timeLeft);
            long beforeReceive = System.currentTimeMillis();
            socketPhase = SOCKET_PHASE_RECEIVE;
            timeoutPhase = EchoStatus.RECEIVE_MESSAGE_TIMEOUT;

            byte[] inBuff = new byte[outBuff.length];
            int offset = 0;
            int length = 0;
            while (offset < inBuff.length)
            {
                length = socketInput.read(inBuff, offset, inBuff.length
                        - offset);
                offset += length;
            }
            String echoMessage = new String(inBuff);
            long timeToReceiveMs = System.currentTimeMillis() - beforeReceive;
            statusAndResult.setLong(TIME_TO_RECEIVE_MS, timeToReceiveMs);

            timeLeft = (int) (timeLeft - timeToReceiveMs);
            if (timeLeft <= 0)
            {
                statusMessage = String
                        .format("Timeout while reading: %d ms exceeds allowed timeout of %d ms.",
                                timeToReceiveMs, timeLeft);

                statusAndResult.setObject(STATUS_KEY,
                        EchoStatus.RECEIVE_MESSAGE_TIMEOUT);
                statusAndResult.setString(STATUS_MESSAGE_KEY, statusMessage);
                logger.warn(formatExecStatus(statusAndResult));
                return logAndReturnProperties(statusAndResult);
            }

            if (!message.equals(echoMessage))
            {
                statusMessage = String
                        .format("Corrupted message: expected '%s' with len=%d but got '%s' with len=%d.",
                                message, message.length(), echoMessage,
                                echoMessage.length());

                statusAndResult.setObject(STATUS_KEY,
                        EchoStatus.MESSAGE_CORRUPT);
                statusAndResult.setString(STATUS_MESSAGE_KEY, statusMessage);
                logger.warn(formatExecStatus(statusAndResult));
                return logAndReturnProperties(statusAndResult);
            }

            statusMessage = String.format("Ping to %s:%d succeeded.", hostName,
                    portNumber);
            statusAndResult.setString(STATUS_MESSAGE_KEY, statusMessage);
            statusAndResult.setObject(STATUS_KEY, EchoStatus.OK);
            return logAndReturnProperties(statusAndResult);
        }
        catch (SocketTimeoutException so)
        {
            statusMessage = String.format(
                    "Socket timeout while %s a socket %s:%d\nException='%s'",
                    socketPhase, hostName, portNumber, so);

            statusAndResult.setObject(STATUS_KEY, timeoutPhase);
            statusAndResult.setString(STATUS_MESSAGE_KEY, statusMessage);
            statusAndResult.setObject(STATUS_EXCEPTION, so);
            logger.warn(formatExecStatus(statusAndResult));
            return logAndReturnProperties(statusAndResult);
        }
        catch (IOException ioe)
        {
            if ("Host is down".toLowerCase().contains(
                    ioe.getMessage().toLowerCase()))
            {
                statusMessage = String
                        .format("Host '%s' is down detected while %s a socket to %s:%d\nException='%s'",
                                hostName, socketPhase, hostName, portNumber,
                                ioe);
                statusAndResult.setObject(STATUS_KEY, EchoStatus.HOST_IS_DOWN);
                statusAndResult.setString(STATUS_MESSAGE_KEY, statusMessage);
                statusAndResult.setObject(STATUS_EXCEPTION, ioe);
                logger.warn(formatExecStatus(statusAndResult));
                return logAndReturnProperties(statusAndResult);
            }
            if ("No route to host".toLowerCase().contains(
                    ioe.getMessage().toLowerCase()))
            {
                statusMessage = String
                        .format("No route to host '%s' detected while %s a socket to %s:%d\nException='%s'",
                                hostName, socketPhase, hostName, portNumber,
                                ioe);

                statusAndResult.setObject(STATUS_KEY,
                        EchoStatus.NO_ROUTE_TO_HOST);
                statusAndResult.setString(STATUS_MESSAGE_KEY, statusMessage);
                statusAndResult.setObject(STATUS_EXCEPTION, ioe);
                logger.warn(formatExecStatus(statusAndResult));
                return logAndReturnProperties(statusAndResult);
            }
            if (ioe.getMessage().toLowerCase()
                    .contains("cannot assign requested address"))
            {
                statusMessage = String
                        .format("I/O exception while %s a socket to %s:%d\nException='%s'\n"
                                + "Your open file limit may be too low.  Check with 'ulimit -n' and increase if necessary.",
                                socketPhase, hostName, portNumber, ioe);
                statusAndResult.setObject(STATUS_KEY,
                        EchoStatus.OPEN_FILE_LIMIT_ERROR);
                statusAndResult.setString(STATUS_MESSAGE_KEY, statusMessage);
                statusAndResult.setObject(STATUS_EXCEPTION, ioe);
                logger.warn(formatExecStatus(statusAndResult));
                return logAndReturnProperties(statusAndResult);
            }

            if (ioe.toString().contains("java.net.UnknownHostException"))
            {
                statusMessage = String
                        .format("I/O exception while %s a socket to %s:%d\nException='%s'\n"
                                + "There may be an issue with your DNS for this host or your /etc/hosts entry is not correct.",
                                socketPhase, hostName, portNumber, ioe);
                statusAndResult.setObject(STATUS_KEY, EchoStatus.UNKNOWN_HOST);
                statusAndResult.setString(STATUS_MESSAGE_KEY, statusMessage);
                statusAndResult.setObject(STATUS_EXCEPTION, ioe);
                logger.warn(formatExecStatus(statusAndResult));
                return logAndReturnProperties(statusAndResult);
            }

            statusMessage = String
                    .format("I/O exception caught while %s a socket to %s:%d\nException='%s'",
                            socketPhase, hostName, portNumber, ioe);

            statusAndResult.setObject(STATUS_KEY, EchoStatus.SOCKET_IO_ERROR);
            statusAndResult.setString(STATUS_MESSAGE_KEY, statusMessage);
            statusAndResult.setObject(STATUS_EXCEPTION, ioe);
            logger.warn(formatExecStatus(statusAndResult));
            return logAndReturnProperties(statusAndResult);
        }
        finally
        {
            if (socketOutput != null)
            {
                try
                {
                    socketOutput.close();
                }
                catch (Exception ignored)
                {
                }
                finally
                {
                    socketOutput = null;
                }
            }

            if (socketInput != null)
            {
                try
                {
                    socketInput.close();
                }
                catch (Exception ignored)
                {
                }
                finally
                {
                    socketInput = null;
                }
            }

            if (socket != null)
            {
                try
                {
                    socket.close();
                }
                catch (IOException i)
                {
                    logger.warn("Exception while closing socket", i);
                }
                finally
                {
                    socket = null;
                }
            }
        }
    }

    /**
     * Formats a TungstenProperties for human-friendly output
     * 
     * @param props The tungsten properties to format
     */
    private static String formatExecStatus(TungstenProperties props)
    {

        EchoStatus echoStatus = (EchoStatus) props.getObject(STATUS_KEY);
        String statusMessage = props.getString(STATUS_MESSAGE_KEY);

        return String.format("%s\n%s", echoStatus.toString(), statusMessage);
    }

    /**
     * Log the given TungstenProperties
     * 
     * @param props The tungsten properties to log
     */

    private static TungstenProperties logAndReturnProperties(
            TungstenProperties props)
    {
        if (logger.isTraceEnabled())
        {
            logger.trace(props);
        }

        return props;
    }

    /**
     * Main method to permit external invocation.
     * 
     * @param argv The program arguments.
     */
    public static void main(String argv[])
    {
        if (argv.length != 3)
        {
            System.out.println("Tungsten ping utility");
            System.out.println("Usage: tping hostname port timeout");
            System.out.println("   timeout is in milliseconds");
            System.exit(1);
        }
        else
        {
            try
            {
                String hostName = argv[0];
                int portNumber = Integer.parseInt(argv[1]);
                int timeout = Integer.parseInt(argv[2]);
                TungstenProperties result = isReachable(hostName, portNumber,
                        timeout);

                if (result.getObject(Echo.STATUS_KEY) == EchoStatus.OK)
                {
                    System.exit(0);
                }
                else
                {
                    EchoStatus echoStatus = (EchoStatus) result
                            .getObject(STATUS_KEY);
                    System.exit(1 + echoStatus.ordinal());
                }
            }
            catch (NumberFormatException e)
            {
                System.out.println("Error parsing number: " + e.getMessage());
                System.exit(1);
            }
        }
    }
}