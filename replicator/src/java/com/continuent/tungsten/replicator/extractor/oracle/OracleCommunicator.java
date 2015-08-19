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
 * Initial developer(s): Scott Martin
 * Contributor(s): Stephane Giron
 */

package com.continuent.tungsten.replicator.extractor.oracle;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.lang.Thread;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.SocketChannel;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.extractor.ExtractorException;
import com.continuent.tungsten.common.exec.ProcessExecutor;

/**
 * This class defines a OracleCommunicator. This is the interface to the Oracle
 * redo log extractor.
 * 
 * @author <a href="mailto:scott.martin@continuent.com">Scott Martin</a>
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class OracleCommunicator
{
    private static Logger logger         = Logger
                                                 .getLogger(OracleExtractor.class);

    private String        hostName;
    private String        instanceName;
    private int           portNumber;
    private boolean       connected;
    private SocketChannel listenerSocket = null;
    private PrintWriter   out            = null;
    private InputStream   in             = null;

    /*
     * Start the buffer out with medium size.  It can dynamically grow as need be
     */
    private byte[]        replyBuffer     = new byte[20*1024];
    private int           replyBufferSize = 20*1024;
    private byte[]        lengthBuffer    = new byte[11];
    private int           msgCount        = 0;

    /*
     * this is the version number shared very early in the interaction between
     * the C based extractor and the Java API to the C based extractor. If the C
     * and Java portions of the API do not have the same version, further
     * communication is illadvised and should probably end in error. The string
     * does not need to be numeric in form. It could be something like
     * "Beta-2.0" but using strings like this gets confusing when the API does
     * not change for several releases and you find yourself releasing Beta-5.2
     * with "Beta-2.0" as your api string. The confusion intensifies when these
     * strings begin to show up in user level error strings.
     */
    //String                apiVersion     = "1"; // initial
    //String                apiVersion     = "2"; // Added support for username/password in connect
    //String                apiVersion     = "3"; // Mixing statement level and row level
    //String                apiVersion     = "4"; // Restartability upgrade
    //String                apiVersion     = "5"; // SCN in eventID
    //String                apiVersion     = "6"; // Length prepended to message
    String                apiVersion     = "7"; // 4 byte column length

    class SendReturn
    {
        public String reply;
        public int    type;
    }

    private final class Const
    {
        /*
         * Various constants related to message types between Java client and C
         * based Oracle listener.
         */
        // public static final int MessageReserved     = 0;
        public static final int MessageControl      = 1;
        // public static final int MessageDisconnect   = 2;
        public static final int MessageSetParameter = 3;
        public static final int MessageGetStatus    = 4;
        public static final int MessageGetSQL       = 5;
        public static final int MessageConnect      = 6;
        public static final int MessageVersion      = 7;

        public static final int ReplyReserved       = 0;
        // public static final int ReplyOK             = 1;
        public static final int ReplyError          = 2;
    }

    /**
     * Creates a new <code>OracleCommunicator</code> object
     * 
     * @param hostName Name of the host to log into
     * @param instanceName Value of the ORACLE_SID of the instance to connect to
     * @param portNumber Port number of the dslisten process typically 51060
     */
    public OracleCommunicator(String hostName, String instanceName,
            int portNumber)
    {
        this.hostName = hostName;
        this.instanceName = instanceName;
        this.portNumber = portNumber;
        this.connected = false;
    }

    /**
     * Perform given command.
     * @param cmd : command to execute
     * @param waitTime : time in milliseconds to wait for command to complete.
     */
    private void doListener(String[] cmd, int waitTime)
    {
        ProcessExecutor pe = new ProcessExecutor();
        pe.setCommands(cmd);
        pe.setTimeout(2000);
        pe.run();

        if (waitTime != 0)
        {
            try
            {
                Thread.sleep(waitTime);
            }
            catch (InterruptedException e)
            {
            }
        }
    }

    /**
     * Starts the C based listener
     */
    private void startListener()
    {
        logger.info("Starting dslisten with \"service tdslisten start\"");
        doListener(new String [] {"service", "tdslisten", "start"}, 3000);
        logger.info("dslisten Started");
    }

    /**
     * stop the C based listener
     */
    private void stopListener()
    {
        logger.info("Stopping dslisten with \"service tdslisten stop\"");
        doListener(new String [] {"service", "tdslisten", "stop"}, 1000);
    }

    /**
     * Connect to the c based dslisten process
     * 
     * @param userName Oracle username dslisten uses to connect to Oracle
     * @param password Oracle password dslisten uses to connect to Oracle
     * @param scn Starting system commit number to extract from
     * @param rowLevel TRUE IFF performing row level extraction else performing statement level extraction
     * @throws ExtractorException
     */
    public void connect(String userName, String password, String scn, boolean rowLevel, String seq)
        throws ExtractorException
    {
        String spawnMessage = "1";
        String message;
        String connectMessage = "DEV10:TXID";

        if (connected)
            throw new OracleExtractException("Already connected");

        if (logger.isInfoEnabled())
        {
            logger.info("Connecting to instance " + instanceName + " on host "
                    + hostName + " (port=" + portNumber + ")");
        }

        startListener();

        try
        {
            listenerSocket=SocketChannel.open(new InetSocketAddress(hostName, portNumber)); 
        }
        catch (UnknownHostException e)
        {
            throw new OracleExtractException(
                    "IP address could not be determined (" + hostName + ":"
                            + portNumber + ")", e);
        }
        catch (IOException e)
        {
            throw new OracleExtractException(
                    "An I/O error occured when creating the socket (" + hostName
                            + ":" + portNumber + ")", e);
        }

        try
        {
            out = new PrintWriter(listenerSocket.socket().getOutputStream(), true);
        }
        catch (IOException e)
        {
            throw new OracleExtractException(
                    "An I/O error occurs when creating the output stream", e);
        }

        try
        {
            in = listenerSocket.socket().getInputStream();
        }
        catch (IOException e)
        {
            throw new OracleExtractException(
                    "An I/O error occurs when creating the input stream", e);
        }

        if (logger.isInfoEnabled())
        {
            logger.info("Connected.");
        }
        connected = true;

        /* Send message to listener requesting child spawn */
        try 
        {
            send(Const.MessageControl, spawnMessage);
        }
        catch (InterruptedException e)
        {
            throw new ExtractorException(e);
        }
        sleep();
        
        /* Send version message */
        message = apiVersion;
        try
        {
            send(Const.MessageVersion, message);
        }
        catch (OracleExtractException e)
        {
            throw new OracleExtractException(
                    "Incompatible C/Java API in Oracle extractor", e);
        }
        catch (InterruptedException e)
        {
            throw new ExtractorException(e);
        }
        
        if (scn != null && scn.length() > 0)
        {
            sleep();
            /* Send parameter message */
            message = "scn:" + scn;
            try 
            {
                send(Const.MessageSetParameter, message);
            }
            catch (InterruptedException e)
            {
                throw new ExtractorException(e);
            }
        }

        if (rowLevel)
        {
            sleep();
            /* Send parameter message */
            message = "row_level:1";
            try 
            {
                send(Const.MessageSetParameter, message);
            }
            catch (InterruptedException e)
            {
                throw new ExtractorException(e);
            }
        }

        if (seq != null && seq.length() > 0)
        {
            sleep();
            /* Send parameter message */
            message = "seq:" + seq;
            try
            {
                send(Const.MessageSetParameter, message);
            }
            catch (InterruptedException e)
            {
                throw new ExtractorException(e);
            }
        }

        sleep();
        /* Send connect message */
        connectMessage = instanceName + ":" + hostName + ":" + userName + ":" + password;
        try 
        {
            send(Const.MessageConnect, connectMessage);
        }
        catch (InterruptedException e)
        {
            throw new ExtractorException(e);
        }
    }

    /**
     * Disconnect from dslisten
     * 
     * @throws ExtractorException
     */
    public void disconnect() throws ExtractorException
    {
        if (!connected)
            throw new OracleExtractException("Not connected");

        stopListener();
        out.close();
        connected = false;
    }

    /**
     * Send stop message to dslisten.
     * 
     * @throws ExtractorException
     */
    public void stop() throws ExtractorException
    {
        String stopMessage = "0";

        if (connected)
            throw new OracleExtractException("Cannot stop already connected");

        if (logger.isInfoEnabled())
        {
            logger.info("Stopping listener on host " + hostName + " (port="
                    + portNumber + ")");
        }
        try
        {
            listenerSocket=SocketChannel.open(new InetSocketAddress(hostName, portNumber)); 
        }
        catch (UnknownHostException e)
        {
            throw new OracleExtractException(
                    "IP address could not be determined (" + hostName + ":"
                            + portNumber + ")", e);
        }
        catch (IOException e)
        {
            throw new OracleExtractException(
                    "An I/O error occurs when creating the socket (" + hostName
                            + ":" + portNumber + ")", e);
        }
        try
        {
            out = new PrintWriter(listenerSocket.socket().getOutputStream(), true);
        }
        catch (IOException e)
        {
            throw new OracleExtractException(
                    "An I/O error occurs when creating the output stream", e);
        }

        if (logger.isInfoEnabled())
        {
            logger.info("Connected.");
        }
        connected = true;

        if (logger.isInfoEnabled())
        {
            logger.info("Now stopping...");
        }
        /* Send message to listener requesting child spawn */
        try 
        {
            send(Const.MessageControl, stopMessage);
        }
        catch (InterruptedException e)
        {
            throw new ExtractorException(e);
        }
        if (logger.isInfoEnabled())
        {
            logger.info("Stopped.");
        }
    }

    /**
     * Send a set parameter message to dslisten
     * 
     * @param parameter Name of the parameter to set
     * @param value Value of the parameter
     * @throws ExtractorException
     */
    public void setParameter(String parameter, String value)
            throws ExtractorException
    {
        String message = parameter + ":" + value;
        try 
        {
            send(Const.MessageSetParameter, message);
        }
        catch (InterruptedException e)
        {
            throw new ExtractorException(e);
        }
    }

    /**
     * Send a getStatus message to dslisten
     * 
     * @throws ExtractorException
     */
    public void getStatus() throws ExtractorException
    {
        String message = "GetStatusRequest";
        try 
        {
            send(Const.MessageGetStatus, message);
        }
        catch (InterruptedException e)
        {
            throw new ExtractorException(e);
        }
    }

    /**
     * Send a getSQL message to dslisten and wait for the result
     * 
     * @return a string containing SQL statement
     * @throws ExtractorException
     */
    public String getSQL() throws InterruptedException, ExtractorException
    {
        String message = "GetSQLRequest";
        SendReturn retval;

        retval = send(Const.MessageGetSQL, message);
        return retval.reply;
    }
    
    private void increaseReplyBuffer(int requiredAmount)
    {
        int newSize;
        byte [] newBuffer;
        
        if (requiredAmount <= replyBufferSize) return;
        
        for (newSize = replyBufferSize * 2; newSize < requiredAmount; newSize *= 2);
        
        // This "copyOf" does not appear to exist in my environment.  The documentation
        // says that is it available in "1.2".  For now, simply before byte by byte
        // copy.  This should only happen once or twice during the lifetime of
        // the replicator so the peformance is not that critical.
        // newBuffer = java.util.Arrays.copyOf(replyBuffer, newSize);
        
        newBuffer = new byte[newSize];
        
        for (int i = 0; i < replyBufferSize; i++) newBuffer[i] = replyBuffer[i];
        
        logger.info("Receive buffer size increased from " + replyBufferSize + " bytes to " + newSize + " bytes.");
        replyBuffer = newBuffer;
        replyBufferSize = newSize;       
    }

    /**
     * Send message to dslisten - typically an extract next change message
     * 
     * @param messageType Type of message to send (e.g. MessageGetSQL)
     * @param message Message to be parsed, based on message type, in dslisten.
     * @return a SendResult object containing the result of the send operation
     * @throws ExtractorException
     */
    private SendReturn send(int messageType, String message)
            throws InterruptedException, ExtractorException
    {
        /**
         * Format of message is, one byte length of length (in ASCII, that is
         * "1" is 1). length of message not including msg # (in ASCII, that is
         * "107" is 107"). 4 byte message number (in ASCII ("0017" is message #
         * 17). content of message. example "2150003BufferSize:200" the length
         * of length is "2" the length is "15" the msg type is "0003" the msg
         * content is "BufferSize:200".
         */

        String length = String.format("%d", message.length());
        String lenlen = String.format("%d", length.length());
        String type = String.format("%04d", messageType);
        String outMessage;
        int bytesRead;
        String replyString;
        int ll;
        int t;
        SendReturn retval = new SendReturn();
        boolean lengthKnown;
        int totalBytesRead = 0;
        int totalBytesNeeded;

        /* This needs to be enabled with a coresponding change on the C side to support
         * large messages.  Large message support is needed to support BLOB/CLOB
         */
        boolean longMessageSupport = true;

        if (!connected)
            throw new OracleExtractException("Not connected");

        outMessage = lenlen + length + type + message;

        if (logger.isDebugEnabled())
        {
            logger.debug("Sending Message type " + messageType);
            logger.debug(" - Message is -> " + message);
            logger.debug(" - Therefore sending -> " + outMessage);
        }
        out.println(outMessage);

        if (messageType != Const.MessageControl)
        {
            /* read reply */
            try
            {
                if (longMessageSupport)
                {
                    lengthKnown = false;
                    totalBytesRead = 0;
                    totalBytesNeeded = 0;
                    for(;;)
                    {
                        // Be sure to save one byte for null termination in replyBuffer
                       bytesRead = in.read(replyBuffer, totalBytesRead, replyBufferSize - totalBytesRead - 1);
                       if (bytesRead == -1) // End of stream
                       {
                           // This is where we end up if dslisten dies cleanly or not so.
                           throw new OracleExtractException("End of Stream");
                       }
                       totalBytesRead += bytesRead;
                       logger.debug("Read = " + bytesRead + " total = " + totalBytesRead + 
                               " needed = " + totalBytesNeeded);
                       if (lengthKnown)
                       {
                           if (totalBytesRead >= totalBytesNeeded) break;
                           else continue; // not done yet.  Read more
                       }
                       // Still searching for length at head of message.
                       if (totalBytesRead <= 9) continue;
                       /* The first 9 bytes are represented by the printf format string
                        * printf("8%08x", length);
                        */
                       lengthKnown = true;
                       replyBuffer[totalBytesRead] = '\0'; // be sure enough space to hold null terminator
                       lengthBuffer[0] = replyBuffer[0];
                       lengthBuffer[1] = replyBuffer[1];
                       lengthBuffer[2] = replyBuffer[2];
                       lengthBuffer[3] = replyBuffer[3];
                       lengthBuffer[4] = replyBuffer[4];
                       lengthBuffer[5] = replyBuffer[5];
                       lengthBuffer[6] = replyBuffer[6];
                       lengthBuffer[7] = replyBuffer[7];
                       lengthBuffer[8] = replyBuffer[8];
                       lengthBuffer[9] = replyBuffer[9];
                       lengthBuffer[10] = '\0';
                       String lengthString = new String(lengthBuffer);
                       totalBytesNeeded = Integer.parseInt(lengthString.substring(1, 9) , 16);
                       totalBytesNeeded += 13;
                       // request needed space (+1 for null termination)
                       if (totalBytesNeeded + 1 > replyBufferSize) increaseReplyBuffer(totalBytesNeeded + 1);
                       if (totalBytesRead >= totalBytesNeeded) break;
                    }
                } else {
                   totalBytesRead = in.read(replyBuffer);
                }
            }
            catch (ClosedByInterruptException e) {
                // This is the normal code path when the dslisten process has been stopped
                // via another thread executing "tdslisten service stop".  The way this thread
                // indicates to THL.run() that it is complete is by simply raising an
                // InterruptedException.
                throw new InterruptedException("Oracle extractor was interrupted");
            }
            catch (IOException e)
            {
                throw new OracleExtractException("End of Stream");
            }
            if (totalBytesRead == -1)
            {
                throw new OracleExtractException("End of Stream");
            }
            replyBuffer[totalBytesRead] = '\0';
            replyString = new String(replyBuffer, 0, totalBytesRead);

            if (logger.isDebugEnabled())
            {
                logger.debug("Reply = \"" + replyString + "\"\n");
            }
            ll = Integer.parseInt(replyString.substring(0, 1));
            // l = Integer.parseInt(replyString.substring(1, 1 + ll));
            t = Integer.parseInt(replyString.substring(1 + ll, 1 + ll + 4));
            replyString = replyString.substring(1 + ll + 4);

            if (logger.isDebugEnabled())
            {
                logger.debug("Reply = \"" + replyString + "\"\n");
            }
            retval.type = t;
            retval.reply = replyString;
        }
        else
        {
            retval.type = Const.ReplyReserved;
            retval.reply = new String("Empty");
        }

        if (retval.type == Const.ReplyError)
            throw new OracleExtractException("Error from C Oracle Extractor");

        msgCount++;
        return retval;
    }

    private void sleep()
    {
        try
        {
            Thread.sleep(1000);
        }
        catch (InterruptedException ignore)
        {
        }
    }
}
