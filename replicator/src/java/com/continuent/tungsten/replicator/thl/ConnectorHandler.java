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
 * Initial developer(s): Teemu Ollakka
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.replicator.thl;

import java.io.EOFException;
import java.io.IOException;

import javax.net.ssl.SSLHandshakeException;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.sockets.SocketWrapper;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.EventId;
import com.continuent.tungsten.replicator.database.EventIdFactory;
import com.continuent.tungsten.replicator.event.DBMSEmptyEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSFilteredEvent;
import com.continuent.tungsten.replicator.event.ReplEvent;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.plugin.ReplicatorPlugin;
import com.continuent.tungsten.replicator.thl.log.LogConnection;
import com.continuent.tungsten.replicator.thl.log.LogTimeoutException;

/**
 * This class defines a ConnectorHandler
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class ConnectorHandler implements ReplicatorPlugin, Runnable
{
    private Server           server          = null;
    private PluginContext    context         = null;
    private Thread           thd             = null;
    private SocketWrapper    socket;
    private THL              thl             = null;
    private int              resetPeriod;
    private int              heartbeatMillis;
    private long             altSeqno        = -1;
    private volatile boolean cancelled       = false;
    private volatile boolean finished        = false;

    private String           rmiHost         = null;
    private String           rmiPort         = null;

    private volatile boolean checkFirstSeqno = true;

    private static Logger    logger          = Logger.getLogger(ConnectorHandler.class);

    // Implements call-back to check log consistency between client and
    // master.
    class LogValidator implements ProtocolHandshakeResponseValidator
    {
        LogValidator()
        {
        }

        /**
         * Ensure that if the client has supplied log position information we
         * validate that the last epoch number and seqno match our log.
         * 
         * @param handshakeResponse Response from client
         * @throws ReplicatorException Thrown if logs appear to diverge
         */
        public void validateResponse(ProtocolHandshakeResponse handshakeResponse)
                throws InterruptedException, ReplicatorException
        {
            // Get the heartbeat interval and check it.
            heartbeatMillis = handshakeResponse.getHeartbeatMillis();
            logger.info("New THL client connection: sourceID="
                    + handshakeResponse.getSourceId()
                    + " heartbeatMillis="
                    + heartbeatMillis
                    + " socketType="
                    + socket.getSocket().getClass().getSimpleName()
                    + " options="
                    + new TungstenProperties(handshakeResponse.getOptions())
                            .toString());

            setRmiHost(handshakeResponse.getOption(ProtocolParams.RMI_HOST));
            setRmiPort(handshakeResponse.getOption(ProtocolParams.RMI_PORT));

            if (heartbeatMillis <= 0)
                throw new THLException(
                        "Client heartbeat requests must be greater than zero: "
                                + heartbeatMillis);

            // Vet the epoch number and sequence number.
            long clientLastEpochNumber = handshakeResponse.getLastEpochNumber();
            long clientLastSeqno = handshakeResponse.getLastSeqno();
            long masterMaxSeqno = thl.getMaxStoredSeqno();
            long masterMinSeqno = thl.getMinStoredSeqno();

            String eventIdString = handshakeResponse
                    .getOption(ProtocolParams.INIT_EVENT_ID);

            if ((clientLastEpochNumber < 0 || clientLastSeqno < 0)
                    && eventIdString == null)
            {
                logger.info("Client log checking disabled; not checking for diverging histories");
            }
            else if (masterMaxSeqno == -1 && masterMinSeqno == -1)
            {
                // If the master log is empty, we cannot check anything. Defer
                // the log consistency check until later.
                checkFirstSeqno = true;
                logger.info("Server log is empty; deferring log consistency checking until first transaction");
            }
            else
            {
                LogConnection conn = thl.connect(true);
                try
                {
                    // Look for the indicated event.
                    THLEvent event = null;
                    if (conn.seek(clientLastSeqno))
                        event = conn.next(false);

                    // Did we find it?
                    if (event == null)
                    {
                        // We didn't find the sequence number but that could
                        // happen if the master is a relay that just loaded a
                        // backup and has logs that start after the last
                        // committed seqno. See if the next seqno is present.
                        if (conn.seek(clientLastSeqno + 1))
                            event = conn.next(false);

                        // If it's still null, we really don't have the seqno
                        // and the slave is out of luck.
                        if (event == null)
                        {
                            throw new THLException(
                                    "Master log does not contain requested transaction: client source ID="
                                            + handshakeResponse.getSourceId()
                                            + " seqno=" + clientLastSeqno
                                            + " epoch number="
                                            + clientLastEpochNumber
                                            + " master min seqno="
                                            + masterMinSeqno
                                            + " master max seqno="
                                            + masterMaxSeqno);
                        }
                        else
                        {
                            // Otherwise we give the slave a pass and let it
                            // connect.
                            logger.info("Master log has starting client seqno but does not have previous committed seqno;"
                                    + " not checking for diverging histories");
                        }
                    }
                    else if (event.getEpochNumber() != clientLastEpochNumber)
                    {
                        throw new THLException(
                                "Log epoch numbers do not match: client source ID="
                                        + handshakeResponse.getSourceId()
                                        + " seqno=" + clientLastSeqno
                                        + " server epoch number="
                                        + event.getEpochNumber()
                                        + " client epoch number="
                                        + clientLastEpochNumber);
                    }
                    else
                    {
                        logger.info("Log epoch numbers checked and match: client source ID="
                                + handshakeResponse.getSourceId()
                                + " seqno="
                                + clientLastSeqno
                                + " epoch number="
                                + clientLastEpochNumber);
                    }

                    // If we have an event ID, we need to search forward to find
                    // it.
                    if (eventIdString != null)
                    {
                        // Parse the event ID.
                        EventIdFactory factory = EventIdFactory.getInstance();
                        EventId eventId = factory.createEventId(eventIdString);
                        if (eventId == null || !eventId.isValid())
                        {
                            throw new THLException(
                                    "Unable to parse eventId requested by client: client source ID="
                                            + handshakeResponse.getSourceId()
                                            + " requested event ID=" + eventId);
                        }

                        // Seek the event.
                        boolean found = false;
                        while (!found)
                        {
                            // Get the current seqno and event ID.
                            EventId currentEventId = factory
                                    .createEventId(event.getEventId());
                            long currentSeqno = event.getSeqno();

                            int comp = eventId.compareTo(currentEventId);
                            if (comp > 0)
                            {
                                // We have not found the sequence number.
                                if (logger.isDebugEnabled())
                                {
                                    logger.debug("Skipping event: seqno="
                                            + currentSeqno + " eventId="
                                            + currentEventId);
                                }
                            }
                            else if (comp == 0)
                            {
                                // We found a match, so the next sequence number
                                // should be where we want to seek before
                                // starting.
                                altSeqno = currentSeqno + 1;
                                logger.info("Found alterative seqno requested by client using eventId: seqno="
                                        + altSeqno + " eventId=" + eventId);
                                break;
                            }
                            else
                            {
                                throw new THLException(
                                        "Client seeking event ID that does not exist or may be too old: client source ID="
                                                + handshakeResponse
                                                        .getSourceId()
                                                + " requested event ID="
                                                + eventId
                                                + " closest server seqno="
                                                + currentSeqno
                                                + " closest server event ID="
                                                + currentEventId);
                            }

                            // Look for the next event. Generate an error if
                            // we don't find one.
                            event = conn.next(false);
                            if (event == null)
                            {
                                throw new THLException(
                                        "Client seeking non-existent event ID: client source ID="
                                                + handshakeResponse
                                                        .getSourceId()
                                                + " requested event ID="
                                                + eventId
                                                + " last client seqno="
                                                + clientLastSeqno
                                                + " closest server seqno="
                                                + currentSeqno
                                                + " closest server event ID="
                                                + currentEventId);
                            }
                        }
                    }
                }
                finally
                {
                    if (conn != null)
                    {
                        conn.release();
                    }
                }
            }
        }
    }

    /**
     * Creates a new <code>ConnectorHandler</code> object
     */
    public ConnectorHandler()
    {
    }

    /**
     * Returns true if this handler has terminated and may be discarded.
     */
    public boolean isFinished()
    {
        return finished;
    }

    /**
     * Implements the connector handler loop, which runs until we are
     * interrupted.
     */
    public void run()
    {
        LogConnection connection = null;
        Protocol protocol;
        try
        {
            protocol = new Protocol(context, socket, resetPeriod);
        }
        catch (SSLHandshakeException e)
        {
            // Issue 727 : Add debug info
            logger.error("Received SSL handshake exception", e);
            logger.error("SSL handshake failed; ensure client replicator has SSL enabled: host="
                    + socket.getSocket().getInetAddress().toString());
            return;
        }
        catch (IOException e)
        {
            logger.error("Unable to start connector handler", e);
            return;
        }
        try
        {
            long minSeqno, maxSeqno;
            maxSeqno = thl.getMaxStoredSeqno();
            minSeqno = thl.getMinStoredSeqno();
            LogValidator logValidator = new LogValidator();

            // TUC-2 Added log validator to check log for divergent
            // epoch numbers on last common sequence number.
            protocol.serverHandshake(logValidator, minSeqno, maxSeqno);

            // Name the thread so that developers can see which source ID we
            // are serving.
            Thread.currentThread().setName(
                    "connector-handler-" + protocol.getClientSourceId());

            // Loop until we are cancelled.
            while (!cancelled)
            {
                ProtocolReplEventRequest request;

                // Get the client request.
                request = protocol.waitReplEventRequest();
                long seqno = request.getSeqNo();
                if (logger.isDebugEnabled())
                    logger.debug("Request " + seqno);
                long prefetchRange = request.getPrefetchRange();
                short fragno = 0;

                // If we don't have a connection to the log, make it now.
                if (connection == null)
                {
                    // If we have an alternate sequence number from an event ID,
                    // seek it instead of the requested sequence number.
                    if (this.altSeqno > -1)
                    {
                        logger.info("Seeking alternate sequence number: seqno="
                                + altSeqno);
                        seqno = altSeqno;
                        altSeqno = -1;
                    }

                    // Establish the connection.
                    connection = thl.connect(true);
                    if (!connection.seek(seqno))
                    {
                        String message = "Requested event (#" + seqno + " / "
                                + fragno + ") not found in database";
                        logger.warn(message);
                        sendError(protocol, message);
                        return;
                    }

                    // Set the connection timeout to match the requested
                    // heartbeat interval.
                    connection.setTimeoutMillis(heartbeatMillis);
                }

                long i = 0;
                while (i < prefetchRange)
                {
                    // Get the next event from the log, waiting if necessary. If
                    // the read times out send a heartbeat and try again.
                    THLEvent event = null;
                    try
                    {
                        event = connection.next(true);
                    }
                    catch (LogTimeoutException e)
                    {
                        sendHeartbeat(protocol);
                        continue;
                    }

                    if (event == null)
                    {
                        // connection.next can return null if the event was not
                        // yet fully flushed on disk, for example, by the writer
                        // thread. Just try to read it again.
                        continue;
                    }

                    // If we could not check the first event back to confirm log
                    // consistency, do that now.
                    if (checkFirstSeqno)
                    {
                        logger.info("Checking first seqno returned by THL for consistency: client expected seqno="
                                + seqno
                                + " server returned seqno="
                                + event.getSeqno());

                        if (event.getSeqno() != seqno)
                        {
                            THLException e = new THLException(
                                    "Server does not have seqno expected by the client: client requested seqno="
                                            + seqno + " server seqno returned="
                                            + event.getSeqno());
                            protocol.sendError(e.getMessage());
                            throw e;
                        }
                        checkFirstSeqno = false;
                    }
                    // Peel off and process the underlying replication event.
                    ReplEvent revent = event.getReplEvent();
                    if (revent instanceof ReplDBMSEvent
                            && ((ReplDBMSEvent) revent).getDBMSEvent() instanceof DBMSEmptyEvent)
                    {
                        if (logger.isDebugEnabled())
                            logger.debug("Got an empty event");
                        sendEvent(protocol, revent,
                                (seqno + i >= thl.getMaxStoredSeqno()));
                        i++;
                        fragno = 0;
                    }
                    else
                    {
                        if (revent instanceof ReplDBMSEvent)
                        {
                            ReplDBMSEvent replDBMSEvent = (ReplDBMSEvent) revent;
                            if (replDBMSEvent.getLastFrag())
                            {
                                if (replDBMSEvent instanceof ReplDBMSFilteredEvent)
                                {
                                    ReplDBMSFilteredEvent ev = (ReplDBMSFilteredEvent) replDBMSEvent;
                                    i += 1 + ev.getSeqnoEnd() - ev.getSeqno();
                                }
                                else
                                {
                                    if (logger.isDebugEnabled())
                                        logger.debug("Last fragment of event "
                                                + replDBMSEvent.getSeqno()
                                                + " reached : "
                                                + replDBMSEvent.getFragno());
                                    i++;
                                }
                                fragno = 0;
                            }
                            else
                            {
                                if (logger.isDebugEnabled())
                                    logger.debug("Not the last frag for event "
                                            + replDBMSEvent.getSeqno() + "("
                                            + replDBMSEvent.getFragno() + ")");
                                if (replDBMSEvent instanceof ReplDBMSFilteredEvent)
                                {
                                    ReplDBMSFilteredEvent ev = (ReplDBMSFilteredEvent) replDBMSEvent;
                                    fragno = (short) (ev.getFragnoEnd() + 1);
                                }
                                else
                                    fragno++;
                            }
                        }
                        else
                        {
                            if (logger.isDebugEnabled())
                                logger.debug("Got " + revent.getClass());
                            i++;
                            fragno = 0;
                        }
                        sendEvent(protocol, revent,
                                (seqno + i >= thl.getMaxStoredSeqno()));
                    }
                }
            }
        }
        catch (InterruptedException e)
        {
            if (cancelled)
                logger.info("Connector handler cancelled");
            else
                logger.error(
                        "Connector handler terminated by unexpected interrupt",
                        e);
        }
        catch (EOFException e)
        {
            // The EOF exception happens on a slave being promoted to master
            if (logger.isDebugEnabled())
                logger.debug(
                        "Connector handler terminated by java.io.EOFException",
                        e);
            else
                logger.info("Connector handler terminated by java.io.EOFException");
        }
        catch (IOException e)
        {
            // The IOException occurs normally when a client goes away.
            if (logger.isDebugEnabled())
                logger.debug("Connector handler terminated by i/o exception", e);
            else
                logger.info("Connector handler terminated by i/o exception");
        }
        catch (THLException e)
        {
            // If this is an error we need to signal back to the slave, if it is
            // there.
            String message = "Connector handler terminated by THL exception: "
                    + e.getMessage();

            logger.error(message, e);
            try
            {
                sendError(protocol, message);
            }
            catch (IOException e2)
            {
                logger.warn("Unable to send error to client, disconnecting");
            }
        }
        catch (Throwable t)
        {
            String message = "Connector handler terminated by unexpected exception";
            logger.error(message, t);
            try
            {
                sendError(protocol, message);
            }
            catch (IOException e2)
            {
                logger.warn("Unable to send error to client, disconnecting");
            }
        }
        finally
        {
            // Release log connection.
            if (connection != null)
                connection.release();

            // Close TCP/IP.
            try
            {
                socket.close();
            }
            catch (Exception e)
            {
                logger.warn("Error on closing connection handle", e);
            }

            // Tell the server we are done.
            server.removeClient(this);

            // Make sure we can see that the connection ended.
            logger.info("Terminating THL client connection from source ID: "
                    + protocol.getClientSourceId());
        }
    }

    private void sendEvent(Protocol protocol, ReplEvent event, boolean forceSend)
            throws IOException
    {
        protocol.sendReplEvent(event, forceSend);
    }

    private void sendError(Protocol protocol, String message)
            throws IOException
    {
        protocol.sendError(message);
    }

    private void sendHeartbeat(Protocol protocol) throws IOException
    {
        protocol.sendHeartbeat();
    }

    /**
     * Start the thread to serve thl changes to requesting slaves.
     */
    public void start()
    {
        thd = new Thread(this, "ConnectorHandler: initializing");
        thd.start();
    }

    /**
     * Stop the thread which is serving changes to requesting slaves.
     * 
     * @throws InterruptedException
     */
    public void stop() throws InterruptedException
    {
        if (finished)
            return;

        cancelled = true;

        // Stop handler thread.
        try
        {
            thd.interrupt();
            // Bound the wait to prevent hangs.
            thd.join(10000);
            thd = null;
        }
        catch (InterruptedException e)
        {
            // This is a possible JDK bug or at least inscrutable behavior.
            // First call to Thread.join() when deallocating threads seems
            // to trigger an immediate interrupt.
            logger.warn("Connector handler stop operation was interrupted");
            if (thd != null)
                thd.join();
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        this.context = context;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        resetPeriod = thl.getResetPeriod();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
    }

    /**
     * Sets the server value.
     */
    public void setSocket(SocketWrapper socket)
    {
        this.socket = socket;
    }

    /**
     * Sets the server value.
     * 
     * @param server The server to set.
     */
    public void setServer(Server server)
    {
        this.server = server;
    }

    /**
     * Sets the thl value.
     * 
     * @param thl The thl to set.
     */
    public void setThl(THL thl)
    {
        this.thl = thl;
    }

    /**
     * Sets client's RMI host.
     */
    public void setRmiHost(String rmiHost)
    {
        this.rmiHost = rmiHost;
    }

    /**
     * Sets client's RMI port.
     */
    public void setRmiPort(String rmiPort)
    {
        this.rmiPort = rmiPort;
    }

    /**
     * Gets client's RMI host.
     */
    public String getRmiHost()
    {
        return rmiHost;
    }

    /**
     * Gets client's RMI port.
     */
    public String getRmiPort()
    {
        return rmiPort;
    }
}
