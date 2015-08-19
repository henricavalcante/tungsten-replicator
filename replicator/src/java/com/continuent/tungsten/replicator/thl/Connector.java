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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.sockets.ClientSocketWrapper;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplEvent;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.plugin.ReplicatorPlugin;

/**
 * This class defines a Connector
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class Connector implements ReplicatorPlugin
{
    private static Logger       logger          = Logger.getLogger(Connector.class);

    protected PluginContext     pluginContext   = null;
    protected String            host            = null;
    protected int               port            = 2112;
    private ClientSocketWrapper clientSocket;
    private long                minSeqNo        = -1;
    private long                maxSeqNo        = -1;
    private Protocol            protocol        = null;
    private TungstenProperties  serverCapabilities;
    protected boolean           useSSL;

    protected int               resetPeriod;
    protected long              lastSeqno;
    protected long              lastEpochNumber;
    protected int               heartbeatMillis = 3000;
    protected String            lastEventId;

    private String              remoteURI       = null;

    // Marked true to show this connector has been closed.
    boolean                     closed;

    /**
     * Creates a new instance. This is required so the connector can be
     * instantiated as a replicator plugin.
     */
    public Connector()
    {
    }

    /**
     * Creates a new <code>Connector</code> object Creates a new
     * <code>Connector</code> object
     * 
     * @param remoteURI URI string pointing to remote host
     * @param resetPeriod output stream resetting period
     * @throws ReplicatorException
     */
    public Connector(PluginContext context, String remoteURI, int resetPeriod,
            long lastSeqno, long lastEpochNumber, int heartbeatMillis)
            throws ReplicatorException
    {
        this.pluginContext = context;
        this.remoteURI = remoteURI;
        this.lastSeqno = lastSeqno;
        this.lastEpochNumber = lastEpochNumber;
        this.heartbeatMillis = heartbeatMillis;
        this.resetPeriod = resetPeriod;
    }

    /**
     * Connect to master. 
     */
    public void connect() throws ReplicatorException, IOException
    {
        if (logger.isDebugEnabled())
            logger.debug("Connecting to " + host + ":" + port + " useSSL="
                    + useSSL);
        try
        {
            // Create the socket and connect with a relatively short timeout.
            InetSocketAddress address = new InetSocketAddress(host, port);
            if (address.isUnresolved())
            {
                throw new THLException(
                        "THL connection failure; cannot resolve address: host="
                                + host + " port=" + port);
            }
            clientSocket = new ClientSocketWrapper();
            clientSocket.setAddress(address);
            clientSocket.setUseSSL(useSSL);
            clientSocket.setConnectTimeout(heartbeatMillis);
            // Timeout at 10 times the heartbeat interval. This is longer than
            // the connect timeout by design so we don't time out if the server
            // is busy.
            clientSocket.setReadTimeout(heartbeatMillis * 10);
            clientSocket.connect();
        }
        catch (IllegalArgumentException e)
        {
            throw new THLException(
                    "THL connection failure; address is invalid: host=" + host
                            + " port=" + port);
        }

        // Perform handshake with server.
        protocol = new Protocol(pluginContext, clientSocket, resetPeriod);
        SeqNoRange seqNoRange = protocol.clientHandshake(lastEpochNumber,
                lastSeqno, heartbeatMillis, lastEventId);

        minSeqNo = seqNoRange.getMinSeqNo();
        maxSeqNo = seqNoRange.getMaxSeqNo();

        // Store server capabilities
        serverCapabilities = protocol.getServerCapabities();
    }

    /**
     * Close channel. This is synchronized to prevent accidental double calls.
     */
    public synchronized void close()
    {
        if (!closed)
        {
            clientSocket.close();
            closed = true;
        }
    }

    /**
     * Fetch an event from server.
     * 
     * @param seqNo
     * @return ReplEvent
     * @throws ReplicatorException
     * @throws IOException
     */
    public ReplEvent requestEvent(long seqNo) throws ReplicatorException,
            IOException
    {
        ReplEvent retval;
        if (logger.isDebugEnabled())
            logger.debug("Requesting event " + seqNo);
        retval = protocol.requestReplEvent(seqNo);
        if (logger.isDebugEnabled() && retval instanceof ReplDBMSEvent)
        {
            ReplDBMSEvent ev = (ReplDBMSEvent) retval;
            logger.debug("Received event " + ev.getSeqno() + "/"
                    + ev.getFragno());
        }
        return retval;
    }

    /**
     * Return server capability by name.
     */
    public String getServerCapability(String name)
    {
        return serverCapabilities.getString(name);
    }

    /**
     * Return minimum sequence number stored on server.
     */
    public long getMinSeqNo()
    {
        return minSeqNo;
    }

    /**
     * Return maximum sequence number stored on server.
     */
    public long getMaxSeqNo()
    {
        return maxSeqNo;
    }

    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        this.pluginContext = context;
        try
        {
            URI uri = new URI(remoteURI);
            String protocol = uri.getScheme();
            if (THL.PLAINTEXT_URI_SCHEME.equals(protocol))
            {
                this.useSSL = false;
            }
            else if (THL.SSL_URI_SCHEME.equals(protocol))
            {
                this.useSSL = true;
            }
            else
            {
                throw new THLException("Unsupported scheme " + protocol);
            }
            this.host = uri.getHost();
            if ((this.port = uri.getPort()) == -1)
                this.port = 2112;
        }
        catch (URISyntaxException e)
        {
            throw new THLException(e.getMessage(), e);
        }
    }

    public void prepare(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
    }

    public void release(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
    }

    /**
     * Sets the resetPeriod value.
     * 
     * @param resetPeriod The resetPeriod to set.
     */
    public void setResetPeriod(int resetPeriod)
    {
        this.resetPeriod = resetPeriod;
    }

    /**
     * Sets the lastSeqno value.
     * 
     * @param lastSeqno The lastSeqno to set.
     */
    public void setLastSeqno(long lastSeqno)
    {
        this.lastSeqno = lastSeqno;
    }

    /**
     * Sets the lastEpochNumber value.
     * 
     * @param lastEpochNumber The lastEpochNumber to set.
     */
    public void setLastEpochNumber(long lastEpochNumber)
    {
        this.lastEpochNumber = lastEpochNumber;
    }

    public void setURI(String connectUri)
    {
        this.remoteURI = connectUri;
    }

    /** Sets the number of milliseconds between heartbeats. */
    public void setHeartbeatMillis(int heartbeatMillis)
    {
        this.heartbeatMillis = heartbeatMillis;
    }

    public void setLastEventId(String lastEventId)
    {
        this.lastEventId = lastEventId;
    }
}