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

package com.continuent.tungsten.replicator.thl;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.cluster.resource.OpenReplicatorParams;
import com.continuent.tungsten.replicator.InSequenceNotification;
import com.continuent.tungsten.replicator.OutOfSequenceNotification;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSFilteredEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplEvent;
import com.continuent.tungsten.replicator.extractor.Extractor;
import com.continuent.tungsten.replicator.extractor.ExtractorException;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.plugin.PluginLoader;
import com.continuent.tungsten.replicator.plugin.ShutdownHook;

/**
 * Implements an extractor to pull events from a remote THL.
 * <p/>
 * This class has specialized concurrency requirements as there is a potential
 * race condition to close connections within the task thread and thread trying
 * to shut down the pipeline. The race arises due the fact that connections may
 * hang when connecting or reading from a connection to a dropped interface and
 * do not accept interrupts. They need to be interrupted by closing the
 * connection. For this reason, closing the connection is synchronized.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class RemoteTHLExtractor implements Extractor, ShutdownHook
{
    private static Logger    logger               = Logger.getLogger(RemoteTHLExtractor.class);

    // Properties.
    private List<String>     uriList;
    private int              resetPeriod          = 1;
    private boolean          checkSerialization   = true;
    private int              heartbeatMillis      = 3000;
    private String           preferredRole        = null;
    private int              preferredRoleTimeout = 32;
    private int              retryInterval        = 1;

    // Connection control variables.
    private PluginContext    pluginContext;
    private ReplDBMSHeader   lastEvent;
    private String           lastEventId;
    private Connector        conn;

    private ReplEvent        pendingEvent;

    // Set to show that we have been shut down.
    private volatile boolean shutdown             = false;

    // Connection counts for timeouts and retries.
    private long             timeoutCount         = 0;
    private long             attemptCount         = 0;

    /**
     * Create Connector instance.
     */
    public RemoteTHLExtractor()
    {
    }

    public List<String> getConnectUri()
    {
        return uriList;
    }

    /**
     * Set the URI(s) of the store to which we connect.
     * 
     * @param connectUri
     */
    public void setConnectUri(List<String> connectUri)
    {
        this.uriList = connectUri;
    }

    public int getResetPeriod()
    {
        return resetPeriod;
    }

    /**
     * Set the number of iterations before resetting the communications stream.
     * Higher values use more memory but are more efficient.
     */
    public void setResetPeriod(int resetPeriod)
    {
        this.resetPeriod = resetPeriod;
    }

    public boolean isCheckSerialization()
    {
        return checkSerialization;
    }

    /**
     * If true, check epoch number and sequence number of last event we have
     * received.
     * 
     * @param checkSerialization
     */
    public void setCheckSerialization(boolean checkSerialization)
    {
        this.checkSerialization = checkSerialization;
    }

    public int getHeartbeatInterval()
    {
        return heartbeatMillis;
    }

    /**
     * Sets the interval for sending heartbeat events from server to avoid
     * TCP/IP timeout on server connection. The normal read timeout is 10x this
     * value. The value is also used for connection timeouts, where we use 1x
     * this value.
     */
    public void setHeartbeatInterval(int heartbeatMillis)
    {
        this.heartbeatMillis = heartbeatMillis;
    }

    /** Returns the preferred master server role. */
    public String getPreferredRole()
    {
        return preferredRole;
    }

    /**
     * Sets the preferred role of the master replicator. If set to 'slave' we
     * will try to find a slave from the URL list before accepting a master.
     */
    public void setPreferredRole(String preferredRole)
    {
        this.preferredRole = preferredRole;
    }

    public int getPreferredRoleTimeout()
    {
        return preferredRoleTimeout;
    }

    /** Sets the timeout to find the preferred master role in seconds. */
    public void setPreferredRoleTimeout(int preferredRoleTimeout)
    {
        this.preferredRoleTimeout = preferredRoleTimeout;
    }

    public int getRetryInterval()
    {
        return retryInterval;
    }

    /** Sets the timeout between connection retries in seconds. */
    public void setRetryInterval(int retryTimeout)
    {
        this.retryInterval = retryTimeout;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.Extractor#extract()
     */
    public ReplDBMSEvent extract() throws ReplicatorException,
            InterruptedException
    {
        try
        {
            // Open the connector if it is not yet open.
            if (conn == null)
            {
                openConnector();
            }

            // Fetch the event.
            ReplEvent replEvent = null;
            while (replEvent == null && !shutdown)
            {
                // If we have a pending event from an earlier read, return that.
                if (pendingEvent != null)
                {
                    replEvent = pendingEvent;
                    pendingEvent = null;
                    break;
                }

                long seqno = 0;
                try
                {
                    if (lastEvent != null)
                        if (lastEvent.getLastFrag())
                        {
                            if (lastEvent instanceof ReplDBMSFilteredEvent)
                            {
                                ReplDBMSFilteredEvent ev = (ReplDBMSFilteredEvent) lastEvent;
                                seqno = ev.getSeqnoEnd() + 1;
                            }
                            else
                                seqno = lastEvent.getSeqno() + 1;
                        }
                        else
                            seqno = lastEvent.getSeqno();
                    replEvent = conn.requestEvent(seqno);
                    if (replEvent == null)
                        continue;

                    // If the lastEventId was set, we have some housekeeping
                    // ahead of us.
                    if (lastEventId != null)
                    {
                        // Searching for lastEventId can cause skips in the
                        // log. If so, we need to insert a filter event to
                        // avoid breaks and return that first. Otherwise
                        // downstream stages will break due to sequence number
                        // gaps.
                        if (replEvent.getSeqno() > seqno)
                        {
                            pendingEvent = replEvent;
                            replEvent = new ReplDBMSFilteredEvent(lastEventId,
                                    seqno, replEvent.getSeqno() - 1, (short) 0);
                        }

                        // Next, clear the last event ID.
                        if (logger.isDebugEnabled())
                        {
                            logger.debug("Clearing last event ID: "
                                    + lastEventId);
                        }
                        lastEventId = null;
                    }
                }
                catch (IOException e)
                {
                    if (shutdown)
                    {
                        if (logger.isDebugEnabled())
                        {
                            logger.debug(
                                    "Ignoring exception after shutdown request",
                                    e);
                        }
                        logger.info("Connector read failed after shutdown; not attempting to reconnect");
                        break;
                    }
                    else
                    {
                        // If the connection dropped in the middle of a
                        // fragmented transaction, we need to ignore events that
                        // were already stored, otherwise it will generate an
                        // integrity constraint violation
                        reconnect();
                        continue;
                    }
                }

                // Ensure we have the right *sort* of replication event.
                if (replEvent != null && !(replEvent instanceof ReplDBMSEvent))
                    throw new ExtractorException(
                            "Unexpected event type: seqno =" + seqno + " type="
                                    + replEvent.getClass().getName());
            }

            // Remember which event we just read and ask for the next one.
            lastEvent = (ReplDBMSEvent) replEvent;
            return (ReplDBMSEvent) replEvent;
        }
        catch (THLException e)
        {
            // THLException messages are user-readable so just pass 'em along.
            throw new ExtractorException(e.getMessage(), e);
        }

    }

    /** Does not make sense for this extractor type. */
    public String getCurrentResourceEventId() throws ReplicatorException,
            InterruptedException
    {
        return null;
    }

    public boolean hasMoreEvents()
    {
        return false;
    }

    public void setLastEvent(ReplDBMSHeader event) throws ReplicatorException
    {
        lastEvent = event;
    }

    /**
     * Sets the last event ID for extraction. If this is set, we will request
     * (and receive) the first event from the master log that matches this
     * event.
     * 
     * @see com.continuent.tungsten.replicator.extractor.Extractor#setLastEventId(java.lang.String)
     */
    public void setLastEventId(String eventId) throws ReplicatorException
    {
        if (logger.isDebugEnabled())
            logger.debug("Set last event ID on remote THL extractor: "
                    + eventId);
        lastEventId = eventId;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException
    {
        // Store context for later.
        this.pluginContext = context;

        // Set the connect URI to a default if not already set.
        if (this.uriList == null || this.uriList.size() == 0)
        {
            uriList = new ArrayList<String>();
            uriList.add(context.getReplicatorProperties().get(
                    ReplicatorConf.MASTER_CONNECT_URI));
        }

        // See if we have an online option that overrides serialization
        // checking.
        if (pluginContext.getOnlineOptions().getBoolean(
                OpenReplicatorParams.FORCE))
        {
            if (checkSerialization)
            {
                logger.info("Force option enabled; log serialization checking is disabled");
                checkSerialization = false;
            }
        }

        // Adjust the preferred role so that null and empty strings are
        // identical.
        if (preferredRole != null && "".equals(preferredRole.trim()))
            preferredRole = null;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException
    {
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException
    {
        // Clearing the connection must be synchronized.
        // See concurrency note in class header comment.
        synchronized (this)
        {
            if (conn != null)
            {
                conn.close();
                // Do not clear variable. It can cause an NPR in the
                // openConnector() method which may still be attempting to
                // open a connection.
            }
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ShutdownHook#shutdown(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void shutdown(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        // Stop the connector.
        if (logger.isDebugEnabled())
        {
            logger.debug("Shutdown hook invoked; attempting to close connector");
        }
        shutdown = true;
        release(context);
    }

    // Reconnect a failed connection.
    private void reconnect() throws InterruptedException, ReplicatorException
    {
        synchronized (this)
        {
            if (conn != null)
            {
                conn.close();
                conn = null;
            }
        }

        // Reconnect after lost connection.
        logger.info("Connection to remote thl lost; reconnecting");
        pluginContext.getEventDispatcher().put(new OutOfSequenceNotification());
        openConnector();
    }

    /**
     * Open a master connection. The algorithm for connecting cycles through the
     * available THL URIs until a suitable URI is found.
     * <ul>
     * <li>If there is a single URI and it is available, we connect to it once
     * it is available regardless of the role.</li>
     * <li>If there are multiple URIs and all are available, we connect
     * preferentially to the URI whose role matches the value of the
     * preferredRole property.</li>
     * <li>In the event that a preferred URI role is not available, we connect
     * to another URI after the preferred role timeout expires.</li>
     * </ul>
     * We pause following each round of checking for a period of time determined
     * by the retryInterval parameter. This timeout is determined in advance at
     * the start of each round of connection attempts to ensure we retry at
     * consistent intervals that are not affected by time to make connections
     * themselves.
     * 
     * @throws ReplicatorException
     * @throws InterruptedException
     */
    private void openConnector() throws ReplicatorException,
            InterruptedException
    {
        // Set up for connection to remote URI.
        ConnectUriManager uriManager = new ConnectUriManager(uriList);
        logger.info("Opening connection to master: " + uriManager.toString()
                + " preferred role=" + preferredRole
                + " preferred role timeout=" + preferredRoleTimeout);
        String currentUri = null;

        // Compute timeout value for seeking preferred master role.
        long rollTimeoutMillis = System.currentTimeMillis()
                + (preferredRoleTimeout * 1000);
        long iterations = 0;

        // The starting interval for retry attempts. This backs off over
        // time to the value of retryInterval, which is expressed in seconds.
        long retryIntervalMillis = 500;
        long nextIntervalStartMillis = System.currentTimeMillis()
                + retryIntervalMillis;

        for (;;)
        {
            try
            {
                // If we need to check serialization we must supply the seqno
                // and epoch.
                try
                {
                    currentUri = uriManager.next();
                    conn = (Connector) PluginLoader
                            .load(pluginContext
                                    .getReplicatorProperties()
                                    .getString(
                                            ReplicatorConf.THL_PROTOCOL,
                                            ReplicatorConf.THL_PROTOCOL_DEFAULT,
                                            false));
                    conn.setURI(currentUri);
                    conn.setResetPeriod(resetPeriod);
                    conn.setHeartbeatMillis(heartbeatMillis);
                    conn.setLastEventId(this.lastEventId);
                    if (this.lastEvent == null
                            || this.checkSerialization == false)
                    {
                        conn.setLastSeqno(-1);
                        conn.setLastEpochNumber(-1);
                    }
                    else
                    {
                        conn.setLastSeqno(lastEvent.getSeqno());
                        conn.setLastEpochNumber(lastEvent.getEpochNumber());
                    }
                    conn.configure(pluginContext);
                    conn.prepare(pluginContext);
                }
                catch (ReplicatorException e)
                {
                    throw new THLException("Error while initializing plug-in ",
                            e);
                }

                // Try to connect. Accept if the connection matches our
                // criteria.
                conn.connect();
                String masterRole = conn.getServerCapability(Protocol.ROLE);
                if (preferredRole == null)
                {
                    // Accept if there is no preferred role.
                    logger.info("Connection is accepted");
                    break;
                }
                else if (preferredRole.equals(masterRole))
                {
                    // Accept if there is a preferred role and we have a match.
                    logger.info("Connection is accepted by role match against master: preferredRole="
                            + preferredRole + " masterRole=" + masterRole);
                    break;
                }
                else if (uriManager.getIterations() > 0
                        && System.currentTimeMillis() > rollTimeoutMillis)
                {
                    // Accept if we have been through the list at least once
                    // and the timeout has expired.
                    logger.info("Connection is accepted after roll search timed out: preferredRole="
                            + preferredRole
                            + " masterRole="
                            + masterRole
                            + " iterations=" + uriManager.getIterations());
                    break;
                }
                else
                {
                    // If we get here the role did not match and we want to try
                    // for something better.
                    closeConnector();
                }
            }
            catch (SocketTimeoutException e)
            {
                // Timeouts are special, hence should be flagged.
                timeoutCount++;
                prepareRetry(uriManager);
            }
            catch (IOException e)
            {
                prepareRetry(uriManager);
            }
            finally
            {
                // If we did not get a connection in the last full iteration, we
                // should now delay using a backoff interval that increases
                // to the intervalRetry value and also takes into consideration
                // how long we spent trying to connect. To stay sane, this is
                // the only part of the connection code that should pause
                // between retries.
                if (conn == null && uriManager.getIterations() > iterations)
                {
                    if (logger.isDebugEnabled())
                    {
                        logger.debug("Unable to find acceptable connection; new round: iterations="
                                + iterations
                                + " retryInterval="
                                + (retryIntervalMillis / 1000));
                    }
                    iterations = uriManager.getIterations();

                    // See if we need to pause and do so.
                    long sleepMillis = nextIntervalStartMillis
                            - System.currentTimeMillis();
                    if (sleepMillis > 0)
                        Thread.sleep(sleepMillis);

                    // Adjust the interval if necessary up to the max value.
                    // This creates a nice backoff to ensure quick connection
                    // initially. Then compute the start of the next connection
                    // interval from it.
                    if (retryIntervalMillis < (this.retryInterval * 1000))
                    {
                        retryIntervalMillis = Math.min(retryInterval * 1000,
                                retryIntervalMillis * 2);
                    }
                    nextIntervalStartMillis = System.currentTimeMillis()
                            + retryIntervalMillis;
                }
            }
        }

        // Record the current URI so that it is visible to the rest of the
        // replicator.
        pluginContext.setPipelineSource(currentUri);

        // Announce the happy event and reset retry count.
        logger.info("Connected to master on uri=" + currentUri + " after "
                + attemptCount + " retries");
        attemptCount = 0;
        timeoutCount = 0;
        pluginContext.getEventDispatcher().put(new InSequenceNotification());
    }

    // Prepare for a connection retry, which includes incrementing the retry
    // count.
    private void prepareRetry(ConnectUriManager uriManager)
            throws InterruptedException
    {
        // Sleep for 1 second per retry; report every 10 retries.
        closeConnector();
        attemptCount++;
        if ((attemptCount % 10) == 0)
        {
            logger.info("Waiting for master to become available: uri="
                    + uriManager.toString() + " attempts=" + attemptCount
                    + " timeouts=" + timeoutCount);
        }
        if (timeoutCount > 0 && (timeoutCount % 10) == 0)
        {
            logger.info("Timeouts are occurring; check master log to ensure connectivity or increase heartbeatMillis "
                    + "in service properties file: current value="
                    + heartbeatMillis);
        }
    }

    // Close the connector. Clearing the connection must be synchronized.
    // See concurrency note in class header comment.
    private synchronized void closeConnector()
    {
        if (conn != null)
        {
            conn.close();
            conn = null;
        }
    }
}