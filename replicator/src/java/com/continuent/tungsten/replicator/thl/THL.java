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
 * Initial developer(s): Teemu Ollakka, Robert Hodges
 * Contributor(s): Stephane Giron
 */

package com.continuent.tungsten.replicator.thl;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.LinkedList;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.cluster.resource.physical.Replicator;
import com.continuent.tungsten.common.config.Interval;
import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.datasource.CommitSeqno;
import com.continuent.tungsten.replicator.datasource.CommitSeqnoAccessor;
import com.continuent.tungsten.replicator.datasource.UniversalConnection;
import com.continuent.tungsten.replicator.datasource.UniversalDataSource;
import com.continuent.tungsten.replicator.event.ReplControlEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplDBMSHeaderData;
import com.continuent.tungsten.replicator.event.ReplEvent;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.storage.Store;
import com.continuent.tungsten.replicator.thl.log.DiskLog;
import com.continuent.tungsten.replicator.thl.log.LogConnection;
import com.continuent.tungsten.replicator.thl.serializer.ProtobufSerializer;
import com.continuent.tungsten.replicator.util.AtomicCounter;

/**
 * Implements a standard Store interface on the THL (transaction history log).
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class THL implements Store
{
    protected static Logger     logger               = Logger.getLogger(THL.class);

    // Version information and constants.
    public static final int     MAJOR                = 1;
    public static final int     MINOR                = 3;
    public static final String  SUFFIX               = "";
    public static final String  PLAINTEXT_URI_SCHEME = "thl";
    public static final String  SSL_URI_SCHEME       = "thls";

    // Name of this store.
    private String              name;

    /** URL of storage listener. Default listens on all interfaces. */
    private String              storageListenerUri   = "thl://0.0.0.0:2112/";

    private String              logDir               = "/opt/continuent/logs/";
    private String              eventSerializer      = ProtobufSerializer.class
                                                             .getName();

    // Data source with which this THL is associated.
    protected String            dataSource;

    /** Number of events between resets on stream. */
    private int                 resetPeriod          = 1;

    /** Store and compare checksum values on the log. */
    private boolean             doChecksum           = true;

    /** Name of the class used to serialize events. */
    protected String            eventSerializerClass = ProtobufSerializer.class
                                                             .getName();

    /** Log file maximum size in bytes. */
    protected int               logFileSize          = 1000000000;

    /** Log file retention in milliseconds. Defaults to 0 (= forever) */
    protected long              logFileRetainMillis  = 0;

    /** Idle log Connection timeout in seconds. */
    protected int               logConnectionTimeout = 28800;

    /** I/O buffer size in bytes. */
    protected int               bufferSize           = 131072;

    /**
     * Flush data after this many milliseconds. 0 flushes after every write.
     */
    private long                flushIntervalMillis  = 0;

    /** If true, fsync when flushing. */
    private boolean             fsyncOnFlush         = false;

    // Catalog access and disk log.
    private UniversalConnection conn                 = null;
    private CommitSeqno         commitSeqno          = null;
    private CommitSeqnoAccessor commitSeqnoAccessor  = null;
    private DiskLog             diskLog              = null;

    // Storage management variables.
    protected PluginContext     context;
    private AtomicCounter       sequencer;

    // Storage connectivity.
    private Server              server               = null;

    private boolean             readOnly             = false;

    // This indicates whether replicator will stop or keep on trying to extract
    // data despite errors while storing its position into database
    // (CommitSeqno)
    private boolean             stopOnDBError        = true;

    // If true check log consistency with catalog when starting up.
    private boolean             logConsistencyCheck  = false;

    // A restart position that allows downstream stages to set the log
    // position when the log is empty.
    private ReplDBMSHeader      restartPosition;

    /** Creates a store instance. */
    public THL()
    {
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    // Accessors for configuration.
    public String getStorageListenerUri()
    {
        return storageListenerUri;
    }

    public void setStorageListenerUri(String storageListenerUri)
    {
        this.storageListenerUri = storageListenerUri;
    }

    public void setDataSource(String dataSource)
    {
        this.dataSource = dataSource;
    }

    public String getDataSource()
    {
        return dataSource;
    }

    public int getResetPeriod()
    {
        return resetPeriod;
    }

    public void setResetPeriod(int resetPeriod)
    {
        this.resetPeriod = resetPeriod;
    }

    /**
     * Sets the logDir value.
     * 
     * @param logDir The logDir to set.
     */
    public void setLogDir(String logDir)
    {
        this.logDir = logDir;
    }

    /**
     * Sets the logFileSize value in bytes.
     * 
     * @param logFileSize The logFileSize to set.
     */
    public void setLogFileSize(int logFileSize)
    {
        this.logFileSize = logFileSize;
    }

    /**
     * Determines whether to checksum log records.
     * 
     * @param doChecksum If true use checksums
     */
    public void setDoChecksum(boolean doChecksum)
    {
        this.doChecksum = doChecksum;
    }

    /**
     * Sets the event serializer name.
     */
    public void setEventSerializer(String eventSerializer)
    {
        this.eventSerializer = eventSerializer;
    }

    /**
     * Sets the log file retention interval.
     */
    public void setLogFileRetention(String logFileRetention)
    {
        this.logFileRetainMillis = new Interval(logFileRetention).longValue();
    }

    /**
     * Sets the idle log connection timeout in seconds.
     */
    public void setLogConnectionTimeout(int logConnectionTimeout)
    {
        this.logConnectionTimeout = logConnectionTimeout;
    }

    /**
     * Sets the log buffer size.
     */
    public void setBufferSize(int bufferSize)
    {
        this.bufferSize = bufferSize;
    }

    /**
     * Sets the interval between flush calls.
     */
    public void setFlushIntervalMillis(long flushIntervalMillis)
    {
        this.flushIntervalMillis = flushIntervalMillis;
    }

    /**
     * If set to true, perform an fsync with every flush. Warning: fsync is very
     * slow, so you want a long flush interval in this case.
     */
    public void setFsyncOnFlush(boolean fsyncOnFlush)
    {
        this.fsyncOnFlush = fsyncOnFlush;
    }

    public void setReadOnly(String ro)
    {
        readOnly = (ro.equals("true"));
    }

    public void setStopOnDBError(boolean stopOnDBErr)
    {
        this.stopOnDBError = stopOnDBErr;
    }

    public boolean getStopOnDBError()
    {
        return stopOnDBError;
    }

    public boolean isLogConsistencyCheck()
    {
        return logConsistencyCheck;
    }

    public void setLogConsistencyCheck(boolean checkRecoveredMasterLog)
    {
        this.logConsistencyCheck = checkRecoveredMasterLog;
    }

    public void setRestartPosition(ReplDBMSHeader restartPosition)
    {
        this.restartPosition = restartPosition;
    }

    // STORE API STARTS HERE.

    /**
     * Return max stored sequence number.
     */
    public long getMaxStoredSeqno()
    {
        // This prevents race conditions when going offline.
        DiskLog localCopy = diskLog;
        if (localCopy == null)
            return -1;
        else
            return localCopy.getMaxSeqno();
    }

    /**
     * Return minimum stored sequence number.
     */
    public long getMinStoredSeqno()
    {
        // This prevents race conditions when going offline.
        DiskLog localCopy = diskLog;
        if (localCopy == null)
            return -1;
        else
            return localCopy.getMinSeqno();
    }

    /**
     * Updates the active sequence number on the log. Log files can only be
     * deleted if their last sequence number is below this value.
     */
    public void updateActiveSeqno(long activeSeqno)
    {
        diskLog.setActiveSeqno(activeSeqno);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        // Store variables.
        this.context = context;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public synchronized void prepare(PluginContext context)
            throws ReplicatorException, InterruptedException
    {
        // Prepare database connection.
        UniversalDataSource dataSourceImpl = context.getDataSource(dataSource);
        if (dataSourceImpl == null)
        {
            logger.info("Data source is not specified or a dummy; catalog access is disabled");
        }
        else
        {
            logger.info("Connecting to data source");
            commitSeqno = dataSourceImpl.getCommitSeqno();
            conn = dataSourceImpl.getConnection();

            // Suppress logging if we do not want transactions to show up in the
            // log.
            if (context.isMaster())
            {
                if (context.isPrivilegedMaster())
                {
                    logger.info("Suppressing logging on privileged master");
                    conn.setPrivileged(true);
                    conn.setLogged(false);
                }
            }
            else if (context.isSlave())
            {
                if (context.isPrivilegedSlave()
                        && !context.logReplicatorUpdates())
                {
                    logger.info("Suppressing logging on privileged slave");
                    conn.setPrivileged(true);
                    conn.setLogged(false);
                }
            }
            commitSeqnoAccessor = commitSeqno.createAccessor(0, conn);
        }

        // Configure and prepare the log.
        diskLog = new DiskLog();
        diskLog.setDoChecksum(doChecksum || context.isDoChecksum());
        diskLog.setEventSerializerClass(eventSerializer);
        diskLog.setLogDir(logDir);
        diskLog.setLogFileSize(logFileSize);
        diskLog.setLogFileRetainMillis(logFileRetainMillis);
        diskLog.setLogConnectionTimeoutMillis(logConnectionTimeout * 1000);
        diskLog.setBufferSize(bufferSize);
        diskLog.setFsyncOnFlush(fsyncOnFlush);
        if (fsyncOnFlush)
        {
            // Only used with fsync.
            diskLog.setFlushIntervalMillis(flushIntervalMillis);
        }
        diskLog.setReadOnly(readOnly);
        diskLog.prepare();
        logger.info("Log preparation is complete");

        // Ensure the restart position is consistent and adjust if necessary.
        ensureRestartPositionConsistency();

        // Start server for THL connections.
        if (context.isRemoteService() == false)
        {
            try
            {
                server = new Server(context, sequencer, this);
                server.start();
            }
            catch (IOException e)
            {
                throw new ReplicatorException("Unable to start THL server", e);
            }
        }
    }

    /**
     * Ensure that the log and catalog restart point are consistent. If we are
     * going online as a slave and the last online role was master, we expect
     * the trep_commit_seqno restart position matches the end of the THL. Since
     * masters cannot always update the commit position before going offline, we
     * must in that case adjust the position to avoid errors.
     * <p/>
     * This check is a separate function as there are numerous boundary
     * conditions for a successful check.
     */
    private void ensureRestartPositionConsistency() throws ReplicatorException,
            InterruptedException
    {
        // Establish boundary conditions for a check.
        if (!logConsistencyCheck)
        {
            logger.info("Restart consistency checking is disabled");
            return;
        }
        if (commitSeqno == null)
        {
            logger.info("Restart consistency checking skipped because catalog is disabled");
            return;
        }
        if (!"master".equals(context.getLastOnlineRoleName())
                || !context.isSlave())
        {
            logger.info("Restart consistency checking skipped as we are not recovering a master to a slave");
            return;
        }

        // It is now safe to look at the events as we know the catalog exists.
        ReplDBMSHeader lastLogEvent = getLastLoggedEvent();
        ReplDBMSHeader lastCatalogEvent = commitSeqno.minCommitSeqno();

        if (lastCatalogEvent == null)
        {
            logger.info("Restart consistency checking skipped as there is no restart point in catalog");
            return;
        }
        if (lastLogEvent == null)
        {
            logger.info("Restart consistency checking skipped as THL does not contain events");
            return;
        }

        // A check is useful and required.
        logger.info("Checking restart consistency when recovering master to slave: THL seqno="
                + lastLogEvent.getSeqno()
                + " catalog seqno="
                + lastCatalogEvent.getSeqno());
        if (lastLogEvent.getSeqno() > lastCatalogEvent.getSeqno())
        {
            // Bring catalog up to date but only if the epoch numbers match.
            // This ensures we are talking about transactions from the same
            // master and is why epoch numbers were invented.
            if (lastLogEvent.getEpochNumber() == lastCatalogEvent
                    .getEpochNumber())
            {
                logger.info("Updating catalog seqno position to match THL");
                THLEvent thlEvent = new THLEvent(lastLogEvent.getSeqno(),
                        lastLogEvent.getFragno(), lastLogEvent.getLastFrag(),
                        lastLogEvent.getSourceId(), THLEvent.REPL_DBMS_EVENT,
                        lastLogEvent.getEpochNumber(), (Timestamp) null,
                        lastLogEvent.getExtractedTstamp(),
                        lastLogEvent.getEventId(), lastLogEvent.getShardId(),
                        (ReplEvent) null);
                this.updateCommitSeqno(thlEvent);
            }
            else
            {
                logger.warn("Unable to update catalog position as epoch numbers do not match: seqno="
                        + lastLogEvent.getSeqno()
                        + " THL epoch number="
                        + lastLogEvent.getEpochNumber()
                        + " catalog epoch number="
                        + lastCatalogEvent.getEpochNumber());
            }
        }
        else if (lastLogEvent.getSeqno() < lastCatalogEvent.getSeqno())
        {
            // Note if the catalog is higher than the log. This could indicate a
            // consistency issue.
            logger.info("Unable to update catalog position as last THL record is lower than catalog seqno");
            logger.info("This condition may occur naturally if the THL has been truncated");
        }
        else
        {
            logger.info("Restart position is consistent; no need to update");
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public synchronized void release(PluginContext context)
            throws InterruptedException, ReplicatorException
    {
        // Cancel server.
        if (server != null)
        {
            try
            {
                server.stop();
            }
            catch (InterruptedException e)
            {
                logger.warn(
                        "Server stop operation was unexpectedly interrupted", e);
            }
            finally
            {
                server = null;
            }
        }

        if (commitSeqnoAccessor != null)
        {
            commitSeqnoAccessor.close();
        }
        if (conn != null)
        {
            conn.close();
        }
        if (commitSeqno != null)
        {
            commitSeqno.release();
            commitSeqno = null;
        }
        if (diskLog != null)
        {
            diskLog.release();
            diskLog = null;
        }
    }

    /**
     * Connect to the log. Adapters must call this to use the log.
     * 
     * @param readonly If true, this is a readonly connection
     * @return A disk log client
     */
    public LogConnection connect(boolean readonly) throws ReplicatorException
    {
        return diskLog.connect(readonly);
    }

    /**
     * Disconnect from the log. Adapters must call this to free resources and
     * avoid leaks.
     * 
     * @param client a Disk log client to be disconnected
     * @throws ReplicatorException
     */
    public void disconnect(LogConnection client) throws ReplicatorException
    {
        client.release();
    }

    /**
     * Updates the sequence number stored in the catalog trep_commit_seqno. If
     * the catalog is disabled we do nothing, which allows us to run unit tests
     * easily without a DBMS present.
     * 
     * @throws ReplicatorException Thrown if update is unsuccessful
     */
    public void updateCommitSeqno(THLEvent event) throws ReplicatorException,
            InterruptedException
    {
        if (commitSeqno == null)
        {
            if (logger.isDebugEnabled())
                logger.debug("Seqno update is disabled: seqno="
                        + event.getSeqno());
        }
        else
        {
            // Recreate header data.
            long applyLatency = (System.currentTimeMillis() - event
                    .getSourceTstamp().getTime()) / 1000;
            ReplDBMSHeaderData header = new ReplDBMSHeaderData(
                    event.getSeqno(), event.getFragno(), event.getLastFrag(),
                    event.getSourceId(), event.getEpochNumber(),
                    event.getEventId(), event.getShardId(),
                    event.getSourceTstamp(), applyLatency);

            commitSeqnoAccessor.updateLastCommitSeqno(header, applyLatency);
        }
    }

    /**
     * Returns true if the indicated sequence number is available.
     */
    public boolean pollSeqno(long seqno)
    {
        return seqno <= diskLog.getMaxSeqno();
    }

    /**
     * Get the last applied event. We first try the disk log then if that is
     * absent try the catalog. If there is nothing there we must be starting
     * from scratch and return null.
     * 
     * @return An event header or null if log is newly initialized
     * @throws InterruptedException
     * @throws ReplicatorException
     */
    public ReplDBMSHeader getLastAppliedEvent() throws ReplicatorException,
            InterruptedException
    {
        // Look first in the log.
        ReplDBMSHeader lastHeader = getLastLoggedEvent();
        if (lastHeader != null)
        {
            // Return the log position if available.
            return lastHeader;
        }
        else if (commitSeqno != null)
        {
            // Return the trep_commit_seqno position if that can be found.
            return commitSeqno.minCommitSeqno();
        }
        else if (restartPosition != null)
        {
            return restartPosition;
        }
        else
        {
            // Otherwise we have no recorded event, so return null.
            return null;
        }
    }

    /**
     * Get the last event applied to the replicator log or return null if there
     * is no such event.
     * 
     * @return An event header or null if log is newly initialized
     */
    public ReplDBMSHeader getLastLoggedEvent() throws ReplicatorException,
            InterruptedException
    {
        // Look for maximum sequence number in log and use that if available.
        if (diskLog != null)
        {
            long maxSeqno = diskLog.getMaxSeqno();
            if (maxSeqno > -1)
            {
                LogConnection conn = null;
                try
                {
                    // Try to connect and find the event.
                    THLEvent thlEvent = null;
                    conn = diskLog.connect(true);
                    conn.seek(maxSeqno);
                    while ((thlEvent = conn.next(false)) != null
                            && thlEvent.getSeqno() == maxSeqno)
                    {
                        // Return only the last fragment.
                        if (thlEvent.getLastFrag())
                        {
                            ReplEvent event = thlEvent.getReplEvent();
                            if (event instanceof ReplDBMSEvent)
                                return (ReplDBMSEvent) event;
                            else if (event instanceof ReplControlEvent)
                                return ((ReplControlEvent) event).getHeader();
                        }
                    }

                    // If we did not find the last fragment of the event
                    // we need to warn somebody.
                    if (thlEvent != null)
                        logger.warn("Unable to find last fragment of event: seqno="
                                + maxSeqno);
                }
                finally
                {
                    conn.release();
                }
            }
        }

        // If we get to this point, the log is newly initialized and there is
        // nothing to return.
        return null;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.storage.Store#status()
     */
    @Override
    public synchronized TungstenProperties status()
    {
        TungstenProperties props = new TungstenProperties();
        props.setLong(Replicator.MIN_STORED_SEQNO, getMinStoredSeqno());
        props.setLong(Replicator.MAX_STORED_SEQNO, getMaxStoredSeqno());
        props.setLong("activeSeqno", diskLog.getActiveSeqno());
        props.setBoolean("doChecksum", doChecksum);
        props.setString("logDir", logDir);
        props.setInt("logFileSize", logFileSize);
        props.setLong("logFileRetainMillis", logFileRetainMillis);
        props.setLong("logFileSize", diskLog.getLogFileSize());
        props.setLong("timeoutMillis", diskLog.getTimeoutMillis());
        props.setBoolean("fsyncOnFlush", fsyncOnFlush);
        props.setLong("flushIntervalMillis", diskLog.getFlushIntervalMillis());
        props.setLong("timeoutMillis", diskLog.getTimeoutMillis());
        props.setLong("logConnectionTimeout", logConnectionTimeout);
        props.setBoolean("readOnly", readOnly);

        return props;
    }

    /**
     * Returns list of currently connected clients.
     * 
     * @throws ReplicatorException If there's no listener.
     */
    public LinkedList<ConnectorHandler> getClients() throws ReplicatorException
    {
        if (server != null)
            return server.getClients();
        else
            throw new ReplicatorException("THL has no server listener");
    }
}