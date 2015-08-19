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
 * Initial developer(s): Stephane Giron
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.extractor.oracle;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplOptionParams;
import com.continuent.tungsten.replicator.extractor.RawExtractor;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class OracleCDCReaderExtractor implements RawExtractor
{
    private static Logger                  logger                = Logger.getLogger(OracleCDCReaderExtractor.class);

    List<OracleCDCSource>                  sources;

    private String                         lastSCN               = null;

    private ArrayBlockingQueue<CDCMessage> queue;

    private OracleCDCReaderThread          readerThread;

    private boolean                        cancelled;

    private int                            transactionFragSize   = 0;

    private int                            minSleepTimeInSeconds = 1;
    private int                            maxSleepTimeInSeconds = 1;
    private int                            sleepAddition         = 0;

    private int                            queueSize             = 100;

    private int                            maxRowsByBlock        = 50;

    // Reconnect timeout in milliseconds. Oracle extractor will release the
    // connection and open a new one once the full CDC window got processed and
    // the connection is older than the defined timeout.
    private long                           reconnectTimeout      = 60 * 60 * 1000;

    private String                         dataSource;

    /**
     * Sets the maximum sleep time : maximum time the extracting thread will
     * sleep between two calls. The extracting thread goes to sleep when nothing
     * was retrieved from the CDC window. It first goes to sleep for 1s, and
     * then sleep time is doubled until reaching the maximum or new data
     * arrives. In this last case, the sleep time is reset to 1s. If this is set
     * to 1, then the extracting thread will sleep only 1s.
     * 
     * @param maxSleepTime maximum sleep time in seconds
     */
    public void setMaxSleepTime(int maxSleepTime)
    {
        this.maxSleepTimeInSeconds = maxSleepTime;
    }

    /**
     * Sets the minSleepTimeInSeconds value.
     * 
     * @param minSleepTimeInSeconds The minSleepTimeInSeconds to set.
     */
    public void setMinSleepTime(int minSleepTimeInSeconds)
    {
        this.minSleepTimeInSeconds = minSleepTimeInSeconds;
    }

    /**
     * Sets the sleepAddition value.
     * 
     * @param sleepAddition The sleepAddition to set.
     */
    public void setSleepAddition(int sleepAddition)
    {
        this.sleepAddition = sleepAddition;
    }

    /**
     * Sets the transactionFragSize value.
     * 
     * @param transactionFragSize The transactionFragSize to set.
     */
    public void setTransactionFragSize(int transactionFragSize)
    {
        this.transactionFragSize = transactionFragSize;
    }

    /**
     * Sets the queueSize value. This is the maximum number of blocks that can
     * be handled within the queue. One block contains at most maxRowsByBlock
     * rows.
     * 
     * @param queueSize The queueSize to set.
     */
    public void setQueueSize(int queueSize)
    {
        this.queueSize = queueSize;
    }

    /**
     * Sets the maxRowsByBlock value. Fragmentation will happen if an event
     * contains more than maxRowsByBlock rows within the same change table or
     * for each change table which contains rows. The maximum fragment size will
     * then be at most : transactionFragSize * maxRowsByBlock rows
     * 
     * @param maxRowsByBlock The maxRowsByBlock to set.
     */
    public void setMaxRowsByBlock(int maxRowsByBlock)
    {
        this.maxRowsByBlock = maxRowsByBlock;
    }

    /**
     * Sets the reconnectTimeout value. This is the time after which a
     * connection has to be renewed (this happens once a CDC window was fully
     * processed). If set to 0, there won't be any connection renewal.
     * 
     * @param reconnectTimeout The time in seconds after which the connection
     *            has to be renewed.
     */
    public void setReconnectTimeout(long reconnectTimeout)
    {
        this.reconnectTimeout = reconnectTimeout * 1000;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void prepare(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        queue = new ArrayBlockingQueue<CDCMessage>(queueSize);
        readerThread = new OracleCDCReaderThread(context, dataSource, queue,
                lastSCN, minSleepTimeInSeconds, maxSleepTimeInSeconds,
                sleepAddition, maxRowsByBlock, reconnectTimeout);

        readerThread.prepare();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void release(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        cancelled = true;
        if (readerThread != null)
            readerThread.cancel();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.RawExtractor#setLastEventId(java.lang.String)
     */
    @Override
    public void setLastEventId(String eventId) throws ReplicatorException
    {
        if (eventId != null)
        {
            logger.info("Starting from SCN " + eventId);
            lastSCN = eventId;
            readerThread.setLastSCN(lastSCN);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.RawExtractor#extract()
     */
    @Override
    public DBMSEvent extract() throws ReplicatorException, InterruptedException
    {
        if (!readerThread.isAlive())
            readerThread.start();

        ArrayList<DBMSData> data = new ArrayList<DBMSData>();

        int fragSize = 0;
        cancelled = false;

        long currentSCN = -1;
        Timestamp time = null;

        while (!cancelled)
        {
            CDCMessage cdcMsg = queue.take();

            if (cdcMsg instanceof CDCDataMessage)
            {
                CDCDataMessage dataMsg = (CDCDataMessage) cdcMsg;

                if (time == null)
                    time = dataMsg.getTimestamp();

                if (currentSCN < dataMsg.getSCN())
                    currentSCN = dataMsg.getSCN();

                fragSize += 1;
                data.add(dataMsg.getRowChange());

                if (transactionFragSize > 0 && fragSize >= transactionFragSize)
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Fragmenting");
                    // Time to fragment
                    DBMSEvent ev = new DBMSEvent("ora:" + currentSCN, data,
                            false, time);
                    // Oracle event extraction is now time zone-aware.
                    ev.addMetadataOption(ReplOptionParams.TIME_ZONE_AWARE,
                            "true");
                    return ev;
                }
            }
            else if (cdcMsg instanceof CDCCommitMessage)
            {
                currentSCN = ((CDCCommitMessage) cdcMsg).getScn();
                
                DBMSEvent ev = new DBMSEvent("ora:" + currentSCN, data, true, time);
                // Oracle event extraction is now time zone-aware.
                ev.addMetadataOption(ReplOptionParams.TIME_ZONE_AWARE,
                        "true");

                return ev;
            }
            else if (cdcMsg instanceof CDCErrorMessage)
            {
                CDCErrorMessage errorMsg = (CDCErrorMessage) cdcMsg;
                throw new ReplicatorException(errorMsg.getMessage(),
                        errorMsg.getException());
            }
            else
            {
                // Unexpected message
            }
        }
        return null;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.RawExtractor#extract(java.lang.String)
     */
    @Override
    public DBMSEvent extract(String eventId) throws ReplicatorException,
            InterruptedException
    {
        return null;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.RawExtractor#getCurrentResourceEventId()
     */
    @Override
    public String getCurrentResourceEventId() throws ReplicatorException,
            InterruptedException
    {
        return null;
    }

    public void setDataSource(String dataSource)
    {
        this.dataSource = dataSource;
    }

}
