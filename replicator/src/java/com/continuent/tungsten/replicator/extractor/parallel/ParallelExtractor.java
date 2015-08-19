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
 * Contributor(s): Linas Virbalas
 */

package com.continuent.tungsten.replicator.extractor.parallel;

import java.io.File;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.InSequenceNotification;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.datasource.UniversalDataSource;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.DBMSEmptyEvent;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplOptionParams;
import com.continuent.tungsten.replicator.extractor.RawExtractor;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class ParallelExtractor implements RawExtractor
{
    private static Logger                 logger                = Logger.getLogger(ParallelExtractor.class);

    private String                        datasourceName;
    private UniversalDataSource           dataSource;

    private boolean                       addTruncateTable      = false;
    private long                          chunkSize             = -1;

    private int                           extractChannels       = 1;

    // Default queue size is set to 20.
    private int                           queueSize             = 20;

    private List<ParallelExtractorThread> threads;
    private ArrayBlockingQueue<DBMSEvent> queue;
    private ArrayBlockingQueue<Chunk>     chunks;

    private boolean                       threadsStarted        = false;

    private ChunksGeneratorThread         chunksGeneratorThread = null;
    private int                           activeThreads         = 0;
    private PluginContext                 context;
    private String                        chunkDefinitionFile   = null;

    private Hashtable<String, Long>       tableBlocks;

    protected String                      eventId               = null;

    /**
     * Sets the addTruncateTable value.
     * 
     * @param addTruncateTable The addTruncateTable to set.
     */
    public void setAddTruncateTable(boolean addTruncateTable)
    {
        this.addTruncateTable = addTruncateTable;
    }

    /**
     * Sets the chunkSize value.
     * 
     * @param chunkSize The chunkSize to set.
     */
    public void setChunkSize(long chunkSize)
    {
        this.chunkSize = chunkSize;
    }

    public void setDataSource(String dataSource) throws ReplicatorException
    {
        this.datasourceName = dataSource;
    }

    /**
     * Sets the extractChannels value.
     * 
     * @param extractChannels The extractChannels to set.
     */
    public void setExtractChannels(int extractChannels)
    {
        this.extractChannels = extractChannels;
    }

    /**
     * Sets the queueSize value.
     * 
     * @param queueSize The queueSize to set.
     */
    public void setQueueSize(int queueSize)
    {
        this.queueSize = queueSize;
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
        this.context = context;
        if (chunkDefinitionFile == null)
            logger.info("No chunk definition file provided. Scanning whole database.");
    }

    /**
     * {@inheritDoc}
     * 
     * @throws Exception
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void prepare(PluginContext context) throws ReplicatorException
    {
        // Establish a connection to the data source.
        this.dataSource = context.getDataSource(datasourceName);
        if (dataSource == null)
        {
            throw new ReplicatorException("Unable to locate data source: name="
                    + dataSource);
        }

        chunks = new ArrayBlockingQueue<Chunk>(5 * extractChannels);

        queue = new ArrayBlockingQueue<DBMSEvent>(queueSize);

        chunksGeneratorThread = new ChunksGeneratorThread(dataSource,
                extractChannels, chunks, chunkDefinitionFile, chunkSize);

        tableBlocks = new Hashtable<String, Long>();

        threads = new ArrayList<ParallelExtractorThread>();
        for (int i = 0; i < extractChannels; i++)
        {
            // Create extractor threads
            ParallelExtractorThread extractorThread = new ParallelExtractorThread(
                    dataSource, chunks, queue);
            extractorThread.setName("ParallelExtractorThread-" + i);
            activeThreads++;
            threads.add(extractorThread);
        }
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
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.RawExtractor#setLastEventId(java.lang.String)
     */
    @Override
    public void setLastEventId(String eventId) throws ReplicatorException
    {
        if (eventId == null || eventId.length() == 0)
        {
            Database connection = null;
            try
            {
                connection = (Database) dataSource.getConnection();
                this.eventId = connection.getCurrentPosition(true);
            }
            catch (ReplicatorException e)
            {
                logger.warn(
                        "Error while connecting to database ("
                                + dataSource.getName() + ")", e);
            }
            finally
            {
                if (connection != null)
                    connection.close();
            }
        }
        else
            this.eventId = eventId;

        chunksGeneratorThread.setEventId(this.eventId);
        for (int i = 0; i < extractChannels; i++)
        {
            threads.get(i).setEventId(this.eventId);
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
        if (!threadsStarted)
        {
            chunksGeneratorThread.start();

            for (Iterator<ParallelExtractorThread> iterator = threads
                    .iterator(); iterator.hasNext();)
            {
                iterator.next().start();
            }

            threadsStarted = true;
        }

        DBMSEvent event = queue.take();
        if (event instanceof DBMSEmptyEvent)
        {
            activeThreads--;
            if (activeThreads == 0)
            {
                // Job is now complete. Check whether we can go back to offline
                // state
                context.getEventDispatcher().put(new InSequenceNotification());
            }
            return null;
        }
        else
        {
            if (addTruncateTable)
            {
                // Check metadata of the event
                String entry = event.getMetadataOptionValue("schema") + "."
                        + event.getMetadataOptionValue("table");

                Long blk = tableBlocks.remove(entry);
                if (blk != null)
                {
                    // Table already in there... no need to add TRUNCATE, but
                    // decrement number of remaining blocks
                    if (blk > 1)
                        // If the number reaches 0, table was fully processed :
                        // no
                        // need to put tables back in there
                        tableBlocks.put(entry, blk - 1);
                }
                else
                {
                    // Issue 842 - do not hardcode schema name in SQL text.
                    // Instead, set it as default schema parameter.
                    StatementData sd = new StatementData("TRUNCATE TABLE "
                            + event.getMetadataOptionValue("table"), null,
                            event.getMetadataOptionValue("schema"));
                    sd.addOption("foreign_key_checks", "0");
                    event.getData().add(0, sd);

                    blk = Long
                            .valueOf(event.getMetadataOptionValue("nbBlocks"));
                    if (blk > 1)
                    {
                        tableBlocks.put(entry, blk - 1);
                    }
                }
            }
        }

        // Event extraction is now time zone-aware.
        event.addMetadataOption(ReplOptionParams.TIME_ZONE_AWARE, "true");
        return event;
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

    /**
     * Sets the path to the chunk definition file.
     * 
     * @param chunkDefinitionFile Chunk definition file to use.
     */
    public void setChunkDefinitionFile(String chunkDefinitionFile)
    {
        File f = new File(chunkDefinitionFile);
        if (f.isFile() && f.canRead())
        {
            this.chunkDefinitionFile = chunkDefinitionFile;
        }
    }
}
