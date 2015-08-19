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
 * Contributor(s): 
 */

package com.continuent.tungsten.replicator.thl;

import java.io.File;
import java.sql.Timestamp;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.applier.DummyApplier;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplOptionParams;
import com.continuent.tungsten.replicator.extractor.DummyExtractor;
import com.continuent.tungsten.replicator.pipeline.PipelineConfigBuilder;
import com.continuent.tungsten.replicator.storage.InMemoryMultiQueue;
import com.continuent.tungsten.replicator.storage.InMemoryMultiQueueApplier;
import com.continuent.tungsten.replicator.storage.InMemoryTransactionalQueue;
import com.continuent.tungsten.replicator.storage.InMemoryTransactionalQueueApplier;
import com.continuent.tungsten.replicator.storage.parallel.HashPartitioner;

/**
 * Contains helper functions for parallel queue testing.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class THLParallelQueueHelper
{
    private static Logger logger = Logger.getLogger(THLParallelQueueHelper.class);

    /**
     * Generate configuration properties for a three stage-pipeline that loads
     * events into a THL then loads a parallel queue. Input is from a dummy
     * extractor.
     */
    public TungstenProperties generateTHLParallelQueueProps(String schemaName,
            int channels) throws Exception
    {
        // Clear the THL log directory.
        prepareLogDir(schemaName);

        // Create pipeline.
        PipelineConfigBuilder builder = new PipelineConfigBuilder();
        builder.setProperty(ReplicatorConf.SERVICE_NAME, "test");
        builder.setRole("master");
        builder.setProperty(ReplicatorConf.METADATA_SCHEMA, schemaName);
        builder.addPipeline("master", "extract,feed,apply", "thl,thl-queue");
        builder.addStage("extract", "dummy", "thl-apply", null);
        builder.addStage("feed", "thl-extract", "thl-queue-apply", null);
        builder.addStage("apply", "thl-queue-extract", "dummy", null);

        // Define stores.
        builder.addComponent("store", "thl", THL.class);
        builder.addProperty("store", "thl", "logDir", schemaName);
        builder.addComponent("store", "thl-queue", THLParallelQueue.class);
        builder.addProperty("store", "thl-queue", "maxSize", "5");

        // Extract stage components.
        builder.addComponent("extractor", "dummy", DummyExtractor.class);
        builder.addProperty("extractor", "dummy", "nFrags", "1");
        builder.addComponent("applier", "thl-apply", THLStoreApplier.class);
        builder.addProperty("applier", "thl-apply", "storeName", "thl");

        // Feed stage components.
        builder.addComponent("extractor", "thl-extract",
                THLStoreExtractor.class);
        builder.addProperty("extractor", "thl-extract", "storeName", "thl");
        builder.addComponent("applier", "thl-queue-apply",
                THLParallelQueueApplier.class);
        builder.addProperty("applier", "thl-queue-apply", "storeName",
                "thl-queue");

        // Apply stage components.
        builder.addComponent("extractor", "thl-queue-extract",
                THLParallelQueueExtractor.class);
        builder.addProperty("extractor", "thl-queue-extract", "storeName",
                "thl-queue");
        builder.addComponent("applier", "dummy", DummyApplier.class);

        return builder.getConfig();
    }

    /**
     * Generate configuration properties for a two-stage pipeline that connects
     * a THL to a THLParallelQueue to an in-memory multi queue, which can mimic
     * parallel apply on DBMS instances. Clients use direct calls to the stores
     * to write to and read from the pipeline.
     * <p/>
     * Note that we support two types of parallel queues for testing.
     * Multi-queues keep transactions in separate queues per channel.
     * Transactional queues serialize them in a manner analogous to a DBMS.
     */
    public TungstenProperties generateTHLParallelPipeline(String schemaName,
            int partitions, int blockCommit, int mqSize, boolean multiQueue)
            throws Exception
    {
        // Clear the THL log directory.
        prepareLogDir(schemaName);

        // Convert values to strings so we can use them.
        String partitionsAsString = new Integer(partitions).toString();
        String blockCommitAsString = new Integer(blockCommit).toString();

        // Create pipeline.
        PipelineConfigBuilder builder = new PipelineConfigBuilder();
        builder.setProperty(ReplicatorConf.SERVICE_NAME, "test");
        builder.setRole("master");
        builder.setProperty(ReplicatorConf.METADATA_SCHEMA, schemaName);
        builder.addPipeline("master", "thl-to-q,q-to-mq",
                "thl,thl-queue, multi-queue");

        // Define stores.
        builder.addComponent("store", "thl", THL.class);
        builder.addProperty("store", "thl", "logDir", schemaName);
        builder.addComponent("store", "thl-queue", THLParallelQueue.class);
        builder.addProperty("store", "thl-queue", "maxSize", "100");
        builder.addProperty("store", "thl-queue", "partitions", new Integer(
                partitions).toString());
        builder.addProperty("store", "thl-queue", "partitionerClass",
                HashPartitioner.class.getName());
        if (multiQueue)
            builder.addComponent("store", "multi-queue",
                    InMemoryMultiQueue.class);
        else
            builder.addComponent("store", "multi-queue",
                    InMemoryTransactionalQueue.class);
        builder.addProperty("store", "multi-queue", "maxSize", new Integer(
                mqSize).toString());
        builder.addProperty("store", "multi-queue", "partitions",
                partitionsAsString);

        // Feed1 stage components.
        builder.addStage("thl-to-q", "thl-extract", "thl-queue-apply", null);
        builder.addProperty("stage", "thl-to-q", "blockCommitRowCount",
                blockCommitAsString);
        builder.addComponent("extractor", "thl-extract",
                THLStoreExtractor.class);
        builder.addProperty("extractor", "thl-extract", "storeName", "thl");
        builder.addComponent("applier", "thl-queue-apply",
                THLParallelQueueApplier.class);
        builder.addProperty("applier", "thl-queue-apply", "storeName",
                "thl-queue");

        // Feed2 stage components.
        builder.addStage("q-to-mq", "thl-queue-extract", "multi-queue-apply",
                null);
        builder.addProperty("stage", "q-to-mq", "taskCount", partitionsAsString);
        builder.addProperty("stage", "q-to-mq", "blockCommitRowCount",
                blockCommitAsString);
        builder.addComponent("extractor", "thl-queue-extract",
                THLParallelQueueExtractor.class);
        builder.addProperty("extractor", "thl-queue-extract", "storeName",
                "thl-queue");
        if (multiQueue)
            builder.addComponent("applier", "multi-queue-apply",
                    InMemoryMultiQueueApplier.class);
        else
            builder.addComponent("applier", "multi-queue-apply",
                    InMemoryTransactionalQueueApplier.class);
        builder.addProperty("applier", "multi-queue-apply", "storeName",
                "multi-queue");

        return builder.getConfig();
    }

    /**
     * Create an empty log directory or if the directory exists remove any files
     * within it.
     */
    private File prepareLogDir(String logDirName) throws Exception
    {
        File logDir = new File(logDirName);
        // Delete old log if present.
        if (logDir.exists())
        {
            logger.info("Clearing log dir: " + logDir.getAbsolutePath());
            for (File f : logDir.listFiles())
            {
                f.delete();
            }
            logDir.delete();
        }

        // If the log directory exists now, we have a problem.
        if (logDir.exists())
        {
            throw new Exception(
                    "Unable to clear log directory, test cannot start: "
                            + logDir.getAbsolutePath());
        }

        // Create new log directory.
        logDir.mkdirs();
        return logDir;
    }

    /**
     * Returns a well-formed ReplDBMSEvent fragment with a specified shard ID.
     */
    public ReplDBMSEvent createEvent(long seqno, short fragNo,
            boolean lastFrag, String shardId, Timestamp timestamp)
    {
        return createEvent(seqno, fragNo, lastFrag, shardId, timestamp, 0);
    }

    /**
     * Returns a well-formed ReplDBMSEvent fragment with a specified shard ID and
     * epoch number in addition to other useful information. 
     */
    public ReplDBMSEvent createEvent(long seqno, short fragNo,
            boolean lastFrag, String shardId, Timestamp timestamp,
            long epochNumber)
    {
        ArrayList<DBMSData> t = new ArrayList<DBMSData>();
        t.add(new StatementData("SELECT 1"));
        DBMSEvent dbmsEvent = new DBMSEvent(new Long(seqno).toString(), null,
                t, true, timestamp);
        ReplDBMSEvent replDbmsEvent = new ReplDBMSEvent(seqno, fragNo,
                lastFrag, "NONE", epochNumber, timestamp, dbmsEvent);
        if (shardId != null)
        {
            replDbmsEvent.getDBMSEvent().addMetadataOption(
                    ReplOptionParams.SHARD_ID, shardId);
        }
        return replDbmsEvent;
    }

    /**
     * Convenience event to generate an event fragment with a default timestamp
     * value.
     */
    public ReplDBMSEvent createEvent(long seqno, short fragNo,
            boolean lastFrag, String shardId)
    {
        return createEvent(seqno, fragNo, lastFrag, shardId, new Timestamp(
                System.currentTimeMillis()));
    }

    /**
     * Convenience method to generate an unfragmented event.
     */
    public ReplDBMSEvent createEvent(long seqno, String shardId)
    {
        return createEvent(seqno, (short) 0, true, shardId);
    }
}