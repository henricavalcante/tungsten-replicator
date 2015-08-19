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

package com.continuent.tungsten.replicator.applier.batch;

import java.io.File;
import java.sql.Timestamp;
import java.util.ArrayList;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.file.FileIO;
import com.continuent.tungsten.common.file.FilePath;
import com.continuent.tungsten.common.file.JavaFileIO;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.datasource.DataSourceService;
import com.continuent.tungsten.replicator.datasource.FileDataSource;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplOptionParams;
import com.continuent.tungsten.replicator.pipeline.PipelineConfigBuilder;
import com.continuent.tungsten.replicator.storage.InMemoryQueueAdapter;
import com.continuent.tungsten.replicator.storage.InMemoryQueueStore;

/**
 * Contains helper functions for testing pipelines containing batch applier.
 */
public class BatchApplyHelper
{
    private FileIO fileIO = new JavaFileIO();

    /**
     * Generate configuration properties for a single-stage pipeline that
     * connects a batch applier to an in-memory queue. We use direct calls to
     * the stores to load up row data.
     */
    public TungstenProperties generateBatchApplyProps(File testDir,
            String serviceName, boolean partitioned) throws Exception
    {
        // Clear the test directory and create a test load script.
        File scriptFile = this.createMergeProcedure(testDir, "test.js");

        // Start the definition and provide a service name.
        PipelineConfigBuilder builder = new PipelineConfigBuilder();
        builder.setProperty(ReplicatorConf.SERVICE_NAME, serviceName);

        // Create pipeline.
        builder.setRole("master");
        builder.setProperty(ReplicatorConf.METADATA_SCHEMA, serviceName);
        builder.addPipeline("master", "q-to-batch-apply", "queue", "datasource");

        // Define store and assign adequate capacity. 
        builder.addComponent("store", "queue", InMemoryQueueStore.class);
        builder.addProperty("store", "queue", "maxSize", "500");

        // Define the single stage with no filters.
        builder.addStage("q-to-batch-apply", "q-extract", "batch-applier", null);
        builder.addProperty("stage", "q-to-batch-apply", "blockCommitRowCount",
                new Integer(1).toString());
        builder.addComponent("extractor", "q-extract",
                InMemoryQueueAdapter.class);
        builder.addProperty("extractor", "q-extract", "storeName", "queue");
        builder.addComponent("applier", "batch-applier",
                SimpleBatchApplier.class);

        // The batch applier uses a datasource called fs and has a load script
        // in the replicator project.
        builder.addProperty("applier", "batch-applier", "dataSource", "fs");
        builder.addProperty("applier", "batch-applier", "loadScript",
                scriptFile.getAbsolutePath());

        // Set standard timezone and charset values.
        builder.addProperty("applier", "batch-applier", "timezone", "GMT+0:00");
        builder.addProperty("applier", "batch-applier", "charset", "UTF-8");

        // Control where to write CSV and headers to include.
        builder.addProperty("applier", "batch-applier", "stageDirectory",
                new File(testDir, "staging").getAbsolutePath());
        builder.addProperty("applier", "batch-applier", "stageTablePrefix",
                "stage_xxx_");
        builder.addProperty("applier", "batch-applier", "stageColumnPrefix",
                "tungsten_");

        // Enable optional partition by support.
        if (partitioned)
            builder.addProperty("applier", "batch-applier", "partitionBy",
                    "tungsten_commit_timestamp");
        builder.addProperty("applier", "batch-applier", "partitionByClass",
                DateTimeValuePartitioner.class.getName());
        builder.addProperty("applier", "batch-applier", "partitionByFormat",
                "'commit_hour='yyyy-MM-dd-HH");

        // Configure datasource management service.
        builder.addComponent("service", "datasource", DataSourceService.class);
        builder.setProperty("replicator.datasources", "fs");
        builder.addComponent("datasource", "fs", FileDataSource.class);
        builder.addProperty("datasource", "fs", "serviceName", serviceName);
        builder.addProperty("datasource", "fs", "directory", new File(testDir,
                "data").getAbsolutePath());
        builder.addProperty("datasource", "fs", "csvType", "default");

        return builder.getConfig();
    }

    /**
     * Create a test Javascript procedure that follows load conventions and
     * generates a write to a file for each call to the various load methods.
     */
    public File createMergeProcedure(File dir, String loadName)
    {
        // Write the code.
        StringBuffer code = new StringBuffer();
        code.append("function prepare() {\n")
                .append("  dir = runtime.getContext().getServiceName();\n")
                .append("  runtime.exec('echo prepare >> ' + dir + '/prepare.stat');\n")
                .append("}\n")
                .append("function begin() {\n")
                .append("  runtime.exec('echo begin >> ' + dir + '/begin.stat');\n")
                .append("}\n")
                .append("function apply(csvinfo) {\n")
                .append("  logger.info('Applying csv: table=' + csvinfo.baseTableMetadata.getName());")
                .append("  if (csvinfo.key == '') {\n")
                .append("    output_csv = csvinfo.baseTableMetadata.getName() + '.data';\n")
                .append("  } else {\n")
                .append("    output_csv = csvinfo.baseTableMetadata.getName() + '-' + csvinfo.key + '.data';\n")
                .append("  }\n")
                .append("  runtime.exec('echo ' + csvinfo.file.getName() + "
                        + "'>> ' + dir + '/apply.stat');\n")
                .append("  runtime.exec('cat ' + csvinfo.file.getAbsolutePath() + ' >> '")
                .append("     + dir + '/' + output_csv);\n")
                .append("}\n")
                .append("function commit() {\n")
                .append("  runtime.exec('echo commit >> ' + dir + '/commit.stat');\n")
                .append("}\n")
                .append("function release() {\n")
                .append("  runtime.exec('echo release >> ' + dir + '/release.stat');\n")
                .append("}");

        // Write to a file and return handle to same to caller.
        File scriptFile = new File(dir, loadName);
        fileIO.write(new FilePath(scriptFile.getAbsolutePath()),
                code.toString());
        return scriptFile;
    }

    /**
     * Ensure that a name file exists.
     */
    public void assertFileExistence(File dir, String name, String message)
            throws Exception
    {
        File file = new File(dir, name);
        if (!file.exists())
        {
            throw new Exception("Could not find expected file: name="
                    + file.getAbsolutePath() + " message=" + message);
        }
    }

    /**
     * Create an empty test directory or if the directory exists remove any
     * files within it.
     */
    public File prepareTestDir(String logDirName) throws Exception
    {
        File logDir = new File(logDirName);
        FilePath path = new FilePath(logDir.getAbsolutePath());
        fileIO.delete(path, true);
        fileIO.mkdir(path);
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
     * Returns a well-formed ReplDBMSEvent fragment with a specified shard ID
     * and epoch number in addition to other useful information.
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