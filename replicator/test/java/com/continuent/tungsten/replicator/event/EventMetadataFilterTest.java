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

package com.continuent.tungsten.replicator.event;

import java.util.Arrays;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.applier.DummyApplier;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.conf.ReplicatorMonitor;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.extractor.DummyExtractor;
import com.continuent.tungsten.replicator.filter.FilterVerificationHelper;
import com.continuent.tungsten.replicator.management.MockOpenReplicatorContext;
import com.continuent.tungsten.replicator.pipeline.PipelineConfigBuilder;

/**
 * This class implements a test of the EventMetadataFilter, which is a complex
 * and gnarly bit of Java responsible for assigning a shardId to transactions
 * and adding metadata to track the originating service of transactions.
 */
public class EventMetadataFilterTest
{
    private static Logger            logger       = Logger.getLogger(EventMetadataFilterTest.class);
    private static String            OUR_SERVICE  = "our_service";
    private FilterVerificationHelper filterHelper = new FilterVerificationHelper();
    private EventGenerationHelper    eventHelper  = new EventGenerationHelper();

    /**
     * Setup.
     * 
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
    }

    /**
     * Teardown.
     * 
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        filterHelper.done();
    }

    /**
     * Verify that the filter correctly assigns a source comment to statements
     * that do not have one.
     */
    @Test
    public void testAssignServiceComment() throws Exception
    {
        filterHelper.setContext(generateDefaultRuntime(true));
        filterHelper.setFilter(new EventMetadataFilter());

        // Generate simple event and confirm that service is correctly added.
        ReplDBMSEvent e = this.eventHelper.eventFromStatement(1, "test",
                "insert into foo(id) values(1)");
        checkServiceAddition(e, true);
    }

    /**
     * Verify that the filter does not assign a service comment if comments are
     * suppressed.
     */
    @Test
    public void testAssignNoServiceComment() throws Exception
    {
        filterHelper.setContext(generateDefaultRuntime(false));
        filterHelper.setFilter(new EventMetadataFilter());

        // Generate simple event and confirm that no service is added.
        ReplDBMSEvent e = this.eventHelper.eventFromStatement(1, "test",
                "insert into foo(id) values(1)");
        checkServiceAddition(e, false);
    }

    /**
     * Verify that the filter does not corrupt queries when statements are in
     * binary but the encoding is wrong. This can occur in cases where MySQL
     * statements are loaded using the wrong character set.
     */
    @Test
    public void testNoServiceAssignmentCorruption() throws Exception
    {
        filterHelper.setContext(generateDefaultRuntime(false));
        filterHelper.setFilter(new EventMetadataFilter());

        // Generate a query with incorrect encoding and confirm it is not
        // corrupted when comments are disabled.
        String japaneseInsert = "insert into foo values('\u306B\u307B\u3093')";
        byte[] japaneseInsertUtf8 = japaneseInsert.getBytes("utf8");
        ReplDBMSEvent e = this.eventHelper.eventFromBinaryStatement(1, "test",
                japaneseInsertUtf8, 0, true, "ISO8859_1");
        checkBinaryConsistency(e);

        // Try again but this time use ISO-1 characters that are marked
        // as UTF8.
        String euroInsert = "insert into foo values('\u00A1Hola Se\u00F1or!')";
        byte[] euroInsertIso_1 = euroInsert.getBytes("ISO8859_1");
        ReplDBMSEvent e2 = this.eventHelper.eventFromBinaryStatement(1, "test",
                euroInsertIso_1, 0, true, "utf8");
        checkBinaryConsistency(e2);
    }

    // Check that we add the service name correctly.
    public void checkBinaryConsistency(ReplDBMSEvent e)
            throws ReplicatorException, InterruptedException
    {
        // Generate first event and confirm it has a no service comment.
        StatementData sd = (StatementData) e.getDBMSEvent().getData().get(0);
        byte[] qb = sd.getQueryAsBytes();
        logger.info("Unfiltered query: " + sd.getQuery());

        // Filter the event.
        ReplDBMSEvent e2 = filterHelper.filter(e);
        StatementData sd2 = (StatementData) e2.getDBMSEvent().getData().get(0);
        logger.info("Filtered query: " + sd.getQuery());

        // Get the binary query and confirm that it is not corrupted.
        byte[] qb2 = sd2.getQueryAsBytes();
        Assert.assertEquals("Binary query should not be changed",
                Arrays.toString(qb), Arrays.toString(qb2));
    }

    /**
     * Verify that the filter correctly assigns a service comment to binary
     * values and obeys the character set if the statement data are in binary.
     */
    @Test
    public void testAssignServiceCommentToBinary() throws Exception
    {
        filterHelper.setContext(generateDefaultRuntime(true));
        filterHelper.setFilter(new EventMetadataFilter());

        // Generate a binary string in UTF-8 with Japanese phonetic characters
        // (hiragana) and check that we correctly add the filter name.
        String japaneseInsert = "insert into foo values('\u306B\u307B\u3093')";
        byte[] japaneseInsertUtf8 = japaneseInsert.getBytes("utf8");
        ReplDBMSEvent e = this.eventHelper.eventFromBinaryStatement(1, "test",
                japaneseInsertUtf8, 0, true, "utf8");
        checkServiceAddition(e, true);

        // Generate a binary string in ISO-8859-1 with non-ASCII characters and
        // check that we correctly add the filter name.
        String euroInsert = "insert into foo values('\u00A1Hola Se\u00F1or!')";
        byte[] euroInsertIso_1 = euroInsert.getBytes("ISO8859_1");
        ReplDBMSEvent e2 = this.eventHelper.eventFromBinaryStatement(1, "test",
                euroInsertIso_1, 0, true, "ISO8859_1");
        checkServiceAddition(e2, true);

        // Generate a binary string in ISO-8859-1 with non-ASCII characters with
        // trailing nulls. This seems to be generated by some MySQL tools.
        byte[] euroInsertIso_1_null = Arrays.copyOf(euroInsertIso_1,
                euroInsertIso_1.length + 2);
        ReplDBMSEvent e3 = this.eventHelper.eventFromBinaryStatement(1, "test",
                euroInsertIso_1_null, 0, true, "ISO8859_1");
        checkServiceAddition(e3, true);

        // Generate a binary string in UTF-8 with a Unicode null and check that
        // we correctly add the service name.
        String nullTerminatedInsert = "insert into foo values('nihon')\u0000";
        byte[] nullTerminatedInsertUtf8 = nullTerminatedInsert.getBytes("utf8");
        ReplDBMSEvent e5 = this.eventHelper.eventFromBinaryStatement(1, "test",
                nullTerminatedInsertUtf8, 0, true, "utf8");
        checkServiceAddition(e5, true);
    }

    // Check that we add the service name correctly.
    public void checkServiceAddition(ReplDBMSEvent e, boolean addServiceName)
            throws ReplicatorException, InterruptedException
    {
        // Generate first event and confirm it has a no service comment.
        StatementData sd = (StatementData) e.getDBMSEvent().getData().get(0);
        String q = sd.getQuery();
        logger.info("Unfiltered query: " + q);
        Assert.assertFalse("Service does not contain expected service name",
                q.contains(OUR_SERVICE));

        // Filter the event.
        ReplDBMSEvent e2 = filterHelper.filter(e);
        Assert.assertNotNull("Event should not be filtered out", e2);
        StatementData sd2 = (StatementData) e2.getDBMSEvent().getData().get(0);

        // Check according to whether we expect a service name addition.
        String q2 = sd2.getQuery();
        logger.info("Filtered query: " + q2);
        if (addServiceName)
        {
            // Check to see that the filter contains the source ID
            Assert.assertTrue("Service contains expected service name",
                    q2.contains(OUR_SERVICE));
        }
        else
        {
            // Check to ensure that the query is the same as before.
            Assert.assertEquals("Query should be unchanged", q, q2);
        }
    }

    // Generate sample subset of configuration properties appropriate to MySQL.
    public ReplicatorRuntime generateDefaultRuntime(boolean commentsEnabled)
            throws Exception
    {
        // Configure a dummy pipeline.
        PipelineConfigBuilder builder = new PipelineConfigBuilder();
        builder.setProperty(ReplicatorConf.SERVICE_NAME, OUR_SERVICE);
        builder.setRole("dummy");
        builder.addPipeline("dummy", "d-stage1", null);
        builder.addStage("d-stage1", "dummy", "dummy", null);
        builder.addComponent("extractor", "dummy", DummyExtractor.class);
        builder.addComponent("applier", "dummy", DummyApplier.class);

        // Add generic properties required by EventMetadataFtiler filter.
        builder.setProperty(ReplicatorConf.RESOURCE_JDBC_URL,
                "jdbc:mysql:thin://localhost/tungsten");
        builder.setProperty(ReplicatorConf.GLOBAL_DB_USER, "user1");
        builder.setProperty(ReplicatorConf.GLOBAL_DB_PASSWORD, "secret");
        builder.setProperty(ReplicatorConf.SERVICE_COMMENTS_ENABLED,
                new Boolean(commentsEnabled).toString());
        builder.setProperty(ReplicatorConf.SHARD_DEFAULT_DB_USAGE, "stringent");

        TungstenProperties tp = builder.getConfig();

        ReplicatorRuntime runtime = new ReplicatorRuntime(tp,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        return runtime;
    }
}