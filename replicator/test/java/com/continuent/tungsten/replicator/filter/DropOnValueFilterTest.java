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
 * Initial developer(s): Linas Virbalas
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.filter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.applier.DummyApplier;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.conf.ReplicatorMonitor;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.event.EventGenerationHelper;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.extractor.DummyExtractor;
import com.continuent.tungsten.replicator.management.MockOpenReplicatorContext;
import com.continuent.tungsten.replicator.pipeline.PipelineConfigBuilder;

/**
 * This class implements a set of tests for DropOnValueFilter.
 */
public class DropOnValueFilterTest extends TestCase
{
    private Logger                   logger            = Logger.getLogger(DropOnValueFilterTest.class);

    private FilterVerificationHelper filterHelper      = new FilterVerificationHelper();
    private EventGenerationHelper    eventHelper       = new EventGenerationHelper();

    /**
     * Is this test in progress?
     */
    private static boolean           testing           = false;

    private String                   service           = "test";

    private static ReplicatorRuntime replicatorContext = null;

    private final String             definitionsFile   = "DropOnValueFilterTest.json";

    /**
     * Start a network server to test the filter against once for the whole test
     * class only once.
     */
    @Before
    public void setUp() throws Exception
    {
        // Do only once for this test class.
        if (!testing)
        {
            testing = true;

            // Creating context only once to avoid repeated log messages.
            replicatorContext = getDummyContext();
        }
    }

    /**
     * Teardown.
     * 
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        File file = new File(definitionsFile);
        file.delete();
    }

    private void createDefinitionsFile() throws IOException
    {
        // Prepare definitions file.
        PrintWriter out = new PrintWriter(new FileWriter(definitionsFile));
        out.println("[");
        out.println("  {");
        out.println("    \"schema\": \"demo\",");
        out.println("    \"table\": \"devices\",");
        out.println("    \"column\": \"device_serial\",");
        out.println("    \"values\": [");
        out.println("      \"10001\",");
        out.println("      \"10002\",");
        out.println("      \"10003\",");
        out.println("      ]");
        out.println("  },");
        out.println("  {");
        out.println("    \"schema\": \"demo\",");
        out.println("    \"table\": \"devices\",");
        out.println("    \"column\": \"device_id\",");
        out.println("    \"values\": [");
        out.println("      \"10\",");
        out.println("      \"11\"");
        out.println("      ]");
        out.println("  }");
        out.println("]");
        out.close();
    }

    /**
     * Verify that the filter raises exception if no definitions file is
     * provided.
     */
    public void testUnspecifiedProperties() throws InterruptedException
    {
        NetworkClientFilter ncf = new NetworkClientFilter();
        ncf.setTungstenSchema("tungsten_foo");
        try
        {
            filterHelper.setContext(replicatorContext);
            filterHelper.setFilter(ncf);
            filterHelper.done();

            fail("Exception not thrown during configure though definitionsFile property was not set");
        }
        catch (ReplicatorException e)
        {
            // OK - it should have threw an exception.
            logger.info("Expected error received: " + e);
        }
    }

    private ReplicatorRuntime getDummyContext() throws Exception
    {
        // Configure a dummy pipeline.
        PipelineConfigBuilder builder = new PipelineConfigBuilder();
        builder.setProperty(ReplicatorConf.SERVICE_NAME, service);
        builder.setRole("dummy");
        builder.addPipeline("dummy", "d-stage1", null);
        builder.addStage("d-stage1", "dummy", "dummy", null);
        builder.addComponent("extractor", "dummy", DummyExtractor.class);
        builder.addComponent("applier", "dummy", DummyApplier.class);

        TungstenProperties tp = builder.getConfig();
        ReplicatorRuntime runtime = new ReplicatorRuntime(tp,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();

        return runtime;
    }

    /**
     * Check that exception is thrown if definitions file is not a valid JSON.
     */
    public void testInvalidDefinitionsFile() throws Exception
    {
        NetworkClientFilter ncf = new NetworkClientFilter();
        ncf.setTungstenSchema("tungsten_foo");
        ncf.setDefinitionsFile(definitionsFile);
        try
        {
            // Prepare an invalid definitions file.
            PrintWriter out = new PrintWriter(new FileWriter(definitionsFile));
            out.println("{");
            out.println("\"BLOB_to_String_v1\": [");
            out.println("{");
            out.println("  \"schema\": \"vip\",");
            out.println("  \"table\": \"clients\",");
            out.println("  \"columns\": [");
            out.println("    \"personal_code\",");
            out.println("    \"birth_date\",");
            out.println("    \"email\"");
            out.println("    ]");
            out.println("}"); // JSON not properly closed.
            out.close();

            filterHelper.setContext(replicatorContext);
            filterHelper.setFilter(ncf);
            filterHelper.done();

            fail("Exception not thrown during preparation though definitions file was an invalid JSON");
        }
        catch (ReplicatorException e)
        {
            // OK - it should have threw an exception.
            logger.info("Expected error received: " + e);
        }
    }

    /**
     * Check that exception is thrown if JSON definitions file is structured not
     * as expected.
     */
    public void testBadlyStructuredDefinitionsFile() throws Exception
    {
        DropOnValueFilter dovf = new DropOnValueFilter();
        dovf.setTungstenSchema("tungsten_foo");
        dovf.setDefinitionsFile(definitionsFile);
        try
        {
            // Prepare an invalid definitions file.
            PrintWriter out = new PrintWriter(new FileWriter(definitionsFile));
            out.println("[");
            out.println("{");
            out.println("  \"schema\": \"vip\",");
            out.println("  \"table\": \"clients\",");
            out.println("  \"columns\": [");
            out.println("    \"personal_code\",");
            out.println("    \"birth_date\",");
            out.println("    \"email\"");
            out.println("    ]");
            out.println("}");
            out.println("]");
            out.close();

            filterHelper.setContext(replicatorContext);
            filterHelper.setFilter(dovf);
            filterHelper.done();

            fail("Exception not thrown during preparation though definitions file is structured incorrectly");
        }
        catch (ReplicatorException e)
        {
            // OK - it should have threw an exception.
            logger.info("Expected error received: " + e);
        }
    }

    /**
     * Checks that a valid JSON file is parsed without errors.
     */
    public void testDefinitionsParsing() throws Exception
    {
        DropOnValueFilter dovf = new DropOnValueFilter();
        dovf.setTungstenSchema("tungsten_foo");
        dovf.setDefinitionsFile(definitionsFile);
        createDefinitionsFile();

        filterHelper.setContext(replicatorContext);
        filterHelper.setFilter(dovf);

        assertEquals("Incorrect count of value entries from parsed JSON", 5,
                dovf.getDefinedValueEntries());

        filterHelper.done(); // Release the filter.

        assertEquals("Incorrect count of value entries after releasing filter",
                0, dovf.getDefinedValueEntries());

        filterHelper.setContext(replicatorContext);
        filterHelper.setFilter(dovf); // Check that prepare works 2nd time too.

        assertEquals("Incorrect count of value entries after 2nd prepare", 5,
                dovf.getDefinedValueEntries());

        filterHelper.done();
    }

    public int getRandomPositiveInt()
    {
        Random rand = new Random();
        return rand.nextInt(Integer.MAX_VALUE);
    }

    public void testDropOnValue() throws Exception
    {
        DropOnValueFilter dovf = new DropOnValueFilter();
        dovf.setTungstenSchema("tungsten_foo");
        dovf.setDefinitionsFile(definitionsFile);
        createDefinitionsFile();

        filterHelper.setContext(replicatorContext);
        filterHelper.setFilter(dovf); // Prepare filter.

        {
            //
            // Test a row change event.
            //
            String columns[] = {"id", "device_serial", "label"};
            String values[] = new String[columns.length];
            values[0] = "1";
            values[1] = "10003";
            values[2] = "Should be removed";
            ReplDBMSEvent e = eventHelper.eventFromRowInsert(
                    getRandomPositiveInt(), "demo", "devices", columns, values,
                    0, true);

            // Transform.
            ReplDBMSEvent e2 = filterHelper.filter(e);

            // Row should have been dropped.
            assertNull("Filter didn't drop the offending event", e2);
        }
        
        {
            //
            // Test a row change event.
            //
            String columns[] = {"device_id", "device_serial", "label"};
            String values[] = new String[columns.length];
            values[0] = "11";
            values[1] = "N/A";
            values[2] = "Should be removed";
            ReplDBMSEvent e = eventHelper.eventFromRowInsert(
                    getRandomPositiveInt(), "demo", "devices", columns, values,
                    0, true);

            // Transform.
            ReplDBMSEvent e2 = filterHelper.filter(e);

            // Row should have been dropped.
            assertNull("Filter didn't drop the offending event", e2);
        }

        {
            //
            // Test a row change event.
            //
            String columns[] = {"id", "device_serial", "label"};
            String values[] = new String[columns.length];
            values[0] = "1";
            values[1] = "999";
            values[2] = "Shouldn't filter";
            ReplDBMSEvent e = eventHelper.eventFromRowInsert(
                    getRandomPositiveInt(), "demo", "devices", columns, values,
                    0, true);

            // Transform.
            ReplDBMSEvent e2 = filterHelper.filter(e);

            // Row should have been dropped.
            assertEquals("Filter changed row count when it shouldn't", 1, e2
                    .getData().size());
        }

        filterHelper.done(); // Release the filter.
    }
}