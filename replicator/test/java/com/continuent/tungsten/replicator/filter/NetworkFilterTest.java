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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.After;
import org.junit.Before;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.applier.DummyApplier;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.conf.ReplicatorMonitor;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.event.EventGenerationHelper;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.extractor.DummyExtractor;
import com.continuent.tungsten.replicator.management.MockOpenReplicatorContext;
import com.continuent.tungsten.replicator.pipeline.PipelineConfigBuilder;

/**
 * This class implements a set of tests of NetworkFilter and its components. TCP
 * server is established to test the protocol too.
 */
public class NetworkFilterTest extends TestCase
{
    private Logger                                     logger            = Logger.getLogger(NetworkFilterTest.class);

    private FilterVerificationHelper                   filterHelper      = new FilterVerificationHelper();
    private EventGenerationHelper                      eventHelper       = new EventGenerationHelper();

    private JSONParser                                 parser            = new JSONParser();

    private static final int                           serverPort        = 3112;
    private static final int                           timeout           = 30;

    /**
     * Is this test in progress?
     */
    private static boolean                             testing           = false;

    private String                                     service           = "test";

    private NetworkClientFilter.ClientMessageGenerator generator         = new NetworkClientFilter.ClientMessageGenerator(
                                                                                 service);
    private static NetworkFilterServer                 server            = new NetworkFilterServer();
    private static ReplicatorRuntime                   replicatorContext = null;

    private final String                               definitionsFile   = "NetworkFilterTest.json";

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

            server.setName("NetworkFilterServer");
            server.start();

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
        out.println("{");
        out.println("\"String_to_HEX_v1\": [");
        out.println("{");
        out.println("  \"schema\": \"vip\",");
        out.println("  \"table\": \"clients\",");
        out.println("  \"columns\": [");
        out.println("    \"personal_code\",");
        out.println("    \"birth_date\",");
        out.println("    \"email\"");
        out.println("    ]");
        out.println("},");
        out.println("{");
        out.println("  \"schema\": \"*\",");
        out.println("  \"table\": \"credit_cards\",");
        out.println("  \"columns\": [");
        out.println("    \"cc_type\",");
        out.println("    \"cc_number\"");
        out.println("    ]");
        out.println("},");
        out.println("{");
        out.println("  \"schema\": \"*\",");
        out.println("  \"table\": \"*\",");
        out.println("  \"columns\": [");
        out.println("    \"photo\"");
        out.println("    ]");
        out.println("}");
        out.println("],");
        out.println("\"Make_Empty_v1\": [");
        out.println("{");
        out.println("  \"schema\": \"vip\",");
        out.println("  \"table\": \"clients\",");
        out.println("  \"columns\": [");
        out.println("    \"password\"");
        out.println("    ]");
        out.println("},");
        out.println("{");
        out.println("  \"schema\": \"*\",");
        out.println("  \"table\": \"credit_cards\",");
        out.println("  \"columns\": [");
        out.println("    \"pin\"");
        out.println("    ]");
        out.println("}");
        out.println("],");
        out.println("\"Make_Null_v1\": [");
        out.println("{");
        out.println("  \"schema\": \"*\",");
        out.println("  \"table\": \"*\",");
        out.println("  \"columns\": [");
        out.println("    \"dummy\"");
        out.println("]");
        out.println("}");
        out.println("]");
        out.println("}");
        out.close();
    }

    /**
     * Initial check to make sure that the test TCP server is functioning.
     */
    public void testServer() throws Exception
    {
        for (int client = 0; client < 2; client++)
        {
            // Connect to test server.
            InetAddress host = InetAddress.getByName("localhost");
            logger.info("Connecting to server on port " + serverPort);

            Socket socket = new Socket(host, serverPort);
            logger.info("Connected to " + socket.getRemoteSocketAddress());
            PrintWriter toServer = new PrintWriter(socket.getOutputStream(),
                    true);
            BufferedReader fromServer = new BufferedReader(
                    new InputStreamReader(socket.getInputStream()));

            {
                // Send prepare message.
                toServer.print(generator.prepare());
                toServer.flush();

                // Receive & check acknowledged message.
                String header = fromServer.readLine();
                logger.info("Received header: " + header);

                JSONObject obj = (JSONObject) parser.parse(header);
                long payloadLen = (Long) obj.get("payload");
                logger.info("Payload length: " + payloadLen);

                String payload = NetworkClientFilter.Protocol.readPayload(
                        fromServer, (int) payloadLen);
                logger.info("Received payload: " + payload);

                String type = (String) obj.get("type");
                String service = (String) obj.get("service");
                long returnCode = (Long) obj.get("return");

                assertEquals("Server returned unexpected message type",
                        NetworkClientFilter.Protocol.TYPE_ACKNOWLEDGED, type);
                assertEquals("Server returned different service name",
                        this.service, service);
                assertEquals("Server returned an error code", 0, returnCode);
                assertEquals("Server returned unexpected payload",
                        server.getServerName(), payload);
            }

            {
                // Send filter message.
                String value = "This message will be converted to hex representation!";
                long seqno = getRandomPositiveInt();
                long row = getRandomPositiveInt();
                String schema = "demo";
                String table = "testtable";
                toServer.print(generator.filter("String_to_HEX_v1", seqno, row,
                        schema, table, "bloby", value));
                toServer.flush();

                // Receive & check filtered message.
                String header = fromServer.readLine();
                logger.info("Received header: " + header);

                JSONObject obj = (JSONObject) parser.parse(header);
                long payloadLen = (Long) obj.get("payload");
                logger.info("Payload length: " + payloadLen);

                String payload = NetworkClientFilter.Protocol.readPayload(
                        fromServer, (int) payloadLen);
                logger.info("Received payload: " + payload);

                String type = (String) obj.get("type");
                long newSeqno = (Long) obj.get("seqno");
                long newRow = (Long) obj.get("row");
                String newSchema = (String) obj.get("schema");
                String newTable = (String) obj.get("table");
                long returnCode = (Long) obj.get("return");

                String expectedNewValue = NetworkFilterServer.toHex(value);

                assertEquals("Server returned unexpected message type",
                        NetworkClientFilter.Protocol.TYPE_FILTERED, type);
                assertEquals("Server returned an error code", 0, returnCode);
                assertEquals("Server returned unexpected payload",
                        expectedNewValue, payload);
                assertEquals("Server changed seqno", seqno, newSeqno);
                assertEquals("Server changed row number", row, newRow);
                assertEquals("Server changed schema", schema, newSchema);
                assertEquals("Server changed table", table, newTable);
            }

            {
                // Send release message.
                toServer.print(generator.release());
                toServer.flush();

                // Receive & check acknowledged message.
                String header = fromServer.readLine();
                logger.info("Received header: " + header);

                JSONObject obj = (JSONObject) parser.parse(header);
                long payloadLen = (Long) obj.get("payload");
                logger.info("Payload length: " + payloadLen);

                String payload = NetworkClientFilter.Protocol.readPayload(
                        fromServer, (int) payloadLen);
                logger.info("Received payload: " + payload);

                String type = (String) obj.get("type");

                assertEquals("Server returned unexpected message type",
                        NetworkClientFilter.Protocol.TYPE_ACKNOWLEDGED, type);
            }

            // Disconnect.
            toServer.close();
            fromServer.close();
            socket.close();
        }
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
        NetworkClientFilter ncf = new NetworkClientFilter();
        ncf.setTungstenSchema("tungsten_foo");
        ncf.setDefinitionsFile(definitionsFile);
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
            filterHelper.setFilter(ncf);
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
        NetworkClientFilter ncf = new NetworkClientFilter();
        ncf.setTungstenSchema("tungsten_foo");
        ncf.setDefinitionsFile(definitionsFile);
        createDefinitionsFile();

        filterHelper.setContext(replicatorContext);
        filterHelper.setFilter(ncf);

        assertEquals("Incorrect count of transformations from parsed JSON", 3,
                ncf.getDefinedTransformations());
        assertEquals("Incorrect count of column entries from parsed JSON", 9,
                ncf.getDefinedColumnEntries());

        filterHelper.done(); // Release the filter.

        assertEquals(
                "Incorrect count of transformations after releasing filter", 0,
                ncf.getDefinedTransformations());
        assertEquals(
                "Incorrect count of column entries after releasing filter", 0,
                ncf.getDefinedColumnEntries());

        filterHelper.setContext(replicatorContext);
        filterHelper.setFilter(ncf); // Check that prepare works 2nd time too.

        assertEquals("Incorrect count of transformations after 2nd prepare", 3,
                ncf.getDefinedTransformations());
        assertEquals("Incorrect count of column entries after 2nd prepare", 9,
                ncf.getDefinedColumnEntries());

        filterHelper.done();
    }

    public int getRandomPositiveInt()
    {
        Random rand = new Random();
        return rand.nextInt(Integer.MAX_VALUE);
    }

    public void testNetworkFilter() throws Exception
    {
        NetworkClientFilter ncf = new NetworkClientFilter();
        ncf.setTungstenSchema("tungsten_foo");
        ncf.setDefinitionsFile(definitionsFile);
        createDefinitionsFile();

        filterHelper.setContext(replicatorContext);
        filterHelper.setFilter(ncf); // Prepare filter.

        {
            //
            // Test a row change event.
            //
            String columns[] = {"id", "name", "birth_date", "photo"};
            String values[] = new String[columns.length];
            values[0] = "1";
            values[1] = "Vardenis";
            values[2] = "Testing\rvarious\nnew line\r\nvariants don't break protocol";

            // Try large payload.
            StringBuilder largePayload = new StringBuilder();
            int largePayloadK = 10;
            for (int i = 0; i < 1024 * largePayloadK; i++)
                largePayload.append(Character.toChars(i % 256));
            values[3] = largePayload.toString();

            ReplDBMSEvent e = eventHelper.eventFromRowInsert(
                    getRandomPositiveInt(), "vip", "clients", columns, values,
                    0, true);

            // Transform.
            ReplDBMSEvent e2 = filterHelper.filter(e);

            // Confirm results are correct.
            RowChangeData row = (RowChangeData) e2.getDBMSEvent().getData()
                    .get(0);
            OneRowChange orc = row.getRowChanges().get(0);

            // Columns that should have been left as is.
            for (int col = 0; col <= 1; col++)
            {
                assertEquals(
                        "Filter changed column which was not requested to be changed",
                        values[col], orc.getColumnValues().get(0).get(col)
                                .getValue());
            }

            // Columns that should have been transformed.
            for (int col = 2; col <= 3; col++)
            {
                String oldValue = values[col];
                String expectedNewValue = NetworkFilterServer.toHex(oldValue);
                String actualNewValue = (String) orc.getColumnValues().get(0)
                        .get(col).getValue();
                if (col == 2)
                logger.info(oldValue + " -> " + actualNewValue);
                else if (col == 3)
                    logger.info(oldValue.substring(0, 100) + "...("
                            + oldValue.length() + "B total) -> "
                            + actualNewValue.substring(0, 100) + "...");

                assertEquals("Server returned unexpected payload",
                        expectedNewValue, actualNewValue);
            }
        }

        {
            //
            // Test a row change event with different transformations.
            //
            String columns[] = {"id", "status", "cc_type", "cc_number",
                    "dummy", "pin"};
            String values[] = new String[columns.length];
            values[0] = "1";
            values[1] = "Valid";
            values[2] = "VISA Electron";
            values[3] = "1234098712340987";
            values[4] = "---";
            values[5] = "0184";
            ReplDBMSEvent e = eventHelper.eventFromRowInsert(
                    getRandomPositiveInt(), "vip", "credit_cards", columns,
                    values, 0, true);

            // Transform.
            ReplDBMSEvent e2 = filterHelper.filter(e);

            // Confirm results are correct.
            RowChangeData row = (RowChangeData) e2.getDBMSEvent().getData()
                    .get(0);
            OneRowChange orc = row.getRowChanges().get(0);

            // Column that should have been left as is.
            assertEquals(
                    "Filter changed column which was not requested to be changed",
                    values[1], orc.getColumnValues().get(0).get(1).getValue());

            // Columns that should have been transformed with String_to_HEX_v1.
            for (int col = 2; col <= 3; col++)
            {
                String oldValue = values[col];
                String expectedNewValue = NetworkFilterServer.toHex(oldValue);
                String actualNewValue = (String) orc.getColumnValues().get(0)
                        .get(col).getValue();
                logger.info(oldValue + " -String_to_HEX_v1-> " + actualNewValue);
                assertEquals("Server returned unexpected payload",
                        expectedNewValue, actualNewValue);
            }

            // Column which should have been transformed with Make_Null_v1.
            {
                String oldValue = values[4];
                String actualNewValue = (String) orc.getColumnValues().get(0)
                        .get(4).getValue();
                logger.info(oldValue + " -Make_Null_v1-> " + actualNewValue);
                assertEquals("Server returned unexpected payload", null,
                        actualNewValue);
            }

            // Column which should have been transformed with Make_Empty_v1.
            {
                String oldValue = values[5];
                String actualNewValue = (String) orc.getColumnValues().get(0)
                        .get(5).getValue();
                logger.info(oldValue + " -Make_Empty_v1-> " + actualNewValue);
                assertEquals("Server returned unexpected payload",
                        NetworkFilterServer.toEmpty(oldValue), actualNewValue);
            }
        }

        filterHelper.done(); // Release the filter.
    }

    /**
     * Tests that Replicator exception is thrown if server returns an error code
     * during filtering.
     */
    public void testNetworkFilterFailure() throws Exception
    {
        NetworkClientFilter ncf = new NetworkClientFilter();
        ncf.setTungstenSchema("tungsten_foo");
        ncf.setDefinitionsFile(definitionsFile);
        createDefinitionsFile();

        filterHelper.setContext(replicatorContext);
        filterHelper.setFilter(ncf); // Prepare filter.

        //
        // Test a row change event.
        //
        String columns[] = {"id", "name", "password", "photo"};
        String values[] = new String[columns.length];
        values[0] = "1";
        values[1] = "Vardenis";
        values[2] = "secret";
        values[3] = "my.png";
        ReplDBMSEvent e = eventHelper.eventFromRowInsert(
                getRandomPositiveInt(), "vip", "clients", columns, values, 0,
                true);

        try
        {
            // Transform.
            filterHelper.filter(e);

            fail("Filtering server should have sent a non-zero return code and filter should have thrown an exception");
        }
        catch (ReplicatorException ex)
        {
            logger.info("Expected error received: " + ex);
        }

        filterHelper.done(); // Release the filter.
    }

    /**
     * A test network server implementing NetworkClientFilter TCP protocol.
     */
    static public class NetworkFilterServer extends Thread
    {
        private Logger                                     logger     = Logger.getLogger(NetworkFilterServer.class);

        private JSONParser                                 parser     = new JSONParser();
        private NetworkClientFilter.ServerMessageGenerator generator  = new NetworkClientFilter.ServerMessageGenerator();

        private String                                     serverName = "JAVA HEX transformer\nNew line test for payload";

        public String getServerName()
        {
            return serverName;
        }

        public static void main(String[] args)
        {
            NetworkFilterServer server = new NetworkFilterServer();
            server.run(); // Start server in this thread.
        }

        public static String toHex(String value)
        {
            return String.format("%x", new BigInteger(1, value.getBytes()));
        }

        public static String toEmpty(String value)
        {
            return "";
        }

        public void run()
        {
            try
            {
                ServerSocket serverSocket = new ServerSocket(serverPort);
                serverSocket.setSoTimeout(timeout * 1000);
                while (true)
                {
                    logger.info("Waiting for a client");
                    Socket server = serverSocket.accept();
                    logger.info("Connected to "
                            + server.getRemoteSocketAddress());

                    PrintWriter toClient = new PrintWriter(
                            server.getOutputStream(), true);
                    BufferedReader fromClient = new BufferedReader(
                            new InputStreamReader(server.getInputStream()));

                    // Our test server is one client at a time.
                    while (true)
                    {
                        // Receive message.
                        String header = fromClient.readLine();
                        if (header == null)
                            break; // Next client.
                        logger.info("Received header: " + header);

                        JSONObject obj = (JSONObject) parser.parse(header);
                        long payloadLen = (Long) obj.get("payload");
                        logger.info("Payload length: " + payloadLen);

                        String payload = NetworkClientFilter.Protocol
                                .readPayload(fromClient, (int) payloadLen);
                        logger.info("Received payload: " + payload);

                        String type = (String) obj.get("type");
                        String service = (String) obj.get("service");

                        if (type.equals(NetworkClientFilter.Protocol.TYPE_PREPARE))
                        {
                            // Send acknowledged message.
                            int returnCode = 0;
                            toClient.print(generator.acknowledged(service,
                                    returnCode, serverName));
                            toClient.flush();
                        }
                        else if (type
                                .equals(NetworkClientFilter.Protocol.TYPE_FILTER))
                        {
                            // Send filtered message.
                            String transformation = (String) obj
                                    .get("transformation");
                            Long seqno = (Long) obj.get("seqno");
                            Long row = (Long) obj.get("row");
                            String schema = (String) obj.get("schema");
                            String table = (String) obj.get("table");
                            String column = (String) obj.get("column");

                            int returnCode = 0;
                            String newValue = payload;
                            if (transformation.equals("String_to_HEX_v1"))
                            {
                                newValue = toHex(payload);
                            }
                            else if (transformation.equals("Make_Empty_v1"))
                            {
                                newValue = toEmpty(payload);
                            }
                            else if (transformation.equals("Make_Null_v1"))
                            {
                                newValue = null;
                            }

                            // An error introduce on purpose for this column.
                            if (column.equals("password"))
                            {
                                returnCode = 1;
                                newValue = "Password columns cannot be transformed";
                            }

                            toClient.print(generator.filtered(service,
                                    transformation, returnCode, seqno, row,
                                    schema, table, column, newValue));
                            toClient.flush();
                        }
                        else if (type
                                .equals(NetworkClientFilter.Protocol.TYPE_RELEASE))
                        {
                            // Send acknowledged message.
                            int returnCode = 0;
                            toClient.print(generator.acknowledged(service,
                                    returnCode, serverName));
                            toClient.flush();
                        }
                    }
                }
            }
            catch (UnknownHostException ex)
            {
                ex.printStackTrace();
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
            catch (ParseException pe)
            {
                logger.error("Error parsing JSON: " + pe.getPosition());
                logger.error(pe);
            }
            catch (ReplicatorException e)
            {
                logger.error(e);
            }
            finally
            {
                logger.info("Closed");
            }
        }
    }
}