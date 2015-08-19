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
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import javax.sql.rowset.serial.SerialException;

import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.database.DatabaseHelper;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.extractor.mysql.SerialBlob;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Filter which sends column values to a TCP server for processing, receives the
 * processed results and uses it as a new value.
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 * @version 1.0
 */
public class NetworkClientFilter implements Filter
{
    private static Logger          logger               = Logger.getLogger(NetworkClientFilter.class);

    /**
     * Path to definition file.
     */
    private String                 definitionsFile      = null;

    /**
     * TCP port filtering server is listening on.
     */
    private int                    serverPort           = 3112;

    /**
     * Socket timeout in seconds.
     */
    private int                    timeout              = 10;

    /**
     * Parsed JSON holder.
     */
    private Map<String, JSONArray> definitions          = null;

    /**
     * Count of column entries in the definitions file.
     */
    private int                    definedColumnEntries = 0;

    /**
     * Name of current replication service's internal tungsten schema.
     */
    private String                 tungstenSchema;

    /**
     * Parser used to read column definition file.
     */
    private static JSONParser      parser               = new JSONParser();

    private Socket                 socket               = null;
    private PrintWriter            toServer             = null;
    private BufferedReader         fromServer           = null;

    private ClientMessageGenerator messageGenerator     = null;

    /**
     * Sets the path to definition file.
     * 
     * @param definitionsFile Path to file.
     */
    public void setDefinitionsFile(String definitionsFile)
    {
        this.definitionsFile = definitionsFile;
    }

    /**
     * Set TCP port that filtering server is listening on.
     */
    public void setServerPort(int serverPort)
    {
        this.serverPort = serverPort;
    }

    /**
     * Sets a timeout (in seconds) for network socket operations.
     */
    public void setTimeout(int timeout)
    {
        this.timeout = timeout;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.filter.Filter#filter(com.continuent.tungsten.replicator.event.ReplDBMSEvent)
     */
    public ReplDBMSEvent filter(ReplDBMSEvent event) throws ReplicatorException
    {
        ArrayList<DBMSData> data = event.getData();
        for (Iterator<DBMSData> iterator = data.iterator(); iterator.hasNext();)
        {
            DBMSData dataElem = iterator.next();
            if (dataElem instanceof RowChangeData)
            {
                RowChangeData rdata = (RowChangeData) dataElem;
                for (Iterator<OneRowChange> iterator2 = rdata.getRowChanges()
                        .iterator(); iterator2.hasNext();)
                {
                    OneRowChange orc = iterator2.next();

                    // Don't analyze tables from Tungsten schema.
                    if (orc.getSchemaName().compareToIgnoreCase(tungstenSchema) == 0)
                    {
                        if (logger.isDebugEnabled())
                            logger.debug("Ignoring " + tungstenSchema
                                    + " schema");
                        continue;
                    }

                    // Loop through defined transformations (usually one).
                    Iterator<String> it = definitions.keySet().iterator();
                    while (it.hasNext())
                    {
                        String transformation = it.next();

                        JSONArray array = definitions.get(transformation);
                        
                        // Not using any hash, because simple loop should be
                        // fast enough for a few expected entries.
                        for (Object o : array)
                        {
                            JSONObject jo = (JSONObject) o;
                            String defSchema = (String) jo.get("schema");
                            String defTable = (String) jo.get("table");

                            // Found a filter request for this schema & table?
                            if ((defSchema.equals("*") || defSchema.equals(orc
                                    .getSchemaName()))
                                    && (defTable.equals("*") || defTable
                                            .equals(orc.getTableName())))
                            {
                                // Defined columns to filter.
                                JSONArray defColumns = (JSONArray) jo
                                        .get("columns");

                                // Filter column values.
                                ArrayList<ColumnSpec> colSpecs = orc
                                        .getColumnSpec();
                                ArrayList<ArrayList<OneRowChange.ColumnVal>> colValues = orc
                                        .getColumnValues();
                                for (int c = 0; c < colSpecs.size(); c++)
                                {
                                    ColumnSpec colSpec = colSpecs.get(c);
                                    if (colSpec.getName() != null)
                                    {
                                        // Have this column in definitions?
                                        if (defColumns.contains(colSpec
                                                .getName()))
                                        {
                                            // Iterate through all rows in the
                                            // column.
                                            for (int row = 0; row < colValues
                                                    .size(); row++)
                                            {
                                                ColumnVal colValue = colValues
                                                        .get(row).get(c);
                                                if (colValue.getValue() != null)
                                                {
                                                    if (logger.isDebugEnabled())
                                                        logger.debug("Sending value: "
                                                                + colValue
                                                                        .getValue());

                                                    // Send to server.
                                                    Object newValue = sendToFilter(
                                                            transformation,
                                                            event.getSeqno(),
                                                            row,
                                                            orc.getSchemaName(),
                                                            orc.getTableName(),
                                                            colSpec.getName(),
                                                            colValue.getValue());
                                                    colValue.setValue((Serializable) newValue);

                                                    if (logger.isDebugEnabled())
                                                        logger.debug("Received value: "
                                                                + newValue);
                                                }
                                            }
                                        }
                                    }
                                    else
                                    {
                                        if (logger.isDebugEnabled())
                                        {
                                            logger.debug("Expected to filter column, but column name is undefined: "
                                                    + orc.getSchemaName()
                                                    + "."
                                                    + orc.getTableName()
                                                    + "["
                                                    + colSpec.getIndex() + "]");
                                        }
                                    }
                                }

                                // Filter key values.
                                ArrayList<ColumnSpec> keySpecs = orc
                                        .getKeySpec();
                                ArrayList<ArrayList<OneRowChange.ColumnVal>> keyValues = orc
                                        .getKeyValues();
                                for (int k = 0; k < keySpecs.size(); k++)
                                {
                                    ColumnSpec keySpec = keySpecs.get(k);
                                    if (keySpec.getName() != null)
                                    {
                                        if (defColumns.contains(keySpec
                                                .getName()))
                                        {
                                            // Iterate through all rows in the
                                            // key.
                                            for (int row = 0; row < keyValues
                                                    .size(); row++)
                                            {
                                                ColumnVal keyValue = keyValues
                                                        .get(row).get(k);
                                                if (keyValue.getValue() != null)
                                                {
                                                    if (logger.isDebugEnabled())
                                                        logger.debug("Sending value: "
                                                                + keyValue
                                                                        .getValue());

                                                    // Send to server.
                                                    Object newValue = sendToFilter(
                                                            transformation,
                                                            event.getSeqno(),
                                                            row,
                                                            orc.getSchemaName(),
                                                            orc.getTableName(),
                                                            keySpec.getName(),
                                                            keyValue.getValue());
                                                    keyValue.setValue((Serializable) newValue);

                                                    if (logger.isDebugEnabled())
                                                        logger.debug("Received value: "
                                                                + newValue);
                                                }
                                            }
                                        }
                                    }
                                    else
                                    {
                                        if (logger.isDebugEnabled())
                                        {
                                            logger.debug("Expected to filter key, but column name is undefined: "
                                                    + orc.getSchemaName()
                                                    + "."
                                                    + orc.getTableName()
                                                    + "["
                                                    + keySpec.getIndex() + "]");
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            else if (dataElem instanceof StatementData)
            {
                // Not supported.
            }
        }
        return event;
    }

    /**
     * Sets the Tungsten schema, which we ignore to prevent problems with the
     * replicator. This is mostly used for filter testing, which runs without a
     * pipeline.
     */
    public void setTungstenSchema(String tungstenSchema)
    {
        this.tungstenSchema = tungstenSchema;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException
    {
        if (tungstenSchema == null)
        {
            tungstenSchema = context.getReplicatorProperties().getString(
                    ReplicatorConf.METADATA_SCHEMA);
        }
        if (definitionsFile == null)
        {
            throw new ReplicatorException(
                    "definitionsFile property not set - specify a path to JSON file");
        }
    }

    /**
     * Reads the whole text file into a String.
     * 
     * @throws Exception if the file cannot be found.
     */
    public static String readDefinitionsFile(String file) throws IOException
    {
        BufferedReader br = new BufferedReader(new FileReader(file));
        try
        {
            StringBuilder builder = new StringBuilder();
            String line = null;
            String newLine = System.getProperty("line.separator");
            while ((line = br.readLine()) != null)
            {
                builder.append(line);
                builder.append(newLine);
            }
            return builder.toString();
        }
        finally
        {
            try
            {
                if (br != null)
                    br.close();
            }
            catch (IOException ex)
            {
                logger.warn("Unable to close file " + file + ": "
                        + ex.toString());
            }
        }
    }

    /**
     * Returns how many different transformations are defined in the JSON
     * definitions file. Usually that's one, unless server supports multiple
     * transformation.
     */
    public int getDefinedTransformations()
    {
        if (definitions != null)
        {
            return definitions.keySet().size();
        }
        else
        {
            return 0;
        }
    }

    /**
     * Returns how many column entries were parsed out of the JSON file.
     */
    public int getDefinedColumnEntries()
    {
        return definedColumnEntries;
    }

    /**
     * Initial validation of the JSON definitions file.
     */
    private void initDefinitionsFile() throws ReplicatorException
    {
        try
        {
            logger.info("Using: " + definitionsFile);

            String jsonText = readDefinitionsFile(definitionsFile);
            Object obj = parser.parse(jsonText);
            @SuppressWarnings("unchecked")
            Map<String, JSONArray> map = (Map<String, JSONArray>) obj;
            definitions = map;

            Iterator<String> it = definitions.keySet().iterator();
            while (it.hasNext())
            {
                String transformation = it.next();
                logger.info("Transformation: " + transformation);

                JSONArray array = definitions.get(transformation);
                for (Object o : array)
                {
                    JSONObject jo = (JSONObject) o;
                    String schema = (String) jo.get("schema");
                    String table = (String) jo.get("table");
                    JSONArray columns = (JSONArray) jo.get("columns");
                    logger.info("  In " + schema + "." + table + ": ");
                    for (Object c : columns)
                    {
                        String column = (String) c;
                        definedColumnEntries++;
                        logger.info("    " + column);
                    }
                }
            }
        }
        catch (ClassCastException e)
        {
            throw new ReplicatorException(
                    "Unable to read definitions file (is JSON structure correct?): "
                            + e, e);
        }
        catch (ParseException e)
        {
            throw new ReplicatorException(
                    "Unable to read definitions file (error parsing JSON): "
                            + e, e);
        }
        catch (IOException e)
        {
            throw new ReplicatorException(e);
        }
    }

    /**
     * Connects to the filtering server and prepares I/O streams.
     */
    private void initConnection() throws ReplicatorException
    {
        try
        {
            // Connect to filtering server.
            logger.info("Connecting to the filtering server on port "
                    + serverPort);
            InetAddress host = InetAddress.getByName("localhost");
            socket = new Socket(host, serverPort);
            socket.setSoTimeout(timeout * 1000);
            logger.debug("Receive buffer size: "
                    + socket.getReceiveBufferSize());
            logger.info("Connected to " + socket.getRemoteSocketAddress());

            toServer = new PrintWriter(socket.getOutputStream(), true);
            fromServer = new BufferedReader(new InputStreamReader(
                    socket.getInputStream()));
        }
        catch (UnknownHostException e)
        {
            throw new ReplicatorException(
                    "Unable to connect to filtering server: " + e, e);
        }
        catch (IOException e)
        {
            throw new ReplicatorException(
                    "Unable to connect to filtering server: " + e, e);
        }
    }

    /**
     * Send prepare message to the server and check that a valid acknowledged
     * message is received.
     */
    private void doHandshake(String contextService) throws ReplicatorException
    {
        try
        {
            // Send prepare message.
            toServer.print(messageGenerator.prepare());
            toServer.flush();

            // Receive & check acknowledged message.
            String header = fromServer.readLine();
            if (logger.isDebugEnabled())
                logger.debug("Received header: " + header);

            JSONObject obj = (JSONObject) parser.parse(header);
            long payloadLen = (Long) obj.get("payload");
            if (logger.isDebugEnabled())
                logger.debug("Payload length: " + payloadLen);

            String payload = NetworkClientFilter.Protocol.readPayload(
                    fromServer, (int) payloadLen);
            if (logger.isDebugEnabled())
                logger.debug("Received payload: " + payload);

            String type = (String) obj.get("type");
            String service = (String) obj.get("service");
            long returnCode = (Long) obj.get("return");

            validateMessage(Protocol.TYPE_ACKNOWLEDGED, type, returnCode,
                    service, payload);

            logger.info("Server: " + payload);
        }
        catch (ParseException e)
        {
            throw new ReplicatorException(
                    "Server returned an invalid message during prepare-acknowledged message handshake: "
                            + e, e);
        }
        catch (IOException e)
        {
            throw new ReplicatorException(
                    "prepare-acknowledged message handshake failed: " + e, e);
        }
    }

    /**
     * Converts underlying data type value to string. Exception is thrown if
     * type is unsupported.
     */
    private String valueToString(Object value) throws ReplicatorException
    {
        if (value instanceof String)
        {
            return value.toString();
        }
        else if (value instanceof Integer)
        {
            return value.toString();
        }
        else if (value instanceof SerialBlob)
        {
            try
            {
                SerialBlob blob = (SerialBlob) value;
                return new String(blob.getBytes(1, (int) blob.length()));
            }
            catch (SerialException e)
            {
                throw new ReplicatorException(
                        "Unable to convert SerialBlob to String: " + e, e);
            }
        }
        else
        {
            // For other data types using cast instead of toString() on purpose.
            // This way we get an exception if cast is unsupported, while
            // toString() would just return a JAVA class name in such cases.
            return (String) value;
        }
    }

    /**
     * Converts string back to correct (previous) data type.
     * 
     * @throws ReplicatorException
     */
    private Object stringToValue(Object oldValue, String newValue)
            throws ReplicatorException
    {
        if (oldValue instanceof String)
        {
            return newValue;
        }
        else if (oldValue instanceof Integer)
        {
            return Integer.valueOf(newValue);
        }
        else if (oldValue instanceof SerialBlob)
        {
            try
            {
                return DatabaseHelper.getSafeBlob(newValue.getBytes());
            }
            catch (SQLException e)
            {
                throw new ReplicatorException(
                        "Unable to convert back from String to SerialBlob: "
                                + e, e);
            }
        }
        else
        {
            return newValue;
        }
    }

    /**
     * Sends column/key value to the server and receives filtered result.
     */
    private Object sendToFilter(String transformation, long seqno, int row,
            String schema, String table, String column, Object value)
            throws ReplicatorException
    {
        try
        {
            // Convert various data types to string for transfer.
            String str = valueToString(value);

            // Send filter message.
            String send = messageGenerator.filter(transformation, seqno, row,
                    schema, table, column, str);
            toServer.print(send);
            toServer.flush();

            // Receive & check filtered message.
            String header = fromServer.readLine();
            if (logger.isDebugEnabled())
                logger.debug("Received header: " + header);

            if (header == null)
                throw new ReplicatorException(
                        "Server didn't send response to a filter request: "
                                + send);

            JSONObject obj = (JSONObject) parser.parse(header);
            long payloadLen = (Long) obj.get("payload");
            if (logger.isDebugEnabled())
                logger.debug("Payload length: " + payloadLen);

            String payload = NetworkClientFilter.Protocol.readPayload(
                    fromServer, (int) payloadLen);
            if (logger.isDebugEnabled())
                logger.debug("Received payload: " + payload);

            String type = (String) obj.get("type");
            long newSeqno = (Long) obj.get("seqno");
            long newRow = (Long) obj.get("row");
            String newSchema = (String) obj.get("schema");
            String newTable = (String) obj.get("table");
            long returnCode = (Long) obj.get("return");
            String service = (String) obj.get("service");

            // Validate that returned information matches what we requested.
            validateMessage(Protocol.TYPE_FILTERED, type, returnCode, service,
                    payload);
            if (newSeqno != seqno)
                throw new ReplicatorException("Expected to receive seqno "
                        + seqno + ", but server sent " + newSeqno
                        + " instead: " + header + payload);
            if (newRow != row)
                throw new ReplicatorException("Expected to receive row " + row
                        + ", but server sent " + newRow + " instead: " + header
                        + payload);
            if (!newSchema.equals(schema))
                throw new ReplicatorException("Expected to receive schema "
                        + schema + ", but server sent " + newSchema
                        + " instead: " + header + payload);
            if (!newTable.equals(table))
                throw new ReplicatorException("Expected to receive table "
                        + table + ", but server sent " + newTable
                        + " instead: " + header + payload);

            // Convert result back to correct data type.
            return stringToValue(value, payload);
        }
        catch (ParseException e)
        {
            throw new ReplicatorException(
                    "Server returned an invalid message during prepare-acknowledged message handshake: "
                            + e, e);
        }
        catch (IOException e)
        {
            throw new ReplicatorException(
                    "prepare-acknowledged message handshake failed: " + e, e);
        }
    }

    /**
     * Protocol safety checks for the server returned message.
     */
    private void validateMessage(String expectedType, String type,
            long returnCode, String service, String payload)
            throws ReplicatorException
    {
        if (!type.equals(expectedType))
        {
            throw new ReplicatorException(
                    "Server should have returned message of type \""
                            + Protocol.TYPE_FILTERED + "\", but returned \""
                            + type + "\" instead");
        }
        else if (returnCode != 0)
        {
            throw new ReplicatorException("Server returned a non-zero code ("
                    + returnCode + "), payload: " + payload);
        }
        else if (!service.equals(messageGenerator.getService()))
        {
            throw new ReplicatorException(
                    "Server returned unexpected service name in the message: received \""
                            + service + "\", but expected \""
                            + messageGenerator.getService() + "\"");
        }
    }

    /**
     * Prepares connection to the filtering server and parses definition file.
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException
    {
        messageGenerator = new ClientMessageGenerator(context.getServiceName());

        initDefinitionsFile();
        initConnection();
        doHandshake(context.getServiceName());
    }

    /**
     * Sends release message to the server. Confirms the success. Failure are
     * logged, but otherwise ignored.
     */
    private void sendRelease()
    {
        try
        {
            // Send release message.
            toServer.print(messageGenerator.release());
            toServer.flush();

            // Receive & check acknowledged message.
            String header = fromServer.readLine();
            if (header != null)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Received header: " + header);

                JSONObject obj = (JSONObject) parser.parse(header);
                long payloadLen = (Long) obj.get("payload");
                if (logger.isDebugEnabled())
                    logger.debug("Payload length: " + payloadLen);

                String payload = NetworkClientFilter.Protocol.readPayload(
                        fromServer, (int) payloadLen);
                if (logger.isDebugEnabled())
                    logger.debug("Received payload: " + payload);

                String type = (String) obj.get("type");
                long returnCode = (Long) obj.get("return");

                if (type.equals(Protocol.TYPE_ACKNOWLEDGED))
                {
                    if (returnCode == 0)
                    {
                        logger.info("Server acknowledged filter release: "
                                + payload);
                    }
                    else
                    {
                        logger.warn("Server returned a non-zero code ("
                                + returnCode
                                + ") in response to release message: "
                                + payload);

                    }
                }
                else
                {
                    logger.warn("Server should have returned message of type \""
                            + Protocol.TYPE_ACKNOWLEDGED
                            + "\", but returned \""
                            + type
                            + "\" instead. Full message: " + header + payload);
                }
            }
            else
            {
                logger.warn("Server didn't send response to a release request");
            }
        }
        catch (ParseException e)
        {
            logger.warn("Error parsing message received back from the filtering server after release message (ignoring): "
                    + e);
        }
        catch (IOException e)
        {
            logger.warn("Sending of release message to the filtering server failed (ignoring): "
                    + e);
        }
        catch (ReplicatorException e)
        {
            logger.warn("Sending of release message to the filtering server failed (ignoring): "
                    + e);
    }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException
    {
        definitions = null;
        definedColumnEntries = 0;

        try
        {
            if (socket != null)
            {
            logger.info("Disconnecting from the server "
                    + socket.getRemoteSocketAddress());
            sendRelease();
            }
            if (toServer != null)
            toServer.close();
            if (fromServer != null)
            fromServer.close();
            if (socket != null)
            socket.close();

        }
        catch (IOException e)
        {
            // It's OK if disconnect fails.
        }
    }

    /**
     * Filtering server protocol. Protocol defines that message consists of a
     * header following by a payload. Header is a single-level JSON object
     * ending with a closing curly brace and a new line feed terminator. Payload
     * is following afterwards. Payload length is determined from JSON's
     * "payload" property. Payload may contain new line feeds if required -
     * protocol does not assume any terminating character for the payload, as
     * its length is known.
     */
    static class Protocol
    {
        public static final String VERSION           = "v0_9";

        /** Message types. */
        public static final String TYPE_PREPARE      = "prepare";
        public static final String TYPE_FILTER       = "filter";
        public static final String TYPE_RELEASE      = "release";
        public static final String TYPE_ACKNOWLEDGED = "acknowledged";
        public static final String TYPE_FILTERED     = "filtered";

        /**
         * Reads given size payload from a socket. Socket must be open. After
         * reading checks that payload is of expected length.
         */
        public static String readPayload(BufferedReader socketReader,
                int payloadLength) throws IOException, ReplicatorException
        {
            if (payloadLength == 0)
            {
                // Don't try to read if payload is an empty string.
                return "";
        }
            else if (payloadLength == -1)
            {
                // Protocol defines that null is sent as payload of -1 length.
                return null;
            }

            // Read whole payload in a few iterations.
            char[] buf = new char[1024];
            StringBuilder payload = new StringBuilder();
            do
        {
                int bytesRead = socketReader.read(buf, 0, buf.length);
                if (bytesRead > 0)
                    payload.append(new String(buf, 0, bytesRead));
        }
            while (payload.length() < payloadLength);

            if (payload.length() != payloadLength)
            {
                throw new ReplicatorException(
                        "Size of received payload is incorrect (expected="
                                + payloadLength + ", received="
                                + payload.length() + "):" + payload);
    }

            return payload.toString();
        }
    }

    /**
     * Generator of client messages.
     */
    static class ClientMessageGenerator
    {
        private String service;

        public ClientMessageGenerator(String service)
        {
            this.service = service;
        }

        public String getService()
        {
            return service;
        }

        public String prepare()
        {
            StringBuilder sb = new StringBuilder();

            sb.append("{");
            sb.append("\"protocol\":\"" + Protocol.VERSION + "\",");
            sb.append("\"type\":\"" + Protocol.TYPE_PREPARE + "\",");
            sb.append("\"service\":\"" + service + "\",");
            sb.append("\"payload\":-1");
            sb.append("}\n");

            return sb.toString();
        }

        public String filter(String transformation, long seqno, long row,
                String schema, String table, String column, String payload)
        {
            StringBuilder sb = new StringBuilder();

            int payloadLen = 0;
            if (payload == null)
            {
                // Protocol defines that null is sent as payload of -1 length.
                payloadLen = -1;
            }
            else
            {
                payloadLen = payload.length();
            }

            sb.append("{");
            sb.append("\"protocol\":\"" + Protocol.VERSION + "\",");
            sb.append("\"type\":\"" + Protocol.TYPE_FILTER + "\",");
            sb.append("\"transformation\":\"" + transformation + "\",");
            sb.append("\"service\":\"" + service + "\",");
            sb.append("\"seqno\":" + seqno + ",");
            sb.append("\"row\":" + row + ",");
            sb.append("\"schema\":\"" + schema + "\",");
            sb.append("\"table\":\"" + table + "\",");
            sb.append("\"column\":\"" + column + "\",");
            sb.append("\"fragment\":1,");
            sb.append("\"fragments\":1,");
            sb.append("\"payload\":" + payloadLen + "");
            sb.append("}\n");
            sb.append(payload);

            return sb.toString();
        }

        public String release()
        {
            StringBuilder sb = new StringBuilder();

            sb.append("{");
            sb.append("\"protocol\":\"" + Protocol.VERSION + "\",");
            sb.append("\"type\":\"" + Protocol.TYPE_RELEASE + "\",");
            sb.append("\"service\":\"" + service + "\",");
            sb.append("\"payload\":-1");
            sb.append("}\n");

            return sb.toString();
        }
    }

    /**
     * Generator of server messages.
     */
    static class ServerMessageGenerator
    {
        public String filtered(String service, String transformation,
                int returnCode, long seqno, long row, String schema,
                String table, String column, String payload)
        {
            StringBuilder sb = new StringBuilder();

            int payloadLen = 0;
            if (payload == null)
            {
                // Protocol defines that null is sent as payload of -1 length.
                payloadLen = -1;
            }
            else
            {
                payloadLen = payload.length();
            }

            sb.append("{");
            sb.append("\"protocol\":\"" + Protocol.VERSION + "\",");
            sb.append("\"type\":\"" + Protocol.TYPE_FILTERED + "\",");
            sb.append("\"transformation\":\"" + transformation + "\",");
            sb.append("\"return\":" + returnCode + ",");
            sb.append("\"service\":\"" + service + "\",");
            sb.append("\"seqno\":" + seqno + ",");
            sb.append("\"row\":" + row + ",");
            sb.append("\"schema\":\"" + schema + "\",");
            sb.append("\"table\":\"" + table + "\",");
            sb.append("\"column\":\"" + column + "\",");
            sb.append("\"fragment\":1,");
            sb.append("\"fragments\":1,");
            sb.append("\"payload\":" + payloadLen + "");
            sb.append("}\n");
            sb.append(payload);

            return sb.toString();
        }

        public String acknowledged(String service, int returnCode,
                String payload)
        {
            StringBuilder sb = new StringBuilder();

            sb.append("{");
            sb.append("\"protocol\":\"" + Protocol.VERSION + "\",");
            sb.append("\"type\":\"" + Protocol.TYPE_ACKNOWLEDGED + "\",");
            sb.append("\"return\":" + returnCode + ",");
            sb.append("\"service\":\"" + service + "\",");
            sb.append("\"payload\":" + payload.length() + "");
            sb.append("}\n");
            sb.append(payload);

            return sb.toString();
        }
    }
}
