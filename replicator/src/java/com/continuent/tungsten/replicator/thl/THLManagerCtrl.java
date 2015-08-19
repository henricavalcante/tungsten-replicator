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
 * Contributor(s): Stephane Giron, Robert Hodges
 */

package com.continuent.tungsten.replicator.thl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import javax.sql.rowset.serial.SerialBlob;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.exec.ArgvIterator;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntimeConf;
import com.continuent.tungsten.replicator.datatypes.SQLTypes;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.LoadDataFileFragment;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.RowIdData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.DBMSEmptyEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSFilteredEvent;
import com.continuent.tungsten.replicator.event.ReplEvent;
import com.continuent.tungsten.replicator.event.ReplOption;
import com.continuent.tungsten.replicator.thl.log.DiskLog;
import com.continuent.tungsten.replicator.thl.log.LogConnection;
import com.continuent.tungsten.replicator.thl.log.LogEventReadFilter;
import com.continuent.tungsten.replicator.thl.log.LogEventReplReader;

/**
 * This class defines a THLManagerCtrl that implements a utility to access
 * THLManager methods. See the printHelp() command for a description of current
 * commands.
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 * @version 1.0
 */
public class THLManagerCtrl
{
    private static Logger         logger             = Logger.getLogger(THLManagerCtrl.class);

    /**
     * Default path to replicator.properties if user not specified other.
     */
    protected static final String defaultConfigPath  = ".."
                                                             + File.separator
                                                             + "conf"
                                                             + File.separator
                                                             + "static-default.properties";

    /**
     * Maximum length of characters to print out for a BLOB. If BLOB is larger,
     * it is truncated and "<...>" is added to the end.<br/>
     */
    private static final int      maxBlobPrintLength = 1000;
    protected static ArgvIterator argvIterator       = null;
    protected String              configFile         = null;
    private boolean               doChecksum;
    private String                logDir;
    private DiskLog               diskLog;

    protected static SQLTypes     sqlTypes           = new SQLTypes();

    /**
     * Creates a new <code>THLManagerCtrl</code> object.
     * 
     * @param configFile Path to the Tungsten properties file.
     * @param doChecksum If false disable checksums on log.
     * @throws Exception
     */
    public THLManagerCtrl(String configFile, boolean doChecksum)
            throws Exception
    {
        // Set path to configuration file.
        this.configFile = configFile;

        // Read properties required to connect to database.
        TungstenProperties properties = readConfig();
        logDir = properties.getString("replicator.store.thl.log_dir");
        this.doChecksum = doChecksum;
    }

    /**
     * Reads the replicator.properties.
     */
    protected TungstenProperties readConfig() throws Exception
    {
        TungstenProperties conf = null;

        // Open configuration file.
        File propsFile = new File(configFile);
        if (!propsFile.exists() || !propsFile.canRead())
        {
            throw new Exception("Properties file not found: "
                    + propsFile.getAbsolutePath(), null);
        }
        conf = new TungstenProperties();

        // Read configuration.
        try
        {
            conf.load(new FileInputStream(propsFile));
        }
        catch (IOException e)
        {
            throw new Exception(
                    "Unable to read properties file: "
                            + propsFile.getAbsolutePath() + " ("
                            + e.getMessage() + ")", null);
        }
        return conf;
    }

    /**
     * Connect to the underlying database containing THL.
     * 
     * @throws ReplicatorException
     */
    public void prepare(boolean readOnly) throws ReplicatorException,
            InterruptedException
    {
        diskLog = new DiskLog();
        diskLog.setLogDir(logDir);
        diskLog.setReadOnly(readOnly);
        diskLog.setDoChecksum(doChecksum);
        diskLog.prepare();
    }

    /**
     * Disconnect from the THL database.
     */
    public void release()
    {
        if (diskLog != null)
        {
            try
            {
                diskLog.release();
            }
            catch (ReplicatorException e)
            {
                logger.warn("Unable to release log", e);
            }
            catch (InterruptedException e)
            {
                logger.warn("Unexpected interruption while closing log", e);
            }
            diskLog = null;
        }
    }

    /**
     * Queries THL for summary information.
     * 
     * @return Info holder
     * @throws ReplicatorException
     */
    public InfoHolder getInfo() throws ReplicatorException
    {
        long minSeqno = diskLog.getMinSeqno();
        long maxSeqno = diskLog.getMaxSeqno();
        String logDirName = diskLog.getLogDir();

        int logFiles = diskLog.getLogFileNames().length;

        File logDir = new File(logDirName);

        // Calculate total size.
        File[] logs = DiskLog.listLogFiles(logDir, diskLog.getFilePrefix());
        long logsSize = 0;
        for (File log : logs)
        {
            logsSize += log.length();
        }

        // Get oldest and newest files.
        File oldestFile = null;
        File newestFile = null;
        if (diskLog.getFirstFile() != null)
            oldestFile = new File(logDir + File.separator
                    + diskLog.getFirstFile());
        if (diskLog.getLastFile() != null)
            newestFile = new File(logDir + File.separator
                    + diskLog.getLastFile());

        return new InfoHolder(logDirName, minSeqno, maxSeqno, maxSeqno
                - minSeqno, -1, logFiles, oldestFile, newestFile, logsSize);
    }

    /**
     * Formats column and column value for printing.
     * 
     * @param charset character set name to be used to decode byte arrays in row
     *            replication
     * @param specs Provide column specifications.
     */
    public static String formatColumn(OneRowChange.ColumnSpec colSpec,
            OneRowChange.ColumnVal value, String prefix, String charset,
            boolean hex, boolean specs, DateFormat dateFormatter)
    {
        String log = "  - " + prefix + "(";
        if (colSpec != null)
        {
            if (specs)
            {
                log += "index=" + colSpec.getIndex();
                log += " name=" + colSpec.getName();
                log += " type=" + colSpec.getType();
                log += " [" + sqlTypes.sqlTypeToString(colSpec.getType()) + "]";
                log += " length=" + colSpec.getLength();
                log += " unsigned=" + colSpec.isUnsigned();
                log += " blob=" + colSpec.isBlob();
                log += " desc=" + colSpec.getTypeDescription();
            }
            else
            {
                log += colSpec.getIndex() + ": " + colSpec.getName();
            }
        }
        log += ") = ";
        if (value != null)
            if (value.getValue() != null)
            {
                if (value.getValue() instanceof SerialBlob)
                {
                    try
                    {
                        SerialBlob blob = (SerialBlob) value.getValue();
                        String blobString = new String(blob.getBytes(1,
                                maxBlobPrintLength));
                        log += blobString;
                        if (blob.length() > maxBlobPrintLength)
                            log += "<...>";
                    }
                    catch (Exception e)
                    {
                        log += value.getValue().toString();
                    }
                }
                else if (value.getValue() instanceof byte[] && charset != null)
                {
                    try
                    {
                        byte[] byteValue = (byte[]) value.getValue();
                        log += new String(byteValue, charset);
                        if (hex)
                        {
                            StringBuffer hexValue = new StringBuffer();
                            hexValue.append(" (x");
                            for (byte b : byteValue)
                            {
                                hexValue.append(String.format("%2.2x", b));
                            }
                            hexValue.append(")");
                            log += hexValue.toString();
                        }
                    }
                    catch (UnsupportedEncodingException e)
                    {
                        logger.warn("Unsupported encoding " + charset, e);
                    }
                }
                else if (colSpec.getType() == Types.DATE
                        && value.getValue() instanceof Timestamp)
                {
                    Timestamp ts = (Timestamp) value.getValue();
                    StringBuffer date = new StringBuffer(
                            dateFormatter.format(ts));
                    if (ts.getNanos() > 0)
                    {
                        date.append(".");
                        date.append(String.format("%09d%n", ts.getNanos()));
                    }
                    log += date.toString();
                }
                else
                    log += value.getValue().toString();
            }
            else
                log += "NULL";
        else
            log += "NULL";
        return log;
    }

    /**
     * Format and print schema name if it differs from the last printed schema
     * name.
     * 
     * @param sb StringBuilder on which to print
     * @param schema Schema name to print.
     * @param lastSchema Last printed schema name.
     * @param pureSQL If true, use pure SQL output. Formatted form otherwise.
     * @return Schema name as given in the schema parameter.
     */
    private static String printSchema(StringBuilder sb, String schema,
            String lastSchema, boolean pureSQL)
    {
        if (schema != null
                && (lastSchema == null || (lastSchema != null && lastSchema
                        .compareTo(schema) != 0)))
        {
            if (pureSQL) // Does not handle Oracle and `USE`...
            {
                // Print only meaningful statement.
                if (schema.length() > 0)
                    println(sb, "USE " + schema + ";");
            }
            else
                println(sb, "- SCHEMA = " + schema);
        }
        return schema;
    }

    /**
     * List THL events within the given range.
     * 
     * @param low Sequence number specifying the beginning of the range. Leave
     *            null to start from the very beginning of the table.
     * @param high Sequence number specifying the end of the range. Leave null
     *            to end at the very end of the table.
     * @param by
     * @param pureSQL Output events in the pure SQL form if true, formatted form
     *            otherwise.
     * @param charset character set name to be used to decode byte arrays in row
     *            replication
     * @param hex If true print hex representation of strings
     * @throws ReplicatorException
     * @throws InterruptedException
     */
    public void listEvents(Long low, Long high, Long by, boolean pureSQL,
            boolean headersOnly, boolean json, String charset, boolean hex,
            boolean specs) throws ReplicatorException, InterruptedException
    {
        // Make sure range is OK.
        if (low != null && high != null && low > high)
        {
            logger.error("Invalid range:  -low must be equal to or lower than -high value");
            fail();
        }

        // Initialize log.
        prepare(true);

        // Adjust for missing ranges.
        long lowIndex;
        if (low == null)
            lowIndex = diskLog.getMinSeqno();
        else
            lowIndex = low;

        long highIndex;
        if (high == null)
            highIndex = diskLog.getMaxSeqno();
        else
            highIndex = high;

        // Find low value.
        LogConnection conn = diskLog.connect(true);
        if (!conn.seek(lowIndex))
        {
            if (lowIndex == diskLog.getMinSeqno())
            {
                logger.info("No events found; log is empty");
                conn.release();
                return;
            }
            else
            {
                logger.error("Unable to find sequence number: " + lowIndex);
                fail();
            }
        }

        if (json)
            println("[");

        if (headersOnly)
        {
            // Add a read filter that will accept only events that are in this
            // partition. We use an inner class so we can access the partitioner
            // and task id easily.
            LogEventReadFilter filter = new LogEventReadFilter()
            {
                public boolean accept(LogEventReplReader reader)
                        throws ReplicatorException
                {
                    return false;
                }
            };
            conn.setReadFilter(filter);
        }

        // Iterate until we run out of sequence numbers.
        THLEvent thlEvent = null;
        int found = 0;
        while (lowIndex <= highIndex && (thlEvent = conn.next(false)) != null)
        {
            // Make sure event is within range.
            lowIndex = thlEvent.getSeqno();
            if (lowIndex > highIndex)
                break;

            // Print it.
            found++;
            if (!pureSQL)
            {
                StringBuilder sb = new StringBuilder();
                if (json && found > 1)
                    sb.append(",\n");

                // Choose appropriate format for the header.
                int format = 0;
                if (json)
                    format = 1;
                else if (headersOnly && !json)
                    format = 2;
                printHeader(sb, thlEvent, format);

                print(sb.toString());
            }
            if (!headersOnly)
            {
                ReplEvent replEvent = thlEvent.getReplEvent();
                if (replEvent instanceof ReplDBMSEvent)
                {
                    ReplDBMSEvent event = (ReplDBMSEvent) replEvent;
                    StringBuilder sb = new StringBuilder();
                    printReplDBMSEvent(sb, event, pureSQL, charset, hex, specs);
                    print(sb.toString());
                }
                else
                {
                    println("# " + replEvent.getClass().getName()
                            + ": not supported.");
                }
            }
        }

        if (json)
            println("\n]");

        // Corner case and kludge: if lowIndex is 0 and we find no events,
        // log was empty.
        if (!json && found == 0)
        {
            if (lowIndex == 0)
            {
                logger.info("No events found; log is empty");
                conn.release();
                return;
            }
            else
            {
                logger.error("Unable to find sequence number: " + lowIndex);
                fail();
            }

        }

        // Disconnect.
        conn.release();
        release();
    }

    /**
     * Prints a formatted header into StringBuilder for the given THLEvent.
     * 
     * @param stringBuilder StringBuilder object to append formatted contents
     *            to.
     * @param thlEvent THLEvent to print out.
     * @param format 0 - human readable, 1 - JSON, 2 - tab-delimited.
     * @see #printHeader(StringBuilder, ReplDBMSEvent)
     */
    public static void printHeader(StringBuilder stringBuilder,
            THLEvent thlEvent, int format)
    {
        if (format == 1)
        {
            stringBuilder.append("{\n");
            stringBuilder.append("\"seqno\": ");
            stringBuilder.append(thlEvent.getSeqno());
            stringBuilder.append(",\n");
            stringBuilder.append("\"epoch\": ");
            stringBuilder.append(thlEvent.getEpochNumber());
            stringBuilder.append(",\n");
            stringBuilder.append("\"frag\": ");
            stringBuilder.append(thlEvent.getFragno());
            stringBuilder.append(",\n");
            stringBuilder.append("\"lastFrag\": ");
            stringBuilder.append(thlEvent.getLastFrag());
            stringBuilder.append(",\n");
            stringBuilder.append("\"time\": \"");
            stringBuilder.append(thlEvent.getSourceTstamp());
            stringBuilder.append("\",\n");
            stringBuilder.append("\"eventId\": \"");
            stringBuilder.append(thlEvent.getEventId());
            stringBuilder.append("\",\n");
            stringBuilder.append("\"sourceId\": \"");
            stringBuilder.append(thlEvent.getSourceId());
            stringBuilder.append("\",\n");
            stringBuilder.append("\"comments\": \"");
            if (thlEvent.getComment() != null
                    && thlEvent.getComment().length() > 0)
                stringBuilder.append(thlEvent.getComment());
            stringBuilder.append("\"\n");
            stringBuilder.append("}");
        }
        else if (format == 2)
        {
            stringBuilder.append(thlEvent.getSeqno());
            stringBuilder.append("\t");
            stringBuilder.append(thlEvent.getEpochNumber());
            stringBuilder.append("\t");
            stringBuilder.append(thlEvent.getFragno());
            stringBuilder.append("\t");
            stringBuilder.append(thlEvent.getLastFrag());
            stringBuilder.append("\t");
            stringBuilder.append(thlEvent.getSourceTstamp());
            stringBuilder.append("\t");
            stringBuilder.append(thlEvent.getEventId());
            stringBuilder.append("\t");
            stringBuilder.append(thlEvent.getSourceId());
            stringBuilder.append("\t");
            if (thlEvent.getComment() != null
                    && thlEvent.getComment().length() > 0)
                stringBuilder.append(thlEvent.getComment());
            stringBuilder.append("\t\n");
        }
        else
        {
            println(stringBuilder,
                    "SEQ# = " + thlEvent.getSeqno() + " / FRAG# = "
                            + thlEvent.getFragno()
                            + (thlEvent.getLastFrag() ? (" (last frag)") : ""));
            println(stringBuilder, "- TIME = " + thlEvent.getSourceTstamp());
            println(stringBuilder, "- EPOCH# = " + thlEvent.getEpochNumber());
            println(stringBuilder, "- EVENTID = " + thlEvent.getEventId());
            println(stringBuilder, "- SOURCEID = " + thlEvent.getSourceId());
            if (thlEvent.getComment() != null
                    && thlEvent.getComment().length() > 0)
                println(stringBuilder, "- COMMENTS = " + thlEvent.getComment());
        }
    }

    /**
     * Prints a formatted header into StringBuilder for the given ReplDBMSEvent.
     * Note that ReplDBMSEvent doesn't contain eventId, thus it is not printed.
     * If you need to print eventId, use
     * {@link #printHeader(StringBuilder, THLEvent, int)}
     * 
     * @param stringBuilder StringBuilder object to append formatted contents
     *            to.
     * @param event ReplDBMSEvent to print out.
     * @see #printHeader(StringBuilder, THLEvent, int)
     */
    public static void printHeader(StringBuilder stringBuilder,
            ReplDBMSEvent event)
    {
        println(stringBuilder, "SEQ# = " + event.getSeqno());
        println(stringBuilder, "- TIME = "
                + event.getDBMSEvent().getSourceTstamp());
        println(stringBuilder, "- SOURCEID = " + event.getSourceId());
    }

    /**
     * Formats and prints ReplDBMSEvent into a given stringBuilder.
     * 
     * @param stringBuilder StringBuilder object to append formatted contents
     *            to.
     * @param event ReplDBMSEvent to print out.
     * @param pureSQL If true, use pure SQL output. Formatted form otherwise.
     * @param charset character set name to be used to decode byte arrays in row
     *            replication
     * @param hex If true print hex representation of strings
     * @param specs Provide column specifications.
     */
    public static void printReplDBMSEvent(StringBuilder stringBuilder,
            ReplDBMSEvent event, boolean pureSQL, String charset, boolean hex,
            boolean specs)
    {
        if (event == null)
        {
            println(stringBuilder, "- TYPE = null");
            return;
        }

        // Hide meta data of the event under SQL mode.
        if (!pureSQL)
        {
            // Add metadata before handling specific types of ReplDBMSEvents.
            List<ReplOption> metadata = event.getDBMSEvent().getMetadata();
            StringBuilder sb = new StringBuilder();
            sb.append("[");
            for (ReplOption option : metadata)
            {
                if (sb.length() > 1)
                    sb.append(";");
                String value = option.getOptionValue();
                sb.append(option.getOptionName())
                        .append((value != null && value.length() > 0 ? "="
                                + value : ""));
            }
            sb.append("]");
            println(stringBuilder, "- METADATA = " + sb.toString());
            println(stringBuilder, "- TYPE = " + event.getClass().getName());
        }

        if (event.getDBMSEvent() instanceof DBMSEmptyEvent)
        {
            println(stringBuilder, "## Empty event ##");
            return;
        }

        if (event instanceof ReplDBMSFilteredEvent)
        {
            println(stringBuilder, "## Filtered events ##");
            println(stringBuilder, "From Seqno# " + event.getSeqno()
                    + " / Fragno# " + event.getFragno());
            println(stringBuilder,
                    "To Seqno# "
                            + ((ReplDBMSFilteredEvent) event).getSeqnoEnd()
                            + " / Fragno# "
                            + ((ReplDBMSFilteredEvent) event).getFragnoEnd());
            return;
        }

        ArrayList<DBMSData> data = event.getData();
        String lastSchema = null;
        for (int i = 0; i < data.size(); i++)
        {
            DBMSData dataElem = data.get(i);
            if (dataElem instanceof RowChangeData)
            {
                RowChangeData rowChange = (RowChangeData) dataElem;
                lastSchema = printRowChangeData(stringBuilder, rowChange,
                        lastSchema, pureSQL, i, charset, hex, specs,
                        event.getSeqno());
            }
            else if (dataElem instanceof StatementData)
            {
                StatementData statement = (StatementData) dataElem;
                lastSchema = printStatementData(stringBuilder, statement,
                        lastSchema, pureSQL, i);
            }
            else if (dataElem instanceof RowIdData)
            {
                RowIdData rowid = (RowIdData) dataElem;
                lastSchema = printRowIdData(stringBuilder, rowid, lastSchema,
                        pureSQL, i);
            }
            else if (dataElem instanceof LoadDataFileFragment)
            {
                LoadDataFileFragment loadDataFileFragment = (LoadDataFileFragment) dataElem;
                String schema = loadDataFileFragment.getDefaultSchema();
                printSchema(stringBuilder, schema, lastSchema, pureSQL);
                lastSchema = schema;
                println("- DATA FILE #" + loadDataFileFragment.getFileID()
                        + " / size : " + loadDataFileFragment.getData().length);
            }
            else
                println(stringBuilder, "# " + dataElem.getClass().getName()
                        + ": not supported.");
        }
    }

    /**
     * Prints RowIdData event.
     * 
     * @param rowid RowidIdData object to format and print.
     * @param lastSchema Last printed schema name.
     * @param pureSQL If true, use pure SQL output. Formatted form otherwise.
     * @param sqlIndex Which SQL event is it.
     * @return Schema name.
     */
    private static String printRowIdData(StringBuilder stringBuilder,
            RowIdData rowid, String lastSchema, boolean pureSQL, int sqlIndex)
    {
        // Output actual DML/DDL statement.
        String type;
        switch (rowid.getType())
        {
            case RowIdData.LAST_INSERT_ID :
                type = "LAST_INSERT_ID";
                break;
            case RowIdData.INSERT_ID :
                type = "INSERT_ID";
                break;
            default :
                type = "INSERT_ID";
                break;
        }
        if (pureSQL)
        {
            println(stringBuilder, "SET " + type + " = " + rowid.getRowId());
        }
        else
        {
            println(stringBuilder, "- SQL(" + sqlIndex + ") = " + "SET " + type
                    + " = " + rowid.getRowId());
        }
        return lastSchema;
    }

    /**
     * Prints StatementData event.
     * 
     * @param statement StatementData object to format and print.
     * @param lastSchema Last printed schema name.
     * @param pureSQL If true, use pure SQL output. Formatted form otherwise.
     * @param sqlIndex Which SQL event is it.
     * @return Schema name.
     */
    private static String printStatementData(StringBuilder stringBuilder,
            StatementData statement, String lastSchema, boolean pureSQL,
            int sqlIndex)
    {
        // Output schema name if needed.
        String schema = statement.getDefaultSchema();
        printOptions(stringBuilder, statement.getOptions(), pureSQL);
        printSchema(stringBuilder, schema, lastSchema, pureSQL);
        String query = statement.getQuery();

        if (query == null)
            query = new String(statement.getQueryAsBytes());

        // Output actual DML/DDL statement.
        if (pureSQL)
        {
            println(stringBuilder, formatSQL(query));
        }
        else
        {
            println(stringBuilder, "- SQL(" + sqlIndex + ") = " + query);
        }
        return schema;
    }

    private static void printOptions(StringBuilder stringBuilder,
            List<ReplOption> optionList, boolean pureSQL)
    {
        if (optionList != null && !pureSQL)
            println(stringBuilder, "- OPTIONS = " + optionList);
    }

    /**
     * Prints RowChangeData event.
     * 
     * @param rowChange RowChangeData object to format and print.
     * @param lastSchema Last printed schema name.
     * @param pureSQL If true, use pure SQL output. Formatted form otherwise.
     * @param sqlIndex Which SQL event is it.
     * @param charset character set name to be used to decode byte arrays in row
     *            replication
     * @param specs Provide column specifications.
     * @return Last printed schema name.
     */
    private static String printRowChangeData(StringBuilder stringBuilder,
            RowChangeData rowChange, String lastSchema, boolean pureSQL,
            int sqlIndex, String charset, boolean hex, boolean specs, long seqno)
    {
        // This will pick up the default time zone or the time zone from
        // -timezone option. 
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        printOptions(stringBuilder, rowChange.getOptions(), pureSQL);
        if (!pureSQL)
            println(stringBuilder, "- SQL(" + sqlIndex + ") =");
        String schema = null;
        for (OneRowChange oneRowChange : rowChange.getRowChanges())
        {
            // Output row change details.
            if (pureSQL)
            {
                stringBuilder.append("/* SEQ# = ");
                stringBuilder.append(seqno);
                stringBuilder
                        .append(" - SQL rendering of row change events is not supported */");
                println(stringBuilder, "");
            }
            else
            {
                println(stringBuilder, " - ACTION = "
                        + oneRowChange.getAction().toString());
                println(stringBuilder,
                        " - SCHEMA = " + oneRowChange.getSchemaName());
                println(stringBuilder,
                        " - TABLE = " + oneRowChange.getTableName());
                ArrayList<OneRowChange.ColumnSpec> keys = oneRowChange
                        .getKeySpec();
                ArrayList<OneRowChange.ColumnSpec> columns = oneRowChange
                        .getColumnSpec();
                ArrayList<ArrayList<OneRowChange.ColumnVal>> keyValues = oneRowChange
                        .getKeyValues();
                ArrayList<ArrayList<OneRowChange.ColumnVal>> columnValues = oneRowChange
                        .getColumnValues();
                for (int row = 0; row < columnValues.size()
                        || row < keyValues.size(); row++)
                {
                    println(stringBuilder, " - ROW# = " + row);
                    // Print column values.
                    if (columnValues.size() > 0)
                    {
                        for (int c = 0; c < columns.size(); c++)
                        {
                            OneRowChange.ColumnSpec colSpec = columns.get(c);
                            ArrayList<OneRowChange.ColumnVal> values = columnValues
                                    .get(row);
                            OneRowChange.ColumnVal value = values.get(c);
                            println(stringBuilder,
                                    formatColumn(colSpec, value, "COL",
                                            charset, hex, specs, formatter));
                        }
                    }
                    else if (columns.size() > 0)
                    {
                        // No values entered, but a list of column specs was
                        // provided (probably by a filter)
                        StringBuffer buf = new StringBuffer(
                                "Column specs only found : ");
                        for (int c = 0; c < columns.size(); c++)
                        {
                            OneRowChange.ColumnSpec colSpec = columns.get(c);
                            if (c > 0)
                                buf.append(", ");
                            buf.append(colSpec.getName());
                        }
                        println(stringBuilder, buf.toString());
                    }

                    // Print key values.
                    for (int k = 0; k < keys.size(); k++)
                    {
                        if (keyValues.size() > 0)
                        {
                            OneRowChange.ColumnSpec colSpec = keys.get(k);
                            ArrayList<OneRowChange.ColumnVal> values = null;
                            if (row < keyValues.size())
                                values = keyValues.get(row);
                            OneRowChange.ColumnVal value = null;
                            if (values != null && k < values.size())
                                value = values.get(k);
                            println(stringBuilder,
                                    formatColumn(colSpec, value, "KEY",
                                            charset, hex, specs, formatter));
                        }
                    }
                }
            }
        }
        return schema;
    }

    /**
     * Formats the given SQL statement into an ANSI compatible form.
     * 
     * @param sql
     * @return Formatted SQL statement.
     */
    private static String formatSQL(String sql)
    {
        if (!sql.endsWith(";"))
            sql += ";";
        return sql;
    }

    /**
     * Purge THL events in the given seqno interval.
     * 
     * @param low Sequence number specifying the beginning of the range. Leave
     *            null to start from the very beginning of the table.
     * @param high Sequence number specifying the end of the range. Leave null
     *            to end at the very end of the table.
     * @throws ReplicatorException
     */
    public void purgeEvents(Long low, Long high) throws ReplicatorException
    {
        LogConnection conn = diskLog.connect(false);
        try
        {
            conn.delete(low, high);
            conn.release();
        }
        catch (InterruptedException e)
        {
            logger.warn("Delete operation was interrupted!");
        }
        logger.info("Transactions deleted");
    }

    /**
     * Main method to run utility.
     * 
     * @param argv optional command string
     */
    public static void main(String argv[])
    {
        try
        {
            // Command line parameters and options.
            String configFile = null;
            String service = null;
            String command = null;
            Long seqno = null;
            Long low = null;
            Long high = null;
            Long by = null;
            Boolean pureSQL = null;
            Boolean specs = null;
            Boolean headersOnly = null;
            Boolean json = null;
            Boolean yesToQuestions = null;
            String fileName = null;
            String charsetName = null;
            boolean hex = false;
            boolean doChecksum = true;
            TimeZone timezone = TimeZone.getTimeZone("UTC");

            // Parse command line arguments.
            ArgvIterator argvIterator = new ArgvIterator(argv);
            String curArg = null;
            while (argvIterator.hasNext())
            {
                curArg = argvIterator.next();
                if ("-conf".equals(curArg))
                    configFile = argvIterator.next();
                else if ("-service".equals(curArg))
                    service = argvIterator.next();
                else if ("-seqno".equals(curArg))
                    seqno = Long.parseLong(argvIterator.next());
                else if ("-low".equals(curArg))
                    low = Long.parseLong(argvIterator.next());
                else if ("-high".equals(curArg))
                    high = Long.parseLong(argvIterator.next());
                else if ("-by".equals(curArg))
                    by = Long.parseLong(argvIterator.next());
                else if ("-sql".equals(curArg))
                    pureSQL = true;
                else if ("-specs".equals(curArg))
                    specs = true;
                else if ("-headers".equals(curArg))
                    headersOnly = true;
                else if ("-json".equals(curArg))
                    json = true;
                else if ("-y".equals(curArg))
                    yesToQuestions = true;
                else if ("-charset".equals(curArg))
                {
                    charsetName = argvIterator.next();
                    if (!Charset.isSupported(charsetName))
                    {
                        println("Unsupported charset " + charsetName
                                + ". Using default.");
                        charsetName = null;
                    }
                }
                else if ("-timezone".equals(curArg))
                {
                    String timezoneName = argvIterator.next();
                    if (timezoneName == null)
                    {
                        println("Time zone name is missing");
                        fail();
                    }
                    TimeZone tz = TimeZone.getTimeZone(timezoneName);
                    if (!timezoneName.equals(tz.getID()))
                    {
                        // If the name and time zone ID do not match, Java has
                        // returned another value, perhaps GMT. Let the user
                        // know.
                        println("WARNING: Your time zone name does not match Java naming; using time zone: "
                                + tz.getID());
                    }
                    // Name is good enough, so let's use it.
                    timezone = tz;
                }
                else if ("-hex".equals(curArg))
                {
                    hex = true;
                }
                else if ("-file".equals(curArg))
                {
                    fileName = argvIterator.next();
                }
                else if ("-no-checksum".equals(curArg))
                {
                    doChecksum = false;
                }
                else if (curArg.startsWith("-"))
                    fatal("Unrecognized option: " + curArg, null);
                else
                    command = curArg;
            }

            // Construct actual THLManagerCtrl and call methods based on a
            // parsed user input.
            if (command == null)
            {
                println("Command is missing!");
                printHelp();
                fail();
            }
            else if (THLCommands.HELP.equals(command))
            {
                printHelp();
                succeed();
            }
            else if (json != null && json == true && headersOnly == null)
            {
                println("Currently JSON output is supported only with -headers flag");
                fail();
            }

            // Set the default time zone in the VM to ensure proper printing of
            // Timestamp values.
            TimeZone.setDefault(timezone);

            // Use default configuration file in case user didn't specify one.
            if (configFile == null)
            {
                if (service == null)
                {
                    configFile = lookForConfigFile();
                    if (configFile == null)
                    {
                        fatal("You must specify either a config file or a service name (-conf or -service)",
                                null);
                    }
                }
                else
                {
                    ReplicatorRuntimeConf runtimeConf = ReplicatorRuntimeConf
                            .getConfiguration(service);
                    configFile = runtimeConf.getReplicatorProperties()
                            .getAbsolutePath();
                }
            }

            if (THLCommands.INFO.equals(command))
            {
                THLManagerCtrl thlManager = new THLManagerCtrl(configFile,
                        doChecksum);
                thlManager.prepare(true);

                InfoHolder info = thlManager.getInfo();
                println("log directory = " + info.getLogDir());
                println("log files = " + info.getLogFiles());
                printLogsSize(info.getLogsSize());
                println("min seq# = " + info.getMinSeqNo());
                println("max seq# = " + info.getMaxSeqNo());
                println("events = " + info.getEventCount());
                printTHLFileInfo("oldest file", info.getOldestFile());
                printTHLFileInfo("newest file", info.getNewestFile());

                thlManager.release();
            }
            else if (THLCommands.LIST.equals(command))
            {
                THLManagerCtrl thlManager = new THLManagerCtrl(configFile,
                        doChecksum);

                if (fileName != null)
                {
                    thlManager.listEvents(fileName, getBoolOrFalse(pureSQL),
                            getBoolOrFalse(headersOnly), getBoolOrFalse(json),
                            charsetName, hex, getBoolOrFalse(specs));
                }
                else if (seqno == null)
                    thlManager.listEvents(low, high, by,
                            getBoolOrFalse(pureSQL),
                            getBoolOrFalse(headersOnly), getBoolOrFalse(json),
                            charsetName, hex, getBoolOrFalse(specs));
                else
                    thlManager.listEvents(seqno, seqno, by,
                            getBoolOrFalse(pureSQL),
                            getBoolOrFalse(headersOnly), getBoolOrFalse(json),
                            charsetName, hex, getBoolOrFalse(specs));
            }
            else if (THLCommands.PURGE.equals(command))
            {
                THLManagerCtrl thlManager = new THLManagerCtrl(configFile,
                        doChecksum);
                thlManager.prepare(false);

                // Ensure we have a writable log.
                if (!thlManager.diskLog.isWritable())
                {
                    println("Fatal error:  The disk log is not writable and cannot be purged.");
                    println("If a replication service is currently running, please set the service");
                    println("offline first using 'trepctl -service svc offline'");
                    fail();
                }

                // Ensure user is OK with this change.
                println("WARNING: The purge command will break replication if you delete all events or delete events that have not reached all slaves.");
                boolean confirmed = true;
                if (!getBoolOrFalse(yesToQuestions))
                {
                    confirmed = false;
                    println("Are you sure you wish to delete these events [y/N]?");
                    if (readYes())
                        confirmed = true;
                    else
                        println("Nothing done.");
                }
                if (confirmed)
                {
                    String log = "Deleting events where";
                    if (seqno == null)
                    {
                        if (low != null)
                            log += " SEQ# >= " + low;
                        if (low != null && high != null)
                            log += " and";
                        if (high != null)
                            log += " SEQ# <=" + high;
                        println(log);
                        thlManager.purgeEvents(low, high);
                    }
                    else
                    {
                        log += " SEQ# = " + seqno;
                        println(log);
                        thlManager.purgeEvents(seqno, seqno);
                    }
                }

                thlManager.release();
            }
            else if (THLCommands.SKIP.equals(command))
            {
                println("SKIP operation is no longer supported");
                println("Please use 'trepctl online -skip-seqno N' to skip over a transaction");
                return;
            }
            else if (command.equals("index"))
            {
                THLManagerCtrl thlManager = new THLManagerCtrl(configFile,
                        doChecksum);
                thlManager.prepare(true);

                thlManager.printIndex();

                thlManager.release();
            }
            else
            {
                println("Unknown command: '" + command + "'");
                printHelp();
                fail();
            }
        }
        catch (Throwable t)
        {
            fatal("Fatal error: " + t.getMessage(), t);
        }
    }

    /**
     * Converts size in bytes to megabytes.
     */
    private static float bytesToMB(long bytes)
    {
        float sizeInMB = (float) bytes / 1024f / 1024f;
        return sizeInMB;
    }

    private static void printLogsSize(long logsSize)
    {
        float sizeInMB = bytesToMB(logsSize);
        println(String.format("logs size = %.2f MB", sizeInMB));
    }

    private static void printTHLFileInfo(String message, File thlFile)
    {
        if (thlFile != null)
        {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            float sizeInMB = bytesToMB(thlFile.length());

            println(String.format("%s = %s (%.2f MB, %s)", message,
                    thlFile.getName(), sizeInMB,
                    sdf.format(thlFile.lastModified())));
        }
        else
        {
            println(message + " = ?");
        }
    }

    // Return the service configuration file if there is one
    // and only one file that matches the static-svcname.properties pattern.
    private static String lookForConfigFile()
    {
        File configDir = ReplicatorRuntimeConf.locateReplicatorConfDir();
        FilenameFilter propFileFilter = new FilenameFilter()
        {
            public boolean accept(File fdir, String fname)
            {
                if (fname.startsWith("static-")
                        && fname.endsWith(".properties"))
                    return true;
                else
                    return false;
            }
        };
        File[] propertyFiles = configDir.listFiles(propFileFilter);
        if (propertyFiles.length == 1)
            return propertyFiles[0].getAbsolutePath();
        else
            return null;
    }

    /**
     * List all events in a particular log file.
     * 
     * @param fileName Simple name of the file
     * @param pureSQL Whether to print SQL
     * @param headersOnly Print only headers
     * @param charset Charset for translation, e.g., utf8
     * @param hex If true print hex representation of strings
     */
    private void listEvents(String fileName, boolean pureSQL,
            boolean headersOnly, boolean json, String charset, boolean hex,
            boolean specs) throws ReplicatorException, IOException,
            InterruptedException
    {
        // Ensure we have a simple file name. Log APIs will not accept an
        // absolute path.
        if (!fileName.startsWith("thl.data."))
        {
            fatal("File name must be a THL log file name like thl.data.0000000001",
                    null);
        }

        // Connect to the log.
        prepare(true);
        LogConnection conn = diskLog.connect(true);
        if (!conn.seek(fileName))
        {
            logger.error("File not found: " + fileName);
            fail();
        }

        if (json)
            println("[");

        THLEvent thlEvent = null;
        boolean first = true;
        while ((thlEvent = conn.next(false)) != null)
        {
            if (!pureSQL)
            {
                StringBuilder sb = new StringBuilder();
                if (json && !first)
                    sb.append(",\n");

                // Choose appropriate format for the header.
                int format = 0;
                if (json)
                    format = 1;
                else if (headersOnly && !json)
                    format = 2;
                printHeader(sb, thlEvent, format);

                print(sb.toString());
            }
            if (!headersOnly)
            {
                ReplEvent replEvent = thlEvent.getReplEvent();
                if (replEvent instanceof ReplDBMSEvent)
                {
                    ReplDBMSEvent event = (ReplDBMSEvent) replEvent;
                    StringBuilder sb = new StringBuilder();
                    printReplDBMSEvent(sb, event, pureSQL, charset, hex, specs);
                    print(sb.toString());
                }
                else
                {
                    println("# " + replEvent.getClass().getName()
                            + ": not supported.");
                }
            }
            first = false;
        }

        if (json)
            println("\n]");

        // Disconnect from log.
        release();
    }

    private void printIndex()
    {
        println(diskLog.getIndex());
    }

    protected static void printHelp()
    {
        println("Tungsten Replicator THL Manager");
        println("Syntax: thl [global-options] command [command-options]");
        println("Global options:");
        println("  -conf path    - Path to a static-<svc>.properties file");
        println("  -service name - Name of a replication service");
        println("Commands and corresponding options:");
        println("  list [-low #] [-high #] [-by #] - Dump THL events from low to high #");
        println("  list [-seqno #]                 - Dump the exact event by a given #");
        println("  list [-file <file_name>]        - Dump the content of the given log file");
        println("       [-charset <charset>] [-hex]  Character set used for decoding row data");
        println("       [-sql]                       Representative (no metadata!) SQL mode");
        println("       [-specs]                     Add detailed column specifications");
        println("       [-headers]                   Print headers only");
        println("       [-json]                      Output in machine-parsable JSON format");
        println("       [-no-checksum]               Suppress checksums");
        println("       [-timezone timezone]         Time used zone for time-related data");
        println("  index [-no-checksum]            - Display index of log files");
        println("  purge [-low #] [-high #]        - Delete THL files identified by the given range");
        println("        [-no-checksum] [-y]         Use -y to suppress prompt");
        println("  info [-no-checksum]             - Display minimum, maximum sequence number");
        println("                                    and other summary information about log");
        println("  help                            - Print this help display");
    }

    /**
     * Appends a message to a given stringBuilder, adds a newline character at
     * the end.
     * 
     * @param msg String to print.
     * @param stringBuilder StringBuilder object to add a message to.
     */
    private static void println(StringBuilder stringBuilder, String msg)
    {
        stringBuilder.append(msg);
        stringBuilder.append("\n");
    }

    /**
     * Print a message to stdout with trailing new line character.
     * 
     * @param msg
     */
    protected static void println(String msg)
    {
        System.out.println(msg);
    }

    /**
     * Print a message to stdout without trailing new line character.
     * 
     * @param msg
     */
    protected static void print(String msg)
    {
        System.out.print(msg);
    }

    /**
     * Abort following a fatal error.
     * 
     * @param msg
     * @param t
     */
    protected static void fatal(String msg, Throwable t)
    {
        System.out.println(msg);
        if (t != null)
            t.printStackTrace();
        fail();
    }

    /**
     * Exit with a process failure code.
     */
    protected static void fail()
    {
        System.exit(1);
    }

    /**
     * Exit with a process success code.
     */
    protected static void succeed()
    {
        System.exit(0);
    }

    /**
     * Reads a character from stdin, blocks until it is not received.
     * 
     * @return true if use pressed `y`, false otherwise.
     */
    protected static boolean readYes() throws IOException
    {
        return (System.in.read() == 'y');
    }

    /**
     * Returns a value of a given Boolean object or false if the object is null.
     * 
     * @param bool Boolean object to check and return.
     * @return the value of a given Boolean object or false if the object is
     *         null.
     */
    protected static boolean getBoolOrFalse(Boolean bool)
    {
        if (bool != null)
            return bool;
        else
            return false;
    }

    /**
     * This class holds elements returned by the info query.
     * 
     * @see com.continuent.tungsten.replicator.thl.THLManagerCtrl#getInfo()
     */
    public static class InfoHolder
    {
        private String logDir                 = "";
        private long   minSeqNo               = -1;
        private long   maxSeqNo               = -1;
        private long   eventCount             = -1;
        private long   highestReplicatedEvent = -1;
        private int    logFiles               = -1;
        private File   oldestFile             = null;
        private File   newestFile             = null;
        private long   logsSize               = -1;

        public InfoHolder(String logDir, long minSeqNo, long maxSeqNo,
                long eventCount, long highestReplicatedEvent, int logFiles,
                File oldestFile, File newestFile, long logsSize)
        {
            this.logDir = logDir;
            this.minSeqNo = minSeqNo;
            this.maxSeqNo = maxSeqNo;
            this.eventCount = eventCount;
            this.highestReplicatedEvent = highestReplicatedEvent;
            this.logFiles = logFiles;
            this.oldestFile = oldestFile;
            this.newestFile = newestFile;
            this.logsSize = logsSize;
        }

        public String getLogDir()
        {
            return logDir;
        }

        public long getMinSeqNo()
        {
            return minSeqNo;
        }

        public long getMaxSeqNo()
        {
            return maxSeqNo;
        }

        public long getEventCount()
        {
            return eventCount;
        }

        public long getHighestReplicatedEvent()
        {
            return highestReplicatedEvent;
        }

        public int getLogFiles()
        {
            return logFiles;
        }

        public File getOldestFile()
        {
            return oldestFile;
        }

        public File getNewestFile()
        {
            return newestFile;
        }

        public long getLogsSize()
        {
            return logsSize;
        }
    }
}
