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
 * Initial developer(s): Teemu Ollakka
 * Contributor(s):Stephane Giron, Robert Hodges
 */

package com.continuent.tungsten.replicator.applier;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.TimeZone;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Table;
import com.continuent.tungsten.replicator.datatypes.MySQLUnsignedNumeric;
import com.continuent.tungsten.replicator.datatypes.Numeric;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.RowIdData;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplOptionParams;
import com.continuent.tungsten.replicator.extractor.mysql.SerialBlob;

/**
 * Stub applier class that automatically constructs url from MySQL-specific
 * properties like host, port, and urlOptions.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class MySQLApplier extends JdbcApplier
{
    private static Logger            logger                 = Logger.getLogger(MySQLApplier.class);
    protected String                 host                   = "localhost";
    protected int                    port                   = 3306;
    protected String                 urlOptions             = null;

    /**
     * If true the replicator is operating time zone unaware compatibility mode.
     * In this mode we set the JVM time zone to the host time zone and set the
     * MySQL session time zone to match the MySQL global time zone. This mode
     * requires extra clean-up at release time to ensure the JVM time zone is
     * set back correctly.
     */
    protected boolean                nonTzAwareMode         = false;

    /**
     * If true this applier will switch the replicator to time zone unaware
     * operation to apply events from a time zone unaware source. This is to
     * enable seamless upgrade of logs from older replicators.
     */
    protected boolean                supportNonTzAwareMode  = true;

    // Formatters for MySQL DATE, TIME, and DATETIME values.
    /**
     * Format DATE value according to MySQL expectations.
     */
    protected final SimpleDateFormat dateFormatter          = new SimpleDateFormat(
                                                                    "yyyy-MM-dd");
    /**
     * Format TIME value according to MySQL expectations.
     */
    protected final SimpleDateFormat timeFormatter          = new SimpleDateFormat(
                                                                    "HH:mm:ss");
    /**
     * Format MySQL DATETIME value according to MySQL expectations. The DATETIME
     * data type cannot change time zones or upgrade breaks.
     */
    protected final SimpleDateFormat mysqlDatetimeFormatter = new SimpleDateFormat(
                                                                    "yyyy-MM-dd HH:mm:ss");

    /**
     * Host name or IP address.
     */
    public void setHost(String host)
    {
        this.host = host;
    }

    /**
     * TCP/IP port number, a positive integer.
     */
    public void setPort(String portAsString)
    {
        this.port = Integer.parseInt(portAsString);
    }

    /**
     * JDBC URL options with a leading ?.
     */
    public void setUrlOptions(String urlOptions)
    {
        this.urlOptions = urlOptions;
    }

    /**
     * If set to true, time stamp-aware unaware events will be processed with
     * old replicator settings. If false, settings will not be altered.
     */
    public void setSupportNonTzAwareMode(boolean supportNonTzAwareMode)
    {
        this.supportNonTzAwareMode = supportNonTzAwareMode;
    }

    /**
     * Check to see if we have change in the time zone awareness of the event
     * and adjust time zone processing accordingly.
     */
    protected void checkEventCompatibility(ReplDBMSHeader header,
            DBMSEvent event) throws ReplicatorException
    {
        // If we don't support compatible operation with non-TZ aware events
        // there is nothing to do. Also, events without any metadata are empty
        // and are ignored.
        if (supportNonTzAwareMode == false)
            return;
        else if (event.getMetadata().size() == 0)
            return;

        // Compute time zone-awareness of the event.
        boolean timeZoneAwareEvent = event
                .getMetadataOptionValue(ReplOptionParams.TIME_ZONE_AWARE) != null;

        if (nonTzAwareMode)
        {
            // We are not enabled for TZ-aware operation. Check for a TZ-enabled
            // event.
            if (timeZoneAwareEvent)
            {
                // We are now processing a time zone-aware event.
                logger.info("Found a time zone-aware event while in non-TZ-aware mode: seqno="
                        + header.getSeqno() + " fragno=" + header.getFragno());
                enableTzAwareMode();
            }
        }
        else
        {
            // We are enabled for TZ-aware operation. Check for an event that is
            // not time zone-aware.
            if (!timeZoneAwareEvent)
            {
                // We are now processing a time zone-unaware event.
                logger.info("Found a non-time zone-aware event while in TZ-aware mode: seqno="
                        + header.getSeqno() + " fragno=" + header.getFragno());
                enableNonTzAwareMode();
            }
        }
    }

    /**
     * Reset formatters to replicator global time zone to time-zone aware
     * operation.
     */
    protected void enableTzAwareMode()
    {
        TimeZone replicatorTz = runtime.getReplicatorTimeZone();
        logger.info("Resetting time zones used for date-time to enable time zone-aware operation: new tz="
                + replicatorTz.getDisplayName());
        dateTimeFormatter.setTimeZone(replicatorTz);
        dateFormatter.setTimeZone(replicatorTz);
        timeFormatter.setTimeZone(replicatorTz);
        // Do not alter the formatter for MySQL DATETIME type.
        nonTzAwareMode = false;
    }

    /**
     * Initiate non-time zone-ware operation, which imitates operation of old
     * prior to the time zone fix.
     */
    protected void enableNonTzAwareMode() throws ReplicatorException
    {
        // Set the session time_zone back to the global time zone;
        logger.info("Resetting MySQL session time zone back to global value");
        String sql = "set session time_zone=@@global.time_zone";
        try
        {
            conn.execute(sql);
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(
                    "Unable to reset MySQL session time_zone: sql=" + sql
                            + " message=" + e.getLocalizedMessage());
        }

        // Set the time zone to the host time zone.
        TimeZone hostTz = runtime.getHostTimeZone();
        logger.info("Resetting time zones used for date-time to enable non-time zone-aware operation: new tz="
                + hostTz.getDisplayName());
        dateTimeFormatter.setTimeZone(hostTz);
        dateFormatter.setTimeZone(hostTz);
        timeFormatter.setTimeZone(hostTz);
        // Do not alter the formatter for MySQL DATETIME type.
        nonTzAwareMode = true;
    }

    protected void applyRowIdData(RowIdData data) throws ReplicatorException
    {
        String query = "SET ";

        switch (data.getType())
        {
            case RowIdData.LAST_INSERT_ID :
                query += "LAST_INSERT_ID";
                break;
            case RowIdData.INSERT_ID :
                query += "INSERT_ID";
                break;
            default :
                // Old behavior
                query += "INSERT_ID";
                break;
        }
        query += " = " + data.getRowId();

        try
        {
            try
            {
                statement.execute(query);
            }
            catch (SQLWarning e)
            {
                String msg = "While applying SQL event:\n" + data.toString()
                        + "\nWarning: " + e.getMessage();
                logger.warn(msg);
            }
            statement.clearBatch();

            if (logger.isDebugEnabled())
            {
                logger.debug("Applied event: " + query);
            }
        }
        catch (SQLException e)
        {
            logFailedStatementSQL(query, e);
            throw new ApplierException(e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.JdbcApplier#setObject(java.sql.PreparedStatement,
     *      int, com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal,
     *      com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec)
     */
    @Override
    protected void setObject(PreparedStatement prepStatement, int bindLoc,
            ColumnVal value, ColumnSpec columnSpec) throws SQLException
    {

        int type = columnSpec.getType();

        if (type == Types.TIMESTAMP && value.getValue() instanceof Integer)
        {
            prepStatement.setInt(bindLoc, 0);
        }
        else if (type == Types.DATE && value.getValue() instanceof Integer)
        {
            prepStatement.setInt(bindLoc, 0);
        }
        else if (type == Types.INTEGER)
        {
            Object valToInsert = null;
            Numeric numeric = new Numeric(columnSpec, value);
            if (columnSpec.isUnsigned() && numeric.isNegative())
            {
                valToInsert = MySQLUnsignedNumeric
                        .negativeToMeaningful(numeric);
                setInteger(prepStatement, bindLoc, valToInsert);
            }
            else
                prepStatement.setObject(bindLoc, value.getValue());
        }
        else if (type == java.sql.Types.BLOB
                && value.getValue() instanceof SerialBlob)
        {
            SerialBlob val = (SerialBlob) value.getValue();
            prepStatement
                    .setBytes(bindLoc, val.getBytes(1, (int) val.length()));
        }
        else
            prepStatement.setObject(bindLoc, value.getValue());
    }

    protected void setInteger(PreparedStatement prepStatement, int bindLoc,
            Object valToInsert) throws SQLException
    {
        prepStatement.setObject(bindLoc, valToInsert);
    }

    private static final char[] hexArray = "0123456789abcdef".toCharArray();

    protected String hexdump(byte[] buffer)
    {
        char[] hexChars = new char[buffer.length * 2];
        for (int j = 0; j < buffer.length; j++)
        {
            int v = buffer[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.JdbcApplier#applyOneRowChangePrepared(com.continuent.tungsten.replicator.dbms.OneRowChange)
     */
    @Override
    protected void applyOneRowChangePrepared(OneRowChange oneRowChange)
            throws ReplicatorException
    {
        // Optimize events is a binary for now. In future we may do so when
        // number of rows is > min or < to max.
        if (optimizeRowEvents)
            if (oneRowChange.getAction() == RowChangeData.ActionType.INSERT
                    && oneRowChange.getColumnValues().size() > 1)
            {
                // optimize inserts
                getColumnInfomation(oneRowChange);

                executePreparedStatement(oneRowChange,
                        prepareOptimizedInsertStatement(oneRowChange),
                        oneRowChange.getColumnSpec(),
                        oneRowChange.getColumnValues());
                return;
            }
            else if (oneRowChange.getAction() == RowChangeData.ActionType.DELETE
                    && oneRowChange.getKeyValues().size() > 1)
            {
                getColumnInfomation(oneRowChange);

                Table t = null;

                try
                {
                    t = getTableMetadata(oneRowChange);
                }
                catch (SQLException e)
                {
                    throw new ApplierException(
                            "Failed to retrieve table metadata from database",
                            e);
                }

                // This can only be applied if table has a single column primary
                // key
                if (t.getPrimaryKey() != null
                        && t.getPrimaryKey().getColumns() != null
                        && t.getPrimaryKey().getColumns().size() == 1)
                {
                    String keyName = t.getPrimaryKey().getColumns().get(0)
                            .getName();

                    executePreparedStatement(
                            oneRowChange,
                            prepareOptimizedDeleteStatement(oneRowChange,
                                    keyName), oneRowChange.getKeySpec(),
                            oneRowChange.getKeyValues());
                    return;
                }
                else if (logger.isDebugEnabled())
                    logger.debug("Unable to optimize delete statement as no suitable primary key was found for : "
                            + oneRowChange.getSchemaName()
                            + "."
                            + oneRowChange.getTableName());
            }
        // No optimization found, let's run the unoptimized statement form.
        super.applyOneRowChangePrepared(oneRowChange);
    }

    /**
     * Build prepare statement for optimized inserts : <br>
     * INSERT INTO table1 VALUES (...) ; INSERT INTO table1 VALUES (...) ; ...
     * would translate into<br>
     * INSERT INTO table1 VALUES (...), (...), ...
     * 
     * @param oneRowChange row event being processed
     */
    private StringBuffer prepareOptimizedInsertStatement(
            OneRowChange oneRowChange)
    {
        StringBuffer stmt;
        stmt = new StringBuffer();
        stmt.append("INSERT INTO ");
        stmt.append(conn.getDatabaseObjectName(oneRowChange.getSchemaName())
                + "." + conn.getDatabaseObjectName(oneRowChange.getTableName()));
        stmt.append(" ( ");
        printColumnSpec(stmt, oneRowChange.getColumnSpec(), null, null,
                PrintMode.NAMES_ONLY, ", ");
        stmt.append(") VALUES (");

        boolean firstRow = true;
        for (ArrayList<ColumnVal> oneRowValues : oneRowChange.getColumnValues())
        {
            if (firstRow)
            {
                firstRow = false;
            }
            else
                stmt.append(", (");

            printColumnSpec(stmt, oneRowChange.getColumnSpec(), null,
                    oneRowValues, PrintMode.PLACE_HOLDER, " , ");

            stmt.append(")");
        }
        return stmt;
    }

    /**
     * Create statement for optimized delete.
     */
    private StringBuffer prepareOptimizedDeleteStatement(
            OneRowChange oneRowChange, String keyName)
    {
        StringBuffer stmt = new StringBuffer();
        stmt.append("DELETE FROM ");
        stmt.append(conn.getDatabaseObjectName(oneRowChange.getSchemaName())
                + "." + conn.getDatabaseObjectName(oneRowChange.getTableName()));
        stmt.append(" WHERE ");
        stmt.append(conn.getDatabaseObjectName(keyName));
        stmt.append(" IN (");

        ArrayList<ArrayList<ColumnVal>> values = oneRowChange.getKeyValues();
        ArrayList<ColumnSpec> keySpec = oneRowChange.getKeySpec();

        boolean firstRow = true;
        for (ArrayList<ColumnVal> oneKeyValues : values)
        {
            if (firstRow)
                firstRow = false;
            else
                stmt.append(", ");

            printColumnSpec(stmt, keySpec, null, oneKeyValues,
                    PrintMode.PLACE_HOLDER, " , ");
        }
        stmt.append(")");
        return stmt;
    }

    /**
     * Execute a single prepared statement.
     */
    private void executePreparedStatement(OneRowChange oneRowChange,
            StringBuffer stmt, ArrayList<ColumnSpec> spec,
            ArrayList<ArrayList<ColumnVal>> values) throws ApplierException
    {
        PreparedStatement prepStatement = null;
        try
        {
            String statement = stmt.toString();
            if (logger.isDebugEnabled())
                logger.debug("Statement is "
                        + statement.substring(1,
                                Math.min(statement.length(), 500)));
            prepStatement = conn.prepareStatement(statement);
            int bindLoc = 1; /* Start binding at index 1 */

            for (ArrayList<ColumnVal> oneRowValues : values)
            {
                bindLoc = bindColumnValues(prepStatement, oneRowValues,
                        bindLoc, spec, false);

            }

            try
            {
                prepStatement.executeUpdate();
            }
            catch (SQLWarning e)
            {
                String msg = "While applying SQL event:\n" + statement
                        + "\nWarning: " + e.getMessage();
                logger.warn(msg);
            }
        }
        catch (SQLException e)
        {
            ApplierException applierException = new ApplierException(e);
            applierException.setExtraData(logFailedRowChangeSQL(stmt,
                    oneRowChange));
            throw applierException;
        }
        finally
        {
            if (prepStatement != null)
                try
                {
                    prepStatement.close();
                }
                catch (SQLException e)
                {
                }
        }
    }
}