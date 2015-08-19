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

package com.continuent.tungsten.replicator.applier;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;

import org.apache.log4j.Logger;
import org.drizzle.jdbc.DrizzleStatement;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.dbms.LoadDataFileQuery;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.ReplOption;
import com.continuent.tungsten.replicator.extractor.mysql.SerialBlob;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class MySQLDrizzleApplier extends MySQLApplier
{
    static Logger   logger        = Logger.getLogger(MySQLDrizzleApplier.class);

    private boolean alreadyLogged = false;

    @Override
    public void configure(PluginContext context) throws ReplicatorException
    {
        super.configure(context);
    }

    @Override
    protected void applyStatementData(StatementData data)
            throws ReplicatorException
    {
        if (!(statement instanceof DrizzleStatement))
        {
            // Check if the right driver is in use
            if (!alreadyLogged)
                logger.warn("Using MySQLDrizzleApplier with the wrong driver."
                        + " Check the driver.");
            super.applyStatementData(data);
            return;
        }
        else if (data.getQueryAsBytes() == null)
        {
            // Use the old code path if the new one is not required
            super.applyStatementData(data);
            return;
        }

        DrizzleStatement drizzleStatement = (DrizzleStatement) statement;

        try
        {
            int[] updateCount = null;
            String schema = data.getDefaultSchema();
            Long timestamp = data.getTimestamp();
            List<ReplOption> options = data.getOptions();

            applyUseSchema(schema);

            applyVariables(timestamp, options);

            // Using drizzle driver specific method to send bytes directly to
            // mysql
            drizzleStatement.addBatch(data.getQueryAsBytes());

            try
            {
                updateCount = drizzleStatement.executeBatch();
            }
            catch (SQLWarning e)
            {
                String msg = "While applying SQL event:\n" + data.toString()
                        + "\nWarning: " + e.getMessage();
                logger.warn(msg);
                updateCount = new int[1];
                updateCount[0] = drizzleStatement.getUpdateCount();
            }
            catch (SQLException e)
            {
                if (data.getErrorCode() == 0)
                {
                    SQLException sqlException = new SQLException(
                            "Statement failed on slave but succeeded on master");
                    sqlException.initCause(e);
                    throw sqlException;
                }
                // Check if the query produced the same error on master
                if (e.getErrorCode() == data.getErrorCode())
                {
                    logger.info("Ignoring statement failure as it also failed "
                            + "on master with the same error code ("
                            + e.getErrorCode() + ")");
                }
                else
                {
                    SQLException sqlException = new SQLException(
                            "Statement failed on slave with error code "
                                    + e.getErrorCode()
                                    + " but failed on master with a different one ("
                                    + data.getErrorCode() + ")");
                    sqlException.initCause(e);
                    throw sqlException;
                }
            }

            while (drizzleStatement.getMoreResults())
            {
                drizzleStatement.getResultSet();
            }
            drizzleStatement.clearBatch();

            if (logger.isDebugEnabled() && updateCount != null)
            {
                int cnt = 0;
                for (int i = 0; i < updateCount.length; cnt += updateCount[i], i++)
                    ;

                if (logger.isDebugEnabled())
                    logger.debug("Applied event (update count " + cnt + "): "
                            + data.toString());
            }
        }
        catch (SQLException e)
        {
            ApplierException applierException = new ApplierException(e);
            String query = data.getQuery();
            if (query.length() > maxSQLLogLength)
                query = query.substring(0, maxSQLLogLength);

            applierException.setExtraData(query);
            throw applierException;
        }

    }

    @Override
    protected void applyLoadDataLocal(LoadDataFileQuery data, File temporaryFile)
            throws ReplicatorException
    {
        try
        {
            int[] updateCount;
            String schema = data.getDefaultSchema();
            Long timestamp = data.getTimestamp();
            List<ReplOption> options = data.getOptions();

            applyUseSchema(schema);

            applyVariables(timestamp, options);

            try
            {
                updateCount = statement.executeBatch();
            }
            catch (SQLWarning e)
            {
                String msg = "While applying SQL event:\n" + data.toString()
                        + "\nWarning: " + e.getMessage();
                logger.warn(msg);
                updateCount = new int[1];
                updateCount[0] = statement.getUpdateCount();
            }
            statement.clearBatch();
        }
        catch (SQLException e)
        {
            logFailedStatementSQL(data.getQuery(), e);
            throw new ApplierException(e);
        }

        try
        {
            FileInputStream fis = new FileInputStream(temporaryFile);
            ((DrizzleStatement) statement).setLocalInfileInputStream(fis);

            int cnt = statement.executeUpdate(data.getQuery());

            if (logger.isDebugEnabled())
                logger.debug("Applied event (update count " + cnt + "): "
                        + data.toString());
        }
        catch (SQLException e)
        {
            logFailedStatementSQL(data.getQuery(), e);
            throw new ApplierException(e);
        }
        catch (FileNotFoundException e)
        {
            logFailedStatementSQL(data.getQuery());
            throw new ApplierException(e);
        }
        finally
        {
            ((DrizzleStatement) statement).setLocalInfileInputStream(null);
        }

        // Clean up the temp file as we may not get a delete file event.
        if (logger.isDebugEnabled())
        {
            logger.debug("Deleting temp file: "
                    + temporaryFile.getAbsolutePath());
        }
        temporaryFile.delete();
    }

    @Override
    protected void setInteger(PreparedStatement prepStatement, int bindLoc,
            Object valToInsert) throws SQLException
    {
        if (valToInsert instanceof BigInteger)
            prepStatement.setString(bindLoc, valToInsert.toString());
        else
            super.setInteger(prepStatement, bindLoc, valToInsert);
    }

    @Override
    protected void setObject(PreparedStatement prepStatement, int bindLoc,
            ColumnVal value, ColumnSpec columnSpec) throws SQLException
    {
        if (value.getValue() == null)
        {
            super.setObject(prepStatement, bindLoc, value, columnSpec);
        }
        else if (columnSpec.getType() == Types.DOUBLE)
        {
            BigDecimal dec = new BigDecimal((Double) value.getValue());
            prepStatement.setBigDecimal(bindLoc, dec);
        }
        else if (columnSpec.getType() == Types.FLOAT)
        {
            BigDecimal dec = new BigDecimal((Float) value.getValue());
            prepStatement.setBigDecimal(bindLoc, dec);
        }
        else if (columnSpec.getType() == Types.VARCHAR
                && value.getValue() instanceof byte[])
        {
            int length = ((byte[]) value.getValue()).length;

            if (columnSpec.getTypeDescription() != null
                    && columnSpec.getTypeDescription().startsWith("BINARY")
                    && length < columnSpec.getLength())
            {
                ByteBuffer bb = ByteBuffer.allocate(columnSpec.getLength());
                bb.put((byte[]) value.getValue());
                for (int i = 0; length + i < columnSpec.getLength(); i++)
                    bb.put("\0".getBytes());
                prepStatement.setString(bindLoc, hexdump(bb.array()));
            }
            else
                prepStatement.setString(bindLoc,
                        hexdump((byte[]) value.getValue()));
        }
        else if (columnSpec.getType() == Types.TIMESTAMP
                && value.getValue() instanceof Timestamp /* Issue 679 */)
        {
            // This is a MySQL TIMESTAMP field.
            Timestamp ts = (Timestamp) value.getValue();
            StringBuffer timeStampString = new StringBuffer(
                    dateTimeFormatter.format(ts));
            if (ts.getNanos() > 0)
            {
                timeStampString.append(".");
                timeStampString.append(String.format("%09d%n", ts.getNanos()));
            }
            prepStatement.setString(bindLoc, timeStampString.toString());
        }
        else if (columnSpec.getType() == Types.DATE
                && value.getValue() instanceof Timestamp)
        {
            // This is a MySQL DATETIME field. For these we need to keep the
            // background replicator time zone even in cases where we are reading
            // older logs. 
            Timestamp ts = (Timestamp) value.getValue();
            StringBuffer datetime = new StringBuffer(mysqlDatetimeFormatter.format(ts));
            if (ts.getNanos() > 0)
            {
                datetime.append(".");
                datetime.append(String.format("%09d%n", ts.getNanos()));
            }
            prepStatement.setString(bindLoc, datetime.toString());
        }
        else if (columnSpec.getType() == Types.DATE
                && value.getValue() instanceof java.sql.Date)
        {
            // This is a MySQL DATE field.
            Date date = (Date) value.getValue();
            StringBuffer dateString = new StringBuffer(
                    dateFormatter.format(date));
            prepStatement.setString(bindLoc, dateString.toString());
        }
        else if (columnSpec.getType() == Types.TIME)
        {
            if (value.getValue() instanceof Timestamp)
            {
                // This is a MySQL TIME field.
                Timestamp timestamp = ((Timestamp) value.getValue());
                StringBuffer time = new StringBuffer(
                        timeFormatter.format(timestamp));
                if (timestamp.getNanos() > 0)
                {
                    time.append(".");
                    time.append(String.format("%09d%n", timestamp.getNanos()));
                }
                prepStatement.setString(bindLoc, time.toString());
            }
            else
            {
                // This is not from MySQL, but we should at least
                // honor it with time-zone aware formatting.
                Time t = (Time) value.getValue();
                prepStatement.setString(bindLoc, timeFormatter.format(t));
            }
        }
        else if (columnSpec.getType() == Types.BLOB
                && value.getValue() instanceof SerialBlob
                && columnSpec.getTypeDescription() != null
                && columnSpec.getTypeDescription().contains("TEXT"))
        {
            SerialBlob val = (SerialBlob) value.getValue();
            byte[] bytes = val.getBytes(1, (int) val.length());
            prepStatement.setString(bindLoc, hexdump(bytes));
        }
        else
            super.setObject(prepStatement, bindLoc, value, columnSpec);
    }
}
