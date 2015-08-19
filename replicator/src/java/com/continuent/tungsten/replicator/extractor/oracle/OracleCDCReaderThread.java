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

package com.continuent.tungsten.replicator.extractor.oracle;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.OracleEventId;
import com.continuent.tungsten.replicator.datasource.UniversalDataSource;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.RowChangeData.ActionType;
import com.continuent.tungsten.replicator.extractor.mysql.SerialBlob;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class OracleCDCReaderThread extends Thread
{
    private static Logger                logger     = Logger.getLogger(OracleCDCReaderThread.class);

    private Database                     connection = null;

    List<OracleCDCSource>                sources;
    private ResultSet                    resultset  = null;

    private Map<String, OracleCDCSource> subscriberViews;

    BlockingQueue<CDCMessage>            queue;

    private boolean                      cancelled  = false;

    private String                       lastSCN;

    private int                          maxSleepTimeInMs;

    private int                          maxRowsByBlock;

    // Reconnect after a number of milliseconds. 0 means never.
    private long                         reconnectTimeout;

    private String                       serviceName;

    private int                          minSleepTimeInMs;

    private int                          sleepAdditionInMs;

    private PluginContext                context;

    private String                       dataSource;

    public OracleCDCReaderThread(PluginContext context, String dataSource,
            BlockingQueue<CDCMessage> queue, String lastSCN,
            int minSleepTimeInSeconds, int maxSleepTimeInSeconds,
            int sleepAddition, int maxRowsByBlock, long reconnectTimeout)
    {
        this.context = context;
        this.queue = queue;
        this.dataSource = dataSource;
        this.minSleepTimeInMs = 1000 * minSleepTimeInSeconds;
        this.maxSleepTimeInMs = 1000 * maxSleepTimeInSeconds;
        this.sleepAdditionInMs = 1000 * sleepAddition;

        this.maxRowsByBlock = maxRowsByBlock;
        this.reconnectTimeout = reconnectTimeout;
        // Oracle doesn't understand lower case objects:
        logger.info("Oracle extraction thread starting using : minSleepTime = "
                + minSleepTimeInMs
                + " ms - maxSleepTime = "
                + maxSleepTimeInMs
                + " ms - sleepTimeIncrement = "
                + sleepAdditionInMs
                + "ms - maxRowsByBlock "
                + maxRowsByBlock
                + " - "
                + (reconnectTimeout > 0 ? "Reconnecting after "
                        + (reconnectTimeout / 1000) + " s" : "No reconnection"));
    }

    public void prepare() throws ReplicatorException
    {
        // Establish a connection to the data source.
        logger.info("Connecting to data source");
        UniversalDataSource dataSourceImpl = context.getDataSource(dataSource);
        if (dataSourceImpl == null)
        {
            throw new ReplicatorException("Unable to locate data source: name="
                    + dataSource);
        }

        // Create a connection.
        connection = (Database) dataSourceImpl.getConnection();

        this.serviceName = dataSourceImpl.getServiceName().toUpperCase();
        this.setName("Oracle CDC Reader Thread for service " + serviceName);

        Statement stmt = null;
        try
        {
            stmt = connection.createStatement();
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(
                    "Unable to create a statement object", e);
        }

        // Step 1 Find the source tables for which the subscriber has access
        // privileges.
        ResultSet rs = null;
        int databaseMajorVersion = 0;
        try
        {
            databaseMajorVersion = connection.getConnection().getMetaData()
                    .getDatabaseMajorVersion();
            if (databaseMajorVersion >= 11)
                rs = stmt.executeQuery("SELECT * FROM TUNGSTEN_SOURCE_TABLES");
            else
                rs = stmt.executeQuery("SELECT * FROM ALL_SOURCE_TABLES");

            sources = new ArrayList<OracleCDCSource>();
            while (rs.next())
            {
                String srcSchema = rs.getString("SOURCE_SCHEMA_NAME");
                String srcTable = rs.getString("SOURCE_TABLE_NAME");
                if (logger.isDebugEnabled())
                    logger.debug("Subscribing to " + srcSchema + "." + srcTable);
                sources.add(new OracleCDCSource(srcSchema, srcTable));
            }
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(
                    "Unable to connect to query source tables", e);
        }
        finally
        {
            if (rs != null)
            {
                try
                {
                    rs.close();
                }
                catch (SQLException ignore)
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Failed to close resultset", ignore);
                }
                rs = null;
            }
        }

        Set<String> changeSets = new LinkedHashSet<String>();

        try
        {
            // Step 2 Find the change set names and columns for which the
            // subscriber has access privileges.
            for (Iterator<OracleCDCSource> iterator = sources.iterator(); iterator
                    .hasNext();)
            {
                OracleCDCSource src = iterator.next();
                try
                {
                    String sql;
                    sql = "SELECT UNIQUE CHANGE_SET_NAME, PUB.COLUMN_NAME,"
                            + " PUB_ID, COL.COLUMN_ID " + " FROM ";
                    if (databaseMajorVersion >= 11)
                        sql += "TUNGSTEN_PUBLISHED_COLUMNS";
                    else
                        sql += "ALL_PUBLISHED_COLUMNS";
                    sql += " PUB, ALL_TAB_COLUMNS COL "
                            + " WHERE SOURCE_SCHEMA_NAME = '" + src.getSchema()
                            + "'" + " AND SOURCE_TABLE_NAME = '"
                            + src.getTable() + "'"
                            + " AND SOURCE_SCHEMA_NAME = COL.OWNER "
                            + " AND SOURCE_TABLE_NAME = COL.TABLE_NAME"
                            + " AND PUB.COLUMN_NAME = COL.COLUMN_NAME";

                    if (serviceName != null)
                        sql += " AND CHANGE_SET_NAME = 'TUNGSTEN_CS_"
                                + serviceName + "'";

                    sql += " ORDER BY COL.COLUMN_ID";

                    if (logger.isDebugEnabled())
                        logger.debug("Executing " + sql);

                    rs = stmt.executeQuery(sql);

                    while (rs.next())
                    {
                        String changeSetName = rs.getString("CHANGE_SET_NAME");
                        String columnName = rs.getString("COLUMN_NAME");
                        long pubId = rs.getLong("PUB_ID");
                        src.addPublication(changeSetName, columnName, pubId);

                        changeSets.add(changeSetName);
                        if (logger.isDebugEnabled())
                            logger.debug("Found column " + changeSetName + "\t"
                                    + columnName + "\t" + pubId);
                    }
                }
                catch (SQLException e)
                {
                    throw new ReplicatorException(
                            "Unable to fetch change set definition", e);

                }
                finally
                {
                    if (rs != null)
                        try
                        {
                            rs.close();
                        }
                        catch (SQLException ignore)
                        {
                            logger.warn("Failed to close resultset", ignore);
                        }
                }
            }
        }
        finally
        {
            // Be sure to close the statement even if something bad happened.
            if (stmt != null)
                try
                {
                    stmt.close();
                }
                catch (SQLException ignore)
                {
                    logger.warn("Failed to close Oracle statement", ignore);
                }
        }
        // Step 3 Create subscriptions.

        // For each publication, create the subscription to the publication if
        // not already done.
        // Then, subscribe
        int i = 1;
        subscriberViews = new HashMap<String, OracleCDCSource>();

        for (Iterator<OracleCDCSource> iterator = sources.iterator(); iterator
                .hasNext();)
        {
            OracleCDCSource src = iterator.next();
            Map<Long, OracleCDCPublication> publications = src
                    .getPublications();

            if (publications.values().size() == 0)
                // This source table does not belong to the change set for this
                // service
                continue;
            // throw new ReplicatorException("Source " + src.getSchema() + "."
            // + src.getTable() + " does not seem to exist anymore");

            StringBuffer subscribeStmt = new StringBuffer();
            for (OracleCDCPublication pub : publications.values())
            {
                if (changeSets.remove(pub.getPublicationName()))
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Creating subscription to "
                                + pub.getPublicationName());

                    /*
                     * Dropping subscription if it already exists : this can
                     * happen if release code was not called
                     */
                    executeQuery("BEGIN DBMS_CDC_SUBSCRIBE.DROP_SUBSCRIPTION("
                            + "subscription_name => 'TUNGSTEN_SUB_"
                            + serviceName + "');END;", true);

                    executeQuery(
                            "BEGIN DBMS_CDC_SUBSCRIBE.CREATE_SUBSCRIPTION("
                                    + "change_set_name => '"
                                    + pub.getPublicationName()
                                    + "', description => 'Change data used by Tungsten', "
                                    + "subscription_name => 'TUNGSTEN_SUB_"
                                    + serviceName + "');end;", false);
                }

                // Step 4 Subscribe to a source table and the columns in the
                // source table.
                String viewName = "VW_TUNGSTEN_" + serviceName + i;
                subscribeStmt
                        .append("DBMS_CDC_SUBSCRIBE.SUBSCRIBE(subscription_name => 'TUNGSTEN_SUB_"
                                + serviceName
                                + "', "
                                + "publication_id    => "
                                + pub.getPublicationId()
                                + ","
                                + "column_list => '"
                                + pub.getColumnList()
                                + "',"
                                + "subscriber_view => '"
                                + viewName
                                + "');");

                subscriberViews.put(viewName, src);
                src.setSubscriptionView(viewName, pub.getPublicationId());

                if (databaseMajorVersion < 11)
                    executeQuery("DROP VIEW " + viewName, true, true);

                if (logger.isDebugEnabled())
                    logger.debug("Creating change view " + viewName
                            + " - Now handling "
                            + subscriberViews.keySet().size() + " views");

                i++;

            }

            executeQuery("BEGIN " + subscribeStmt.toString() + " END;", false);
        }
        // Step 5 Activate the subscription.
        executeQuery("BEGIN DBMS_CDC_SUBSCRIBE.ACTIVATE_SUBSCRIPTION("
                + "subscription_name => 'TUNGSTEN_SUB_" + serviceName
                + "');END;", false);

    }

    private void executeQuery(String query, boolean ignoreError, boolean silent)
            throws ReplicatorException
    {
        Statement stmt = null;
        ResultSet resultset = null;
        try
        {
            stmt = connection.getConnection().createStatement();
            resultset = stmt.executeQuery(query);
        }
        catch (SQLException e)
        {
            if (!ignoreError)
                throw new ReplicatorException("Failed to execute query "
                        + query, e);
            else if (!silent)
                logger.warn("Ignoring exception : " + e.getMessage());
        }
        finally
        {
            try
            {
                if (resultset != null)
                    resultset.close();
            }
            catch (SQLException ignore)
            {
                logger.warn("Failed to close oracle resultset", ignore);
            }
            try
            {
                if (stmt != null)
                    stmt.close();
            }
            catch (SQLException ignore)
            {
                logger.warn("Failed to release oracle statement", ignore);
            }
        }
    }

    /**
     * executeStoredProcedure definition.
     */
    private void executeQuery(String query, boolean ignoreError)
            throws ReplicatorException
    {
        executeQuery(query, ignoreError, false);
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run()
    {
        runTask();
    }

    private void runTask()
    {
        int currentSleepTime = minSleepTimeInMs;
        String operation;
        long currentSCN = -1;
        RowChangeData rowData = null;

        StringBuffer buffer = new StringBuffer();

        boolean alreadyLogged = false;

        long lastConnectionTime = System.currentTimeMillis();

        try
        {
            while (!cancelled)
            {
                boolean noData = true;
                Timestamp sourceTStamp = null;

                if (logger.isDebugEnabled())
                    logger.debug("Extending Window");

                executeQuery(
                        "BEGIN DBMS_CDC_SUBSCRIBE.EXTEND_WINDOW(subscription_name => 'TUNGSTEN_SUB_"
                                + serviceName + "');END;", false);

                if (logger.isDebugEnabled())
                    logger.debug("Handling " + subscriberViews.keySet().size()
                            + "views");

                int rowCount = 0;
                for (String view : subscriberViews.keySet())
                {
                    OracleCDCSource cdcSource = subscriberViews.get(view);

                    Statement stmt = connection.getConnection()
                            .createStatement();

                    String statement;
                    if (lastSCN != null)
                    {
                        statement = "SELECT "
                                + cdcSource.getPublication(view)
                                        .getColumnList()
                                + " , CSCN$, COMMIT_TIMESTAMP$, OPERATION$"
                                + " from " + view + " where cscn$ > " + lastSCN
                                + "  order by cscn$, rsid$";
                    }
                    else
                        statement = "SELECT "
                                + cdcSource.getPublication(view)
                                        .getColumnList()
                                + " , CSCN$, COMMIT_TIMESTAMP$, OPERATION$"
                                + " from " + view + "  order by cscn$, rsid$";

                    resultset = stmt.executeQuery(statement);
                    int userColumns = cdcSource.getPublication(view)
                            .getColumnsCount();

                    if (logger.isDebugEnabled())
                        logger.debug("Running " + statement);

                    OneRowChange oneRowChange = null;

                    boolean rowEventFullyParsed = false;
                    boolean beforeImageDone = false;

                    while (resultset.next())
                    {
                        if (rowData == null)
                            rowData = new RowChangeData();

                        currentSCN = resultset.getLong("CSCN$");
                        operation = resultset.getString("OPERATION$").trim();

                        if ((sourceTStamp == null || sourceTStamp
                                .before(resultset
                                        .getTimestamp("COMMIT_TIMESTAMP$")))
                                && !operation.equals("DD"))
                            sourceTStamp = resultset
                                    .getTimestamp("COMMIT_TIMESTAMP$");

                        if (logger.isDebugEnabled())
                            logger.debug("Receiving data from " + sourceTStamp);
                        else if (alreadyLogged)
                        {
                            alreadyLogged = false;
                            if (logger.isInfoEnabled())
                            {
                                logger.info("Data available... working");
                            }
                        }

                        // Reset sleep time
                        currentSleepTime = minSleepTimeInMs;

                        buffer = new StringBuffer();

                        for (int i = 1; i <= resultset.getMetaData()
                                .getColumnCount(); i++)
                        {
                            if (buffer.length() > 0)
                                buffer.append('\t');
                            buffer.append(resultset.getString(i));
                        }

                        if (logger.isDebugEnabled())
                            logger.debug("Received : " + buffer.toString());

                        if (operation.equals("I"))
                        {
                            noData = false;
                            if (oneRowChange == null
                                    || !oneRowChange.getAction().equals(
                                            ActionType.INSERT))
                            {
                                oneRowChange = new OneRowChange(
                                        cdcSource.getSchema(),
                                        cdcSource.getTable(), ActionType.INSERT);
                                rowData.appendOneRowChange(oneRowChange);
                            }
                            parseRowEvent(oneRowChange, false, userColumns);
                            rowEventFullyParsed = true;
                            rowCount++;
                        }
                        else if (operation.equals("D"))
                        {
                            noData = false;
                            if (oneRowChange == null
                                    || !oneRowChange.getAction().equals(
                                            ActionType.DELETE))
                            {
                                oneRowChange = new OneRowChange(
                                        cdcSource.getSchema(),
                                        cdcSource.getTable(), ActionType.DELETE);
                                rowData.appendOneRowChange(oneRowChange);
                            }
                            parseRowEvent(oneRowChange, true, userColumns);
                            rowEventFullyParsed = true;
                            rowCount++;
                        }
                        else if (operation.startsWith("U"))
                        {
                            noData = false;
                            if (oneRowChange == null
                                    || !oneRowChange.getAction().equals(
                                            ActionType.UPDATE))
                            {
                                oneRowChange = new OneRowChange(
                                        cdcSource.getSchema(),
                                        cdcSource.getTable(), ActionType.UPDATE);

                                rowData.appendOneRowChange(oneRowChange);
                                rowEventFullyParsed = false;
                                beforeImageDone = false;
                            }
                            if (operation.equals("UO")
                                    || operation.equals("UU"))
                            {
                                if (beforeImageDone)
                                {
                                    rowEventFullyParsed = true;
                                    rowCount++;
                                    beforeImageDone = false;
                                }
                                else
                                    beforeImageDone = true;

                                parseRowEvent(oneRowChange, true, userColumns);
                            }
                            else if (operation.equals("UN"))
                            {
                                if (beforeImageDone)
                                {
                                    rowCount++;
                                    rowEventFullyParsed = true;
                                    beforeImageDone = false;
                                }
                                else
                                    beforeImageDone = true;

                                parseRowEvent(oneRowChange, false, userColumns);
                            }
                        }
                        else if (operation.equals("DD"))
                        {
                            if (logger.isDebugEnabled())
                                logger.debug("DDL detected");
                        }
                        else
                        {
                            logger.error("Unable to extract data from CDC (operation should be I, D, UO or UN - found "
                                    + operation + ") \n\tRead :" + buffer);
                        }

                        // Fragment event by rows (maxRowsByBlock rows max)
                        if (rowCount >= maxRowsByBlock && rowEventFullyParsed)
                        {
                            queue.put(new CDCDataMessage(rowData, sourceTStamp,
                                    currentSCN));
                            rowCount = 0;
                            oneRowChange = null;
                            rowData = null;
                            beforeImageDone = false;
                            rowEventFullyParsed = false;
                        }
                    }
                    if (rowCount > 0)
                    {
                        queue.put(new CDCDataMessage(rowData, sourceTStamp,
                                currentSCN));
                        rowCount = 0;
                        oneRowChange = null;
                        rowData = null;
                        beforeImageDone = false;
                        rowEventFullyParsed = false;
                    }
                    resultset.close();
                    resultset = null;
                    stmt.close();
                }
                lastSCN = null;
                if (noData)
                {
                    if (logger.isDebugEnabled())
                    {
                        logger.debug("No data available... sleeping ("
                                + currentSleepTime + " ms)");
                    }
                    else if (!alreadyLogged)
                    {
                        alreadyLogged = true;
                        if (logger.isInfoEnabled())
                        {
                            logger.info("No data available... sleeping");
                        }
                    }
                    Thread.sleep(currentSleepTime);

                    // Set sleep time to the next value : either double of
                    // current or the maximum sleep time if reached.
                    currentSleepTime = Math.min(currentSleepTime
                            + sleepAdditionInMs, maxSleepTimeInMs);
                }
                else
                {
                    if (rowCount > 0)
                    {
                        queue.put(new CDCDataMessage(rowData, sourceTStamp,
                                currentSCN));
                        rowCount = 0;
                        rowData = null;
                    }
                    if (logger.isDebugEnabled())
                        logger.debug("Sending commit");
                    // Force commit as the window was fully processed
                    queue.put(new CDCCommitMessage(currentSCN));
                }
                if (logger.isDebugEnabled())
                    logger.debug("Purging window");

                executeQuery(
                        "BEGIN DBMS_CDC_SUBSCRIBE.PURGE_WINDOW(subscription_name => 'TUNGSTEN_SUB_"
                                + serviceName + "');END;", false);

                // Reconnect if needed and time is reached
                if (reconnectTimeout > 0
                        && System.currentTimeMillis() - lastConnectionTime > reconnectTimeout)
                {
                    logger.debug("Reconnecting");
                    connection.close();
                    connection.connect();
                    lastConnectionTime = System.currentTimeMillis();
                }

            }

        }
        catch (Throwable e)
        {
            try
            {
                queue.put(new CDCErrorMessage("Oracle Reader thread failed", e));
            }
            catch (InterruptedException ignore)
            {
            }
        }
    }

    private void parseRowEvent(OneRowChange oneRowChange, boolean isKeySpec,
            int cols) throws SQLException
    {

        if ((isKeySpec && oneRowChange.getKeySpec().isEmpty())
                || (!isKeySpec && oneRowChange.getColumnSpec().isEmpty()))
        {
            ArrayList<ColumnSpec> specs = (isKeySpec ? oneRowChange
                    .getKeySpec() : oneRowChange.getColumnSpec());

            if (logger.isDebugEnabled())
                logger.debug("Adding column or key specs (not defined so far)");

            ColumnSpec colSpec;
            for (int i = 1; i <= cols; i++)
            {
                colSpec = oneRowChange.new ColumnSpec();
                colSpec.setIndex(i);
                int columnType = resultset.getMetaData().getColumnType(i);
                if (columnType == Types.DATE)
                    colSpec.setType(Types.TIMESTAMP);
                else
                    colSpec.setType(columnType);
                colSpec.setName(resultset.getMetaData().getColumnLabel(i));
                specs.add(colSpec);
            }
        }
        ArrayList<OneRowChange.ColumnVal> columns = new ArrayList<ColumnVal>();
        if (isKeySpec)
            oneRowChange.getKeyValues().add(columns);
        else
            oneRowChange.getColumnValues().add(columns);

        ColumnVal value;
        for (int i = 1; i <= cols; i++)
        {
            value = oneRowChange.new ColumnVal();
            int columnType = resultset.getMetaData().getColumnType(i);
            if (columnType == Types.TIMESTAMP || columnType == Types.DATE)
                value.setValue(resultset.getTimestamp(i));
            else if (columnType == Types.BLOB)
            {
                byte[] bytes = resultset.getBytes(i);
                if (!resultset.wasNull())
                    value.setValue(new SerialBlob(bytes));
                // else it will be handled afterwards
            }
            else if (columnType == Types.CLOB)
            {
                value.setValue(resultset.getString(i));
            }
            else
                value.setValue((Serializable) resultset.getObject(i));
            if (resultset.wasNull())
                value.setValueNull();
            columns.add(value);
        }
    }

    public void cancel()
    {
        this.cancelled = true;

        if (connection != null)
        {
            connection.close();
            connection = null;
        }

    }

    public void setLastSCN(String lastSCN) throws ReplicatorException
    {
        OracleEventId eventId = new OracleEventId(lastSCN);
        if (eventId.isValid())
        {
            this.lastSCN = String.valueOf(eventId.getSCN());
        }
        else
            throw new ReplicatorException("Invalid event identifier");
    }

}
