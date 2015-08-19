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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.RowChangeData.ActionType;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplOptionParams;
import com.continuent.tungsten.replicator.extractor.RawExtractor;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class OracleCDCExtractor implements RawExtractor
{
    private static Logger                logger     = Logger.getLogger(OracleCDCExtractor.class);

    private String                       url        = null;
    private String                       user       = "root";
    private String                       password   = "rootpass";
    private Database                     connection = null;

    List<OracleCDCSource>                sources;
    private ResultSet                    resultset  = null;

    private String                       lastSCN    = null;

    private Map<String, OracleCDCSource> subscriberViews;

    public void setUrl(String url)
    {
        this.url = url;
    }

    public void setUser(String user)
    {
        this.user = user;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void prepare(PluginContext context) throws ReplicatorException,
            InterruptedException
    {

        try
        {
            // Oracle JDBC URL, for example :
            // jdbc:oracle:thin:@192.168.0.60:1521:ORCL
            connection = DatabaseFactory.createDatabase(url, user, password);
        }
        catch (SQLException e)
        {
        }

        try
        {
            connection.connect();
        }
        catch (SQLException e)
        {
            throw new ReplicatorException("Unable to connect to Oracle", e);
        }

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
        try
        {
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

        // Step 2 Find the change set names and columns for which the subscriber
        // has access privileges.
        for (Iterator<OracleCDCSource> iterator = sources.iterator(); iterator
                .hasNext();)
        {
            OracleCDCSource src = iterator.next();
            try
            {
                if (logger.isDebugEnabled())
                    logger.debug("Executing"
                            + "SELECT UNIQUE CHANGE_SET_NAME, PUB.COLUMN_NAME,"
                            + " PUB_ID, COL.COLUMN_ID "
                            + " FROM ALL_PUBLISHED_COLUMNS PUB, ALL_TAB_COLUMNS COL "
                            + " WHERE SOURCE_SCHEMA_NAME = '" + src.getSchema()
                            + "'" + " AND SOURCE_TABLE_NAME = '"
                            + src.getTable() + "'"
                            + " AND SOURCE_SCHEMA_NAME = COL.OWNER "
                            + " AND SOURCE_TABLE_NAME = COL.TABLE_NAME"
                            + " AND PUB.COLUMN_NAME = COL.COLUMN_NAME"
                            + " ORDER BY COL.COLUMN_ID");

                rs = stmt
                        .executeQuery("SELECT UNIQUE CHANGE_SET_NAME, PUB.COLUMN_NAME,"
                                + " PUB_ID, COL.COLUMN_ID "
                                + " FROM ALL_PUBLISHED_COLUMNS PUB, ALL_TAB_COLUMNS COL "
                                + " WHERE SOURCE_SCHEMA_NAME = '"
                                + src.getSchema()
                                + "'"
                                + " AND SOURCE_TABLE_NAME = '"
                                + src.getTable()
                                + "'"
                                + " AND SOURCE_SCHEMA_NAME = COL.OWNER "
                                + " AND SOURCE_TABLE_NAME = COL.TABLE_NAME"
                                + " AND PUB.COLUMN_NAME = COL.COLUMN_NAME"
                                + " ORDER BY COL.COLUMN_ID");

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
                        if (logger.isDebugEnabled())
                            logger.debug("Failed to close resultset", ignore);
                    }
            }
        }

        if (stmt != null)
            try
            {
                stmt.close();
            }
            catch (SQLException ignore)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Failed to close statement object", ignore);
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
                            + "subscription_name => 'TUNGSTEN_PUB');END;", true);

                    executeQuery(
                            "BEGIN DBMS_CDC_SUBSCRIBE.CREATE_SUBSCRIPTION("
                                    + "change_set_name => '"
                                    + pub.getPublicationName()
                                    + "', description => 'Change data used by Tungsten', "
                                    + "subscription_name => 'TUNGSTEN_PUB"
                                    + "');end;", false);
                }

                // Step 4 Subscribe to a source table and the columns in the
                // source table.
                String viewName = "VW_TUNGSTEN_CDC" + i;
                subscribeStmt
                        .append("DBMS_CDC_SUBSCRIBE.SUBSCRIBE(subscription_name => 'TUNGSTEN_PUB"
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
                + "subscription_name => 'TUNGSTEN_PUB'" + ");END;", false);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void release(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        // Step 10 End the subscription.
        if (connection != null)
        {
            executeQuery("BEGIN DBMS_CDC_SUBSCRIBE.DROP_SUBSCRIPTION("
                    + "subscription_name => 'TUNGSTEN_PUB');END;", false);
        }

        if (connection != null)
        {
            connection.close();
            connection = null;
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.RawExtractor#setLastEventId(java.lang.String)
     */
    @Override
    public void setLastEventId(String eventId) throws ReplicatorException
    {
        if (eventId != null)
            lastSCN = eventId;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.RawExtractor#extract()
     */
    @Override
    public DBMSEvent extract() throws ReplicatorException, InterruptedException
    {
        ArrayList<DBMSData> data = new ArrayList<DBMSData>();
        long maxSCN = -1;
        Timestamp sourceTStamp = null;

        boolean noData = true;

        try
        {
            if (logger.isDebugEnabled())
                logger.debug("Extending Window");

            executeQuery(
                    "BEGIN DBMS_CDC_SUBSCRIBE.EXTEND_WINDOW(subscription_name => 'TUNGSTEN_PUB');END;",
                    false);

            if (logger.isDebugEnabled())
                logger.debug("Handling " + subscriberViews.keySet().size()
                        + "views");
            for (String view : subscriberViews.keySet())
            {
                OracleCDCSource cdcSource = subscriberViews.get(view);

                Statement stmt = connection.getConnection().createStatement();

                String statement;
                if (lastSCN != null)
                    statement = "SELECT "
                            + cdcSource.getPublication(view).getColumnList()
                            + " , CSCN$, COMMIT_TIMESTAMP$, OPERATION$"
                            + " from " + view + " where cscn$ > " + lastSCN
                            + "  order by cscn$, rsid$";
                else
                    statement = "SELECT "
                            + cdcSource.getPublication(view).getColumnList()
                            + " , CSCN$, COMMIT_TIMESTAMP$, OPERATION$"
                            + " from " + view + "  order by cscn$, rsid$";

                resultset = stmt.executeQuery(statement);

                int userColumns = cdcSource.getPublication(view)
                        .getColumnsCount();

                if (logger.isDebugEnabled())
                    logger.debug("Running " + statement);
                OneRowChange updateRowChange = null;
                OneRowChange oneRowChange = null;

                while (resultset.next())
                {
                    noData = false;
                    long currentSCN = resultset.getLong("CSCN$");
                    if (maxSCN < currentSCN)
                        maxSCN = currentSCN;

                    if (sourceTStamp == null)
                        sourceTStamp = resultset
                                .getTimestamp("COMMIT_TIMESTAMP$");

                    if (logger.isDebugEnabled())
                    {
                        logger.debug("Receiving data");
                        StringBuffer buffer = new StringBuffer();

                        for (int i = 1; i <= resultset.getMetaData()
                                .getColumnCount(); i++)
                        {
                            if (buffer.length() > 0)
                                buffer.append('\t');
                            buffer.append(resultset.getString(i));
                        }
                        logger.debug("Received : " + buffer.toString());
                    }

                    String operation = resultset.getString("OPERATION$").trim();

                    RowChangeData rowData = new RowChangeData();

                    if (operation.equals("I"))
                    {
                        if (oneRowChange == null
                                || !oneRowChange.getAction().equals(
                                        ActionType.INSERT))
                        {
                            oneRowChange = new OneRowChange(
                                    cdcSource.getSchema(),
                                    cdcSource.getTable(), ActionType.INSERT);
                            rowData.appendOneRowChange(oneRowChange);
                            data.add(rowData);
                        }
                        parseRowEvent(oneRowChange, false, userColumns);
                    }
                    else if (operation.equals("D"))
                    {
                        if (oneRowChange == null
                                || !oneRowChange.getAction().equals(
                                        ActionType.DELETE))
                        {
                            oneRowChange = new OneRowChange(
                                    cdcSource.getSchema(),
                                    cdcSource.getTable(), ActionType.DELETE);
                            rowData.appendOneRowChange(oneRowChange);
                            data.add(rowData);
                        }
                        parseRowEvent(oneRowChange, true, userColumns);
                    }
                    else if (operation.startsWith("U"))
                    {
                        if (updateRowChange == null)
                        {
                            updateRowChange = new OneRowChange(
                                    cdcSource.getSchema(),
                                    cdcSource.getTable(), ActionType.UPDATE);
                            rowData.appendOneRowChange(updateRowChange);
                            data.add(rowData);
                            if (operation.equals("UO"))
                            {
                                parseRowEvent(updateRowChange, true,
                                        userColumns);
                            }
                            else if (operation.equals("UN"))
                            {
                                parseRowEvent(updateRowChange, false,
                                        userColumns);
                            }
                        }
                        else
                        {
                            if (operation.equals("UO"))
                            {
                                parseRowEvent(updateRowChange, true,
                                        userColumns);
                            }
                            else if (operation.equals("UN"))
                            {
                                parseRowEvent(updateRowChange, false,
                                        userColumns);
                            }
                        }
                    }
                    else
                    {
                        logger.error("Unable to extract data from CDC (operation should be I, D, UO or UN - found "
                                + operation + ")");
                    }
                }
                resultset.close();
                resultset = null;
                stmt.close();
            }
            lastSCN = null;
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(e);
        }
        finally
        {
            if (resultset != null)
                try
                {
                    resultset.close();
                }
                catch (SQLException e)
                {
                    logger.warn("Failed to close resultset");
                }
            resultset = null;

        }
        if (noData)
        {
            logger.warn("Retrieved empty resultset... no data available... sleeping");
            Thread.sleep(1000);
        }
        if (logger.isDebugEnabled())
            logger.debug("Purging window");

        executeQuery(
                "BEGIN DBMS_CDC_SUBSCRIBE.PURGE_WINDOW(subscription_name => 'TUNGSTEN_PUB');END;",
                false);

        if (data.size() > 0)
        {
            DBMSEvent event = new DBMSEvent(String.valueOf(maxSCN), data,
                    sourceTStamp);

            // Mark the event as coming from Oracle.
            event.setMetaDataOption(ReplOptionParams.DBMS_TYPE, Database.ORACLE);

            // Strings are converted to UTF8 rather than using bytes for this
            // extractor.
            event.setMetaDataOption(ReplOptionParams.STRINGS, "utf8");

            return event;
        }
        else
        {
            return null;
        }
    }

    /**
     * Parse a row event.
     */
    private void parseRowEvent(OneRowChange oneRowChange, boolean isKeySpec,
            int cols) throws SQLException
    {

        ArrayList<ColumnSpec> specs = (isKeySpec
                ? oneRowChange.getKeySpec()
                : oneRowChange.getColumnSpec());
        if (specs.isEmpty())
        {
            if (logger.isDebugEnabled())
                logger.debug("Adding column or key specs (not defined so far)");

            for (int i = 1; i <= cols; i++)
            {
                ColumnSpec colSpec = oneRowChange.new ColumnSpec();
                colSpec.setIndex(i);
                colSpec.setType(resultset.getMetaData().getColumnType(i));
                colSpec.setName(resultset.getMetaData().getColumnLabel(i));
                specs.add(colSpec);
            }
        }
        ArrayList<ArrayList<OneRowChange.ColumnVal>> rows = (isKeySpec
                ? oneRowChange.getKeyValues()
                : oneRowChange.getColumnValues());
        ArrayList<OneRowChange.ColumnVal> columns = new ArrayList<ColumnVal>();
        rows.add(columns);

        for (int i = 1; i <= cols; i++)
        {
            ColumnVal value = oneRowChange.new ColumnVal();
            value.setValue((Serializable) resultset.getObject(i));
            if (resultset.wasNull())
                value.setValueNull();
            columns.add(value);
        }
    }

    /**
     * executeStoredProcedure definition.
     */
    private void executeQuery(String query, boolean ignoreError)
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
            else
                logger.warn("Ignoring exception : " + e.getMessage());
        }
        finally
        {
            try
            {
                if (resultset != null)
                    resultset.close();
                if (stmt != null)
                    stmt.close();
            }
            catch (SQLException ignore)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Failed to close resultset", ignore);
            }
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.RawExtractor#extract(java.lang.String)
     */
    @Override
    public DBMSEvent extract(String eventId) throws ReplicatorException,
            InterruptedException
    {
        return null;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.RawExtractor#getCurrentResourceEventId()
     */
    @Override
    public String getCurrentResourceEventId() throws ReplicatorException,
            InterruptedException
    {
        return null;
    }

}
