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
 * Contributor(s): Stephane Giron
 */

package com.continuent.tungsten.replicator.heartbeat;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.database.Column;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;
import com.continuent.tungsten.replicator.database.Key;
import com.continuent.tungsten.replicator.database.Table;

/**
 * Provides a definition for a heartbeat table, which measures latency between
 * master and slave. The heartbeat table is created with a single row that is
 * then update to track changes. This class provides methods to update the
 * table.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class HeartbeatTable
{
    private static Logger      logger        = Logger.getLogger(HeartbeatTable.class);

    public static final String TABLE_NAME    = "heartbeat";
    public static final String STAGE_TABLE_NAME    = "stage_xxx_heartbeat";
    private static final long  KEY           = 1;

    private static AtomicLong  saltValue     = new AtomicLong(0);

    private Table              hbTable;
    private Table              hbStageTable;
    private Column             hbTOpcode;
    private Column             hbTSeqno;
    private Column             hbTRowId;
    private Column             hbTCommitTstamp;;
    private Column             hbId;
    private Column             hbSeqno;
    private Column             hbEventId;
    private Column             hbSourceTstamp;
    private Column             hbTargetTstamp;
    private Column             hbLagMillis;
    private Column             hbSalt;
    private Column             hbName;

    String                     sourceTsQuery = null;

    private String             tableType;
    private String             serviceName;

    public HeartbeatTable(String schema, String tableType)
    {
        this.tableType = tableType;
        initialize(schema);
        initializeStage(schema);
    }

    public HeartbeatTable(String schema, String tableType, String serviceName)
    {
        this.tableType = tableType;
        this.serviceName = serviceName;
        initialize(schema);
        initializeStage(schema);
    }

    private void initialize(String schema)
    {
        hbTable = new Table(schema, TABLE_NAME);
        hbId = new Column("id", Types.BIGINT, true); // true => isNotNull
        hbSeqno = new Column("seqno", Types.BIGINT);
        hbEventId = new Column("eventid", Types.VARCHAR, 128);
        hbSourceTstamp = new Column("source_tstamp", Types.TIMESTAMP);
        hbTargetTstamp = new Column("target_tstamp", Types.TIMESTAMP);
        hbLagMillis = new Column("lag_millis", Types.BIGINT);
        hbSalt = new Column("salt", Types.BIGINT);
        hbName = new Column("name", Types.VARCHAR, 128);

        Key hbKey = new Key(Key.Primary);
        hbKey.AddColumn(hbId);

        hbTable.AddColumn(hbId);
        hbTable.AddColumn(hbSeqno);
        hbTable.AddColumn(hbEventId);
        hbTable.AddColumn(hbSourceTstamp);
        hbTable.AddColumn(hbTargetTstamp);
        hbTable.AddColumn(hbLagMillis);
        hbTable.AddColumn(hbSalt);
        hbTable.AddColumn(hbName);
        hbTable.AddKey(hbKey);

        sourceTsQuery = "SELECT source_tstamp from " + schema + "."
                + TABLE_NAME + " where id=" + KEY;
    }

    private void initializeStage(String schema)
    {
        hbStageTable = new Table(schema, STAGE_TABLE_NAME);
        hbTOpcode = new Column("tungsten_opcode", Types.VARCHAR, 2);
        hbTSeqno = new Column("tungsten_seqno", Types.BIGINT, true);
        hbTRowId = new Column("tungsten_row_id", Types.BIGINT, true);
        hbTCommitTstamp = new Column("tungsten_commit_timestamp", Types.TIMESTAMP);

        Key hbKey = new Key(Key.Primary);

        hbKey.AddColumn(hbTOpcode);
        hbKey.AddColumn(hbTSeqno);
        hbKey.AddColumn(hbTRowId);

        hbStageTable.AddColumn(hbTOpcode);
        hbStageTable.AddColumn(hbTSeqno);
        hbStageTable.AddColumn(hbTRowId);
        hbStageTable.AddColumn(hbTCommitTstamp);
        hbStageTable.AddColumn(hbId);
        hbStageTable.AddColumn(hbSeqno);
        hbStageTable.AddColumn(hbEventId);
        hbStageTable.AddColumn(hbSourceTstamp);
        hbStageTable.AddColumn(hbTargetTstamp);
        hbStageTable.AddColumn(hbLagMillis);
        hbStageTable.AddColumn(hbSalt);
        hbStageTable.AddColumn(hbName);
        hbStageTable.AddKey(hbKey);
   }

    /**
     * Returns metadata used to create the underlying heartbeat table.
     */
    public Table getTable()
    {
        return hbTable;
    }

    /**
     * Returns metadata used to create the underlying heartbeat table.
     */
    public Table getStageTable()
    {
        return hbStageTable;
    }

    /**
     * Set up the heartbeat table on the master.
     */
    public void initializeHeartbeatTable(Database database) throws SQLException
    {
        if (logger.isDebugEnabled())
            logger.debug("Initializing heartbeat table");

        // Create the table if it does not exist.
        if (database.findTable(hbTable.getSchema(), hbTable.getName()) == null)
        {
            database.createTable(this.hbTable, false, this.hbTable.getSchema(),
                    tableType, serviceName);
        }

        // Add an initial heartbeat value if needed
        ResultSet res = null;
        PreparedStatement hbRowCount = null;
        int rows = 0;

        try
        {
            hbRowCount = database.prepareStatement("SELECT count(*) from "
                    + this.hbTable.getSchema() + "." + this.hbTable.getName());
            res = hbRowCount.executeQuery();
            if (res.next())
            {
                rows = res.getInt(1);
            }
        }
        finally
        {
            if (res != null)
            {
                try
                {
                    res.close();
                }
                catch (SQLException e)
                {
                }
            }
            if (hbRowCount != null)
            {
                try
                {
                    hbRowCount.close();
                }
                catch (Exception e)
                {
                }
            }
        }

        if (rows == 0)
        {

            hbId.setValue(KEY);
            hbSourceTstamp.setValue(new Timestamp(System.currentTimeMillis()));
            hbSalt.setValue(saltValue.getAndIncrement());
            database.insert(hbTable);
        }
    }

    public void initializeHeartbeatStageTable(Database database) throws SQLException
    {
        if (logger.isDebugEnabled())
            logger.debug("Initializing heartbeat staging table");

        // Create the table if it does not exist.
        if (database.findTable(hbStageTable.getSchema(), hbStageTable.getName()) == null)
            {
                database.createTable(this.hbStageTable, false, this.hbStageTable.getSchema(),
                                     tableType, serviceName);
            }
    }

    /**
     * Execute this call to start a named heartbeat on the master. The heartbeat
     * table update must be logged as we will expect to see it as a DBMSEvent.
     */
    public void startHeartbeat(Database database, String name)
            throws SQLException
    {
        ArrayList<Column> whereClause = new ArrayList<Column>();
        ArrayList<Column> values = new ArrayList<Column>();
        Timestamp now = new Timestamp(System.currentTimeMillis());

        if (logger.isDebugEnabled())
            logger.debug("Processing master heartbeat update: name=" + name
                    + " time=" + now);

        hbId.setValue(KEY);
        whereClause.add(hbId);
        hbSourceTstamp.setValue(now);
        hbSalt.setValue(saltValue.getAndIncrement());
        hbName.setValue(name);
        values.add(hbSourceTstamp);
        values.add(hbSalt);
        values.add(hbName);

        database.update(hbTable, whereClause, values);
    }

    /**
     * Wrapper for startHeartbeat() call.
     */
    public void startHeartbeat(String url, String user, String password,
                               String name, String initScript) throws SQLException
    {
        Database db = null;
        try
            {
                db = DatabaseFactory.createDatabase(url, user, password);
                if (initScript != null)
                    db.setInitScript(initScript);
                db.connect();
                startHeartbeat(db, name);
            }
        finally
            {
                db.close();
            }
    }

    /**
     * Execute this call to fill in heartbeat data on the slave. This call must
     * be invoked after a heartbeat event is applied.
     */
    public void completeHeartbeat(Database database, long seqno, String eventId)
            throws SQLException
    {
        if (logger.isDebugEnabled())
            logger.debug("Processing slave heartbeat update");

        Statement st = null;
        ResultSet rs = null;
        Timestamp sts = new Timestamp(0);
        Timestamp now = new Timestamp(System.currentTimeMillis());
        ArrayList<Column> whereClause = new ArrayList<Column>();
        ArrayList<Column> values = new ArrayList<Column>();

        if (logger.isDebugEnabled())
            logger.debug("Processing slave heartbeat update: " + now);

        // Get the source timestamp.
        try
        {
            st = database.createStatement();
            rs = st.executeQuery(sourceTsQuery);
            if (rs.next())
                sts = rs.getTimestamp(1);
        }
        finally
        {
            if (rs != null)
            {
                try
                {
                    rs.close();
                }
                catch (SQLException e)
                {
                }
            }
            if (st != null)
            {
                try
                {
                    st.close();
                }
                catch (SQLException e)
                {
                }
            }
        }

        // Compute the difference between source and target.
        long lag_millis = now.getTime() - sts.getTime();

        // Update the heartbeat record with target time and difference.
        hbId.setValue(KEY);
        whereClause.add(hbId);

        hbSeqno.setValue(seqno);
        hbEventId.setValue(eventId);
        hbTargetTstamp.setValue(now);
        hbLagMillis.setValue(lag_millis);
        values.add(hbSeqno);
        values.add(hbEventId);
        values.add(hbTargetTstamp);
        values.add(hbLagMillis);

        database.update(hbTable, whereClause, values);
    }

    /**
     * Applies a heartbeat update on the slave. This call is designed for data
     * warehouses that cannot apply a heartbeat using batch loading mechanisms.
     */
    public void applyHeartbeat(Database database, Timestamp sourceTimestamp,
            String name) throws SQLException
    {
        ArrayList<Column> whereClause = new ArrayList<Column>();
        ArrayList<Column> values = new ArrayList<Column>();

        if (logger.isDebugEnabled())
            logger.debug("Applying heartbeat to slave: name=" + name
                    + " sourceTstamp=" + sourceTimestamp);

        hbId.setValue(KEY);
        whereClause.add(hbId);
        hbSourceTstamp.setValue(sourceTimestamp);
        hbName.setValue(name);
        values.add(hbSourceTstamp);
        values.add(hbName);
        database.update(hbTable, whereClause, values);
    }
}
