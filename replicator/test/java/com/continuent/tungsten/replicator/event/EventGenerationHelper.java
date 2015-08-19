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

import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;

import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;

/**
 * Implements a simple helper to generate events for testing.
 */
public class EventGenerationHelper
{
    /**
     * Creates an event from a query.
     * 
     * @param seqno Sequence number
     * @param defaultSchema Default schema
     * @param query
     * @return A properly constructed event.
     */
    public ReplDBMSEvent eventFromStatement(long seqno, String defaultSchema,
            String query, int fragNo, boolean lastFrag)
    {
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        ArrayList<DBMSData> t = new ArrayList<DBMSData>();
        t.add(new StatementData(query, ts.getTime(), defaultSchema));
        DBMSEvent dbmsEvent = new DBMSEvent(new Long(seqno).toString(), null,
                t, lastFrag, new Timestamp(System.currentTimeMillis()));
        ReplDBMSEvent replDbmsEvent = new ReplDBMSEvent(seqno, (short) fragNo,
                lastFrag, "NONE", 0, ts, dbmsEvent);
        return replDbmsEvent;
    }

    /**
     * Creates an event from a query.
     * 
     * @param seqno Sequence number
     * @param defaultSchema Default schema
     * @param query
     * @return A properly constructed event.
     */
    public ReplDBMSEvent eventFromBinaryStatement(long seqno,
            String defaultSchema, byte[] queryAsBytes, int fragNo,
            boolean lastFrag, String charset)
    {
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        ArrayList<DBMSData> t = new ArrayList<DBMSData>();
        StatementData sd = new StatementData(null, ts.getTime(), defaultSchema);
        sd.setQuery(queryAsBytes);
        sd.setCharset(charset);
        t.add(sd);
        DBMSEvent dbmsEvent = new DBMSEvent(new Long(seqno).toString(), null,
                t, lastFrag, new Timestamp(System.currentTimeMillis()));
        ReplDBMSEvent replDbmsEvent = new ReplDBMSEvent(seqno, (short) fragNo,
                lastFrag, "NONE", 0, ts, dbmsEvent);
        return replDbmsEvent;
    }

    /**
     * Utility method to generate a non-fragment event from a statement.
     */
    public ReplDBMSEvent eventFromStatement(long seqno, String defaultSchema,
            String query)
    {
        return eventFromStatement(seqno, defaultSchema, query, 0, true);
    }

    /**
     * Convenience method to create a transaction event from a row insert using
     * the current time as the commit time.
     */
    public ReplDBMSEvent eventFromRowInsert(long seqno, String schema,
            String table, String[] names, Object[] values, int fragNo,
            boolean lastFrag)
    {
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        return eventFromRowInsert(seqno, schema, table, names, values, fragNo,
                lastFrag, ts);
    }

    /**
     * Convenience method to create a transaction event from a row delete using
     * the current time as the commit time.
     */
    public ReplDBMSEvent eventFromRowDelete(long seqno, String schema,
            String table, String[] names, Object[] values, int fragNo,
            boolean lastFrag)
    {
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        return eventFromRowDelete(seqno, schema, table, names, values, fragNo,
                lastFrag, ts);
    }

    /**
     * Creates a transaction event from a row insert.
     * 
     * @param seqno Sequence number
     * @param schema Schema name
     * @param table Table name
     * @param names Column names
     * @param values Value columns
     * @param fragNo Fragment number within transaction
     * @param lastFrag If true, last fragment in the transaction
     * @param commitTime Time of commit
     * @return A fully formed event containing a single row change
     */
    public ReplDBMSEvent eventFromRowInsert(long seqno, String schema,
            String table, String[] names, Object[] values, int fragNo,
            boolean lastFrag, Timestamp commitTime)
    {
        // Create row change data. This will contain a set of updates.
        OneRowChange rowChange = generateRowChange(schema, table,
                RowChangeData.ActionType.INSERT);

        // Add specifications and values for columns only. Inserts do not
        // normally have key specifications unless they are added later by a
        // filter.
        rowChange.setColumnSpec(generateSpec(rowChange, names));
        rowChange.setColumnValues(generateValues(rowChange, values));

        // Wrap the row change in a change set.
        RowChangeData rowChangeData = new RowChangeData();
        rowChangeData.appendOneRowChange(rowChange);

        // Add the change set to the event array and generate a DBMS
        // transaction.
        ArrayList<DBMSData> data = new ArrayList<DBMSData>();
        data.add(rowChangeData);

        DBMSEvent dbmsEvent = new DBMSEvent(new Long(seqno).toString(), null,
                data, lastFrag, new Timestamp(System.currentTimeMillis()));
        ReplDBMSEvent replDbmsEvent = new ReplDBMSEvent(seqno, (short) fragNo,
                lastFrag, "NONE", 0, commitTime, dbmsEvent);
        return replDbmsEvent;
    }

    /**
     * Creates a transaction event from a row delete.
     * 
     * @param seqno Sequence number
     * @param schema Schema name
     * @param table Table name
     * @param names Column names
     * @param values Value columns
     * @param fragNo Fragment number within transaction
     * @param lastFrag If true, last fragment in the transaction
     * @param commitTime Time of commit
     * @return A fully formed event containing a single row change
     */
    public ReplDBMSEvent eventFromRowDelete(long seqno, String schema,
            String table, String[] names, Object[] values, int fragNo,
            boolean lastFrag, Timestamp commitTime)
    {
        // Create row change data. This will contain a set of updates.
        OneRowChange rowChange = generateRowChange(schema, table,
                RowChangeData.ActionType.DELETE);

        // Add specifications and values for keys only. Deletes do not
        // have column specifications unless they are added later by a
        // filter.
        rowChange.setKeySpec(generateSpec(rowChange, names));
        rowChange.setKeyValues(generateValues(rowChange, values));

        // Wrap the row change in a change set.
        RowChangeData rowChangeData = new RowChangeData();
        rowChangeData.appendOneRowChange(rowChange);

        // Add the change set to the event array and generate a DBMS
        // transaction.
        ArrayList<DBMSData> data = new ArrayList<DBMSData>();
        data.add(rowChangeData);

        DBMSEvent dbmsEvent = new DBMSEvent(new Long(seqno).toString(), null,
                data, lastFrag, new Timestamp(System.currentTimeMillis()));
        ReplDBMSEvent replDbmsEvent = new ReplDBMSEvent(seqno, (short) fragNo,
                lastFrag, "NONE", 0, commitTime, dbmsEvent);
        return replDbmsEvent;
    }

    // Generate table change header.
    private OneRowChange generateRowChange(String schema, String table,
            RowChangeData.ActionType action)
    {
        // Create table change header.
        OneRowChange oneRowChange = new OneRowChange();
        oneRowChange.setSchemaName(schema);
        oneRowChange.setTableName(table);
        oneRowChange.setTableId(1);
        oneRowChange.setAction(action);
        return oneRowChange;
    }

    // Generate specifications for a row event. This currently handles strings
    // only.
    private ArrayList<ColumnSpec> generateSpec(OneRowChange oneRowChange,
            String[] names)
    {
        // Generate for array of column specification data.
        ArrayList<ColumnSpec> specArray = new ArrayList<ColumnSpec>(
                names.length);

        // Iterate through the name array, adding a specification for each.
        for (int i = 0; i < names.length; i++)
        {
            ColumnSpec colSpec = oneRowChange.new ColumnSpec();
            colSpec.setIndex(i + 1);
            colSpec.setName(names[i]);
            colSpec.setType(Types.VARCHAR);
            specArray.add(colSpec);
        }

        return specArray;
    }

    // Generate values for a row event.
    private ArrayList<ArrayList<ColumnVal>> generateValues(
            OneRowChange oneRowChange, Object[] values)
    {
        // Create the array to hold the values.
        ArrayList<ArrayList<ColumnVal>> valueList = new ArrayList<ArrayList<ColumnVal>>(
                1);
        ArrayList<ColumnVal> valueColumns = new ArrayList<ColumnVal>(
                values.length);

        // Iterate through the value columns and add each value.
        for (int i = 0; i < values.length; i++)
        {
            ColumnVal cv1 = oneRowChange.new ColumnVal();
            cv1.setValue(values[i].toString());
            valueColumns.add(cv1);
        }
        valueList.add(valueColumns);

        // Return the resulting list.
        return valueList;
    }
}