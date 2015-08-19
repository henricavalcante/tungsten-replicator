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

package com.continuent.tungsten.replicator.prefetch;

import java.sql.Types;
import java.util.ArrayList;

import junit.framework.TestCase;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.dbms.RowChangeData;

/**
 * Tests behavior of wrapper classes for row events. These test generate raw row
 * event data and then feed them to the wrapper classes.
 */
public class RbrChangeTest extends TestCase
{
    static Logger logger = Logger.getLogger(RbrChangeTest.class);

    /**
     * Verify we can retrieve data for an insert. Inserts have an after image
     * constructed from the column values.
     */
    public void testRbrInsert() throws Exception
    {
        // Create table change header.
        OneRowChange oneRowChange = generateRowChange("foo", "bar",
                RowChangeData.ActionType.INSERT);

        // Add specifications and values for columns. This is the before value.
        oneRowChange.setColumnSpec(generateSpec(oneRowChange));
        oneRowChange.setColumnValues(generateValues(oneRowChange, 1, "two"));

        // Generate a table change set and check header data.
        RbrTableChangeSet changeSet = new RbrTableChangeSet(oneRowChange);
        assertEquals("check schema", "foo", changeSet.getSchemaName());
        assertEquals("check table", "bar", changeSet.getTableName());
        assertEquals("check rows", 1, changeSet.size());

        // Get the first row change and check operation.
        RbrRowChange rowChange = changeSet.getRowChange(0);
        assertEquals("check schema", "foo", rowChange.getSchemaName());
        assertEquals("check table", "bar", rowChange.getTableName());
        assertTrue(rowChange.isInsert());
        assertFalse(rowChange.isUpdate());
        assertFalse(rowChange.isDelete());

        // Get the before image and ensure it is null.
        RbrRowImage before = rowChange.getBeforeImage();
        assertNull(before);

        // Get the after image and check types and values.
        RbrRowImage after = rowChange.getAfterImage();
        assertNotNull(after);
        assertEquals("c1 is c1", "c1", after.getSpec("c1").getName());
        assertEquals("c2 is char", Types.CHAR, after.getSpec(2).getType());
        assertEquals("key is 1", 1, after.getValue("c1").getValue());
        assertEquals("col is two", "two", after.getValue(2).getValue());
    }

    /**
     * Verify we can retrieve data for an update. Updates have a before image
     * constructed from the keys and an after image constructed from the column
     * values.
     */
    public void testRbrUpdate() throws Exception
    {
        // Create table change header.
        OneRowChange oneRowChange = generateRowChange("foo", "bar",
                RowChangeData.ActionType.UPDATE);

        // Add specifications and values for columns. This is the after value.
        oneRowChange.setColumnSpec(generateSpec(oneRowChange));
        oneRowChange.setColumnValues(generateValues(oneRowChange, 1, "two"));

        // Add specifications and values for keys. This is the before value.
        oneRowChange.setKeySpec(generateSpec(oneRowChange));
        oneRowChange.setKeyValues(generateValues(oneRowChange, 1, "one"));

        // Generate a table change set and check header data.
        RbrTableChangeSet changeSet = new RbrTableChangeSet(oneRowChange);
        assertEquals("check schema", "foo", changeSet.getSchemaName());
        assertEquals("check table", "bar", changeSet.getTableName());
        assertEquals("check rows", 1, changeSet.size());

        // Get the first row change and check operation.
        RbrRowChange rowChange = changeSet.getRowChange(0);
        assertEquals("check schema", "foo", rowChange.getSchemaName());
        assertEquals("check table", "bar", rowChange.getTableName());
        assertTrue(rowChange.isUpdate());
        assertFalse(rowChange.isDelete());
        assertFalse(rowChange.isInsert());

        // Get the before image and check types and values.
        RbrRowImage before = rowChange.getBeforeImage();
        assertNotNull(before);
        assertEquals("c1 is int", Types.INTEGER, before.getSpec(1).getType());
        assertEquals("c2 is c2", "c2", before.getSpec("c2").getName());
        assertEquals("key is 1", 1, before.getValue(1).getValue());
        assertEquals("col is one", "one", before.getValue("c2").getValue());

        // Get the after image and check types and values.
        RbrRowImage after = rowChange.getAfterImage();
        assertNotNull(after);
        assertEquals("c1 is c1", "c1", after.getSpec("c1").getName());
        assertEquals("c2 is char", Types.CHAR, after.getSpec(2).getType());
        assertEquals("key is 1", 1, after.getValue("c1").getValue());
        assertEquals("col is two", "two", after.getValue(2).getValue());
    }

    /**
     * Verify we can retrieve data for a delete. Deletes have only a before
     * image constructed from the keys.
     */
    public void testRbrDelete() throws Exception
    {
        // Create table change header.
        OneRowChange oneRowChange = generateRowChange("foo", "bar",
                RowChangeData.ActionType.DELETE);

        // Add specifications and values for keys. This is the before value.
        oneRowChange.setKeySpec(generateSpec(oneRowChange));
        oneRowChange.setKeyValues(generateValues(oneRowChange, 1, "one"));

        // Generate a table change set and check header data.
        RbrTableChangeSet changeSet = new RbrTableChangeSet(oneRowChange);
        assertEquals("check schema", "foo", changeSet.getSchemaName());
        assertEquals("check table", "bar", changeSet.getTableName());
        assertEquals("check rows", 1, changeSet.size());

        // Get the first row change and check operation.
        RbrRowChange rowChange = changeSet.getRowChange(0);
        assertEquals("check schema", "foo", rowChange.getSchemaName());
        assertEquals("check table", "bar", rowChange.getTableName());
        assertTrue(rowChange.isDelete());
        assertFalse(rowChange.isUpdate());
        assertFalse(rowChange.isInsert());

        // Get the before image and check types and values.
        RbrRowImage before = rowChange.getBeforeImage();
        assertNotNull(before);
        assertEquals("c1 is int", Types.INTEGER, before.getSpec(1).getType());
        assertEquals("c2 is c2", "c2", before.getSpec("c2").getName());
        assertEquals("key is 1", 1, before.getValue(1).getValue());
        assertEquals("col is one", "one", before.getValue("c2").getValue());

        // Get the after image and ensure it is null.
        RbrRowImage after = rowChange.getAfterImage();
        assertNull(after);
    }

    // Generate table change header.
    private OneRowChange generateRowChange(String schema, String table,
            RowChangeData.ActionType action)
    {
        // Create table change header.
        OneRowChange oneRowChange = new OneRowChange();
        oneRowChange.setSchemaName("foo");
        oneRowChange.setTableName("bar");
        oneRowChange.setTableId(1);
        oneRowChange.setAction(action);
        return oneRowChange;
    }

    // Generate specifications.
    private ArrayList<ColumnSpec> generateSpec(OneRowChange oneRowChange)
    {
        ArrayList<ColumnSpec> spec = new ArrayList<ColumnSpec>(2);
        ColumnSpec c1 = oneRowChange.new ColumnSpec();
        c1.setIndex(1);
        c1.setName("c1");
        c1.setType(Types.INTEGER);
        spec.add(c1);
        ColumnSpec c2 = oneRowChange.new ColumnSpec();
        c2.setIndex(2);
        c2.setName("c2");
        c2.setType(Types.CHAR);
        c2.setLength(10);
        spec.add(c2);
        return spec;
    }

    // Generate values.
    private ArrayList<ArrayList<ColumnVal>> generateValues(
            OneRowChange oneRowChange, int v1, String v2)
    {
        ArrayList<ArrayList<ColumnVal>> list = new ArrayList<ArrayList<ColumnVal>>(
                1);
        ArrayList<ColumnVal> values = new ArrayList<ColumnVal>(2);
        ColumnVal cv1 = oneRowChange.new ColumnVal();
        cv1.setValue(new Integer(v1));
        values.add(cv1);
        ColumnVal cv2 = oneRowChange.new ColumnVal();
        cv2.setValue(v2);
        values.add(cv2);
        list.add(values);
        return list;
    }
}