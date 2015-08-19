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

import java.util.ArrayList;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.RowChangeData;

/**
 * Implements a test of row change functionality including functions to check
 * presence of types.
 */
public class RowChangeMetadataTest
{
    private EventGenerationHelper eventHelper = new EventGenerationHelper();

    /**
     * Setup.
     * 
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
    }

    /**
     * Teardown.
     * 
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

    /**
     * Verify that row change metadata correctly counts the number of types
     * present in an insert row change. 
     */
    @Test
    public void testRowChangeTypesCountInsert() throws Exception
    {
        // Create a single insert row change event.
        String names[] = new String[2];
        Integer values[] = new Integer[2];
        for (int i = 0; i < names.length; i++)
        {
            names[i] = "data-" + i;
            values[i] = i;
        }
        ReplDBMSEvent insertEvent = eventHelper.eventFromRowInsert(0, "schema",
                "table", names, values, 0, true);

        // Check for presence of data types.
        ArrayList<DBMSData> eventData = insertEvent.getData();
        RowChangeData rowChangeData = (RowChangeData) eventData.get(0);
        OneRowChange rowChange = rowChangeData.getRowChanges().get(0);

        // We expect to find a two VARCHAR values, since event helper types
        // things as strings.
        Assert.assertEquals("Expect two varchars", 2,
                rowChange.typeCount(java.sql.Types.VARCHAR));
        Assert.assertTrue("Expect varchar to be present",
                rowChange.hasType(java.sql.Types.VARCHAR));

        // We do not expect to find any varbinary values.
        Assert.assertEquals("Varbinary count should be 0", 0,
                rowChange.typeCount(java.sql.Types.VARBINARY));
        Assert.assertFalse("Varbinary should not be present",
                rowChange.hasType(java.sql.Types.VARBINARY));
    }
    /**
     * Verify that row change metadata correctly counts the number of types
     * present in a delete. 
     */
    @Test
    public void testRowChangeTypesCountDelete() throws Exception
    {
        // Create a single insert row change event.
        String names[] = new String[1];
        Integer values[] = new Integer[1];
        for (int i = 0; i < names.length; i++)
        {
            names[i] = "data-" + i;
            values[i] = i;
        }
        ReplDBMSEvent insertEvent = eventHelper.eventFromRowDelete(0, "schema",
                "table", names, values, 0, true);

        // Check for presence of data types.
        ArrayList<DBMSData> eventData = insertEvent.getData();
        RowChangeData rowChangeData = (RowChangeData) eventData.get(0);
        OneRowChange rowChange = rowChangeData.getRowChanges().get(0);

        // We expect to find a two VARCHAR values, since event helper types
        // things as strings.
        Assert.assertEquals("Expect one varchar", 1,
                rowChange.typeCount(java.sql.Types.VARCHAR));
        Assert.assertTrue("Expect varchar to be present",
                rowChange.hasType(java.sql.Types.VARCHAR));

        // We do not expect to find any set values.
        Assert.assertEquals("Varbinary count should be 0", 0,
                rowChange.typeCount(java.sql.Types.VARBINARY));
        Assert.assertFalse("Varbinary should not be present",
                rowChange.hasType(java.sql.Types.VARBINARY));
    }
}