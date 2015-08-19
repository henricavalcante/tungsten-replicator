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

package com.continuent.tungsten.replicator.database;

import junit.framework.Assert;

import org.junit.Test;

import com.continuent.tungsten.replicator.consistency.ConsistencyTable;

public class TestTable
{

    @Test
    public void testClone()
    {
        Table original = ConsistencyTable.getConsistencyTableDefinition("test");
        Table clone = original.clone();

        int originalKeyCount = original.getKeys().size();

        // Remove PK from the cloned table.
        Key primaryKey = clone.getPrimaryKey();
        Assert.assertNotNull(
                "Consistency table has a PK (did declaration changed?)",
                primaryKey);
        Assert.assertTrue("PK is successfully removed",
                clone.getKeys().remove(primaryKey));
        Assert.assertFalse(
                "Clone table has no PK in key array after removing it", clone
                        .getKeys().contains(primaryKey));

        // Removed PK should still be there in the original table.
        Key originalPrimaryKey = original.getPrimaryKey();
        Assert.assertNotNull(
                "Original table contains PK after PK was removed from the clone",
                originalPrimaryKey);
        Assert.assertTrue("Original table key array still contains PK",
                original.getKeys().contains(originalPrimaryKey));
        Assert.assertEquals(
                "Original table key count didn't change after removing PK from the clone",
                originalKeyCount, original.getKeys().size());
    }

}
