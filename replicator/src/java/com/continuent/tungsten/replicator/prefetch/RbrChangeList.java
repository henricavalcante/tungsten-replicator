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

import java.util.ArrayList;
import java.util.List;

import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.RowChangeData;

/**
 * Wrapper for a list of row changes to a set of tables, each of which is
 * represented by a table change set.
 */
public class RbrChangeList
{
    private final RowChangeData rowChangeData;

    /** Instantiates a new row change. */
    public RbrChangeList(RowChangeData rowChangeData)
    {
        this.rowChangeData = rowChangeData;
    }

    /** Return all changes from this set in a newly instantiated list. */
    public List<RbrTableChangeSet> getChanges()
    {
        List<RbrTableChangeSet> changes = new ArrayList<RbrTableChangeSet>(
                this.size());
        for (OneRowChange rowChanges : rowChangeData.getRowChanges())
        {
            changes.add(new RbrTableChangeSet(rowChanges));
        }
        return changes;
    }

    /** Returns a single row change set. */
    public RbrTableChangeSet getChange(int index)
    {
        return new RbrTableChangeSet(rowChangeData.getRowChanges().get(index));
    }

    /** Return number of row changes. */
    public int size()
    {
        return rowChangeData.getRowChanges().size();
    }
}