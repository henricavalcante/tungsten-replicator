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
 * Wrapper for a set of changes on a single table. All changes share the same
 * table as well as change type.
 */
public class RbrTableChangeSet
{
    private final OneRowChange oneRowChange;

    /** Instantiates a single table change set. */
    RbrTableChangeSet(OneRowChange oneRowChange)
    {
        this.oneRowChange = oneRowChange;
    }

    /** Returns true if this is an insert. */
    public boolean isInsert()
    {
        return oneRowChange.getAction() == RowChangeData.ActionType.INSERT;
    }

    /** Returns true if this is an update. */
    public boolean isUpdate()
    {
        return oneRowChange.getAction() == RowChangeData.ActionType.UPDATE;
    }

    /** Returns true if this is a delete. */
    public boolean isDelete()
    {
        return oneRowChange.getAction() == RowChangeData.ActionType.DELETE;
    }

    /** Returns the schema to which underlying table applies. */
    public String getSchemaName()
    {
        return oneRowChange.getSchemaName();
    }

    /** Returns the table to change applies. */
    public String getTableName()
    {
        return oneRowChange.getTableName();
    }

    /** Return number of rows. */
    int size()
    {
        if (isDelete())
            return oneRowChange.getKeyValues().size();
        else
            return oneRowChange.getColumnValues().size();
    }

    /** Return all row changes. */
    public List<RbrRowChange> getRowChanges()
    {
        int size = size();
        ArrayList<RbrRowChange> changes = new ArrayList<RbrRowChange>(size);
        for (int i = 0; i < size; i++)
        {
            changes.add(new RbrRowChange(this, i));
        }
        return changes;
    }

    /**
     * Return a single row change.
     */
    public RbrRowChange getRowChange(int index)
    {
        return new RbrRowChange(this, index);
    }

    /**
     * Returns the underlying source data.
     */
    OneRowChange getOneRowChange()
    {
        return oneRowChange;
    }
}