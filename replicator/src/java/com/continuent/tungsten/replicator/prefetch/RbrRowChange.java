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

import com.continuent.tungsten.replicator.dbms.OneRowChange;

/**
 * Encapsulates changes on a single row in a single table, providing access to
 * the before and after images of the row data.
 */
public class RbrRowChange
{
    private final RbrTableChangeSet changeSet;
    private final int               index;

    /** Instantiates a single row change. */
    RbrRowChange(RbrTableChangeSet changeSet, int index)
    {
        this.changeSet = changeSet;
        this.index = index;
    }

    // Delegate methods on change set.
    public boolean isInsert()
    {
        return changeSet.isInsert();
    }

    public boolean isUpdate()
    {
        return changeSet.isUpdate();
    }

    public boolean isDelete()
    {
        return changeSet.isDelete();
    }

    public String getSchemaName()
    {
        return changeSet.getSchemaName();
    }

    public String getTableName()
    {
        return changeSet.getTableName();
    }

    /**
     * Returns the before image as a separate object or null if it does not
     * exist.
     */
    public RbrRowImage getBeforeImage()
    {
        OneRowChange oneRowChange = changeSet.getOneRowChange();
        if (isInsert())
        {
            // Inserts have no before data.
            return null;
        }
        else
        {
            // For deletes and updates the keys.
            return new RbrRowImage(RbrRowImage.ImageType.BEFORE,
                    this.changeSet, oneRowChange.getKeySpec(), oneRowChange
                            .getKeyValues().get(index));
        }
    }

    /**
     * Returns the before image as a separate object or null if it does not
     * exist;
     */
    public RbrRowImage getAfterImage()
    {
        OneRowChange oneRowChange = changeSet.getOneRowChange();
        if (isDelete())
        {
            // Deletes have no after data.
            return null;
        }
        else
        {
            // For inserts and updates take the values.
            return new RbrRowImage(RbrRowImage.ImageType.AFTER, this.changeSet,
                    oneRowChange.getColumnSpec(), oneRowChange
                            .getColumnValues().get(index));
        }
    }

    /** Return number of columns. */
    int size()
    {
        OneRowChange oneRowChange = changeSet.getOneRowChange();
        if (isDelete())
            return oneRowChange.getKeyValues().size();
        else
            return oneRowChange.getColumnValues().size();
    }
}