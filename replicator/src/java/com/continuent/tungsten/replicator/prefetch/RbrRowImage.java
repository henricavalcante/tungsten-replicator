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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.continuent.tungsten.replicator.dbms.OneRowChange;

/**
 * Data for a single row, regardless of whether before or after the change.
 */
public class RbrRowImage
{
    /** Images can be one of two types. */
    public enum ImageType
    {
        /** Row values prior to change. */
        BEFORE,
        /** Row values after change. */
        AFTER
    }

    // Properties.
    private final ImageType                      type;
    private final RbrTableChangeSet              changeSet;
    private final List<OneRowChange.ColumnVal>   values;
    private final List<OneRowChange.ColumnSpec>  specs;

    // Index of column names.
    private Map<String, OneRowChange.ColumnSpec> colNames;

    /** Creates a row image with the minimum effort required. */
    public RbrRowImage(ImageType type, RbrTableChangeSet changeSet,
            List<OneRowChange.ColumnSpec> specs,
            List<OneRowChange.ColumnVal> values)
    {
        this.type = type;
        this.changeSet = changeSet;
        this.specs = specs;
        this.values = values;
    }

    /** Returns the image type. */
    public ImageType getType()
    {
        return type;
    }

    // Delegated methods to return schema and table names.
    public String getSchemaName()
    {
        return changeSet.getSchemaName();
    }

    public String getTableName()
    {
        return changeSet.getTableName();
    }

    /**
     * Look up the index of a column name, returning -1 if it is not present.
     * Index values start at 1 for first column.
     */
    public int getColumnIndex(String name)
    {
        if (colNames == null)
        {
            colNames = new HashMap<String, OneRowChange.ColumnSpec>(size());
            for (OneRowChange.ColumnSpec spec : specs)
            {
                colNames.put(spec.getName(), spec);
            }
        }
        OneRowChange.ColumnSpec spec = colNames.get(name);
        if (spec == null)
            return -1;
        else
            return spec.getIndex();
    }

    /**
     * Return a specific column specification by index, where index starts at 1
     * for first column.
     */
    OneRowChange.ColumnSpec getSpec(int index)
    {
        return specs.get(index - 1);
    }

    /** Return a specific column specification by name. */
    OneRowChange.ColumnSpec getSpec(String name)
    {
        int colIndex = getColumnIndex(name);
        if (colIndex == -1)
            return null;
        else
            return getSpec(colIndex);
    }

    /**
     * Return a specific column value by index, where index starts at 1 for
     * first column.
     */
    OneRowChange.ColumnVal getValue(int index)
    {
        return values.get(index - 1);
    }

    /** Return a specific column value by name. */
    OneRowChange.ColumnVal getValue(String name)
    {
        int colIndex = getColumnIndex(name);
        if (colIndex == -1)
            return null;
        else
            return getValue(colIndex);
    }

    /** Return number of columns. */
    int size()
    {
        return specs.size();
    }
}