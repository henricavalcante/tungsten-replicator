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

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Column;
import com.continuent.tungsten.replicator.database.Key;
import com.continuent.tungsten.replicator.database.Table;

/**
 * Encapsulates data and statement generation logic for prefetching slices of
 * indexes. This includes holding the key definition as well as the associated
 * values we are selecting from the key.
 */
public class KeySelect
{
    // Parameters.
    private Table    table;
    private Key      key;
    private Object[] values;
    private long     lastInvocation = 0;

    private String   invocationKey;

    /** Create a new instance. */
    public KeySelect(Table table, Key key)
    {
        this.table = table;
        this.key = key;
        this.values = new Object[key.size()];
    }

    public Table getTable()
    {
        return table;
    }

    public Key getKey()
    {
        return key;
    }

    public List<Object> getValues()
    {
        List<Object> valueList = new ArrayList<Object>(values.length);
        for (int i = 0; i < values.length; i++)
            valueList.add(values[i]);
        return valueList;
    }

    public long getLastInvocation()
    {
        return lastInvocation;
    }

    public void setLastInvocation(long lastInvocation)
    {
        this.lastInvocation = lastInvocation;
    }

    /** Returns the number of columns in the key. */
    public int size()
    {
        return key.size();
    }

    /** Returns true if any of the values in the key select is a null. */
    public boolean hasNulls()
    {
        if (values.length != key.getColumns().size())
            return true;

        for (int i = 0; i < values.length; i++)
        {
            if (values[i] == null)
                return true;
        }

        return false;
    }

    /**
     * Sets a column value for the select.
     * 
     * @param index Column index in key where 1 is the first index
     * @param value Value to set
     */
    public void setValue(int index, Object value) throws ReplicatorException
    {
        if (index < 1 || index > key.size())
            throw new ReplicatorException(
                    "Out-of-bounds index value in prefetch request: " + index);

        values[index - 1] = value;
        invocationKey = null;
    }

    /**
     * Gets a column value by index.
     * 
     * @param index Column index in key where 1 is the first index
     */
    public Object getValue(int index)
    {
        return values[index - 1];
    }

    /**
     * Sets a column value for the select.
     * 
     * @param colName Name of the column in key
     * @param value Value to set
     */
    public void setValue(String colName, Object value)
            throws ReplicatorException
    {
        int index = findIndex(colName);
        if (index > 0)
            setValue(index, value);
        else
            throw new ReplicatorException(
                    "Invalid column name in prefetch request: " + colName);
    }

    /**
     * Gets a column value by name.
     * 
     * @param colName Name of the column in key
     */
    public Object getValue(String colName) throws ReplicatorException
    {
        int index = findIndex(colName);
        if (index > 0)
            return getValue(index);
        else
            throw new ReplicatorException(
                    "Invalid column name in prefetch request: " + colName);
    }

    // Finds the index of a column within the key.
    private int findIndex(String colName)
    {
        List<Column> keyCols = key.getColumns();
        for (int i = 1; i <= keyCols.size(); i++)
        {
            String keyColName = keyCols.get(i - 1).getName();
            if (colName.equals(keyColName))
            {
                return i;
            }
        }
        return -1;
    }

    /**
     * Creates generic prefetch prepared statement text appropriate to the index
     * type including drop-outs for key values. For a primary key, we use SELECT
     * * to force reading of the base data, whereas for a secondary index we use
     * SELECT count(*), which reads only index pages.
     */
    public String createPrefetchSelect()
    {
        StringBuffer sb = new StringBuffer();

        // Select all for primary, otherwise use count(*) to force
        // scan of index pages only.
        if (key.isPrimaryKey())
            sb.append("SELECT * FROM `");
        else
            sb.append("SELECT count(*) FROM `");
        sb.append(table.getSchema());
        sb.append("`.`");
        sb.append(table.getName());
        sb.append("`");
        // Use force index hint to ensure our chosen index loads.
        sb.append(" FORCE INDEX (").append(key.getName()).append(")");
        // Supply key columns on which to join.
        sb.append(" WHERE ");
        for (int i = 0; i < key.getColumns().size(); i++)
        {
            Column col = key.getColumns().get(i);
            if (i > 0)
                sb.append(" AND ");
            sb.append(col.getName());
            sb.append("=?");
        }
        sb.append(String.format(
                " /* TUNGSTEN PREFETCH: schema=%s table=%s, key=%s */",
                table.getSchema(), table.getName(), key.getName()));

        return sb.toString();
    }

    /**
     * Returns a reasonably efficient key for this select that incorporates both
     * the fully qualified key name as well as select values. This key can be
     * used for caching information about this particular index prefetch query.
     */
    public String generateKey()
    {
        if (invocationKey == null)
        {
            StringBuffer sb = new StringBuffer();
            sb.append(table.getSchema());
            sb.append(".").append(table.getName());
            sb.append(".").append(key.getName());
            for (Object v : values)
            {
                sb.append("-").append(v.hashCode());
            }
            invocationKey = sb.toString();
        }

        return invocationKey;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        StringBuffer sb = new StringBuffer(this.getClass().getSimpleName());
        sb.append(" ").append(table.getSchema());
        sb.append(".").append(table.getName());
        sb.append(".").append(key.getName());
        for (int i = 1; i <= size(); i++)
        {
            sb.append(" ");
            sb.append(i);
            sb.append("=[");
            sb.append(getValue(i));
            sb.append("]");
        }
        return sb.toString();
    }
}