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
 * Initial developer(s): Scott Martin
 * Contributor(s): Robert Hodges, Stephane Giron
 */

package com.continuent.tungsten.replicator.database;

import java.util.ArrayList;

/**
 * This class defines a Key
 * 
 * @author <a href="mailto:scott.martin@continuent.com">Scott Martin</a>
 * @version 1.0
 */
public class Key
{
    public static final int IllegalType    = 0;
    public static final int NonUnique      = 1;
    public static final int Primary        = 2;
    public static final int Unique         = 3;

    int                     type           = Key.NonUnique;
    String                  name           = null;
    long                    maxCardinality = 0;
    ArrayList<Column>       columns        = null;

    /**
     * Creates a new key with no information.
     */
    public Key()
    {
        this.columns = new ArrayList<Column>();
    }

    /**
     * Creates a new key including the type.
     */
    public Key(int type)
    {
        this();
        this.type = type;
    }

    /**
     * Returns the key type.
     */
    public int getType()
    {
        return this.type;
    }

    /**
     * Sets the key type.
     */
    public synchronized void setType(int type)
    {
        this.type = type;
    }

    /**
     * Returns the index name corresponding to this key.
     */
    public synchronized String getName()
    {
        return name;
    }

    /**
     * Sets the index name corresponding to this key.
     */
    public synchronized void setName(String name)
    {
        this.name = name;
    }

    /**
     * Adds a column to the key definition.
     */
    public void AddColumn(Column column)
    {
        columns.add(column);
    }

    /**
     * Adds a column to the key definition.
     */
    public void AddColumn(Column column, int index)
    {
        if (index - 1 > columns.size())
            columns.add(column);
        else
            columns.add(index - 1, column);
    }

    /**
     * Returns the columns in this key.
     */
    public ArrayList<Column> getColumns()
    {
        return this.columns;
    }

    /**
     * Returns the maximum cardinality of index columns or 0 if cardinality is
     * unknown. Cardinality is used in making decisions about index specificity.
     */
    public synchronized long getMaxCardinality()
    {
        return maxCardinality;
    }

    /**
     * Sets the maximum index cardinality.
     */
    public synchronized void setMaxCardinality(long maxCardinality)
    {
        this.maxCardinality = maxCardinality;
    }

    /**
     * Returns true if key is unique, i.e., has one and only one value per
     * entry.
     */
    public boolean isUnique()
    {
        switch (this.type)
        {
            case IllegalType :
            case NonUnique :
                return false;
            case Primary :
            case Unique :
                return true;
            default :
                return false;
        }
    }

    /**
     * Returns true if this is a primary key.
     */
    public boolean isPrimaryKey()
    {
        return (type == Primary);
    }

    /**
     * Returns true if this is a secondary (i.e., non-primary) key.
     */
    public boolean isSecondaryKey()
    {
        return (type == Unique || type == NonUnique);
    }

    /**
     * Returns the number of columns in the key.
     */
    public int size()
    {
        if (columns == null)
            return 0;
        else
            return columns.size();
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        StringBuffer sb = new StringBuffer("Key name=");
        sb.append(name);
        sb.append(" type=");
        switch (this.type)
        {
            case NonUnique :
                sb.append("NonUnique");
                break;
            case Primary :
                sb.append("Primary");
                break;
            case Unique :
                sb.append("Unique");
                break;
            default :
                sb.append("IllegalType");
                break;
        }
        sb.append(" maxCardinality=").append(maxCardinality);
        sb.append(" columns=");
        for (int i = 0; i < this.columns.size(); i++)
        {
            if (i > 0)
                sb.append(",");
            sb.append(columns.get(i).getName());
        }
        return sb.toString();
    }
}