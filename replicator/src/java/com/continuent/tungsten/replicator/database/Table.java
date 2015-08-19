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
 * Contributor(s): Stephane Giron, Robert Hodges
 */

package com.continuent.tungsten.replicator.database;

import java.sql.PreparedStatement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.continuent.tungsten.replicator.scripting.SqlWrapper;

/**
 * This class defines a table
 * 
 * @author <a href="mailto:scott.martin@continuent.com">Scott Martin</a>
 * @version 1.0
 */
public class Table
{
    String                  schema          = null;
    String                  name            = null;
    boolean                 temporary       = false;
    ArrayList<Column>       allColumns      = null;
    ArrayList<Column>       nonKeyColumns   = null;
    ArrayList<Key>          keys            = null;
    ArrayList<Key>          uniqueKeys      = null;
    Key                     primaryKey      = null;
    PreparedStatement       statements[];
    // Cache of prepared statements
    boolean                 cacheStatements = false;

    static public final int INSERT          = 0;
    static public final int UPDATE1         = 1;
    static public final int UPDATE2         = 2;
    static public final int DELETE          = 3;
    static public final int NPREPPED        = 4;

    // scn is eventually used for caching purpose. This table object will be
    // cached and reused if possible.
    private String          scn;

    // tableId as found in MySQL binlog can be used to detect schema changes.
    // Here, it has the same purpose as previous scn field
    private long            tableId;

    /**
     * Creates a new <code>Table</code> object
     */
    public Table(String schema, String name)
    {
        int i;

        this.schema = schema;
        this.name = name;
        this.allColumns = new ArrayList<Column>();
        this.nonKeyColumns = new ArrayList<Column>();
        this.keys = new ArrayList<Key>();
        this.uniqueKeys = new ArrayList<Key>();
        this.scn = null;
        this.tableId = -1;
        this.statements = new PreparedStatement[Table.NPREPPED];
        this.cacheStatements = false;
        // Following probably not needed
        for (i = 0; i < Table.NPREPPED; i++)
            this.statements[i] = null;
    }

    public Table(String schema, String name, boolean cacheStatements)
    {
        this(schema, name);
        this.cacheStatements = cacheStatements;
    }

    public boolean getCacheStatements()
    {
        return this.cacheStatements;
    }

    void purge(ArrayList<Column> purgeValues, ArrayList<Column> fromList)
    {
        int idx;
        Iterator<Column> i = purgeValues.iterator();
        while (i.hasNext())
        {
            Column c1 = i.next();
            if ((idx = fromList.indexOf(c1)) == -1)
                continue;
            fromList.remove(idx);
        }
    }

    public PreparedStatement getStatement(int statementNumber)
    {
        return this.statements[statementNumber];
    }

    public void setStatement(int statementNumber, PreparedStatement statement)
    {
        // This will leak prepared statements if a statement already
        // exists in the slot but I currently do not want to
        // have a "Table" know about a "Database" which is what we would need
        // to close these statements.
        this.statements[statementNumber] = statement;
    }

    public void AddColumn(Column column)
    {
        allColumns.add(column);
        nonKeyColumns.add(column);
    }

    /**
     * Adds a key definition to the table. This method maintains non-key columns
     * automatically, as well as unique keys.
     */
    public void AddKey(Key key)
    {
        keys.add(key);
        if (key.getType() == Key.Primary)
        {
            primaryKey = key;
            purge(key.getColumns(), nonKeyColumns);
        }
        else if (key.getType() == Key.Unique)
        {
            uniqueKeys.add(key);
        }
    }

    /** Reset keys, which also resets non-key columns and unique keys. */
    public void clearKeys()
    {
        keys.clear();
        uniqueKeys.clear();
        nonKeyColumns.clear();
        nonKeyColumns.addAll(this.allColumns);
    }

    public String getSchema()
    {
        return schema;
    }

    public void setSchema(String schema)
    {
        this.schema = schema;
    }

    public String getName()
    {
        return name;
    }

    public void setTable(String name)
    {
        this.name = name;
    }

    public String fullyQualifiedName()
    {
        return schema + "." + name;
    }

    public synchronized boolean isTemporary()
    {
        return temporary;
    }

    public synchronized void setTemporary(boolean temporary)
    {
        this.temporary = temporary;
    }

    public ArrayList<Column> getAllColumns()
    {
        return allColumns;
    }

    public ArrayList<Column> getNonKeyColumns()
    {
        return nonKeyColumns;
    }

    public ArrayList<Key> getKeys()
    {
        return keys;
    }

    public ArrayList<Key> getUniqueIndexes()
    {
        return uniqueKeys;
    }

    /**
     * Choose unique index which is best suitable as an alternative primary key.
     * The following criteria are weighed:<br/>
     * 1. All columns are NOT NULL.<br/>
     * 2. There are no floating-point type columns.<br/>
     * 3. Consists of least amount of columns.<br/>
     * 
     * @return null, if no suitable unique index found, primary candidate key
     *         otherwise.
     */
    public Key getPKFromUniqueIndex()
    {
        Key candidate = null;
        for (Key uIdx : getUniqueIndexes())
        {
            if (candidate == null
                    || uIdx.getColumns().size() <= candidate.getColumns()
                            .size())
            {
                boolean valid = true;
                for (Column col : uIdx.getColumns())
                {
                    if (!col.isNotNull())
                    {
                        valid = false;
                        break;
                    }
                    else if (col.getType() == Types.FLOAT
                            || col.getType() == Types.DOUBLE
                            || col.getType() == Types.REAL)
                    {
                        valid = false;
                        break;
                    }
                }
                if (valid)
                    candidate = uIdx;
            }
        }
        return candidate;
    }

    public Key getPrimaryKey()
    {
        return primaryKey;
    }

    /*
     * columnNumbers here are one based. perhaps we should record the column
     * number in the Column class as well.
     */
    public Column findColumn(int columnNumber)
    {
        /* This assumes column were added in column number order */
        return allColumns.get(columnNumber - 1);
    }

    public int getColumnCount()
    {
        return allColumns.size();
    }

    /**
     * Returns the estimates number of rows in the table, if known, or 0 if not.
     * This method works by calling the method of the same name on the primary
     * key if there is one.
     */
    public long getMaxCardinality()
    {
        if (primaryKey == null)
            return 0;
        else
            return primaryKey.getMaxCardinality();
    }

    /*
     * Following methods are used for tables cache management
     */

    /**
     * getSCN returns the scn associated to this table, if any.
     * 
     * @return the scn value
     */
    public String getSCN()
    {
        return scn;
    }

    /**
     * setSCN stores a scn value associated to this table
     * 
     * @param scn the scn that is associated with this table
     */
    public void setSCN(String scn)
    {
        this.scn = scn;
    }

    /**
     * Sets the tableId value.
     * 
     * @param tableId The tableId to set.
     */
    public void setTableId(long tableId)
    {
        this.tableId = tableId;
    }

    /**
     * Returns the tableId value.
     * 
     * @return Returns the tableId.
     */
    public long getTableId()
    {
        return tableId;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        sb.append("Table name=");
        sb.append(this.schema).append(".").append(this.name);
        sb.append(" (");
        for (int c = 0; c < getColumnCount(); c++)
        {
            if (c > 0)
                sb.append(", ");
            Column col = this.getAllColumns().get(c);
            sb.append(col);
        }
        sb.append(")");
        return sb.toString();
    }

    /**
     * Provide a more detailed table definition showing columns as well as keys
     * than is provided by toString();
     */
    public String toExtendedString()
    {
        StringBuffer buf = new StringBuffer();
        buf.append(this.toString());
        buf.append(" columns=");
        buf.append(listColumns(allColumns));
        buf.append(" keys=(");
        for (int i = 0; i < this.keys.size(); i++)
        {
            if (i > 0)
                buf.append(",");
            buf.append(keys.get(i));
        }
        buf.append(")");

        return buf.toString();
    }

    // Print a list of column names.
    private String listColumns(List<Column> cols)
    {
        StringBuffer colNames = new StringBuffer();
        colNames.append("(");
        for (int i = 0; i < cols.size(); i++)
        {
            if (i > 0)
                colNames.append(",");
            colNames.append(cols.get(i).getName());
        }
        colNames.append(")");

        return colNames.toString();
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#clone()
     */
    public Table clone()
    {
        // Data structures are copied but individual columns and keys
        // are shared with the original.
        Table newTable = new Table(schema, name);
        newTable.setTemporary(this.temporary);
        newTable.setTableId(this.tableId);
        for (Column col : allColumns)
        {
            newTable.AddColumn(col);
        }
        for (Key key : keys)
        {
            newTable.AddKey(key);
        }
        return newTable;
    }

    public String fullyQualifiedName(SqlWrapper connection)
    {
        return connection.getDatabaseObjectName(schema) + "."
                + connection.getDatabaseObjectName(name);
    }
}