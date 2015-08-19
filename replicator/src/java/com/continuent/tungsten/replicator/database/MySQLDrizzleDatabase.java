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
 * Initial developer(s): Stephane Giron
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.database;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.extractor.mysql.SerialBlob;

/**
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class MySQLDrizzleDatabase extends MySQLDatabase
{
    public MySQLDrizzleDatabase() throws SQLException
    {
        super();
        dbDriver = "org.drizzle.jdbc.DrizzleDriver";
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.MySQLDatabase#getColumnsResultSet(java.sql.DatabaseMetaData,
     *      java.lang.String, java.lang.String)
     */
    public ResultSet getColumnsResultSet(DatabaseMetaData md,
            String schemaName, String tableName) throws SQLException
    {
        // Drizzle driver uses schema argument for MySQL database name vs.
        // catalog name for Connector/J. Unclear who is right...
        return md.getColumns(null, schemaName, tableName, null);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.MySQLDatabase#getTablesResultSet(java.sql.DatabaseMetaData,
     *      java.lang.String, boolean)
     */
    protected ResultSet getTablesResultSet(DatabaseMetaData md,
            String schemaName, boolean baseTablesOnly) throws SQLException
    {
        String types[] = null;
        if (baseTablesOnly)
            types = new String[]{"TABLE"};

        return md.getTables(null, schemaName, null, types);
    }

    @Override
    protected ResultSet getPrimaryKeyResultSet(DatabaseMetaData md,
            String schemaName, String tableName) throws SQLException
    {
        return md.getPrimaryKeys(null, schemaName, tableName);
    }

    @Override
    public String getPlaceHolder(ColumnSpec col, Object colValue,
            String typeDesc)
    {
        if (col.getType() == Types.BLOB && typeDesc != null
                && typeDesc.contains("TEXT"))
        {
            if (colValue == null)
                return " NULL ";
            else if (colValue instanceof SerialBlob)
                return " UNHEX( ? ) ";
        }
        else if (col.getType() == Types.VARCHAR)
            if (colValue == null)
                return " NULL ";
            else if (colValue instanceof byte[])
                return " UNHEX( ? ) ";
        return super.getPlaceHolder(col, colValue, typeDesc);
    }

    @Override
    public boolean nullsBoundDifferently(ColumnSpec col)
    {
        String typeDescription = col.getTypeDescription();
        return ((col.getType() == Types.BLOB
                && typeDescription != null && typeDescription
                .contains("TEXT")) || col.getType() == Types.VARCHAR);
    }

    @Override
    public boolean nullsEverBoundDifferently()
    {
        return true;
    }

}
