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

package com.continuent.tungsten.replicator.database;

import java.io.BufferedWriter;
import java.io.Serializable;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import com.continuent.tungsten.common.csv.CsvWriter;
import com.continuent.tungsten.common.csv.NullPolicy;
import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * Implements DBMS-specific operations for the Derby database.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class DerbyDatabase extends AbstractDatabase
{
    /** Create a new instance. */
    public DerbyDatabase()
    {
        dbms = DBMS.DERBY;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#getSqlNameMatcher()
     */
    @Override
    public SqlOperationMatcher getSqlNameMatcher() throws ReplicatorException
    {
        // Return MySQL matcher for now. 
        return new MySQLOperationMatcher();
    }

    /**
     * Provide column specifications that work in Derby, which hews very closely
     * to the SQL-92 standard. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#columnToTypeString(com.continuent.tungsten.replicator.database.Column,
     *      java.lang.String)
     */
    protected String columnToTypeString(Column c, String tableType)
    {
        switch (c.getType())
        {
            case Types.INTEGER :
                return "INTEGER";

            case Types.BIGINT :
                return "BIGINT";

            case Types.SMALLINT :
                return "SMALLINT";

            case Types.TINYINT :
                return "SMALLINT";

            case Types.CHAR :
                return "CHAR(" + c.getLength() + ")";

            case Types.VARCHAR :
                return "VARCHAR(" + c.getLength() + ")";

            case Types.DATE :
                return "DATE";

            case Types.TIMESTAMP :
                return "TIMESTAMP";

            case Types.CLOB :
                return "CLOB";

            case Types.BLOB :
                return "BLOB";

            case Types.BOOLEAN :
                return "BOOLEAN";

            default :
                return "UNKNOWN";
        }
    }

    protected int executePrepare(Table table, List<Column> columns, String SQL,
            boolean keep, int type) throws SQLException
    {
        int bindNo = 1;

        PreparedStatement statement = null;
        int affectedRows = 0;

        try
        {
            statement = dbConn.prepareStatement(SQL);

            for (Column c : columns)
            {
                Serializable val = c.getValue();
                if (val == null)
                    statement.setNull(bindNo++, c.getType());
                else
                    statement.setObject(bindNo++, val);
            }
            affectedRows = statement.executeUpdate();
        }
        finally
        {
            if (statement != null && !keep)
            {
                statement.close();
                statement = null;
            }
        }
        if (keep && type > -1)
            table.setStatement(type, statement);

        return affectedRows;
    }

    /**
     * Derby does not support REPLACE. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#supportsReplace()
     */
    public boolean supportsReplace()
    {
        return false;
    }

    public ArrayList<String> getSchemas() throws SQLException
    {
        throw new UnsupportedOperationException("Not implemented.");
    }

    public ResultSet getColumnsResultSet(DatabaseMetaData md,
            String schemaName, String tableName) throws SQLException
    {
        return md.getColumns(null, schemaName.toUpperCase(),
                tableName.toUpperCase(), null);
    }

    protected ResultSet getPrimaryKeyResultSet(DatabaseMetaData md,
            String schemaName, String tableName) throws SQLException
    {
        return md.getPrimaryKeys(null, schemaName.toUpperCase(),
                tableName.toUpperCase());
    }

    protected ResultSet getIndexResultSet(DatabaseMetaData md,
            String schemaName, String tableName, boolean unique)
            throws SQLException
    {
        throw new UnsupportedOperationException("Not implemented.");
    }

    protected ResultSet getTablesResultSet(DatabaseMetaData md,
            String schemaName, boolean baseTablesOnly) throws SQLException
    {
        String[] types = {"TABLE"};
        return md.getTables("%", schemaName, "%", types);
    }

    public String getNowFunction()
    {
        return "CURRENT_TIMESTAMP";
    }

    /**
     * getTimeDiff returns the database-specific way of subtracting two "dates"
     * and return the result in seconds complete with space for the two bind
     * variables. I, Scott, am not sure how to express the differences between
     * two dates in derby in seconds. This function is currently only called by
     * the time based "thl purge" command. For now, simply subtract the two
     * dates.
     */
    public String getTimeDiff(String string1, String string2)
    {
        String retval = "";
        if (string1 == null)
            retval += "?";
        else
            retval += string1;
        retval += " - ";
        if (string2 == null)
            retval += "?";
        else
            retval += string2;
        retval += "";

        return retval;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#getCsvWriter(java.io.BufferedWriter)
     */
    public CsvWriter getCsvWriter(BufferedWriter writer)
    {
        if (this.csvSpec == null)
        {
            CsvWriter csv = new CsvWriter(writer);
            csv.setQuoteChar('"');
            csv.setQuoted(true);
            csv.setEscapeChar('\\');
            csv.setEscapedChars("\\");
            csv.setNullPolicy(NullPolicy.skip);
            csv.setWriteHeaders(false);
            return csv;
        }
        else
            return csvSpec.createCsvWriter(writer);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.AbstractDatabase#dropSchema(java.lang.String)
     */
    public void dropSchema(String schema) throws SQLException
    {
        // Derby does not cascade, so we have to delete any tables first.
        List<Table> tables = getTables(schema, true);
        for (Table table : tables)
        {
            dropTable(table);
        }
        execute("DROP SCHEMA " + schema + " RESTRICT");
    }
}
