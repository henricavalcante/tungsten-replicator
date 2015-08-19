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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Iterator;

import org.apache.log4j.Logger;

/**
 * Implements DBMS-specific operations for Vertica.
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 */
public class VerticaDatabase extends PostgreSQLDatabase
{
    private static Logger logger = Logger.getLogger(VerticaDatabase.class);

    public VerticaDatabase() throws SQLException
    {
        dbms = DBMS.VERTICA;
        // Hard code the version 4.x driver so it gets loaded.
        dbDriver = "com.vertica.Driver";
        // Check to see if the version 4.x driver is present. If not, use
        // the 5.x driver name.
        try
        {
            logger.debug("Checking for default driver: " + dbDriver);
            Class.forName(dbDriver);
        }
        catch (Exception e)
        {
            dbDriver = "com.vertica.jdbc.Driver";
            logger.debug("Unable to load Vertica 4.x JDBC driver; will default to Version 5.x name: "
                    + dbDriver);
        }

    }

    /**
     * Overload connect method to issue call to ensure default projections are
     * enabled.
     */
    public synchronized void connect() throws SQLException
    {
        // Connect first.
        super.connect();

        // Issue call to define projections.
        // if (connected)
        // execute("SELECT implement_temp_design('')");
    }

    /**
     * Ensure projection is created for new table. (Otherwise metadata
     * operations may fail if we try to select off a new table.)
     * 
     * @see com.continuent.tungsten.replicator.database.Database#createTable(com.continuent.tungsten.replicator.database.Table,
     *      boolean)
     */
    public void createTable(Table t, boolean replace) throws SQLException
    {
        // Ensure that we have an ordering column for the table. This defaults
        // to the first column.
        String orderCol = t.getAllColumns().get(0).getName();
        boolean comma = false;

        if (replace)
            dropTable(t);
        else
        {
            // If table already exists, do nothing. This behavior is a mimic of
            // MySQLDatabase for initial configuration to work. For some reason,
            // Replicator is trying to create Tungsten tables more than once.
            if (tableExists(t))
                return;
        }

        String temporary = t.isTemporary() ? "TEMPORARY " : "";
        // Temporary tables cannot specify a schema name:
        String SQL = "CREATE " + temporary + "TABLE " + t.getSchema()
                + (t.isTemporary() ? "_" : ".") + t.getName() + " (";

        Iterator<Column> i = t.getAllColumns().iterator();
        while (i.hasNext())
        {
            Column c = i.next();
            SQL += (comma ? ", " : "") + c.getName() + " "
                    + columnToTypeString(c, null)
                    + (c.isNotNull() ? " NOT NULL" : "");
            comma = true;
        }

        // Iterate over primary keys to get ordering columns.
        Iterator<Key> j = t.getKeys().iterator();
        while (j.hasNext())
        {
            Key key = j.next();
            if (key.isPrimaryKey() && key.getName() != null)
            {
                orderCol = key.getName();
                break;
            }
        }
        SQL += ")";

        // Add the order column. In Vertica 6 this creates a default
        // projection at creation time.
        SQL += " ORDER BY " + orderCol;

        // Create the table.
        execute(SQL);

        // Ensure projection is created.
        // execute("SELECT implement_temp_design('')");
    }

    /**
     * Checks whether the given table exists in the currently connected database
     * using Vertica-specific v_catalog.tables view.
     * 
     * @return true, if table exists, false, if not.
     */
    protected boolean tableExists(Table t) throws SQLException
    {
        String sql = String
                .format("SELECT * FROM v_catalog.tables WHERE table_schema='%s' AND table_name='%s'",
                        t.getSchema(), t.getName());
        Statement stmt = dbConn.createStatement();
        try
        {
            ResultSet rs = stmt.executeQuery(sql);
            return rs.next();
        }
        finally
        {
            if (stmt != null)
            {
                try
                {
                    stmt.close();
                }
                catch (SQLException e)
                {
                }
            }
        }
    }

    /**
     * Converts column types according to standard Vertica names.
     */
    protected String columnToTypeString(Column c, String tableType)
    {
        switch (c.getType())
        {
            case Types.TINYINT :
                return "SMALLINT";

            case Types.SMALLINT :
                return "SMALLINT";

            case Types.INTEGER :
                return "INTEGER";

            case Types.BIGINT :
                return "BIGINT";

            case Types.CHAR :
            {
                if (c.getLength() == 1)
                    // This is a provisional hack, written to support storing
                    // boolean values into "character(1)" type "last_frag" field
                    // of "trep_commit_seqno" and "history" tables.
                    return "CHAR(5)";
                else
                    return "CHAR(" + c.getLength() + ")";
            }

            case Types.VARCHAR :
                return "VARCHAR(" + c.getLength() + ")";

            case Types.DATE :
                return "DATE";

            case Types.TIMESTAMP :
                return "TIMESTAMP";

            case Types.CLOB :
                // Vertica does not support TEXT directly, so we use biggest
                // allowed varchar.
                return "VARCHAR(65000)";

                // Vertica does not have a true BLOB type, so we use biggest
                // allowed varbinary.
            case Types.BLOB :
                return "VARBINARY(65000)";

            default :
                return "UNKNOWN";
        }
    }
}