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
 * Initial developer(s): Alex Yurchenko
 * Contributor(s): Linas Virbalas
 */

package com.continuent.tungsten.replicator.consistency;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.database.Column;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.Table;

/**
 * This class defines a ConsistencyCheckMD5
 * 
 * @author <a href="mailto:alexey.yurchenko@continuent.com">Alex Yurchenko</a>
 * @version 1.0
 */
public class ConsistencyCheckMD5 extends ConsistencyCheckAbstract
{
    private static final long   serialVersionUID = 8055678665808795613L;

    private static Logger       logger           = Logger.getLogger(ConsistencyCheckMD5.class);

    private boolean             usePKForLimit    = false;
    private int                 rowFrom          = ConsistencyTable.ROW_UNSET;
    private int                 rowLimit         = ConsistencyTable.ROW_UNSET;

    private boolean             checkColumnNames;
    private boolean             checkColumnTypes;

    static private final String CONCAT_SEPARATOR = ",";

    /**
     * Creates a new <code>ConsistencyCheckMD5</code> object
     */
    public ConsistencyCheckMD5(int id, Table table, int rowFrom, int rowLimit,
            boolean checkColumnNames, boolean checkColumnTypes)
    {
        super(id, table, ConsistencyCheck.Method.MD5);
        this.rowFrom = rowFrom;
        this.rowLimit = rowLimit;
        this.checkColumnNames = checkColumnNames;
        this.checkColumnTypes = checkColumnTypes;
    }

    /**
     * Creates a new <code>ConsistencyCheckMD5</code> object
     */
    public ConsistencyCheckMD5(int id, Table table, int rowFrom, int rowLimit,
            boolean checkColumnNames, boolean checkColumnTypes,
            boolean usePKForLimit)
    {
        super(id, table, ConsistencyCheck.Method.MD5PK);
        this.rowFrom = rowFrom;
        this.rowLimit = rowLimit;
        this.checkColumnNames = checkColumnNames;
        this.checkColumnTypes = checkColumnTypes;
        this.usePKForLimit = true;
    }

    /**
     * Creates a new <code>ConsistencyCheckMD5</code> object
     */
    public ConsistencyCheckMD5(int id, Table table, boolean checkColumnNames,
            boolean checkColumnTypes)
    {
        this(id, table, ConsistencyTable.ROW_UNSET, ConsistencyTable.ROW_UNSET,
                checkColumnNames, checkColumnTypes);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.consistency.ConsistencyCheckAbstract#getRowOffset()
     */
    public int getRowOffset()
    {
        return rowFrom;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.consistency.ConsistencyCheckAbstract#getRowLimit()
     */
    public int getRowLimit()
    {
        return rowLimit;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.consistency.ConsistencyCheckAbstract#performConsistencyCheck(com.continuent.tungsten.replicator.database.Database)
     */
    @Override
    public ResultSet performConsistencyCheck(Database conn)
            throws ConsistencyException
    {
        switch (conn.getType())
        {
            case MYSQL :
                return ConsistencyCheckMySQL(conn);
            case ORACLE :
                return ConsistencyCheckOracle(conn);
            case POSTGRESQL :
                return ConsistencyCheckPostgreSQL(conn);
            case DERBY :
                throw new UnsupportedOperationException("Not implemented.");
        }
        return null;
    }

    /**
     * Generates a separated list of column names.
     * 
     * @param conn
     */
    private String columnsWithSeparator(Database conn,
            ArrayList<Column> columns, String separator)
    {
        if (columns != null && columns.size() > 0)
        {
            StringBuffer sb = new StringBuffer();

            sb.append(conn.getDatabaseObjectName(columns.get(0).getName()));
            for (int i = 1; i < columns.size(); i++)
            {
                sb.append(separator);
                sb.append(conn.getDatabaseObjectName(columns.get(i).getName()));
            }
            return sb.toString();
        }
        else
        {
            return null;
        }
    }

    // a helper function to create a comma-spearated list of columns
    private String columnsMySQL(Database conn, ArrayList<Column> columns)
    {
        return columnsWithSeparator(conn, columns, ",");
    }

    /**
     * Constructs a comma-separated list of column names (if checkColumnNames)
     * and their corresponding types (java.sql.Types) (if checkColumnTypes).
     * 
     * @param columns
     * @return "col1 -1,col2 7,col3 3" or "col1,col2,col3" or "-1,7,3" or ""
     *         depending on checkColumnNames and checkColumnTypes settings.
     *         null, if there are no columns.
     * @see java.sql.Types
     */
    private String columnInfoMySQL(ArrayList<Column> columns)
    {
        if (columns != null && columns.size() > 0)
        {
            StringBuffer sb = new StringBuffer();

            if (checkColumnNames || checkColumnTypes)
            {
                for (int i = 0; i < columns.size(); i++)
                {
                    if (i > 0)
                        sb.append(',');
                    if (checkColumnNames)
                        sb.append(columns.get(i).getName());
                    if (checkColumnNames && checkColumnTypes)
                        sb.append(" ");
                    if (checkColumnTypes)
                        sb.append(columns.get(i).getType());
                }
            }

            return sb.toString();
        }
        else
        {
            return null;
        }
    }

    private ResultSet ConsistencyCheckMySQL(Database conn)
            throws ConsistencyException
    {
        String schemaName = table.getSchema();
        String tableName = table.getName();
        String allColumns = columnsMySQL(conn, table.getAllColumns());
        String keyColumns = null;
        if (table.getPrimaryKey() != null)
        {
            keyColumns = columnsMySQL(conn, table.getPrimaryKey().getColumns());
        }

        //
        // Just to show what sort of SELECT we try to generate here:
        // SELECT
        // COUNT(*) AS this_cnt,
        // RIGHT(MAX(@crc := CONCAT(LPAD(@cnt := @cnt + 1, 16, '0'),
        // MD5(CONCAT(@crc, MD5(CONCAT_WS(',',id,value))))
        // )), 32) AS this_crc
        // FROM (SELECT * FROM test.auto ORDER BY id LIMIT 0,3
        // LOCK IN SHARE MODE
        // ) AS tungsten_consistency_check_tmp
        //
        // id,values correspond to allColumns below
        //
        // First SELECT selects a page from the table according to limit
        // Second SELECT performs MD5 digest on its "concatenated" values.
        //
        StringBuffer sb = new StringBuffer();
        sb.append("SELECT ");
        sb.append("COUNT(*) AS this_cnt,");
        sb.append(" IFNULL("); // TREP-67, TREP-268
        sb.append("RIGHT(MAX(@crc := CONCAT(LPAD(@cnt := @cnt + 1, 16, '0'), MD5(CONCAT(@crc, MD5(CONCAT_WS('");
        sb.append(CONCAT_SEPARATOR);
        sb.append("',");
        sb.append(allColumns);
        sb.append(")))))), 32)");
        sb.append(", @colcrc)");
        sb.append(" AS this_crc FROM (SELECT * FROM ");
        sb.append(schemaName);
        sb.append('.');
        sb.append(tableName);
        if (rowFrom >= 0)
        {
            if (usePKForLimit)
            {
                if (keyColumns != null)
                {
                    if (table.getPrimaryKey().getColumns().size() == 1)
                    {
                        sb.append(" WHERE ");
                        sb.append(keyColumns);
                        sb.append(" >= ");
                        sb.append(rowFrom);
                        sb.append(" AND ");
                        sb.append(keyColumns);
                        sb.append(" < ");
                        // Addition below is to make the meaning of rowLimit
                        // consistent with the SQL LIMIT variant:
                        sb.append(rowFrom + rowLimit);
                    }
                    else
                    {
                        throw new ConsistencyException(
                                "Row limiting with the use of PK is supported only on tables with a single PK, which is not the case in "
                                        + schemaName
                                        + "."
                                        + tableName
                                        + " ("
                                        + table.getPrimaryKey().getColumns()
                                                .size() + "): " + keyColumns,
                                null);
                    }
                }
                else
                {
                    throw new ConsistencyException(
                            "Use of PK for row limiting selected, but no PK available in "
                                    + schemaName + "." + tableName, null);
                }
            }
            else
            {
                sb.append(" LIMIT ");
                sb.append(rowFrom);
                sb.append(',');
                sb.append(rowLimit);
            }
        }
        sb.append(" LOCK IN SHARE MODE) AS tungsten_consistency_check_tmp");
        sb.append(" ORDER BY ");
        if (keyColumns != null)
        {
            sb.append(keyColumns);
        }
        else
        {
            sb.append(allColumns);
        }

        try
        {
            Statement st = conn.createStatement();
            // Include column names and their types in the MD5 calculation
            // (TREP-268) or just an empty string if settings are not
            // to check for column names/types (TREP-67).
            if (checkColumnNames || checkColumnTypes)
            {
                if (logger.isDebugEnabled())
                    logger.debug("SET @colcrc := MD5('"
                            + columnInfoMySQL(table.getAllColumns()) + "')");
                st.execute("SET @colcrc := MD5('"
                        + columnInfoMySQL(table.getAllColumns()) + "')");
            }
            else
                st.execute("SET @colcrc := ''");
            st.execute("SET @crc := @colcrc, @cnt := 0");
            if (logger.isDebugEnabled())
                logger.debug(sb.toString());
            ResultSet rs = st.executeQuery(sb.toString());
            if (logger.isDebugEnabled())
            {
                logger.debug(rs.toString());
                logger.debug("Warnings: " + st.getWarnings());
            }
            return rs;
        }
        catch (SQLException e)
        {
            String msg = "Consistency check failed: " + e.getMessage();
            throw new ConsistencyException(msg, e);
        }
    }

    /**
     * Translation of ConsistencyCheckMySQL(Database conn) into PostgreSQL
     * dialect. In order to achieve anologous functionality 5 trivial functions
     * in tungsten schema are being defined as well as temporary table for
     * storing session variables needed to calculate consistency checksum.
     * 
     * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
     */
    private ResultSet ConsistencyCheckPostgreSQL(Database conn)
            throws ConsistencyException
    {
        String schemaName = table.getSchema();
        String tableName = table.getName();
        String allColumns = columnsWithSeparator(conn, table.getAllColumns(),
                " || ',' || ");
        String keyColumns = null;
        if (table.getPrimaryKey() != null)
        {
            keyColumns = columnsMySQL(conn, table.getPrimaryKey().getColumns());
        }

        StringBuffer sb = new StringBuffer();
        sb.append("SELECT ");
        sb.append("COUNT(*) AS this_cnt,");
        sb.append(" COALESCE("); // TREP-67, TREP-268
        sb.append("tungsten.right(MAX(tungsten.set_crc(tungsten.concat(LPAD(tungsten.inc_cnt()::text, 16, '0'), MD5(tungsten.concat(tungsten.get_crc(), MD5(");
        sb.append(allColumns);
        sb.append(")))))), 32)");
        sb.append(", (SELECT colcrc FROM tungsten_consistency_check_vars))");
        sb.append(" AS this_crc FROM (SELECT * FROM ");
        sb.append(schemaName);
        sb.append('.');
        sb.append(tableName);
        sb.append(" ORDER BY ");
        if (keyColumns != null)
        {
            sb.append(keyColumns);
        }
        else
        {
            sb.append(allColumns);
        }
        if (rowFrom >= 0)
        {
            sb.append(" LIMIT ");
            sb.append(rowFrom);
            sb.append(',');
            sb.append(rowLimit);
        }
        sb.append(") AS tungsten_consistency_check_tmp");

        try
        {
            Statement st = conn.createStatement();

            // Define generic string functions needed for consistency check SQL:
            st.execute("CREATE OR REPLACE FUNCTION tungsten.right(text, integer)\n"
                    + "RETURNS text AS $$\n"
                    + "  SELECT substring($1 FROM pg_catalog.length($1) + 1 - $2);\n"
                    + "$$ IMMUTABLE STRICT LANGUAGE SQL;");
            st.execute("CREATE OR REPLACE FUNCTION tungsten.concat(text, text)\n"
                    + "RETURNS text AS $$\n"
                    + "SELECT $1 || $2;\n"
                    + "$$ IMMUTABLE STRICT LANGUAGE sql;");

            if (checkColumnNames || checkColumnTypes)
            {
                logger.warn("Column name and type checking is not implemented for PostgreSQL!");
            }

            // Clear temporary variables' table:
            st.execute("DROP TABLE IF EXISTS tungsten_consistency_check_vars;");
            st.execute("CREATE TEMP TABLE tungsten_consistency_check_vars AS SELECT 0::bigint AS cnt, ''::text AS colcrc, ''::text AS crc;");

            // Define functions for operating on temporary variables:
            st.execute("CREATE OR REPLACE FUNCTION tungsten.get_crc()\n"
                    + "RETURNS char(32) AS $$\n"
                    + "  SELECT crc FROM tungsten_consistency_check_vars;\n"
                    + "$$ LANGUAGE sql;");
            st.execute("CREATE OR REPLACE FUNCTION tungsten.set_crc(value text)\n"
                    + "RETURNS char(32) AS $$\n"
                    + "  UPDATE tungsten_consistency_check_vars SET crc = $1;\n"
                    + "  SELECT $1;\n" + "$$ LANGUAGE sql;");
            st.execute("CREATE OR REPLACE FUNCTION tungsten.inc_cnt()\n"
                    + "RETURNS bigint AS $$\n"
                    + "  UPDATE tungsten_consistency_check_vars SET cnt = cnt + 1;\n"
                    + "  SELECT cnt FROM tungsten_consistency_check_vars;\n"
                    + "$$ LANGUAGE sql;");

            // Finally, execute the consistency check SQL!
            ResultSet rs = st.executeQuery(sb.toString());
            if (logger.isDebugEnabled())
            {
                logger.debug(sb.toString());
                logger.debug(rs.toString());
                logger.debug("Warnings: " + st.getWarnings());
            }
            return rs;
        }
        catch (SQLException e)
        {
            String msg = "Consistency check failed: " + e.getMessage();
            throw new ConsistencyException(msg, e);
        }
    }

    private ResultSet ConsistencyCheckOracle(Database conn)
            throws ConsistencyException
    {
        throw new UnsupportedOperationException();
    }
}
