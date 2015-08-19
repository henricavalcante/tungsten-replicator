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
 * Contributor(s): Stephane Giron, Robert Hodges
 */

package com.continuent.tungsten.replicator.filter;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Column;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.MySQLOperationMatcher;
import com.continuent.tungsten.replicator.database.SqlOperation;
import com.continuent.tungsten.replicator.database.SqlOperationMatcher;
import com.continuent.tungsten.replicator.database.Table;
import com.continuent.tungsten.replicator.datasource.SqlDataSource;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplOptionParams;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * EnumToStringFilter transforms enum data type values to corresponding string
 * representation as follows:<br/>
 * 1. On each event it checks whether targeted table has enum data type column.<br/>
 * 2. If it does, corresponding enum column values of the event are mapped from
 * integer into string representations.<br/>
 * <br/>
 * The filter is to be used with row replication.<br/>
 * <br/>
 * Filter takes an optional parameter for performance tuning. Instead of
 * checking all the tables you may define only a specific comma-delimited list
 * in process_tables_schemas parameter. Eg.:<br/>
 * replicator.filter.enumtostringfilter.process_tables_schemas=myschema.mytable1
 * ,myschema.mytable2
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 * @version 1.0
 */
public class EnumToStringFilter implements Filter
{
    static class TableWithEnums
    {
        Table                      table           = null;
        HashMap<Integer, String[]> enumDefinitions = null;

        TableWithEnums(Table table)
        {
            this.table = table;
        }

        public Table getTable()
        {
            return this.table;
        }

        public HashMap<Integer, String[]> getEnumDefinitions()
        {
            return this.enumDefinitions;
        }

        public void setEnumDefinitions(
                HashMap<Integer, String[]> enumDefinitions)
        {
            this.enumDefinitions = enumDefinitions;
        }
    }

    private static Logger                                        logger               = Logger.getLogger(EnumToStringFilter.class);

    // Metadata cache is a hashtable indexed by the database name and each
    // database uses a hashtable indexed by the table name (This is done in
    // order to be able to drop all table definitions at once if a DROP DATABASE
    // is trapped). Filling metadata cache is done in a lazy way. It will be
    // updated only when a table is used for the first time by a row event.
    private Hashtable<String, Hashtable<String, TableWithEnums>> metadataCache;

    // Connection information.
    private SqlDataSource                                        dataSourceImpl;
    private String                                               dataSource;
    Database                                                     conn                 = null;

    private List<String>                                         tables               = null;
    private List<String>                                         schemas              = null;
    private String                                               processTablesSchemas = null;

    // SQL parser.
    SqlOperationMatcher                                          sqlMatcher           = new MySQLOperationMatcher();

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException
    {
        if (processTablesSchemas != null)
        {
            if (logger.isDebugEnabled())
                logger.debug("Tables to process: " + processTablesSchemas);

            tables = new ArrayList<String>();
            schemas = new ArrayList<String>();

            String[] list = processTablesSchemas.split(",");
            for (int i = 0; i < list.length; i++)
            {
                String t = list[i].trim().toUpperCase();
                if (t.contains("."))
                    tables.add(t);
                else
                    schemas.add(t);
            }
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException
    {
        metadataCache = new Hashtable<String, Hashtable<String, TableWithEnums>>();

        // Locate our data source that we use to pick up metadata and create
        // connection.
        logger.info("Connecting to data source");
        dataSourceImpl = (SqlDataSource) context.getDataSource(dataSource);
        if (dataSourceImpl == null)
        {
            throw new ReplicatorException("Unable to locate data source: name="
                    + dataSource);
        }
        conn = dataSourceImpl.getConnection();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException
    {
        if (metadataCache != null)
        {
            metadataCache.clear();
            metadataCache = null;
        }
        if (conn != null)
        {
            dataSourceImpl.releaseConnection(conn);
            conn = null;
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.filter.Filter#filter(com.continuent.tungsten.replicator.event.ReplDBMSEvent)
     */
    public ReplDBMSEvent filter(ReplDBMSEvent event)
            throws ReplicatorException, InterruptedException
    {
        // This filter only works on MySQL data. Exit if we have an event from
        // another DBMS type.
        String dbms = event.getMetadataOption(ReplOptionParams.DBMS_TYPE);
        if (!Database.MYSQL.equals(dbms))
            return event;

        // Now process the data.
        ArrayList<DBMSData> data = event.getData();
        if (data == null)
            return event;
        for (DBMSData dataElem : data)
        {
            if (dataElem instanceof RowChangeData)
            {
                RowChangeData rdata = (RowChangeData) dataElem;
                for (OneRowChange orc : rdata.getRowChanges())
                    try
                    {
                        checkForListType(orc);
                    }
                    catch (SQLException e)
                    {
                        throw new ReplicatorException(
                                "Filter failed processing primary key information",
                                e);
                    }
            }
            else if (dataElem instanceof StatementData)
            {
                StatementData sdata = (StatementData) dataElem;
                // Parse statements in order to update table definitions if
                // needed. e.g. DROP DATABASE should drop information about keys
                // which are defined for this database tables, ...
                String query = sdata.getQuery();
                if (query == null)
                    query = new String(sdata.getQueryAsBytes());

                SqlOperation sqlOperation = sqlMatcher.match(query);

                if (sqlOperation.getOperation() == SqlOperation.DROP
                        && sqlOperation.getObjectType() == SqlOperation.SCHEMA)
                {
                    // "drop database" statement detected : remove database
                    // metadata
                    String dbName = sqlOperation.getSchema();
                    if (metadataCache.remove(dbName) != null)
                    {
                        if (logger.isDebugEnabled())
                            logger.debug("DROP DATABASE detected - Removing database metadata for '"
                                    + dbName + "'");
                    }
                    else if (logger.isDebugEnabled())
                        logger.debug("DROP DATABASE detected - no cached database metadata to delete for '"
                                + dbName + "'");
                    continue;
                }
                else if (sqlOperation.getOperation() == SqlOperation.ALTER)
                {
                    // Detected an alter table statement / Dropping table
                    // metadata for the concerned table
                    String name = sqlOperation.getName();
                    String defaultDB = sdata.getDefaultSchema();
                    removeTableMetadata(name, sqlOperation.getSchema(),
                            defaultDB);
                    continue;
                }

            }
        }
        return event;
    }

    private void removeTableMetadata(String tableName, String schemaName,
            String defaultDB)
    {
        if (schemaName != null)
        {
            Hashtable<String, TableWithEnums> tableCache = metadataCache
                    .get(schemaName);
            if (tableCache != null && tableCache.remove(tableName) != null)
            {
                if (logger.isDebugEnabled())
                    logger.debug("ALTER TABLE detected - Removing table metadata for '"
                            + schemaName + "." + tableName + "'");
            }
            else if (logger.isDebugEnabled())
                logger.debug("ALTER TABLE detected - no cached table metadata to remove for '"
                        + schemaName + "." + tableName + "'");
        }
        else
        {
            Hashtable<String, TableWithEnums> tableCache = metadataCache
                    .get(defaultDB);
            if (tableCache != null && tableCache.remove(tableName) != null)
                logger.info("ALTER TABLE detected - Removing table metadata for '"
                        + defaultDB + "." + tableName + "'");
            else
                logger.info("ALTER TABLE detected - no cached table metadata to remove for '"
                        + defaultDB + "." + tableName + "'");
        }
    }

    protected String[] parseListType(String listTypeDefinition)
    {
        return parseEnumeration(listTypeDefinition);
    }

    /**
     * Parses MySQL enum type definition statement. Eg.:<br/>
     * enum('Active','Inactive','Removed')<br/>
     * enum('No','Yes')<br/>
     * etc.<br/>
     * 
     * @param enumDefinition String of the following form:
     *            enum('val1','val2',...)
     * @return Enumeration elements in an array. Unquoted. Eg.:
     *         Active,Inactive,Removed
     */
    public static String[] parseEnumeration(String enumDefinition)
    {
        return parseListDefString("enum", enumDefinition);
    }

    /**
     * Parses strings of the following form:<br/>
     * enum('val1','val2',...)<br/>
     * set('val1','val2',...)
     * 
     * @param colType "enum" or "string"
     * @param definition String like "enum('val1','val2',...)" or
     *            "set('val1','val2',...)".
     * @return Elements in the definition (val1,val2,...).
     */
    public static String[] parseListDefString(String colType, String definition)
    {
        // Parse out what's inside brackets.
        String keyword = colType + "(";
        int iA = definition.toLowerCase().indexOf(keyword);
        int iB = definition.indexOf(')', iA);
        String list = definition.substring(iA + keyword.length(), iB);

        // Split by comma, remove quotes and save into array.
        String[] listArray = list.split(",");
        String[] elements = new String[listArray.length];
        for (int i = 0; i < listArray.length; i++)
        {
            String elementQuoted = listArray[i];
            String element = elementQuoted.substring(1,
                    elementQuoted.length() - 1);
            elements[i] = element;
        }

        return elements;
    }

    /**
     * @see com.continuent.tungsten.replicator.filter.EnumToStringFilter#largestElement(String[])
     * @param enumDefinition String of the following form:
     *            enum('val1','val2',...)
     * @return Length of the largest element in given enumeration definition.
     */
    public static int largestElementLen(String enumDefinition)
    {
        return largestElementLen(parseEnumeration(enumDefinition));
    }

    /**
     * Returns how long is the largest element of enumeration.
     * 
     * @param enumValues Values of enumeration. Eg.: 'No','Yes'
     * @return Length of the largest element.
     */
    public static int largestElementLen(String[] enumValues)
    {
        return enumValues[largestElement(enumValues)].length();
    }

    /**
     * @return Position of the largest element in the array.
     */
    public static int largestElement(String[] enumValues)
    {
        int largestPos = 0;
        int largestLen = 0;
        for (int i = 0; i < enumValues.length; i++)
        {
            if (enumValues[i].length() > largestLen)
            {
                largestPos = i;
                largestLen = enumValues[i].length();
            }
        }
        return largestPos;
    }

    /**
     * Connects to the MySQL database, executes SHOW COLUMNS for a defined table
     * and parses out allowed ENUM values to their corresponding String
     * representations.
     * 
     * @param schemaTable String in form of "schema.table" to execute SHOW
     *            COLUMNS against.
     * @param column ENUM type column to retrieve values about.
     * @return Array of allowed ENUM values for the column. NOTE: index, as in
     *         arrays, starts from zero (0), but MySQL ENUM values' numeric
     *         representation starts from one (1) - thus, make sure you retrieve
     *         the correct value from this array by shifting the key by one,
     *         i.e.: [MySQLENUMValueNumeric - 1]
     * @throws SQLException
     */
    private String[] retrieveEnumeration(String schemaTable, String column)
            throws SQLException
    {
        String[] enumElements = null;

        // Get the allowed enum values.
        String query = "SHOW COLUMNS FROM " + schemaTable + " WHERE Field='"
                + column + "'";
        Statement st = null;
        ResultSet rs = null;
        try
        {
            st = conn.createStatement();
            rs = st.executeQuery(query);
            if (rs.next())
            {
                String enumDefinition = rs.getString("Type");
                if (logger.isDebugEnabled())
                    logger.debug(enumDefinition);

                enumElements = parseListType(enumDefinition);
            }
        }
        finally
        {
            if (rs != null)
            {
                try
                {
                    rs.close();
                }
                catch (SQLException e)
                {
                }
            }
            if (st != null)
            {
                try
                {
                    st.close();
                }
                catch (SQLException e)
                {
                }
            }
        }

        return enumElements;
    }

    /**
     * Checks for ENUM columns in the event. If found, transforms values from
     * integers to corresponding strings.
     * 
     * @param orc
     * @throws SQLException
     * @throws ReplicatorException
     */
    protected void checkForListType(OneRowChange orc) throws SQLException,
            ReplicatorException
    {
        checkForListType(orc, "ENUM");
    }

    /**
     * Checks for ENUM/SET type columns in the event. If found, transforms
     * values from integers to corresponding strings.
     * 
     * @param type "ENUM" or "SET".
     */
    protected void checkForListType(OneRowChange orc, String type)
            throws SQLException, ReplicatorException
    {
        String tableName = orc.getTableName();

        // Check only a specific list of tables?
        if (schemas != null
                && (!schemas.contains(orc.getSchemaName().toUpperCase()) && !tables
                        .contains((orc.getSchemaName() + "." + tableName)
                                .toUpperCase())))
        {
            if (logger.isDebugEnabled())
                logger.debug("Table " + orc.getSchemaName() + "." + tableName
                        + " not taken into account");
            return;
        }

        if (!metadataCache.containsKey(orc.getSchemaName()))
        {
            // Nothing defined yet in this database
            metadataCache.put(orc.getSchemaName(),
                    new Hashtable<String, TableWithEnums>());
        }

        Hashtable<String, TableWithEnums> dbCache = metadataCache.get(orc
                .getSchemaName());

        if (!dbCache.containsKey(tableName)
                || orc.getTableId() == -1
                || dbCache.get(tableName).getTable() == null
                || dbCache.get(tableName).getTable().getTableId() != orc
                        .getTableId())
        {
            // This table was not processed yet or schema changed since it was
            // cached : fetch information about its primary key
            if (dbCache.remove(tableName) != null && logger.isDebugEnabled())
                logger.debug("Detected a schema change for table "
                        + orc.getSchemaName() + "." + tableName
                        + " - Removing table metadata from cache");
            Table newTable = conn.findTable(orc.getSchemaName(),
                    orc.getTableName(), false);
            // If we cannot find the table, it is possible it has been deleted,
            // in which case there is nothing to be done.
            if (newTable == null)
            {
                if (logger.isDebugEnabled())
                {
                    logger.debug("Ignored a missing table: name="
                            + orc.getSchemaName() + "." + tableName);
                }
                return;
            }
            newTable.setTableId(orc.getTableId());
            dbCache.put(tableName, new TableWithEnums(newTable));
        }

        // Is there any enum columns in this table? If so, retrieve enum
        // definitions of each enum column.
        TableWithEnums table = dbCache.get(tableName);
        // Have we already cached enum definitions?
        HashMap<Integer, String[]> enumDefinitions = table.getEnumDefinitions();
        if (enumDefinitions != null)
        {
            if (logger.isDebugEnabled())
                logger.debug("Using cache (columns: " + enumDefinitions.size()
                        + ") @ " + table.getTable().getSchema() + "."
                        + table.getTable().getName());
        }
        else
        {
            // Enum definitions not in cache, retrieve & cache.
            enumDefinitions = new HashMap<Integer, String[]>();
            for (Column col : table.getTable().getAllColumns())
            {
                if (col.getTypeDescription() != null)
                {
                    if (col.getTypeDescription().startsWith(type))
                    {
                        if (logger.isDebugEnabled())
                            logger.debug(type + " @ " + col.getPosition()
                                    + " : " + table.getTable().getSchema()
                                    + "." + table.getTable().getName() + "."
                                    + col.getName());
                        String[] enumDefinition = retrieveEnumeration(table
                                .getTable().getSchema()
                                + "."
                                + table.getTable().getName(), col.getName());
                        if (enumDefinition == null)
                        {
                            logger.error("Failed to retrieve enumeration definition for "
                                    + table.getTable().getSchema()
                                    + "."
                                    + table.getTable().getName()
                                    + "."
                                    + col.getName());
                            return;
                        }
                        else
                            enumDefinitions.put(col.getPosition(),
                                    enumDefinition);
                    }
                }
                else
                    logger.error("Column type description is null for "
                            + table.getTable().getName() + "." + col.getName());
            }
            // Cache the retrieved definitions.
            if (logger.isDebugEnabled())
                logger.debug("Saving " + type + " definitions (columns: "
                        + enumDefinitions.size() + ") to cache @ "
                        + table.getTable().getSchema() + "."
                        + table.getTable().getName());
            table.setEnumDefinitions(enumDefinitions);
        }
        if (enumDefinitions.size() == 0)
        {
            if (logger.isDebugEnabled())
                logger.debug("No " + type + " columns @ "
                        + table.getTable().getSchema() + "."
                        + table.getTable().getName());
            return;
        }

        // Table columns of enum type identified.
        // 1. Transform event's columns.
        ArrayList<ColumnSpec> columns = orc.getColumnSpec();
        ArrayList<ArrayList<ColumnVal>> columnValues = orc.getColumnValues();
        transformColumns(columns, columnValues, enumDefinitions, "COL");
        // 2. Transform event's keys.
        ArrayList<ColumnSpec> keys = orc.getKeySpec();
        ArrayList<ArrayList<ColumnVal>> keyValues = orc.getKeyValues();
        transformColumns(keys, keyValues, enumDefinitions, "KEY");
    }

    protected void transformColumns(ArrayList<ColumnSpec> columns,
            ArrayList<ArrayList<ColumnVal>> columnValues,
            HashMap<Integer, String[]> enumDefinitions, String typeCaption)
            throws ReplicatorException
    {
        // Looping through all and checking the real underlying index of each,
        // because there might be gaps as an outcome of some other filters.
        for (int c = 0; c < columns.size(); c++)
        {
            ColumnSpec colSpec = columns.get(c);
            if (enumDefinitions.containsKey(colSpec.getIndex()))
            {
                if (logger.isDebugEnabled())
                    logger.debug("Transforming " + typeCaption + "("
                            + colSpec.getIndex() + ")");
                if (colSpec.getType() == java.sql.Types.OTHER /* ENUM */
                        || colSpec.getType() == java.sql.Types.NULL)
                {
                    // Change the underlying type in the event.
                    colSpec.setType(java.sql.Types.VARCHAR);

                    // Iterate through all rows in the event and transform each.
                    for (int row = 0; row < columnValues.size(); row++)
                    {
                        // Fetch the column value. Note that rows may not be
                        // complete.
                        List<ColumnVal> colValues = columnValues.get(row);
                        if (colValues.size() <= c)
                            continue;
                        ColumnVal colValue = colValues.get(c);

                        // It must be integer at this point.
                        if (colValue.getValue() != null)
                        {
                            int currentValue = (Integer) colValue.getValue();
                            String enumDefs[] = enumDefinitions.get(colSpec
                                    .getIndex());
                            String newValue = null;
                            if (currentValue > enumDefs.length)
                            {
                                throw new ReplicatorException("MySQL value ("
                                        + currentValue + ") for ENUM @ Col "
                                        + colSpec.getIndex() + " Row " + row
                                        + " is greater than available values ("
                                        + enumDefs.length + ")");
                            }
                            else if (currentValue == 0)
                            {
                                // MySQL stores an empty string in enum 0:
                                newValue = "";
                            }
                            else
                                newValue = enumDefs[currentValue - 1];
                            colValue.setValue(newValue);
                            if (logger.isDebugEnabled())
                                logger.debug("Col " + colSpec.getIndex()
                                        + " Row " + row + ": " + currentValue
                                        + " -> " + newValue);
                        }
                        else
                        {
                            if (logger.isDebugEnabled())
                                logger.debug("Col " + colSpec.getIndex()
                                        + " Row " + row + ": null");
                        }
                    }
                }
                else if (colSpec.getType() == java.sql.Types.VARCHAR)
                    logger.warn("Column type is already VARCHAR! Assuming it is because this event was already transformed by this filter. Ignoring this column");
                else
                    logger.error("Unexpected column type ("
                            + colSpec.getType()
                            + ") in supposedly ENUM column! Ignoring this column");
            }
        }
    }

    /** Declares the data source name for this filter. */
    public void setDataSource(String dataSource)
    {
        this.dataSource = dataSource;
    }

}
