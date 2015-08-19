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
 * Initial developer(s): Jeff Mace
 */

package com.continuent.tungsten.replicator.loader;

import java.io.Serializable;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.dbms.RowChangeData.ActionType;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplOptionParams;
import com.continuent.tungsten.replicator.plugin.PluginContext;

public abstract class JdbcLoader extends Loader
{
    private static Logger           logger                        = Logger.getLogger(JdbcLoader.class);

    private ReplicatorRuntime       runtime                       = null;

    protected String                driver                        = null;
    protected String                url                           = null;
    protected String                user                          = null;
    protected String                password                      = null;
    protected Database              conn                          = null;
    protected Statement             statement                     = null;
    protected DatabaseMetaData      metadata                      = null;
    protected ResultSet             importTables                  = null;
    protected List<String>          includeSchemas                = null;
    protected ArrayList<ColumnSpec> columnDefinitions             = null;
    protected String                tungstenServiceSchema         = null;
    protected String                tungstenServiceSchemaPosition = null;
    protected boolean               includeStructure              = true;
    int                             currentTablePosition          = 0;
    boolean                         extractCreateTableStatement   = false;

    /**
     * Set the MySQL user to connect with
     * 
     * @param user
     */
    public void setUser(String user)
    {
        this.user = user;
    }

    /**
     * Set the MySQL password to connect with
     * 
     * @param password
     */
    public void setPassword(String password)
    {
        this.password = password;
    }

    public void setTungstenServiceSchema(String schemaName)
    {
        this.tungstenServiceSchema = schemaName;
    }

    public String getTungstenServiceSchema()
    {
        return this.tungstenServiceSchema;
    }

    /**
     * Set the list of schemas to include when extracting events. A
     * comma-separated list should be provided.
     * 
     * @param includeSchemas
     */
    public void setIncludeSchemas(String includeSchemas)
    {
        this.includeSchemas = Arrays.asList(includeSchemas.split(","));
    }

    public void setIncludeStructure(boolean includeStructure)
    {
        this.includeStructure = includeStructure;
    }

    public String getTungstenSchema()
    {
        return runtime.getReplicatorProperties().getString(
                ReplicatorConf.METADATA_SCHEMA);
    }

    /**
     * Prepare to start extraction from the first table found {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.RawExtractor#setLastEventId(java.lang.String)
     */
    @Override
    public void setLastEventId(String eventId) throws ReplicatorException
    {
        try
        {
            if (eventId != null)
            {
                throw new ReplicatorException(
                        "Unable to start extraction from " + eventId);
            }
            else
            {
                nextTable();
            }
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(e);
        }
    }

    /**
     * Prepare the next table for extraction by resetting the position and
     * parsing column definitions
     * 
     * @throws ReplicatorException
     * @throws SQLException
     */
    protected void nextTable() throws ReplicatorException, SQLException
    {
        while (importTables.next())
        {
            if (includeImportTable() == true)
            {
                currentTablePosition = 0;
                extractCreateTableStatement = this.includeStructure;
                prepareImportTable();
                break;
            }
        }
    }

    /**
     * Should the current table be imported
     */
    protected boolean includeImportTable() throws SQLException
    {
        if (includeSchemas == null)
        {
            return true;
        }

        if (includeSchemas.contains(importTables.getString("TABLE_SCHEM")) == true)
        {
            return true;
        }
        else
        {
            return false;
        }
    }

    /**
     * Parse column definitions for the current table
     * 
     * @throws SQLException
     */
    protected void prepareImportTable() throws SQLException
    {
        ResultSet columnList = null;
        OneRowChange specOrc = new OneRowChange();

        columnDefinitions = new ArrayList<ColumnSpec>();

        try
        {
            columnList = metadata.getColumns(
                    importTables.getString("TABLE_CAT"),
                    importTables.getString("TABLE_SCHEM"),
                    importTables.getString("TABLE_NAME"), null);
            while (columnList.next())
            {
                ColumnSpec cSpec = specOrc.new ColumnSpec();
                cSpec.setName(columnList.getString("COLUMN_NAME"));
                cSpec.setType(extractColumnType(columnList));
                cSpec.setLength(columnList.getInt("COLUMN_SIZE"));
                columnDefinitions.add(cSpec);

                logger.debug("Import column "
                        + columnList.getString("TABLE_SCHEM") + ":"
                        + columnList.getString("TABLE_NAME") + ":"
                        + columnList.getString("COLUMN_NAME") + ":"
                        + columnList.getString("DATA_TYPE") + ":"
                        + columnList.getString("TYPE_NAME") + ":"
                        + columnList.getInt("COLUMN_SIZE"));
            }
        }
        finally
        {
            if (columnList != null)
            {
                columnList.close();
            }
        }
    }

    /**
     * Determine the java.sql.Types type of a column based on the column
     * metadata
     * 
     * @param columnList
     * @return The java.sql.Types value for the column
     * @throws SQLException
     */
    protected int extractColumnType(ResultSet columnList) throws SQLException
    {
        int columnType = columnList.getInt("DATA_TYPE");
        int returnType = java.sql.Types.NULL;

        switch (columnType)
        {
            case java.sql.Types.BIGINT :
                returnType = java.sql.Types.NUMERIC;
                break;
            case 0 :
                String typeName = columnList.getString("TYPE_NAME");
                if (typeName.startsWith("char") == true)
                {
                    returnType = java.sql.Types.CHAR;
                }
                else if ("float".equals(typeName) == true)
                {
                    returnType = java.sql.Types.FLOAT;
                }
                else if (typeName.startsWith("decimal"))
                {
                    returnType = java.sql.Types.DECIMAL;
                }
                break;
            default :
                returnType = columnType;
                break;
        }

        return returnType;
    }

    /**
     * Take a raw value and return the proper Java data type for the
     * java.sql.Types type given
     */
    protected Serializable extractRowValue(int type, ResultSet rowValues,
            String columnName) throws Exception
    {
        switch (type)
        {
            case java.sql.Types.BIT :
            case java.sql.Types.BOOLEAN :
            {
                return rowValues.getBoolean(columnName);
            }

            case java.sql.Types.CHAR :
            case java.sql.Types.VARCHAR :
            case java.sql.Types.LONGVARCHAR :
            case java.sql.Types.NCHAR :
            case java.sql.Types.NVARCHAR :
            case java.sql.Types.LONGNVARCHAR :
            case java.sql.Types.NCLOB :
            case java.sql.Types.CLOB :
            {
                return rowValues.getString(columnName);
            }

            case java.sql.Types.TINYINT :
            case java.sql.Types.SMALLINT :
            case java.sql.Types.INTEGER :
            {
                return rowValues.getInt(columnName);
            }

            case java.sql.Types.FLOAT :
            {
                return rowValues.getFloat(columnName);
            }

            case java.sql.Types.DOUBLE :
            {
                return rowValues.getDouble(columnName);
            }

            case java.sql.Types.REAL :
            {
                return rowValues.getFloat(columnName);
            }

            case java.sql.Types.DECIMAL :
            case java.sql.Types.NUMERIC :
            {
                return rowValues.getBigDecimal(columnName);
            }

            case java.sql.Types.TIMESTAMP :
            {
                return java.sql.Timestamp.valueOf(rowValues
                        .getString(columnName));
            }

            case java.sql.Types.DATE :
            {
                return java.sql.Date.valueOf(rowValues.getString(columnName));
            }

            case java.sql.Types.TIME :
            {
                return java.sql.Time.valueOf(rowValues.getString(columnName));
            }

            case java.sql.Types.BINARY :
            case java.sql.Types.VARBINARY :
            case java.sql.Types.LONGVARBINARY :
            case java.sql.Types.BLOB :
            {
                throw new Exception(
                        "THL loader does not yet support binary data");
            }

            case java.sql.Types.NULL :
            case java.sql.Types.BIGINT :
            case java.sql.Types.OTHER :
            case java.sql.Types.JAVA_OBJECT :
            case java.sql.Types.DISTINCT :
            case java.sql.Types.STRUCT :
            case java.sql.Types.ARRAY :
            case java.sql.Types.REF :
            case java.sql.Types.DATALINK :
            case java.sql.Types.ROWID :
            case java.sql.Types.SQLXML :
            {
                throw new Exception("unsupported data type " + type);
            }

            default :
            {
                throw new Exception("unknown data type " + type);
            }
        }
    }

    /**
     * Extract a THL event up to getChunkSize() rows {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.RawExtractor#extract()
     */
    @Override
    public synchronized DBMSEvent extract() throws ReplicatorException,
            InterruptedException
    {
        DBMSEvent dbmsEvent = null;
        ArrayList<DBMSData> dataArray = null;
        RowChangeData rowChangeData = null;
        StatementData statementData = null;
        String createSchemaStatement = null;
        String createTableStatement = null;

        try
        {
            if (importTables.isClosed() == true)
            {
                return null;
            }

            if (importTables.isAfterLast() == true)
            {
                importTables.close();
                /**
                 * There are no more tables to import
                 */
                return getFinishLoadEvent();
            }
            else
            {
                dataArray = new ArrayList<DBMSData>();

                if (extractCreateTableStatement == true)
                {
                    extractCreateTableStatement = false;
                    createSchemaStatement = buildCreateSchemaStatement();
                    createTableStatement = buildCreateTableStatement();
                }

                /**
                 * Add the CREATE SCHEMA/TABLE statements to the top of the
                 * event
                 */
                if (createTableStatement != null)
                {
                    if (createSchemaStatement != null)
                    {
                        statementData = new StatementData(createSchemaStatement);
                        dataArray.add(statementData);
                    }

                    statementData = new StatementData(createTableStatement);
                    statementData.setDefaultSchema(importTables
                            .getString("TABLE_SCHEM"));
                    dataArray.add(statementData);
                }

                /**
                 * Build this list of rows to insert
                 */
                rowChangeData = extractRowChangeData();
                if (rowChangeData != null)
                {
                    dataArray.add(rowChangeData);
                }

                /**
                 * Nothing to do for this table so we need to move on
                 */
                if (dataArray.size() == 0)
                {
                    try
                    {
                        nextTable();
                    }
                    catch (SQLException e)
                    {
                        throw new ReplicatorException(e);
                    }

                    return null;
                }

                runtime.getMonitor().incrementEvents(dataArray.size());
                dbmsEvent = new DBMSEvent(importTables.getString("TABLE_SCHEM")
                        + "." + importTables.getString("TABLE_NAME"), null,
                        dataArray, true, null);
                dbmsEvent.setMetaDataOption(ReplOptionParams.SHARD_ID,
                        dbmsEvent.getEventId());

                return dbmsEvent;
            }
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(e);
        }
        finally
        {
        }
    }

    /**
     * Return a statement that will create the schema, null if no create schema
     * can be given
     */
    protected String buildCreateSchemaStatement() throws ReplicatorException
    {
        return null;
    }

    /**
     * Return a statement that will create the table structure, null if no
     * create table can be given
     */
    protected String buildCreateTableStatement() throws ReplicatorException
    {
        return null;
    }

    /**
     * Extract the actual rows from the database and build the change set
     */
    @SuppressWarnings("unchecked")
    protected RowChangeData extractRowChangeData() throws SQLException
    {
        OneRowChange orc = null;
        ArrayList<ColumnVal> columnValues = null;
        ColumnSpec cDef = null;
        ColumnVal cVal = null;
        RowChangeData rowChangeData = null;
        ResultSet extractedRows = null;

        rowChangeData = new RowChangeData();

        orc = new OneRowChange();
        orc.setAction(ActionType.INSERT);
        orc.setSchemaName(importTables.getString("TABLE_SCHEM"));
        orc.setTableName(importTables.getString("TABLE_NAME"));
        orc.setColumnSpec((ArrayList<ColumnSpec>) columnDefinitions.clone());

        rowChangeData.appendOneRowChange(orc);

        logger.debug("SELECT * FROM " + importTables.getString("TABLE_SCHEM")
                + "." + importTables.getString("TABLE_NAME") + " LIMIT "
                + currentTablePosition + " , " + getChunkSize());
        extractedRows = statement.executeQuery("SELECT * FROM "
                + importTables.getString("TABLE_SCHEM") + "."
                + importTables.getString("TABLE_NAME") + " LIMIT "
                + currentTablePosition + " , " + getChunkSize());
        while (extractedRows.next())
        {
            columnValues = new ArrayList<ColumnVal>();

            for (int i = 0; i < columnDefinitions.size(); i++)
            {
                cDef = columnDefinitions.get(i);
                cVal = orc.new ColumnVal();

                try
                {
                    logger.debug(orc.getSchemaName() + "." + orc.getTableName()
                            + "." + cDef.getName() + " = "
                            + extractedRows.getString(cDef.getName()));
                    cVal.setValue(extractRowValue(cDef.getType(),
                            extractedRows, cDef.getName()));
                    if (cVal.getValue() != null)
                    {
                        logger.debug("Extracted value is "
                                + cVal.getValue().toString());
                    }
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                    cVal.setValue(null);
                    logger.error("Unable to extract value of "
                            + extractedRows.getString(cDef.getName()) + " for "
                            + orc.getSchemaName() + "." + orc.getTableName()
                            + "." + cDef.getName() + " of Type "
                            + cDef.getType());
                }

                columnValues.add(cVal);
            }
            currentTablePosition++;

            orc.getColumnValues().add(columnValues);
        }

        /**
         * Do not return an empty event if there are no column values
         */
        if (orc.getColumnValues().size() == 0)
        {
            return null;
        }

        return rowChangeData;
    }

    @Override
    public DBMSEvent extract(String eventId) throws ReplicatorException,
            InterruptedException
    {
        setLastEventId(eventId);
        return extract();
    }

    @Override
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        runtime = (ReplicatorRuntime) context;
    }

    @Override
    public void prepare(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        ResultSet tungstenSchemaTables = null;
        ResultSet trepCommitRows = null;

        /**
         * Initiate the JDBC connection
         */
        try
        {
            // Load driver if provided.
            if (driver != null)
            {
                try
                {
                    Class.forName(driver);
                }
                catch (Exception e)
                {
                    throw new ReplicatorException("Unable to load driver: "
                            + driver, e);
                }
            }

            // Create the database.
            conn = DatabaseFactory.createDatabase(url, user, password, true);
            conn.connect();
            statement = conn.createStatement();
        }
        catch (SQLException e)
        {
            String message = String.format("Failed using url=%s, user=%s", url,
                    user);
            throw new ReplicatorException(message, e);
        }

        if (getLockTables() == true)
        {
            try
            {
                lockTables();
            }
            catch (SQLException e)
            {
                String message = "Unable to lock tables : " + e.getMessage();
                throw new ReplicatorException(message, e);
            }
        }

        try
        {
            metadata = conn.getDatabaseMetaData();
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(e);
        }

        /**
         * If we are using a Tungsten service, make sure it exists and has a
         * single row in trep_commit_seqno
         */
        if (getTungstenServiceSchema() != null)
        {
            try
            {
                tungstenSchemaTables = metadata.getTables(null,
                        getTungstenServiceSchema(), null, null);
                while (tungstenSchemaTables.next())
                {
                    if ("trep_commit_seqno"
                            .equalsIgnoreCase(tungstenSchemaTables
                                    .getString("TABLE_NAME")) == true)
                    {
                        logger.debug("Get eventid from "
                                + tungstenSchemaTables.getString("TABLE_SCHEM")
                                + "."
                                + tungstenSchemaTables.getString("TABLE_NAME"));

                        trepCommitRows = statement
                                .executeQuery("SELECT COUNT(*) as `cnt` FROM "
                                        + tungstenSchemaTables
                                                .getString("TABLE_SCHEM")
                                        + "."
                                        + tungstenSchemaTables
                                                .getString("TABLE_NAME"));
                        if (trepCommitRows.first() != true)
                        {
                            throw new ReplicatorException(
                                    "Unable to determine the number of rows in "
                                            + tungstenSchemaTables
                                                    .getString("TABLE_SCHEM")
                                            + "."
                                            + tungstenSchemaTables
                                                    .getString("TABLE_NAME"));
                        }

                        if (trepCommitRows.getInt("cnt") != 1)
                        {
                            throw new ReplicatorException(
                                    "There are more than 1 row in "
                                            + tungstenSchemaTables
                                                    .getString("TABLE_SCHEM")
                                            + "."
                                            + tungstenSchemaTables
                                                    .getString("TABLE_NAME"));
                        }

                        trepCommitRows.close();
                        trepCommitRows = statement
                                .executeQuery("SELECT * FROM "
                                        + tungstenSchemaTables
                                                .getString("TABLE_SCHEM")
                                        + "."
                                        + tungstenSchemaTables
                                                .getString("TABLE_NAME"));
                        if (trepCommitRows.first() != true)
                        {
                            throw new ReplicatorException(
                                    "Unable to determine the eventid from "
                                            + tungstenSchemaTables
                                                    .getString("TABLE_SCHEM")
                                            + "."
                                            + tungstenSchemaTables
                                                    .getString("TABLE_NAME"));
                        }

                        this.tungstenServiceSchemaPosition = trepCommitRows
                                .getString("eventid");
                    }
                }

                if (this.tungstenServiceSchemaPosition == null)
                {
                    throw new ReplicatorException(
                            "Unable to determine the eventid from "
                                    + getTungstenServiceSchema());
                }
            }
            catch (SQLException e)
            {
                throw new ReplicatorException(e);
            }
            finally
            {
                try
                {
                    if (tungstenSchemaTables != null)
                    {
                        tungstenSchemaTables.close();
                    }

                    if (trepCommitRows != null)
                    {
                        trepCommitRows.close();
                    }
                }
                catch (SQLException e)
                {
                    throw new ReplicatorException(e);
                }
            }
        }

        /**
         * Initiate the list of tables to load data from
         */
        try
        {
            importTables = metadata.getTables(null, null, null, null);
            while (importTables.next())
            {
                if (includeImportTable() == true)
                {
                    logger.info("Import table "
                            + importTables.getString("TABLE_SCHEM") + "."
                            + importTables.getString("TABLE_NAME"));
                }
            }
            importTables.first();
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(e);
        }
    }

    @Override
    public void release(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        /**
         * Close all open connections and result sets
         */
        try
        {
            if (importTables != null)
            {
                importTables.close();
                importTables = null;
            }
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(e);
        }

        statement = null;
        if (conn != null)
        {
            conn.close();
            conn = null;
        }
    }

    /**
     * Parse the value of tungstenServiceSchemaPosition for the current eventId
     * 
     * @throws ReplicatorException
     * @throws InterruptedException
     */
    public String getCurrentResourceEventId() throws ReplicatorException,
            InterruptedException
    {
        if (getTungstenServiceSchema() != null)
        {
            int dotIndex = this.tungstenServiceSchemaPosition.indexOf('.');
            int semiIndex = this.tungstenServiceSchemaPosition.indexOf(';');

            if (dotIndex == -1)
            {
                throw new ReplicatorException(
                        "Unable to find '.' separator in Tungsten service position "
                                + this.tungstenServiceSchemaPosition);
            }

            if (semiIndex == -1)
            {
                return this.tungstenServiceSchemaPosition
                        .substring(dotIndex + 1);
            }
            else
            {
                return this.tungstenServiceSchemaPosition.substring(
                        dotIndex + 1, semiIndex);
            }
        }
        else
        {
            return null;
        }
    }

    /**
     * A placeholder function for loaders that can lock tables
     * 
     * @throws SQLException
     */
    public void lockTables() throws SQLException
    {
        // Do Nothing
    }
}