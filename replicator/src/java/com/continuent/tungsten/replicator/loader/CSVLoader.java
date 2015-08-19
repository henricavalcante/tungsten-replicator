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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import au.com.bytecode.opencsv.CSVParser;
import au.com.bytecode.opencsv.CSVReader;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.RowChangeData.ActionType;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplOptionParams;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Load THL events from a series of CSV files
 * 
 * @author <a href="mailto:jeff.mace@continuent.com">Jeff Mace</a>
 * @version 1.0
 */
public class CSVLoader extends Loader
{
    private static Logger                  logger               = Logger.getLogger(CSVLoader.class);

    ArrayList<String>                      tableNames           = null;
    HashMap<String, ArrayList<ColumnSpec>> columnDefinitions    = null;
    String                                 currentTableName     = null;
    int                                    currentTablePosition = 0;

    CSVParser                              parser               = null;
    private boolean                        hasNext              = true;
    private LineNumberReader               lineReader           = null;

    private ReplicatorRuntime              runtime              = null;

    /**
     * Complete plug-in configuration. This is called after setters are invoked
     * at the time that the replicator goes through configuration.
     * 
     * @throws ReplicatorException Thrown if configuration is incomplete or
     *             fails
     */
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        runtime = (ReplicatorRuntime) context;
    }

    /**
     * Prepare plug-in for use. This method is assumed to allocate all required
     * resources. It is called before the plug-in performs any operations.
     * 
     * @throws ReplicatorException Thrown if resource allocation fails
     */
    public void prepare(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        logger.info("Import tables from " + this.uri.getPath() + " to the "
                + this.getDefaultSchema() + " schema");

        tableNames = new ArrayList<String>();
        columnDefinitions = new HashMap<String, ArrayList<ColumnSpec>>();

        parser = new CSVParser(',', '"');

        File importDirectory = new File(this.uri.getPath());
        if (!importDirectory.exists())
        {
            throw new ReplicatorException("The " + this.uri.getPath()
                    + " directory does not exist");
        }

        for (File f : importDirectory.listFiles())
        {
            if (f.getName().endsWith(".def"))
            {
                this.prepareTableDefinition(f);
            }
        }

        if (this.tableNames.size() == 0)
        {
            throw new ReplicatorException("There are no tables to load");
        }
    }

    /**
     * Parse the table columns and data types from the table definition file
     * 
     * @param f
     * @throws ReplicatorException
     */
    protected void prepareTableDefinition(File f) throws ReplicatorException
    {
        ArrayList<ColumnSpec> columns = null;
        CSVReader columnReader = null;
        String[] columnDef = null;
        ColumnSpec cSpec = null;
        OneRowChange specOrc = new OneRowChange();
        String tableName = f.getName().substring(0, f.getName().length() - 4);

        try
        {
            tableNames.add(tableName);
            logger.info("Parse column definition for " + tableName);

            columns = new ArrayList<ColumnSpec>();
            columnReader = new CSVReader(new FileReader(f), ',', '"');
            while ((columnDef = columnReader.readNext()) != null)
            {
                if (columnDef.length < 2)
                {
                    throw new ReplicatorException(
                            "The column definition is not formatted properly");
                }

                cSpec = specOrc.new ColumnSpec();
                cSpec.setName(columnDef[0]);
                cSpec.setType(new Integer(columnDef[1]));

                if (columnDef.length == 3)
                {
                    cSpec.setLength(new Integer(columnDef[2]));
                }
                columns.add(cSpec);
            }

            columnDefinitions.put(tableName, columns);
        }
        catch (FileNotFoundException e)
        {
            /*
             * Do nothing, we won't import the file if the definition is
             * missing.
             */
        }
        catch (NumberFormatException e)
        {
            throw new ReplicatorException(e);
        }
        catch (IOException e)
        {
            throw new ReplicatorException(e);
        }
        catch (Exception e)
        {
            throw new ReplicatorException(e);
        }
        finally
        {
            try
            {
                if (columnReader != null)
                {
                    columnReader.close();
                }
            }
            catch (IOException e)
            {
                throw new ReplicatorException(e);
            }
        }
    }

    /**
     * Release all resources used by plug-in. This is called before the plug-in
     * is deallocated.
     * 
     * @throws ReplicatorException Thrown if resource deallocation fails
     */
    public void release(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        try
        {
            if (lineReader != null)
            {
                lineReader.close();
            }
        }
        catch (IOException e)
        {
            throw new ReplicatorException("Unable to close the CSV reader");
        }
    }

    /**
     * Set the value of the last event ID we have processed. The extractor is
     * responsible for returning the next event ID in sequence after this one
     * the next time extract() is called.
     * 
     * @param eventId Event ID at which to begin extracting
     * @throws ReplicatorException
     */
    public void setLastEventId(String eventId) throws ReplicatorException
    {
        if (eventId != null)
        {
            logger.info("Starting from an explicit event ID: " + eventId);
            int colonIndex = eventId.indexOf(':');

            currentTableName = eventId.substring(0, colonIndex);

            currentTablePosition = Integer.valueOf(eventId
                    .substring(colonIndex + 1));
        }
        else
        {
            if (this.tableNames.size() == 0)
            {
                currentTableName = null;
            }
            else
            {
                currentTableName = this.tableNames.get(0);
            }

            currentTablePosition = 0;
        }

        this.prepareCurrentTable();
    }

    /**
     * Update variables to point to the next table to load values for
     * 
     * @throws ReplicatorException
     */
    protected void nextTable() throws ReplicatorException
    {
        if (this.tableNames.size() == 0)
        {
            currentTableName = null;
        }
        else
        {
            currentTableName = this.tableNames.get(0);
        }

        currentTablePosition = 0;

        this.prepareCurrentTable();
    }

    /**
     * Open the CSV file containing the values for the current table
     * 
     * @throws ReplicatorException
     */
    protected void prepareCurrentTable() throws ReplicatorException
    {
        if (currentTableName == null)
        {
            hasNext = false;
            return;
        }

        this.tableNames.remove(currentTableName);

        try
        {
            hasNext = true;
            String fileName = uri.getPath() + "/" + currentTableName + ".txt";
            lineReader = new LineNumberReader(new InputStreamReader(
                    new FileInputStream(fileName)));

            for (int i = 0; i < currentTablePosition; i++)
            {
                lineReader.readLine();
            }
        }
        catch (FileNotFoundException e)
        {
            throw new ReplicatorException("Unable to find import file for "
                    + currentTableName);
        }
        catch (IOException e)
        {
            throw new ReplicatorException("Unable to skip "
                    + currentTablePosition + " characters in "
                    + currentTableName);
        }
    }

    /**
     * Extract the next available DBMSEvent from the CSV file
     * 
     * @return next DBMSEvent found in the logs
     * @throws IOException
     */
    public synchronized DBMSEvent extract() throws ReplicatorException,
            InterruptedException
    {
        String[] rowDef = null;
        OneRowChange orc = null;
        ArrayList<ColumnVal> columnValues = null;
        ColumnSpec cDef = null;
        ColumnVal cVal = null;
        DBMSEvent dbmsEvent = null;
        ArrayList<DBMSData> dataArray = null;
        RowChangeData rowChangeData = null;

        // Nothing more to import
        if (this.currentTableName == null)
        {
            return this.getFinishLoadEvent();
        }

        try
        {
            rowChangeData = new RowChangeData();
            dataArray = new ArrayList<DBMSData>();

            orc = new OneRowChange();
            orc.setAction(ActionType.INSERT);
            orc.setSchemaName(this.getDefaultSchema());
            orc.setTableName(this.currentTableName);
            orc.setColumnSpec(this.columnDefinitions.get(this.currentTableName));

            try
            {
                while ((rowDef = this.readNext(lineReader)) != null)
                {
                    columnValues = new ArrayList<ColumnVal>();

                    for (int i = 0; i < rowDef.length; i++)
                    {
                        cDef = this.columnDefinitions
                                .get(this.currentTableName).get(i);
                        cVal = orc.new ColumnVal();

                        try
                        {
                            cVal.setValue(this.parseStringValue(cDef.getType(),
                                    rowDef[i]));
                        }
                        catch (Exception e)
                        {
                            throw new ReplicatorException(
                                    "Unable to parse value for "
                                            + cDef.getName() + " from "
                                            + rowDef[i]);
                        }

                        columnValues.add(cVal);
                    }

                    orc.getColumnValues().add(columnValues);

                    // Limit the size of each INSERT to the chunkSize
                    if (orc.getColumnValues().size() >= this.chunkSize)
                    {
                        break;
                    }
                }
            }
            catch (IOException e)
            {
                throw new ReplicatorException("Unable to read next line from "
                        + this.currentTableName);
            }

            rowChangeData.appendOneRowChange(orc);
            dataArray.add(rowChangeData);

            runtime.getMonitor().incrementEvents(dataArray.size());
            dbmsEvent = new DBMSEvent(this.currentTableName + ":"
                    + lineReader.getLineNumber(), null, dataArray, true, null);
            dbmsEvent.setMetaDataOption(ReplOptionParams.SHARD_ID,
                    dbmsEvent.getEventId());

            // Do not return an empty event if there are no column values
            if (orc.getColumnValues().size() == 0)
            {
                return null;
            }

            return dbmsEvent;
        }
        finally
        {
            if (hasNext == false)
            {
                this.nextTable();
            }
        }
    }

    /**
     * Load the next set of CSV values from the file
     * 
     * @param reader
     * @return An array of strings representing the next row
     * @throws IOException
     */
    protected String[] readNext(LineNumberReader reader) throws IOException
    {
        String[] rowDef = null;

        do
        {
            String nextLine = reader.readLine();
            if (nextLine == null)
            {
                hasNext = false;
                return rowDef;
            }

            String[] r = parser.parseLineMulti(nextLine);
            if (r.length > 0)
            {
                if (rowDef == null)
                {
                    rowDef = r;
                }
                else
                {
                    String[] t = new String[rowDef.length + r.length];
                    System.arraycopy(rowDef, 0, t, 0, rowDef.length);
                    System.arraycopy(r, 0, t, rowDef.length, r.length);
                    rowDef = t;
                }
            }
        }
        while (parser.isPending());

        return rowDef;
    }

    /**
     * Extract starting after the event ID provided as an argument. This is
     * equivalent to invoking setLastEventId() followed by extract().
     * 
     * @param eventId Event ID at which to begin extracting
     * @return DBMSEvent corresponding to the id
     * @throws ReplicatorException Thrown if extractor processing fails
     * @throws InterruptedException Thrown if the applier is interrupted
     */
    public DBMSEvent extract(String eventId) throws ReplicatorException,
            InterruptedException
    {
        this.setLastEventId(eventId);
        return this.extract();
    }

    /**
     * Returns the last event ID committed in the database from which we are
     * extracting. It is used to help synchronize state between the database and
     * the transaction history log. Values returned from this call must
     * correspond with the last extracted DBMSEvent.eventId as follows:
     * <ol>
     * <li>If the returned value is greater than DBMSEvent.eventId, the database
     * has more recent updates</li>
     * <li>If the returned value is equal to DBMSEvent.eventId, all events have
     * been extracted</li>
     * </ol>
     * It should not be possible to receive a value that is less than the last
     * extracted DBMSEvent.eventId as this implies that the extractor is somehow
     * ahead of the state of the database, which would be inconsistent.
     * 
     * @return A current event ID that can be compared with event IDs in
     *         DBMSEvent
     * @throws ReplicatorException
     * @throws InterruptedException
     */
    public String getCurrentResourceEventId() throws ReplicatorException,
            InterruptedException
    {
        List<String> values = this.params.get("eventid");
        if (values != null)
        {
            return values.get(0);
        }
        else
        {
            throw new ReplicatorException(
                    "Unable to determine the final event id");
        }
    }

    /**
     * Return the source ID to use in the THL events
     */
    protected String getSourceID() throws Exception
    {
        return this.uri.getHost();
    }

    /**
     * Return the schema to use in the THL events
     */
    protected String getDefaultSchema() throws ReplicatorException
    {
        List<String> values = this.params.get("schema");
        if (values != null)
        {
            return values.get(0);
        }
        else
        {
            throw new ReplicatorException("Unable to determine the schema");
        }
    }
}
