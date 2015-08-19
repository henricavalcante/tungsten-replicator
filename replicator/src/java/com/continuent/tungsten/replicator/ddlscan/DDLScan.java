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
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.replicator.ddlscan;

import java.io.IOException;
import java.io.Writer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;

import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.exception.MethodInvocationException;
import org.apache.velocity.exception.ParseErrorException;
import org.apache.velocity.exception.ResourceNotFoundException;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.log.Log4JLogChute;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntimeConf;
import com.continuent.tungsten.replicator.database.Column;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;
import com.continuent.tungsten.replicator.database.MySQLDatabase;
import com.continuent.tungsten.replicator.database.OracleDatabase;
import com.continuent.tungsten.replicator.database.Table;
import com.continuent.tungsten.replicator.filter.EnumToStringFilter;
import com.continuent.tungsten.replicator.filter.RenameDefinitions;

/**
 * Main DDLScan functionality is programmed here.
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 * @version 1.0
 */
public class DDLScan
{
    private String            url                 = null;
    private String            dbName              = null;
    private String            user                = null;
    private String            pass                = null;

    private Template          template            = null;

    private Database          db                  = null;

    private ArrayList<String> reservedWordsOracle = null;
    private ArrayList<String> reservedWordsMySQL  = null;

    VelocityEngine            velocity            = null;
    RenameDefinitions         renameDefinitions   = null;

    /**
     * Creates a new <code>DDLScan</code> object from provided JDBC URL
     * connection credentials and template file.
     * 
     * @param url JDBC URL connection string.
     * @param dbName Database/schema to connect to.
     * @throws Exception
     */
    public DDLScan(String url, String dbName, String user, String pass)
            throws ReplicatorException
    {
        this.url = url;
        this.user = user;
        this.pass = pass;
        this.dbName = dbName;
    }

    /**
     * Connect to the underlying database.
     * 
     * @throws ReplicatorException
     */
    public void prepare(String additionalPath) throws ReplicatorException,
            InterruptedException, SQLException
    {
        db = DatabaseFactory.createDatabase(url, user, pass);
        db.connect();

        // Prepare reserved words lists.
        OracleDatabase oracle = new OracleDatabase();
        reservedWordsOracle = oracle.getReservedWords();
        MySQLDatabase mysql = new MySQLDatabase();
        reservedWordsMySQL = mysql.getReservedWords();

        // Do we need additional paths for loader?
        String userPath = "";
        if (additionalPath != null)
            userPath = "," + additionalPath;

        // Configure and initialize Velocity engine. Using ourselves as a
        // logger.
        velocity = new VelocityEngine();
        velocity.setProperty(RuntimeConstants.RUNTIME_LOG_LOGSYSTEM_CLASS,
                "org.apache.velocity.runtime.log.Log4JLogChute");
        velocity.setProperty(Log4JLogChute.RUNTIME_LOG_LOG4J_LOGGER,
                DDLScan.class.toString());
        velocity.setProperty(RuntimeConstants.FILE_RESOURCE_LOADER_PATH, ".,"
                + ReplicatorRuntimeConf.locateReplicatorHomeDir()
                + "/support/ddlscan" + userPath);
        // Must allow setting of null values in the RHS #set operator to be
        // compatible with the way JAVA methods communicate.
        velocity.setProperty(RuntimeConstants.SET_NULL_ALLOWED, true);
        velocity.init();
    }

    /**
     * Compiles the given Velocity template file.
     */
    public void parseTemplate(String templateFile) throws ReplicatorException
    {
        try
        {
            // Parse the template.
            template = velocity.getTemplate(templateFile);
        }
        catch (ResourceNotFoundException rnfe)
        {
            throw new ReplicatorException("Couldn't open the template", rnfe);
        }
        catch (ParseErrorException pee)
        {
            throw new ReplicatorException("Problem parsing the template", pee);
        }
    }

    /**
     * Tries to load rename definitions file. It will be used for all subsequent
     * scan calls.
     * 
     * @throws ReplicatorException On parsing or CSV format errors.
     * @throws IOException If file cannot be read.
     * @see #resetRenameDefinitions()
     * @see #scan(String, Hashtable, Writer)
     */
    public void parseRenameDefinitions(String definitionsFile)
            throws ReplicatorException, IOException
    {
        renameDefinitions = new RenameDefinitions(definitionsFile);
        renameDefinitions.parseFile();
    }

    /**
     * Stop using rename definitions file for future scan(...) calls.
     * 
     * @see #parseRenameDefinitions(String)
     * @see #scan(String, Hashtable, Writer)
     */
    public void resetRenameDefinitions()
    {
        renameDefinitions = null;
    }

    /**
     * Scans and extracts metadata from the database of requested tables. Calls
     * merge(...) against each found table.
     * 
     * @param tablesToFind Comma-separated list of tables to find (don't specify
     *            schema name), null for all tables in schema. Regular
     *            expressions are *not* supported.
     * @param templateOptions Options (option->value) to pass to the template.
     * @param writer Writer object to use for appending rendered template. Make
     *            sure to initialize it before and flush/close it after
     *            manually.
     * @return Rendered template data.
     */
    public String scan(String tablesToFind,
            Hashtable<String, String> templateOptions, Writer writer)
            throws ReplicatorException, InterruptedException, SQLException,
            IOException
    {
        // How many tables were actually matched?
        int tablesRendered = 0;

        ArrayList<Table> tables = null;
        if (tablesToFind == null)
        {
            // Retrieve all tables available with unique index information.
            tables = db.getTables(dbName, true, true);
        }
        else
        {
            // Retrieve only requested tables.
            tables = new ArrayList<Table>();
            String[] tableNames = tablesToFind.split(",");
            for (String tableName : tableNames)
            {
                Table table = db.findTable(dbName, tableName, true);
                if (table != null)
                    tables.add(table);
            }
        }

        // Make a context object and populate with the data. This is where
        // the Velocity engine gets the data to resolve the references in
        // the template.
        VelocityContext context = new VelocityContext();

        // User options passed to the template.
        for (String option : templateOptions.keySet())
        {
            context.put(option, templateOptions.get(option));
        }

        // Source connection details.
        context.put("dbName", dbName);
        context.put("user", user);
        context.put("url", url);

        // Database object.
        context.put("db", db);

        // RenameDefinitions object used (if any).
        context.put("renameDefinitions", renameDefinitions);

        // Some handy utilities.
        context.put("enum", EnumToStringFilter.class);
        context.put("date", new java.util.Date()); // Current time.
        context.put("reservedWordsOracle", reservedWordsOracle);
        context.put("reservedWordsMySQL", reservedWordsMySQL);
        context.put("velocity", velocity);
        
        // Iterate through all available tables in the database.
        int size = tables.size();
        for (int i = 0; i < size; i++)
        {
            Table table = tables.get(i);

            // If this is the first table, mark the context appropriately.
            if (i == 0)
                context.put("first", true);
            else
                context.put("first", false);

            // Similarly for the last table mark the context accordingly.
            if (i >= size - 1)
                context.put("last", true);
            else
                context.put("last", false);

            // If requested, do the renaming.
            rename(table);

            // Velocity merge.
            merge(context, table, writer);

            tablesRendered++;
        }

        // No tables have been found and/or matched.
        if (tablesRendered == 0)
        {
            // Render the template once without table data. Eg. to output help.
            merge(context, null, writer);
        }

        return writer.toString();
    }

    /**
     * If renameDefinitions object is prepared, does the lookup and renaming of
     * schema, table and columns. Nothing is done if renameDefinitions is null.
     * 
     * @see #parseRenameDefinitions(String)
     */
    private void rename(Table table)
    {
        if (renameDefinitions != null)
        {
            // Rename columns.
            for (Column col : table.getAllColumns())
            {
                String newColName = renameDefinitions.getNewColumnName(
                        table.getSchema(), table.getName(), col.getName());
                if (newColName != null)
                    col.setName(newColName);
            }

            // Get new table name if there's a request.
            String newTableName = renameDefinitions.getNewTableName(
                    table.getSchema(), table.getName());

            // Get new schema name if there's a request.
            String newSchemaName = renameDefinitions.getNewSchemaName(
                    table.getSchema(), table.getName());

            // Finally, do the actual renaming of schema and table.
            if (newTableName != null)
                table.setTable(newTableName);
            if (newSchemaName != null)
                table.setSchema(newSchemaName);
        }
    }

    /**
     * Generate output by merging extracted metadata to a template.
     * 
     * @param context Context is shared each time, only table is changed.
     * @param templateFile Path to Velocity template.
     * @param writer Initialized Writer object to append output to.
     */
    private void merge(VelocityContext context, Table table, Writer writer)
            throws ReplicatorException
    {
        try
        {
            // Actual table with columns, keys, etc.
            context.put("table", table);

            // Now have the template engine process the template using the data
            // placed into the context. Think of it as a 'merge' of the template
            // and the data to produce the output stream.
            if (template != null)
                template.merge(context, writer);
        }
        catch (MethodInvocationException mie)
        {
            throw new ReplicatorException(
                    "Something invoked in the template caused problem", mie);
        }
        catch (Exception e)
        {
            throw new ReplicatorException(e);
        }
    }

    /**
     * Disconnect from the THL database.
     */
    public void release()
    {
        if (db != null)
        {
            // This also closes connection.
            db.close();
            db = null;
        }
    }

}
