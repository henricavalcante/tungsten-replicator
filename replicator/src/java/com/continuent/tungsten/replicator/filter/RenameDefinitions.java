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

package com.continuent.tungsten.replicator.filter;

import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;

import au.com.bytecode.opencsv.CSVReader;

/**
 * Definitions of requests to rename schemas, tables and columns.
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 * @version 1.0
 */
public class RenameDefinitions
{
    private static Logger logger = Logger.getLogger(RenameDefinitions.class);

    static class RenameRequest
    {
        private final String origSchema;
        private final String origTable;
        private final String origColumn;
        private final String newSchema;
        private final String newTable;
        private final String newColumn;

        public RenameRequest(String origSchema, String origTable,
                String origColumn, String newSchema, String newTable,
                String newColumn)
        {
            this.origSchema = origSchema;
            this.origTable = origTable;
            this.origColumn = origColumn;
            this.newSchema = newSchema;
            this.newTable = newTable;
            this.newColumn = newColumn;
        }

        public String getOrigSchema()
        {
            return origSchema;
        }

        public String getOrigTable()
        {
            return origTable;
        }

        public String getOrigColumn()
        {
            return origColumn;
        }

        public String getNewSchema()
        {
            return newSchema;
        }

        public String getNewTable()
        {
            return newTable;
        }

        public String getNewColumn()
        {
            return newColumn;
        }

        @Override
        public String toString()
        {
            return origSchema + "." + origTable + "." + origColumn + " -> "
                    + newSchema + "." + newTable + "." + newColumn;
        }
    }

    private Hashtable<String, Hashtable<String, Hashtable<String, RenameRequest>>> lookupSchemaTableCol;

    /**
     * Path to rename definition file.
     */
    private String                                                                 definitionFile;

    public RenameDefinitions(String definitionFile)
    {
        this.definitionFile = definitionFile;
    }
    
    /**
     * Returns rename definitions file name used.
     */
    public String getDefinitionsFile()
    {
        return definitionFile;
    }

    /**
     * Removes trailing spaces and comments from a column.
     */
    private String cleanup(String col)
    {
        int idx = col.indexOf(" ");
        if (idx > 0)
            return col.substring(0, idx);
        else
            return col;
    }

    /**
     * Parses rename definition file, validates format and populates lookup
     * structures.
     * 
     * @throws ReplicatorException On format issues.
     */
    public void parseFile() throws IOException, ReplicatorException
    {
        // Clear previous lookup data.
        lookupSchemaTableCol = new Hashtable<String, Hashtable<String, Hashtable<String, RenameRequest>>>();

        logger.info("Parsing " + definitionFile + ":");
        CSVReader reader = new CSVReader(new FileReader(definitionFile));
        String[] cols;
        while ((cols = reader.readNext()) != null)
        {
            if (cols.length == 1 && cols[0].length() == 0)
            {
                // Empty line.
            }
            else if (cols.length > 0 && cols[0].length() > 0
                    && cols[0].charAt(0) == '#')
            {
                // Comment line.
            }
            else if (cols.length == 6)
            {
                // Last column might containing comments, etc.
                cols[5] = cleanup(cols[5]);

                RenameRequest rename = new RenameRequest(cols[0], cols[1],
                        cols[2], cols[3], cols[4], cols[5]);
                logger.info(rename.toString());

                // Check correct special symbol usage.
                for (int c = 0; c < 6; c++)
                {
                    // There should be no minus on the left side.
                    if (c < 3 && cols[c].compareTo("-") == 0)
                        throw new ReplicatorException(
                                "Minus used on the left side (minus means \"leave as original\"): "
                                        + cols[c]);

                    // There should be no asterisks on the right side.
                    if (c >= 3 && cols[c].compareTo("*") == 0)
                        throw new ReplicatorException(
                                "Asterisk used on the right side (asterisk means \"match all occurances\"): "
                                        + cols[c]);

                    // Regular expression matching not supported.
                    if (cols[c].indexOf('*') > 0)
                        throw new ReplicatorException(
                                "Part-string asterisk matching is not supported: "
                                        + cols[c]);
                }

                // Check correct format usage.
                validate(rename);

                // We have correct format.
                populateLookup(rename);
            }
            else
            {
                // Invalid format row.
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < cols.length; i++)
                {
                    sb.append(cols[i]);
                    if (i < (cols.length - 1))
                        sb.append(",");
                }
                throw new ReplicatorException(
                        "Incorrect row format in "
                                + definitionFile
                                + " (should be six columns, comment or an empty line): "
                                + sb.toString());
            }
        }
    }

    private void validate(RenameRequest rename) throws ReplicatorException
    {
        // Check for nonsense definition: *,*,*,any,any,any
        if (matchAll(rename.getOrigSchema()) && matchAll(rename.getOrigTable())
                && matchAll(rename.getOrigColumn()))
            throw new ReplicatorException(
                    "Invalid definition, can't rename all occurances of all columns: "
                            + rename.toString());

        // Check for nonsense definition: any,any,any,-,-,-
        if (leaveOriginal(rename.getNewSchema())
                && leaveOriginal(rename.getNewTable())
                && leaveOriginal(rename.getNewColumn()))
            throw new ReplicatorException(
                    "Invalid definition of leaving everything to original names: "
                            + rename.toString());

        // Column moving to a different table is unsupported.
        if (!matchAll(rename.getOrigTable())
                && !leaveOriginal(rename.getNewTable())
                && !leaveOriginal(rename.getNewColumn()))
            throw new ReplicatorException(
                    "Unsupported definition, can't move column to another table: "
                            + rename.toString());

        // NOPs not allowed.
        if (rename.getOrigSchema().compareTo(rename.getNewSchema()) == 0
                || rename.getOrigTable().compareTo(rename.getNewTable()) == 0
                || rename.getOrigColumn().compareTo(rename.getNewColumn()) == 0)
            throw new ReplicatorException(
                    "Invalid definition, use \"-\" on the right side to identify objects that are not changed: "
                            + rename.toString());
    }

    /**
     * Check whether specific schema and table has requests to rename columns.
     * Doesn't check asterisk matches.
     */
    private boolean shouldRenameSpecificColumn(String schema, String table)
    {
        Hashtable<String, Hashtable<String, RenameRequest>> lookupTableCol = lookupSchemaTableCol
                .get(schema);
        if (lookupTableCol != null)
        {
            Hashtable<String, RenameRequest> lookupCol = lookupTableCol
                    .get(table);
            if (lookupCol != null)
            {
                if (lookupCol.containsKey("*"))
                {
                    if (lookupCol.size() > 1)
                        return true;
                }
                else
                {
                    if (lookupCol.size() > 0)
                        return true;
                }
            }
        }
        return false;
    }

    /**
     * Check whether specific schema and table has requests to rename columns.
     * Checks for asterisk matches too:<br/>
     * schema.table<br/>
     * *.table<br/>
     * schema.*<br/>
     * *.*<br/>
     * 
     * @return true, if columns are in need for renaming.
     */
    public boolean shouldRenameColumn(String schema, String table)
    {
        if (shouldRenameSpecificColumn(schema, table))
            return true;
        else if (shouldRenameSpecificColumn("*", table))
            return true;
        else if (shouldRenameSpecificColumn(schema, "*"))
            return true;
        else if (shouldRenameSpecificColumn("*", "*"))
            return true;
        else
            return false;
    }

    /**
     * Gets new name for the column. Search for rename definition done in the
     * following order:<br/>
     * 1. schema.table<br/>
     * 2. schema.*<br/>
     * 3. *.table<br/>
     * 4. *.*<br/>
     * 
     * @return Renamed column name or null if no rename definition found.
     */
    public String getNewColumnName(String schema, String table, String column)
    {
        String[] schemas = {schema, "*"};
        for (String s : schemas)
        {
            Hashtable<String, Hashtable<String, RenameRequest>> lookupTableCol = lookupSchemaTableCol
                    .get(s);
            if (lookupTableCol == null)
                continue; // Try the * schema.

            String[] tables = {table, "*"};
            for (String t : tables)
            {
                Hashtable<String, RenameRequest> lookupCol = lookupTableCol
                        .get(t);
                if (lookupCol == null)
                    continue; // Try the * table or * schema.

                // Search for this column (don't match *).
                RenameRequest rename = lookupCol.get(column);
                if (rename == null)
                    continue; // Try the * table or * schema.
                else
                    return rename.getNewColumn();
            }
        }
        return null;
    }

    /**
     * Gets new name for the table. Search for rename definition done in the
     * following order of preference:<br/>
     * 1. schema.table<br/>
     * 2. *.table<br/>
     * 
     * @return Renamed table name or null if no rename definition found.
     */
    public String getNewTableName(String schema, String table)
    {
        String[] schemas = {schema, "*"};
        for (String s : schemas)
        {
            Hashtable<String, Hashtable<String, RenameRequest>> lookupTableCol = lookupSchemaTableCol
                    .get(s);
            if (lookupTableCol == null)
                continue;

            // Search for this table only (don't match *).
            Hashtable<String, RenameRequest> lookupCol = lookupTableCol
                    .get(table);
            if (lookupCol == null)
                continue; // Try the * schema.

            // Search for * in columns, which means rename table.
            RenameRequest rename = lookupCol.get("*");
            if (rename == null)
                continue;
            else
            {
                if (leaveOriginal(rename.getNewTable()))
                    return null;
                else
                    return rename.getNewTable();
            }
        }
        return null;
    }

    /**
     * Gets new name for the schema. Search for rename definition done in the
     * following order of preference:<br/>
     * 1. schema.table<br/>
     * 2. schema.*<br/>
     * 
     * @return Renamed table name or null if no rename definition found.
     */
    public String getNewSchemaName(String schema, String table)
    {
        // Search for this schema only (don't match *).
        Hashtable<String, Hashtable<String, RenameRequest>> lookupTableCol = lookupSchemaTableCol
                .get(schema);
        if (lookupTableCol == null)
            return null;

        String[] tables = {table, "*"};
        for (String t : tables)
        {
            Hashtable<String, RenameRequest> lookupCol = lookupTableCol.get(t);
            if (lookupCol == null)
                continue; // Try the * table.

            // Search for * in columns, which means rename schema and/or table.
            RenameRequest rename = lookupCol.get("*");
            if (rename == null)
                continue;
            else
            {
                if (leaveOriginal(rename.getNewSchema()))
                    return null;
                else
                    return rename.getNewSchema();
            }
        }
        return null;
    }

    /**
     * Puts given RenameRequest to search hash tables.
     * 
     * @throws ReplicatorException If matching part is duplicated.
     */
    private void populateLookup(RenameRequest rename)
            throws ReplicatorException
    {
        // Find schema.
        if (!lookupSchemaTableCol.containsKey(rename.getOrigSchema()))
        {
            Hashtable<String, Hashtable<String, RenameRequest>> lookupTableCol = new Hashtable<String, Hashtable<String, RenameRequest>>();
            lookupSchemaTableCol.put(rename.getOrigSchema(), lookupTableCol);
        }
        Hashtable<String, Hashtable<String, RenameRequest>> lookupTableCol = lookupSchemaTableCol
                .get(rename.getOrigSchema());

        // Find table.
        if (!lookupTableCol.containsKey(rename.getOrigTable()))
        {
            Hashtable<String, RenameRequest> lookupCol = new Hashtable<String, RenameRequest>();
            lookupTableCol.put(rename.getOrigTable(), lookupCol);
        }
        Hashtable<String, RenameRequest> lookupCol = lookupTableCol.get(rename
                .getOrigTable());

        // Find column.
        if (!lookupCol.containsKey(rename.getOrigColumn()))
        {
            lookupCol.put(rename.getOrigColumn(), rename);
        }
        else
            throw new ReplicatorException("Duplicate matching row: "
                    + rename.toString());
    }

    private boolean matchAll(String col)
    {
        if (col.compareTo("*") == 0)
            return true;
        else
            return false;
    }

    private boolean leaveOriginal(String col)
    {
        if (col.compareTo("-") == 0)
            return true;
        else
            return false;
    }
}
