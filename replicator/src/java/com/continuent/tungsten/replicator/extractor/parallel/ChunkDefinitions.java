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

package com.continuent.tungsten.replicator.extractor.parallel;

import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;

import org.apache.log4j.Logger;

import au.com.bytecode.opencsv.CSVReader;

import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * Reads and parses the chunk definitions file and prepares a list of
 * ChunkRequest objects.
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class ChunkDefinitions
{
    private static Logger            logger = Logger.getLogger(ChunkDefinitions.class);

    /**
     * Path to chunk definition file.
     */
    private String                   definitionFile;

    private LinkedList<ChunkRequest> chunksDefinitions;

    static class ChunkRequest
    {
        private final String   schema;
        private final String   table;
        private final String[] columns;
        private final long     chunkSize;

        public ChunkRequest(String schema, String table, long chunkSize,
                String[] columns)
        {
            this.schema = schema;
            this.table = table;
            this.columns = columns;
            this.chunkSize = chunkSize;
        }

        public ChunkRequest(String schema, String table, long chunkSize)
        {
            this(schema, table, chunkSize, null);
        }

        public ChunkRequest(String schema, String table)
        {
            this(schema, table, -1);
        }

        public ChunkRequest(String schema)
        {
            this(schema, null);
        }

        /**
         * Returns the schema value.
         * 
         * @return Returns the schema.
         */
        protected String getSchema()
        {
            return schema;
        }

        /**
         * Returns the table value.
         * 
         * @return Returns the table.
         */
        protected String getTable()
        {
            return table;
        }

        /**
         * Returns the columns value.
         * 
         * @return Returns the columns.
         */
        protected String[] getColumns()
        {
            return columns;
        }

        /**
         * Returns the chunkSize value.
         * 
         * @return Returns the chunkSize.
         */
        protected long getChunkSize()
        {
            return chunkSize;
        }
    }

    public ChunkDefinitions(String definitionFile)
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
     * Parses chunk definition file, validates format and populates lookup
     * structures.
     * 
     * @throws ReplicatorException On format issues.
     * @throws CsvException
     */
    public void parseFile() throws IOException, ReplicatorException
    {
        chunksDefinitions = new LinkedList<ChunkRequest>();

        logger.info("Parsing " + definitionFile);
        FileReader fileReader = new FileReader(definitionFile);
        CSVReader reader = new CSVReader(fileReader);
        String[] cols;
        while ((cols = reader.readNext()) != null)
        {
            int width = cols.length;

            if (width == 0)
                // this is an empty line
                continue;

            if (width >= 1 && cols[0].startsWith("#"))
                // This is a commented line
                continue;

            if (width == 1)
            {
                // Schema
                chunksDefinitions.add(new ChunkRequest(cols[0].trim()));
            }
            else if (width == 2)
            {
                // Schema Table
                chunksDefinitions.add(new ChunkRequest(cols[0].trim(), cols[1]
                        .trim()));
            }
            else if (width == 3)
            {
                // Schema Table ChunkSize
                // TODO : check 3rd column is a number
                chunksDefinitions.add(new ChunkRequest(cols[0].trim(), cols[1]
                        .trim(), Long.valueOf(cols[2].trim())));
            }
            else
            {
                // Schema Table ChunkSize [columns]+
                String[] columns = new String[width - 3];
                for (int i = 0, j = 3; i < columns.length; i++, j++)
                {
                    columns[i] = cols[j].trim();
                }
                chunksDefinitions.add(new ChunkRequest(cols[0].trim(), cols[1]
                        .trim(), Long.valueOf(cols[2].trim()), columns));

            }
        }
        reader.close();
        fileReader.close();
    }

    /**
     * Returns the chunksDefinitions value.
     * 
     * @return Returns the chunksDefinitions.
     */
    protected LinkedList<ChunkRequest> getChunksDefinitions()
    {
        return chunksDefinitions;
    }
}
