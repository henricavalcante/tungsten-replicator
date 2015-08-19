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

package com.continuent.tungsten.replicator.csv;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.continuent.tungsten.common.csv.CsvException;
import com.continuent.tungsten.common.csv.CsvWriter;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Column;
import com.continuent.tungsten.replicator.database.Table;
import com.continuent.tungsten.replicator.datasource.UniversalConnection;

/**
 * Holds the data for a set of CSV files associated with a single DBMS table. We
 * call this structure a set because one table may result in multiple CSV files
 * that are partitioned based on file values such as the transaction commit
 * time.
 */
public class CsvFileSet
{
    // Properties that relate to writing csv files.
    UniversalConnection          connection;
    Charset                      outputCharset;
    File                         stageDir;
    String                       rowIdColumn;

    // Header fields common to entire CSV file set.
    private final Table          baseTableMetadata;
    private final Table          stageTableMetadata;
    private long                 startSeqno = -1;
    private String               baseFileName;
    private long                 endSeqno   = -1;

    // Cache of current CSV files.
    private Map<CsvKey, CsvFile> csvFiles   = new TreeMap<CsvKey, CsvFile>();

    /**
     * Creates a new instance.
     * 
     * @param stageTableMetadata Staging table metadata.
     * @param baseTableMetadata Base table metadata.
     */
    public CsvFileSet(Table stageTableMetadata, Table baseTableMetadata,
            long startSeqno)
    {
        this.baseTableMetadata = baseTableMetadata;
        this.stageTableMetadata = stageTableMetadata;
        this.startSeqno = startSeqno;
        baseFileName = baseTableMetadata.getSchema() + "-"
                + baseTableMetadata.getName();
    }

    public void setConnection(UniversalConnection connection)
    {
        this.connection = connection;
    }

    public void setOutputCharset(Charset outputCharset)
    {
        this.outputCharset = outputCharset;
    }

    public void setStageDir(File stageDir)
    {
        this.stageDir = stageDir;
    }

    public void setRowIdColumn(String rowIdColumn)
    {
        this.rowIdColumn = rowIdColumn;
    }

    public void setStartSeqno(long startSeqno)
    {
        this.startSeqno = startSeqno;
    }

    public void setEndSeqno(long endSeqno)
    {
        this.endSeqno = endSeqno;
    }

    /** Returns the count of CSV files in the set. */
    public int size()
    {
        return csvFiles.size();
    }

    /**
     * Returns an open CSV file for the indicated key.
     * 
     * @throws ReplicatorException Thrown if there is an error
     */
    public CsvFile getCsvFile(CsvKey key) throws ReplicatorException
    {
        CsvFile csvFile = this.csvFiles.get(key);
        if (csvFile == null)
        {
            // Generate file name. If the key is non-empty, add it to the file
            // name.
            String fileName;
            if (key.isEmptyKey())
                fileName = this.baseFileName + "-" + startSeqno + ".csv";
            else
            {
                try
                {
                    // Encode the key so that it can contain slashes or other
                    // non-supported characters for file names.
                    String encodedKey = URLEncoder.encode(key.toString(),
                            "UTF8");
                    fileName = this.baseFileName + "-" + encodedKey + "-"
                            + startSeqno + ".csv";
                }
                catch (UnsupportedEncodingException e)
                {
                    throw new ReplicatorException(
                            "Unable to encode key value: key=" + key.toString()
                                    + " message=" + e.getMessage(), e);
                }
            }

            File file = new File(this.stageDir, fileName);

            // Pick the right table to use. For staging tables, we
            // need to use the stage metadata instead of going direct.
            Table csvMetadata = this.stageTableMetadata;

            // Now generate the CSV writer.
            try
            {
                // Ensure the file does not exist. This cleans up from
                // previous transactions.
                if (file.exists())
                {
                    file.delete();
                }
                if (file.exists())
                {
                    throw new ReplicatorException(
                            "Unable to delete CSV file prior to loading new data: "
                                    + file.getAbsolutePath());
                }

                // Generate a CSV writer on the file.
                FileOutputStream outputStream = new FileOutputStream(file);
                OutputStreamWriter streamWriter = new OutputStreamWriter(
                        outputStream, outputCharset);
                BufferedWriter output = new BufferedWriter(streamWriter);
                CsvWriter writer = connection.getCsvWriter(output);
                writer.setNullAutofill(true);

                // Populate columns. The last column is the row ID, which is
                // automatically populated by the CSV writer.
                List<Column> columns = csvMetadata.getAllColumns();
                for (int i = 0; i < columns.size(); i++)
                {
                    Column col = columns.get(i);
                    String name = col.getName();
                    if (rowIdColumn.equals(name))
                        writer.addRowIdName(name);
                    else
                        writer.addColumnName(name);
                }

                // Create a new CsvFile and store it.
                csvFile = new CsvFile(key, file, writer);
                this.csvFiles.put(key, csvFile);
            }
            catch (CsvException e)
            {
                throw new ReplicatorException("Unable to intialize CSV file: "
                        + e.getMessage(), e);
            }
            catch (IOException e)
            {
                throw new ReplicatorException("Unable to intialize CSV file: "
                        + file.getAbsolutePath());
            }
        }
        return csvFile;
    }

    /**
     * Flush and close all CSV writers.
     * 
     * @throws ReplicatorException Thrown if a CSV file cannot be closed
     */
    public void flushAndCloseCsvFiles() throws ReplicatorException
    {
        for (CsvKey key : csvFiles.keySet())
        {
            CsvFile csvFile = csvFiles.get(key);

            // Flush and close the file.
            try
            {
                CsvWriter writer = csvFile.getWriter();
                writer.flush();
                writer.getWriter().close();
            }
            catch (CsvException e)
            {
                throw new ReplicatorException("Unable to close CSV file: "
                        + csvFile.getFile().getAbsolutePath(), e);
            }
            catch (IOException e)
            {
                throw new ReplicatorException("Unable to close CSV file: "
                        + csvFile.getFile().getAbsolutePath());
            }
        }
    }

    /**
     * Return a CsvInfo instance for each CSV file.
     */
    public List<CsvInfo> getCsvInfoList()
    {
        List<CsvInfo> csvList = new ArrayList<CsvInfo>(csvFiles.size());
        for (CsvKey key : csvFiles.keySet())
        {
            // Add header information.
            CsvInfo info = new CsvInfo();
            info.schema = baseTableMetadata.getSchema();
            info.table = baseTableMetadata.getName();
            info.key = key.toString();
            info.baseTableMetadata = baseTableMetadata;
            info.stageTableMetadata = stageTableMetadata;
            info.startSeqno = startSeqno;
            info.endSeqno = endSeqno;

            // Add per-CSV information.
            CsvFile fileInfo = csvFiles.get(key);
            info.file = fileInfo.getFile();

            csvList.add(info);
        }
        return csvList;
    }
}
