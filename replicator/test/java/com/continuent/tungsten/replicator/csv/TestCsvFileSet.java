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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.csv.CsvWriter;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.csv.CsvFile;
import com.continuent.tungsten.replicator.csv.CsvFileSet;
import com.continuent.tungsten.replicator.csv.CsvInfo;
import com.continuent.tungsten.replicator.csv.CsvKey;
import com.continuent.tungsten.replicator.database.Column;
import com.continuent.tungsten.replicator.database.Table;
import com.continuent.tungsten.replicator.datasource.DataSourceManager;
import com.continuent.tungsten.replicator.datasource.FileDataSource;
import com.continuent.tungsten.replicator.datasource.UniversalConnection;
import com.continuent.tungsten.replicator.datasource.UniversalDataSource;

/**
 * Tests operations on CSV file set instances. This checks end-to-end loading
 * and writing of data.
 */
public class TestCsvFileSet
{
    // Automatically managed data source manager.
    private DataSourceManager mgr;

    /**
     * Set up data source manager.
     */
    @Before
    public void setUp() throws Exception
    {
        mgr = new DataSourceManager();
    }

    /** Clear any data sources. */
    @After
    public void teardown() throws Exception
    {
        if (mgr != null)
            mgr.removeAndReleaseAll(true);
    }

    /**
     * Verify that we can open up a CSV set for a single file using an empty key
     * to find the CSV data.
     */
    @Test
    public void testSingleCsv() throws Exception
    {
        // Set up the CsvFileSet.
        CsvFileSet fileSet = createTestFileSet("testSingleCsv");

        // Get a csv writer and write a few lines to it.
        writeDataToCsv(fileSet, CsvKey.emptyKey(), "mydata-", 3);

        // Close everything.
        fileSet.flushAndCloseCsvFiles();

        // Validate the CSV data so written.
        this.readDataFromCsv(fileSet, CsvKey.emptyKey(), "mydata-", 3);

        // Fetch and validate CsvInfo for the single file.
        List<CsvInfo> infos = fileSet.getCsvInfoList();
        Assert.assertEquals("Checking info list size", 1, infos.size());
        CsvInfo info = infos.get(0);
        Assert.assertEquals("Checking key", CsvKey.emptyKey().toString(),
                info.key);
        Assert.assertTrue(
                "CSV info file must exist: " + info.file.getAbsolutePath(),
                info.file.canRead());
    }

    /**
     * Verify that we can operate on multiple CSV files using keys and get an
     * accurate list of CsvInfo instances describing the files.
     */
    @Test
    public void testMultipleCsvByKey() throws Exception
    {
        // Define our keys.
        String k1 = "k1";
        String k2 = "k2";

        // Set up the CsvFileSet.
        CsvFileSet fileSet = createTestFileSet("testMultipleCsvByKey");

        // Write differing rows and values to each key.
        writeDataToCsv(fileSet, new CsvKey(k1), k1 + "-mydata-", 3);
        writeDataToCsv(fileSet, new CsvKey(k2), k2 + "-mydata-", 7);

        // Close everything.
        fileSet.flushAndCloseCsvFiles();

        // Validate the CSV data so written.
        readDataFromCsv(fileSet, new CsvKey(k1), k1 + "-mydata-", 3);
        readDataFromCsv(fileSet, new CsvKey(k2), k2 + "-mydata-", 7);

        // Fetch the CSV info values and confirm we get all of them.
        List<CsvInfo> infos = fileSet.getCsvInfoList();
        Assert.assertEquals("Checking info list size", 2, infos.size());
        Map<String, String> keys = new HashMap<String, String>();
        for (CsvInfo info : infos)
        {
            keys.put(info.key, "OK");
        }
        Assert.assertNotNull("Found k1", keys.get(k1));
        Assert.assertNotNull("Found k2", keys.get(k2));
    }

    /**
     * Creates a new CSV file set for a test.
     */
    private CsvFileSet createTestFileSet(String testName) throws Exception
    {
        // Clear the test data directory.
        File testDir = prepareTestDir(testName);

        // Create a data source.
        UniversalDataSource ds = createDataSource(testName);
        UniversalConnection conn = ds.getConnection();

        // Create some dummy metadata.
        Table staging = createTableMetadata(testName, "staging1", true);
        Table base = createTableMetadata(testName, "base1", false);

        // Set up the CsvFileSet.
        CsvFileSet fileSet;
        fileSet = new CsvFileSet(staging, base, 0);
        fileSet.setConnection(conn);
        fileSet.setRowIdColumn("tungsten_row_id");
        fileSet.setStageDir(testDir);
        fileSet.setOutputCharset(Charset.defaultCharset());

        return fileSet;
    }

    // Create an empty test directory or if the directory exists remove
    // any files within it.
    private File prepareTestDir(String dirName) throws Exception
    {
        File dir = new File(dirName);
        // Delete old log directory.
        if (dir.exists())
        {
            for (File f : dir.listFiles())
            {
                f.delete();
            }
            dir.delete();
        }

        // Create a new directory.
        if (!dir.mkdirs())
        {
            throw new Exception("Unable to create test directory: "
                    + dir.getAbsolutePath());
        }
        return dir;
    }

    /**
     * Creates test table metadata.
     */
    private Table createTableMetadata(String schema, String tableName,
            boolean header)
    {
        // Create table definition.
        Table metadata = new Table(schema, tableName);
        if (header)
        {
            // Add sample header columns.
            metadata.AddColumn(new Column("tungsten_opcode", Types.CHAR, 1));
            metadata.AddColumn(new Column("tungsten_seqno", Types.INTEGER));
            metadata.AddColumn(new Column("tungsten_row_id", Types.INTEGER));
            metadata.AddColumn(new Column("tungsten_commit_timestamp",
                    Types.TIMESTAMP));
        }
        // Add normal data.
        metadata.AddColumn(new Column("id", Types.INTEGER));
        metadata.AddColumn(new Column("mydata", Types.VARCHAR));

        return metadata;
    }

    /**
     * Creates a data source, stores it in the manager, and returns to caller.
     */
    private UniversalDataSource createDataSource(String name)
            throws ReplicatorException, InterruptedException
    {
        // Create the data source definition.
        TungstenProperties datasourceProps = new TungstenProperties();
        datasourceProps.setString("serviceName", name);
        datasourceProps.setLong("channels", 10);
        datasourceProps.setString("directory", name);
        datasourceProps.setString("csvType", "default");

        // Create a separate data source for this test.
        mgr = new DataSourceManager();
        mgr.addAndPrepare(name, FileDataSource.class.getName(), datasourceProps);

        // Get the data source and ensure tables are cleared.
        UniversalDataSource ds = mgr.find(name);
        Assert.assertNotNull("Data source must exist in manager", ds);
        return ds;
    }

    /**
     * Writes a specified number of lines to a CSV file identified by a
     * particular key.
     */
    private void writeDataToCsv(CsvFileSet fileSet, CsvKey key,
            String dataPrefix, int lines) throws Exception
    {
        // Get a csv writer and write a few lines to it.
        for (int i = 0; i < lines; i++)
        {
            CsvFile csvFile = fileSet.getCsvFile(key);
            CsvWriter writer = csvFile.getWriter();

            writer.put("tungsten_opcode", "I");
            writer.put("tungsten_seqno", new Integer(i).toString());
            writer.put("tungsten_commit_timestamp",
                    new Timestamp(System.currentTimeMillis()).toString());
            writer.put("id", "88");
            writer.put("mydata", dataPrefix + i);
            writer.flush();
        }
    }

    /**
     * Verifies that when we read CSV output the expected number of lines are
     * present with expected data.
     */
    private void readDataFromCsv(CsvFileSet fileSet, CsvKey key,
            String dataPrefix, int lines) throws Exception
    {
        // Confim we can find the file and that it exists.
        CsvFile csvFile = fileSet.getCsvFile(key);
        File file = csvFile.getFile();
        Assert.assertTrue("CSV file must exist: " + file.getAbsolutePath(),
                file.canRead());

        // Read file. Confirm that all three lines made it.
        FileReader fr = new FileReader(file);
        BufferedReader br = new BufferedReader(fr);
        for (int i = 0; i < lines; i++)
        {
            String line = br.readLine();
            String data = dataPrefix + i;
            boolean containsData = line.contains(data);
            Assert.assertTrue(
                    "Must find data in output file: name="
                            + file.getAbsolutePath() + " data=" + data,
                    containsData);
        }
        br.close();
    }
}