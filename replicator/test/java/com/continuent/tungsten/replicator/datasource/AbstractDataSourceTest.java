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

package com.continuent.tungsten.replicator.datasource;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.junit.Test;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.csv.CsvWriter;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.csv.CsvDataFormat;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplDBMSHeaderData;

/**
 * Implements test cases that apply to any data source implementation.
 */
public class AbstractDataSourceTest
{
    private static Logger        logger            = Logger.getLogger(AbstractDataSourceTest.class);

    // Properties for data source test.
    protected TungstenProperties datasourceProps;
    protected String             datasourceClass;
    protected DataSourceManager  datasourceManager = new DataSourceManager();

    /**
     * Verify that after initialization the data source contents are available.
     */
    @Test
    public void testInitialization() throws Exception
    {
        if (!assertTestProperties())
            return;

        // Create a separate data source for this test.
        datasourceProps.setString("serviceName", "test_initialization");
        datasourceManager.addAndPrepare("testInitialization", datasourceClass,
                datasourceProps);

        // Get the data source and ensure tables are cleared.
        UniversalDataSource c = datasourceManager.find("testInitialization");
        c.clear();

        // Now initialize the tables.
        c.initialize();

        // Verify that we can find commit seqno data.
        CommitSeqno commitSeqno = c.getCommitSeqno();
        Assert.assertEquals("Looking for initialized commit seqno", -1,
                commitSeqno.minCommitSeqno().getSeqno());
    }

    /**
     * Verify that after a data source clear operation the catalog is gone.
     */
    @Test
    public void testAdminReset() throws Exception
    {
        if (!assertTestProperties())
            return;

        // Create a separate data source for this test.
        String dsName = "testAdminReset";
        datasourceProps.setString("serviceName", "test_admin_reset");
        datasourceManager.addAndPrepare(dsName, datasourceClass,
                datasourceProps);

        // Initialize data for the data source.
        UniversalDataSource c = datasourceManager.find(dsName);
        c.clear();
        c.initialize();

        // Verify that we can find commit seqno data.
        CommitSeqno commitSeqno = c.getCommitSeqno();
        Assert.assertEquals("Looking for initialized commit seqno", -1,
                commitSeqno.minCommitSeqno().getSeqno());

        // Put the said data source into a stubbed-out replicator properties
        // file.
        TungstenProperties replicatorProps = new TungstenProperties();
        replicatorProps.setString("replicator.datasources", dsName);
        replicatorProps.setString("replicator.datasource." + dsName, datasourceClass);
        replicatorProps.putAllWithPrefix(datasourceProps,
                "replicator.datasource." + dsName + ".");

        // Create an admin instance and clear the tables.
        DataSourceAdministrator admin = new DataSourceAdministrator(
                replicatorProps);
        admin.prepare();
        boolean cleared = admin.reset(dsName);
        Assert.assertEquals("Expected to clear catalog data", true, cleared);
        admin.release();
    }

    /**
     * Verify that if we initialize a data source we can update the commit seqno
     * position and read the updated value back.
     */
    @Test
    public void testSeqno() throws Exception
    {
        if (!assertTestProperties())
            return;

        UniversalDataSource c = prepareCatalog("testSeqno");

        // Retrieve the initial data.
        UniversalConnection conn = c.getConnection();
        CommitSeqnoAccessor accessor = c.getCommitSeqno().createAccessor(0,
                conn);
        ReplDBMSHeader initial = accessor.lastCommitSeqno();
        Assert.assertNotNull("Expect non-null initial header", initial);
        Assert.assertEquals("Expected initial seqno", -1, initial.getSeqno());

        // Change the seqno and update.
        ReplDBMSHeaderData newHeader = new ReplDBMSHeaderData(4, (short) 2,
                true, "foo", 1, "someEvent#", "someShard", new Timestamp(
                        10000000), 25);
        accessor.updateLastCommitSeqno(newHeader, 30);

        // Retrieve the header and ensure values match.
        ReplDBMSHeader retrieved = accessor.lastCommitSeqno();
        Assert.assertEquals("Checking seqno", 4, retrieved.getSeqno());
        Assert.assertEquals("Checking fragno", 2, retrieved.getFragno());
        Assert.assertEquals("Checking lastFrag", true, retrieved.getLastFrag());
        Assert.assertEquals("Checking sourceId", "foo", retrieved.getSourceId());
        Assert.assertEquals("Checking epochNumber", 1,
                retrieved.getEpochNumber());
        Assert.assertEquals("Checking event ID", "someEvent#",
                retrieved.getEventId());
        Assert.assertEquals("Checking shard ID", "someShard",
                retrieved.getShardId());
        Assert.assertEquals("Checking extractedTstamp",
                new Timestamp(10000000), retrieved.getExtractedTstamp());
        Assert.assertEquals("Checking appliedLatency", 30,
                retrieved.getAppliedLatency());

        // Release resources and exit.
        c.releaseConnection(conn);
    }

    /**
     * Verify that we can allocate many accessors in succession to read and
     * update the commit seqno position.
     */
    @Test
    public void testSeqnoManyAccessors() throws Exception
    {
        if (!assertTestProperties())
            return;

        UniversalDataSource c = prepareCatalog("testSeqnoManyAccessors");
        CommitSeqno commitSeqno = c.getCommitSeqno();

        // Loop through many times.
        for (int i = 0; i < 100; i++)
        {
            if (i > 0 && (i % 1000) == 0)
                logger.info("Iteration: " + i);
            UniversalConnection conn = c.getConnection();
            CommitSeqnoAccessor accessor = commitSeqno.createAccessor(0, conn);

            // Check the last position updated.
            ReplDBMSHeader lastHeader = accessor.lastCommitSeqno();
            Assert.assertEquals("Checking seqno", i - 1, lastHeader.getSeqno());

            // Update the header to the current position.
            ReplDBMSHeaderData newHeader = new ReplDBMSHeaderData(i, (short) 2,
                    true, "foo", 1, "someEvent#", "someShard", new Timestamp(
                            10000000), 25);
            accessor.updateLastCommitSeqno(newHeader, 25);

            // Discard the accessor and connection.
            accessor.close();
            c.releaseConnection(conn);
        }
    }

    /**
     * Verify that the seqno is correctly stored and returned for each allocated
     * channel.
     */
    @Test
    public void testSeqnoChannels() throws Exception
    {
        if (!assertTestProperties())
            return;

        UniversalDataSource c = prepareCatalog("testSeqnoChannels");
        int channels = c.getChannels();
        UniversalConnection conn = c.getConnection();
        CommitSeqno commitSeqno = c.getCommitSeqno();
        CommitSeqnoAccessor[] accessors = new CommitSeqnoAccessor[channels];

        // Allocate accessor and update for each channel.
        for (int i = 0; i < channels; i++)
        {
            accessors[i] = commitSeqno.createAccessor(i, conn);
            ReplDBMSHeaderData header = new ReplDBMSHeaderData(i * 2,
                    (short) 0, true, "foo", 1, "someEvent#", "someShard",
                    new Timestamp(10000000), 25);
            accessors[i].updateLastCommitSeqno(header, 25);
        }

        // Read back stored header and deallocate accessor for each channel.
        for (int i = 0; i < channels; i++)
        {
            ReplDBMSHeader retrieved = accessors[i].lastCommitSeqno();
            Assert.assertEquals("Checking seqno: channel=" + i, i * 2,
                    retrieved.getSeqno());
            accessors[i].close();
        }

        // Release resources and exit.
        c.releaseConnection(conn);
    }

    /**
     * Verify that we can change channel number when the catalog is released and
     * restart the catalog without error. This test ensures that users do not
     * change channels unexpectedly during operations, which can cause serious
     * configuration errors.
     */
    @Test
    public void testChangingChannels() throws Exception
    {
        if (!assertTestProperties())
            return;

        // Start with 10 channels.
        this.datasourceProps.setInt("channels", 10);
        UniversalDataSource c = prepareCatalog("testChangingChannels");

        int channels = c.getChannels();
        Assert.assertEquals("Expect initial number of channels", 10, channels);

        // Shut down.
        datasourceManager.removeAndRelease("testChangingChannels", true);

        // Start again with 20 channels.
        datasourceProps.setInt("channels", 20);
        UniversalDataSource c2 = prepareCatalog("testChangingChannels", false);

        int channels2 = c2.getChannels();
        Assert.assertEquals("Expect updated number of channels", 20, channels2);

        // Shut down.
        datasourceManager.removeAndRelease("testChangingChannels", true);
    }

    /**
     * Verify that seqno values are persistent even if we allocate the data
     * source a second time.
     */
    @Test
    public void testSeqnoPersistence() throws Exception
    {
        if (!assertTestProperties())
            return;

        UniversalDataSource c = prepareCatalog("testSeqnoPersistence");
        int channels = c.getChannels();

        // Allocate accessor and update for each channel.
        CommitSeqno commitSeqno = c.getCommitSeqno();
        UniversalConnection conn = c.getConnection();
        for (int i = 0; i < channels; i++)
        {
            CommitSeqnoAccessor accessor = commitSeqno.createAccessor(i, conn);
            ReplDBMSHeaderData header = new ReplDBMSHeaderData(i * 2,
                    (short) 0, true, "foo", 1, "someEvent#", "someShard",
                    new Timestamp(10000000), 25);
            accessor.updateLastCommitSeqno(header, 25);
            accessor.close();
        }
        commitSeqno.release();

        // Close the data source and add a new one.
        c.release();
        datasourceManager.remove("testSeqnoPersistence");
        datasourceManager.addAndPrepare("testSeqnoPersistence",
                datasourceClass, datasourceProps);
        UniversalDataSource c2 = datasourceManager.find("testSeqnoPersistence");

        // Read back stored header and deallocate accessor for each channel.
        UniversalConnection conn2 = c2.getConnection();
        CommitSeqno commitSeqno2 = c2.getCommitSeqno();
        for (int i = 0; i < channels; i++)
        {
            CommitSeqnoAccessor accessor = commitSeqno2
                    .createAccessor(i, conn2);
            ReplDBMSHeader retrieved = accessor.lastCommitSeqno();
            Assert.assertEquals("Checking seqno: channel=" + i, i * 2,
                    retrieved.getSeqno());
            accessor.close();
        }
        commitSeqno.release();

        // Release resources and exit.
        c2.releaseConnection(conn);
    }

    /**
     * Verify that we can obtain both a CSV writer as well as a string
     * formatter, then write successfully to a CSV file.
     */
    @Test
    public void testCsvFormattingAndWriting() throws Exception
    {
        if (!assertTestProperties())
            return;
        File testDir = prepareTestDir("testCsvFormattingAndWriting");

        // Allocate connection and CSV formatter from the data source.
        UniversalDataSource c = prepareCatalog("testCsvFormattingAndWriting");
        UniversalConnection conn = c.getConnection();
        CsvDataFormat formatter = c
                .getCsvStringFormatter(TimeZone.getDefault());
        Assert.assertNotNull("Expected to get a CSV formatter", formatter);

        // Now get a CSV file and write some random data to the same.
        File csvFile = new File(testDir, "test.csv");
        BufferedWriter writer = new BufferedWriter(new FileWriter(csvFile));
        CsvWriter csvWriter = conn.getCsvWriter(writer);

        csvWriter.addColumnName("string");
        csvWriter.addColumnName("date");
        csvWriter.addColumnName("float");
        csvWriter.addColumnName("double");

        String v1 = formatter.csvString("my string", Types.VARCHAR, false);
        String v2 = formatter.csvString(
                new Timestamp(System.currentTimeMillis()), Types.TIMESTAMP,
                false);
        String v3 = formatter.csvString(new Float(2.0), Types.FLOAT, false);
        String v4 = formatter.csvString(new Double(99.9), Types.DOUBLE, false);

        csvWriter.put("string", v1);
        csvWriter.put("date", v2);
        csvWriter.put("float", v3);
        csvWriter.put("double", v4);

        csvWriter.write();
        csvWriter.flush();
        writer.close();

        // Read back CSV file and confirm it has our data.
        String path = csvFile.getAbsolutePath();
        Assert.assertTrue("Ensuring CSV file exists: " + path,
                csvFile.canRead());
        List<String> contents = getFileContents(csvFile);
        Assert.assertEquals("Checking file length: " + path, 1, contents.size());
        String line1 = contents.get(0);
        Assert.assertTrue("Ensuring data are present: " + path,
                line1.contains("my string"));
    }

    /**
     * Prepares a data source and returns same to caller.
     */
    private UniversalDataSource prepareCatalog(String name, boolean clear)
            throws ReplicatorException, InterruptedException
    {
        datasourceProps.setString("serviceName", name);
        datasourceManager.addAndPrepare(name, datasourceClass, datasourceProps);

        // Get the data source and ensure tables are cleared.
        UniversalDataSource c = datasourceManager.find(name);
        if (clear)
        {
            c.clear();
        }
        c.initialize();

        return c;
    }

    /**
     * Convenience method to prepare catalog with automatic clearing of previous
     * data.
     */
    private UniversalDataSource prepareCatalog(String name)
            throws ReplicatorException, InterruptedException
    {
        return prepareCatalog(name, true);
    }

    // Returns false if the properties instance has not be set and test case
    // should return immediately.
    protected boolean assertTestProperties()
    {
        if (datasourceProps == null)
        {
            logger.warn("Data source properties are not defined; test case will not be run");
            return false;
        }
        else
            return true;
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

    // Read a file and return lines as a list of string values.
    private List<String> getFileContents(File f) throws Exception
    {
        LinkedList<String> lines = new LinkedList<String>();
        FileReader fr = new FileReader(f);
        BufferedReader br = new BufferedReader(fr);
        String nextLine;
        while ((nextLine = br.readLine()) != null)
        {
            lines.add(nextLine);
        }
        br.close();
        return lines;
    }
}