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

import junit.framework.Assert;

import org.junit.BeforeClass;
import org.junit.Test;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.csv.CsvSpecification;

/**
 * Runs tests on the data source manager to ensure we can add, find, and remove
 * data sources.
 */
public class TestDataSourceManager
{
    /**
     * Make sure we have expected test properties.
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
    }

    /**
     * Verify that if you add a data source it is possible to fetch the data
     * source back and get the same properties as originally added and also to
     * remove the data source.
     */
    @Test
    public void testAddRemoveDatasource() throws Exception
    {
        // Create the data source definition.
        TungstenProperties props = new TungstenProperties();
        props.setString("datasources.test", SampleDataSource.class.getName());
        props.setString("serviceName", "mytest");
        props.setLong("channels", 3);
        props.setString("myParameter", "some value");

        // Ensure that data source does not already exist.
        DataSourceManager cm = new DataSourceManager();
        cm.remove("test");
        Assert.assertNull("Ensuring data source does not exist prior to test",
                cm.find("test"));

        // Add new data source, then fetch it back and confirm field values.
        cm.add("test", SampleDataSource.class.getName(), props);
        SampleDataSource c = (SampleDataSource) cm.find("test");
        Assert.assertNotNull("Data source should be available", c);
        Assert.assertEquals("Comparing channels", 3, c.getChannels());
        Assert.assertEquals("Comparing service name", "mytest",
                c.getServiceName());

        // Remove the data source and confirm that it succeeds.
        Assert.assertEquals("Testing data source removal", c, cm.remove("test"));

        // Confirm that attempts to remove or get the data source now fail.
        Assert.assertNull("Ensuring data source does not exist after removal",
                cm.find("test"));
        Assert.assertNull("Ensuring data source cannot be removed twice",
                cm.remove("test"));
    }

    /**
     * Verify that we can add two data sources without errors and then remove
     * them one by one.
     */
    @Test
    public void testAddTwoDataSources() throws Exception
    {
        // Create the data source definitions using different prop files.
        TungstenProperties props1 = new TungstenProperties();
        props1.setString("serviceName", "mytest1");
        TungstenProperties props2 = new TungstenProperties();
        props2.setString("serviceName", "mytest2");

        // Ensure that data sources do not already exist.
        DataSourceManager cm = new DataSourceManager();
        Assert.assertNull("Ensuring data source does not exist prior to test",
                cm.find("test1"));
        Assert.assertNull("Ensuring data source does not exist prior to test",
                cm.find("test2"));

        // Add data sources and confirm that both names are present and that the
        // count of names is 2.
        cm.addAndPrepare("test1", SampleDataSource.class.getName(), props1);
        cm.addAndPrepare("test2", SampleDataSource.class.getName(), props2);
        Assert.assertEquals("Checking number of names", 2, cm.names().size());

        SampleDataSource c1 = (SampleDataSource) cm.find("test1");
        Assert.assertNotNull("Data source should be available", c1);
        Assert.assertEquals("Data source name set", "test1", c1.getName());
        Assert.assertEquals("Comparing service name", "mytest1",
                c1.getServiceName());

        SampleDataSource c2 = (SampleDataSource) cm.find("test2");
        Assert.assertNotNull("Data source should be available", c2);
        Assert.assertEquals("Comparing service name", "mytest2",
                c2.getServiceName());

        // Remove one data source and confirm that it succeeds.
        Assert.assertEquals("Testing data source removal", c1,
                cm.remove("test1"));
        Assert.assertEquals("Checking number of names", 1, cm.names().size());
        Assert.assertNull("Data source not should be available",
                cm.find("test1"));
        Assert.assertNotNull("Data source should be available",
                cm.find("test2"));

        // Confirm that removeAll removes the remaining data source.
        cm.removeAndReleaseAll(true);
        Assert.assertEquals("Checking number of names", 0, cm.names().size());
        Assert.assertNull("Data source should not be available",
                cm.find("test2"));
    }

    /**
     * Verify that if you add a data source with a CsvSpecification you can then
     * get back that specification and generate properly configured CsvWriter
     * and CsvReader instances.
     */
    @Test
    public void testDataSourceWithCsvSpec() throws Exception
    {
        // Create the data source definition.
        TungstenProperties props = new TungstenProperties();
        props.setString("serviceName", "mytest");
        props.setString("csv", CsvSpecification.class.getName());
        props.setString("csv.fieldSeparator", ":");
        props.setString("csv.recordSeparator", "\u0002");
        props.setBoolean("csv.useQuotes", true);
        props.setBeanSupportEnabled(true);

        // Ensure that data source does not already exist.
        DataSourceManager cm = new DataSourceManager();
        cm.remove("test");
        Assert.assertNull("Ensuring data source does not exist prior to test",
                cm.find("test"));

        // Add new data source, then fetch it back.
        cm.addAndPrepare("test", SampleDataSource.class.getName(), props);
        SampleDataSource c = (SampleDataSource) cm.find("test");
        Assert.assertNotNull("Data source should be available", c);

        // Confirm existence of CsvSpecification and validate properties.
        CsvSpecification csv = c.getCsv();
        Assert.assertNotNull("CsvSpecification should be available", csv);
        Assert.assertEquals("Checking field separator", ":",
                csv.getFieldSeparator());
        Assert.assertEquals("Checking record separator", "\u0002",
                csv.getRecordSeparator());
        Assert.assertEquals("Checking use quotes", true, csv.isUseQuotes());

        // Clean up data source.
        cm.removeAndRelease("test", true);
    }

    /**
     * Verify that we can add an alias data source and look up through it to the
     * source.
     */
    @Test
    public void testAddRemoveAliasDataSource() throws Exception
    {
        // Create a data source.
        DataSourceManager cm = new DataSourceManager();

        // Add the alias but without the source.
        TungstenProperties alias = new TungstenProperties();
        alias.setString("serviceName", "test");
        alias.setString("dataSource", "source");
        cm.addAndPrepare("alias", AliasDataSource.class.getName(), alias);

        // Look up alias and expect to get the alias but no source.
        AliasDataSource a1 = (AliasDataSource) cm.find("alias");
        Assert.assertNotNull("Expect to find an alias", a1);
        UniversalDataSource u1 = cm.find(a1.getDataSource());
        Assert.assertNull("Don't expect to find a source", u1);

        // Add the source data set.
        TungstenProperties source = new TungstenProperties();
        source.setString("serviceName", "test");
        cm.addAndPrepare("source", SampleDataSource.class.getName(), source);

        // Now look up alias and expect to get the source.
        AliasDataSource a2 = (AliasDataSource) cm.find("alias");
        Assert.assertNotNull("Expect to find an alias", a2);
        UniversalDataSource u2 = cm.find(a2.getDataSource());
        Assert.assertNotNull("We do expect to find a source", u2);
    }
}