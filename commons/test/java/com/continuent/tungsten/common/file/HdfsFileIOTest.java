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

package com.continuent.tungsten.common.file;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;

import com.continuent.tungsten.common.config.TungstenProperties;

/**
 * Implements unit test for HDFS implementation of FileIO. This requires data
 * from the test.properties file to locate the HDFS cluster used for testing.
 */
public class HdfsFileIOTest extends AbstractFileIOTest
{
    private static Logger logger = Logger.getLogger(Logger.class);

    // URI to connect to HDFS; if unavailable we cannot connect.
    private String        hdfsUri;

    /**
     * Find the HDFS URI and login from that location.
     * 
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        // Set test.properties file name.
        String testPropertiesName = System.getProperty("test.properties");
        if (testPropertiesName == null)
        {
            testPropertiesName = "test.properties";
            logger.info("Setting test.properties file name to default: test.properties");
        }

        // Load properties file.
        TungstenProperties tp = new TungstenProperties();
        File f = new File(testPropertiesName);
        if (f.canRead())
        {
            logger.info("Reading test.properties file: " + testPropertiesName);
            FileInputStream fis = new FileInputStream(f);
            tp.load(fis);
            fis.close();
        }
        else
            logger.warn("Could not find test.properties file!");

        // Find values used for test.
        hdfsUri = tp.getString("hdfs.uri");
        TungstenProperties hdfsProps = tp.subset("hdfs.config.", true);

        // Define our FileIO instance for Hadoop.
        if (hdfsUri == null)
            logger.info("HDFS URI required for this test is not set");
        else
        {
            URI uri = new URI(hdfsUri);
            this.fileIO = new HdfsFileIO(uri, hdfsProps);
        }
    }

    /**
     * Teardown.
     * 
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }
}