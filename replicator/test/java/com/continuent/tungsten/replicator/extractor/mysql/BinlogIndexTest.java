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

package com.continuent.tungsten.replicator.extractor.mysql;

import java.io.File;

import junit.framework.TestCase;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;


/**
 * This class performs a unit test on the BinlogIndex class to ensure we can
 * read MySQL binlog index files accurately. It requires a MySQL binlog index
 * file sample located in test/data/mysql-bin.index.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class BinlogIndexTest extends TestCase
{
    static Logger logger = null;

    protected void setUp() throws Exception
    {
        super.setUp();
        if (logger == null)
        {
            BasicConfigurator.configure();
            logger = Logger.getLogger(BinlogIndexTest.class);
            logger.info("logger initialized");
        }
    }

    /**
     * Show that we correctly parse the binlog file and then return null when
     * the binlog does not exist or is the last name in the list. Otherwise, we
     * should return the next file.
     */
    public void testFiles() throws Exception
    {
        BinlogIndex bi = new BinlogIndex(".", "mysql-bin", true);
        assertEquals("Expect three entries", 3, bi.getBinlogFiles().size());

        File f0 = bi.nextBinlog("foo");
        assertNull("Non-existent file should return null", f0);

        File f1 = bi.nextBinlog("mysql-bin.000001");
        assertEquals("Should return 2nd file", "mysql-bin.000002", f1.getName());

        File f2 = bi.nextBinlog("mysql-bin.000002");
        assertEquals("Should return 3rd file", "mysql-bin.000003", f2.getName());

        File f3 = bi.nextBinlog("mysql-bin.000003");
        assertNull("Last file should return null", f3);
    }

    /**
     * Show that we correct throw exceptions for a non-existent binlog directory
     * or invalid binlog file pattern.
     */
    public void testInvalidIndexFiles() throws Exception
    {
        try
        {
            BinlogIndex bi = new BinlogIndex("/garbage/directory",
                    "mysql-binlog", false);
            throw new Exception(
                    "Able to create binlog index with bad directory: " + bi);
        }
        catch (MySQLExtractException e)
        {
        }

        try
        {
            BinlogIndex bi = new BinlogIndex(".", "mysql-binx", true);
            throw new Exception(
                    "Able to create binlog index with binlog name pattern: "
                            + bi);
        }
        catch (MySQLExtractException e)
        {
        }
    }
}
