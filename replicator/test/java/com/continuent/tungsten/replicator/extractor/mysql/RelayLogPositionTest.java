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

import org.apache.log4j.Logger;

/**
 * Implements a simple unit test on the RelayLogPosition class.  
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class RelayLogPositionTest extends TestCase
{
    static Logger logger = Logger.getLogger(RelayLogPositionTest.class);

    /**
     * Verify that we correctly identify when a log position has reached
     * a particular file and offset. 
     */
    public void testPositionCheck() throws Exception
    {
        RelayLogPosition rlp = new RelayLogPosition();
        rlp.setPosition(new File("/var/lib/mysql/mysql-bin.000077"), 2333308);

        // Check various combinations of the file is too low. 
        assertFalse("File lower, offset lower", rlp.hasReached("mysql-bin.000078", 0));
        assertFalse("File lower, offset match", rlp.hasReached("mysql-bin.000078", 2333308));
        assertFalse("File lower, offset higher", rlp.hasReached("mysql-bin.000078", 2333309));
        
        // Check when the value matches. 
        assertTrue("File and offset match", rlp.hasReached("mysql-bin.000077", 2333308));
        
        // Check when the file and/or offset are higher. 
        assertTrue("File equal, offset higher", rlp.hasReached("mysql-bin.000077", 2333307));
        assertTrue("File higher, offset lower", rlp.hasReached("mysql-bin.000076", 0));
        assertTrue("File higher, offset higher", rlp.hasReached("mysql-bin.000076", 2333309));
    }
    
    /** Verify that we correctly clone values. */
    public void testClone() throws Exception
    {
        RelayLogPosition rlp = new RelayLogPosition();
        File f = new File("/var/lib/mysql/mysql-bin.000077");
        rlp.setPosition(f, 2333308);
        RelayLogPosition rlp2 = rlp.clone();

        // Check the values. 
        assertEquals("File matches", f, rlp2.getFile());
        assertEquals("offset matches", 2333308, rlp2.getOffset());
        
        // Check when the value matches. 
        assertTrue("File and offset match", rlp2.hasReached("mysql-bin.000077", 2333308));
    }
}