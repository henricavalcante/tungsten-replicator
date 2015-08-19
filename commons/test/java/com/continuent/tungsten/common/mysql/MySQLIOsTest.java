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
 * Initial developer(s): Gilles Rayrat
 * Contributor(s): 
 */

package com.continuent.tungsten.common.mysql;

import java.sql.Connection;
import java.sql.DriverManager;

import com.continuent.tungsten.common.mysql.MySQLIOs;

import junit.framework.TestCase;

/**
 * Tests MySQLIOs by creating a connection and .
 * 
 * @author <a href="gilles.rayrat@continuent.com">Gilles Rayrat</a>
 * @version 1.0
 */
public class MySQLIOsTest extends TestCase
{
    public void testDummy()
    {
        // I'm just here for junit to be happy and find a test.
        // My friends below require some work since they need a host to connect
        // to and should be extended to read and write data to the database
    }

    public void DISABLED_testGetIOsForDrizzle() throws Exception
    {
        Class.forName("org.drizzle.jdbc.DrizzleDriver").newInstance();

        Connection conn = DriverManager
                .getConnection("jdbc:mysql:thin://tungsten:secret@localhost/test");

        MySQLIOs.getMySQLIOs(conn);
        conn.close();
    }
    public void DISABLED_testGetIOsForMySQL() throws Exception
    {
        Class.forName("com.mysql.jdbc.Driver").newInstance();

        Connection conn = DriverManager
                .getConnection("jdbc:mysql://localhost/test", "tungsten", "secret");

        MySQLIOs.getMySQLIOs(conn);
        conn.close();
    }
}
