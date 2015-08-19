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
 * Initial developer(s): Robert Hodges and Scott Martin
 * Contributor(s): Linas Virbalas
 */

package com.continuent.tungsten.replicator.database;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;

/**
 * This class tests the Database interface and associated implementations.
 * Properties are specified using test.properties. If test.properties cannot be
 * found, the test automatically uses Derby database settings.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class TestDatabase
{
    private static Logger logger = Logger.getLogger(TestDatabase.class);

    private static String vendor;
    private static String driver;
    private static String url;
    private static String user;
    private static String password;
    private static String schema;

    /**
     * Make sure we have expected test properties.
     * 
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
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
            logger.warn("Using default values for test");

        // Set values used for test.
        vendor = tp.getString("database.vendor");
        driver = tp.getString("database.driver",
                "org.apache.derby.jdbc.EmbeddedDriver", true);
        url = tp.getString("database.url", "jdbc:derby:testdb;create=true",
                true);
        user = tp.getString("database.user");
        password = tp.getString("database.password");
        schema = tp.getString("database.schema", "testdb", true);

        // Load driver.
        Class.forName(driver);
    }

    /**
     * Ensure Database instance can be found and can connect.
     */
    @Test
    public void testDatabaseConnect() throws Exception
    {
        Database db = DatabaseFactory.createDatabase(url, user, password,
                false, vendor);
        Assert.assertNotNull(db);
        db.connect();
        db.close();
    }

    /**
     * Verify that connecting with privileged flag set to true results in a
     * privileged connection.
     */
    @Test
    public void testDatabaseConnectPrivileged() throws Exception
    {
        Database db = DatabaseFactory.createDatabase(url, user, password, true,
                vendor);
        Assert.assertNotNull(db);
        Assert.assertTrue(db.isPrivileged());
        db.connect();
        db.close();
    }

    /**
     * Verify that MySQL has expected capabities for privileged accounts. This
     * prevents us from skipping over tests and missing possible errors.
     */
    @Test
    public void testMySQLCapabilities() throws Exception
    {
        // Makes sure MySQL has expected capabilities.
        if (url.indexOf("mysql") < 0)
        {
            logger.info("Skipping MySQL-specific test as URL is non-MySQL: "
                    + url);
            return;
        }

        // Create a privileged connection and ensure expected capabilities are
        // set.
        Database db = DatabaseFactory.createDatabase(url, user, password, true,
                vendor);
        Assert.assertNotNull(db);
        Assert.assertTrue("Control section logging",
                db.supportsControlSessionLevelLogging());
        Assert.assertTrue("Control native slave sync",
                db.supportsNativeSlaveSync());
        Assert.assertTrue("Control sessions", db.supportsUserManagement());
        db.connect();
        db.close();
        db = null;

        // Create a non-privileged connection and ensure expected capabilities
        // are *not* set.
        Database db2 = DatabaseFactory.createDatabase(url, user, password,
                false, vendor);
        Assert.assertNotNull(db2);
        Assert.assertFalse("Do not control section logging",
                db2.supportsControlSessionLevelLogging());
        Assert.assertFalse("Do not control native slave sync",
                db2.supportsNativeSlaveSync());
        Assert.assertFalse("Do not control sessions",
                db2.supportsUserManagement());
        db2.connect();
        db2.close();
    }

    /**
     * Test calls to support session-level binlogging.
     */
    @Test
    public void testSessionLoggingSupport() throws Exception
    {
        Database db = DatabaseFactory.createDatabase(url, user, password, true,
                vendor);
        db.connect();
        if (db.supportsControlSessionLevelLogging())
        {
            db.controlSessionLevelLogging(true);
            db.controlSessionLevelLogging(false);
        }
        db.close();
    }

    /**
     * Test database schema management commands.
     */
    @Test
    public void testSchemaSupport() throws Exception
    {
        Database db = DatabaseFactory.createDatabase(url, user, password,
                false, vendor);
        db.connect();
        if (db.supportsCreateDropSchema())
        {
            db.createSchema("testSchemaSupport");
            if (db.supportsUseDefaultSchema())
            {
                // Let the database set it directly .
                db.useDefaultSchema("testSchemaSupport");

                // Get the use schema query and run it ourselves.
                String useQuery = db.getUseSchemaQuery(schema);
                db.execute(useQuery);
            }
            db.dropSchema("testSchemaSupport");
        }

        if (db.supportsUseDefaultSchema())
            db.useDefaultSchema(schema);

        db.close();
    }

    /**
     * Test timestamp management commands.
     */
    @Test
    public void testTimestampControl() throws Exception
    {
        Database db = DatabaseFactory.createDatabase(url, user, password,
                false, vendor);
        db.connect();
        if (db.supportsControlTimestamp())
        {
            String tsQuery = db.getControlTimestampQuery(System
                    .currentTimeMillis());
            db.execute(tsQuery);
        }
        db.close();
    }

    /**
     * Verify that we can set and get session variable values.
     */
    @Test
    public void testSessionVariables() throws Exception
    {
        Database db = DatabaseFactory.createDatabase(url, user, password,
                false, vendor);
        db.connect();
        if (db.supportsSessionVariables())
        {
            db.setSessionVariable("mytestvar", "testvalue!");
            String value = db.getSessionVariable("mytestvar");
            Assert.assertEquals("Check session variable value", "testvalue!",
                    value);
        }
        db.close();
    }

    /**
     * Ensure that we can create and delete a table containing all table types
     * and with a unique primary key.
     */
    @Test
    public void testColumnTypesWithKey() throws Exception
    {
        Database db = DatabaseFactory.createDatabase(url, user, password,
                false, vendor);

        Column myInt = new Column("my_int", Types.INTEGER);
        Column myBigInt = new Column("my_big_int", Types.BIGINT);
        Column myChar = new Column("my_char", Types.CHAR, 10);
        Column myVarChar = new Column("my_var_char", Types.VARCHAR, 10);
        Column myDate = new Column("my_date", Types.DATE);
        Column myTimestamp = new Column("my_timestamp", Types.TIMESTAMP);
        Column myClob = new Column("my_clob", Types.CLOB);
        Column myBlob = new Column("my_blob", Types.BLOB);

        Table testColumnTypes = new Table(schema, "test_column_types");
        testColumnTypes.AddColumn(myInt);
        testColumnTypes.AddColumn(myBigInt);
        testColumnTypes.AddColumn(myChar);
        testColumnTypes.AddColumn(myVarChar);
        testColumnTypes.AddColumn(myDate);
        testColumnTypes.AddColumn(myTimestamp);
        testColumnTypes.AddColumn(myClob);
        if (db.supportsBLOB())
            testColumnTypes.AddColumn(myBlob);

        Key primary = new Key(Key.Primary);
        primary.AddColumn(myInt);
        testColumnTypes.AddKey(primary);

        // Open database and connect.
        db.connect();
        if (db.supportsUseDefaultSchema())
            db.useDefaultSchema(schema);

        // Create table.
        db.createTable(testColumnTypes, true);

        // Add a row.
        myInt.setValue(23);
        myBigInt.setValue(25L);
        myChar.setValue("myChar");
        myVarChar.setValue("myVarChar");
        myDate.setValue(new Date(System.currentTimeMillis()));
        myTimestamp.setValue(new Date(System.currentTimeMillis()));
        myClob.setValue("myClob");
        if (db.supportsBLOB())
        {
            byte byteData[] = "blobs".getBytes("UTF-8");
            myBlob.setValue(new ByteArrayInputStream(byteData), byteData.length);
        }

        db.insert(testColumnTypes);

        // Update the row we just added.
        myChar.setValue("myChar2");
        ArrayList<Column> updateColumns = new ArrayList<Column>();
        updateColumns.add(myChar);
        db.update(testColumnTypes,
                testColumnTypes.getPrimaryKey().getColumns(), updateColumns);

        // Drop table.
        db.dropTable(testColumnTypes);
    }

    /**
     * Ensure we can connect and manipulate SQL. These calls are similar to
     * those used in the THL and appliers.
     */
    @Test
    public void testTableOperations() throws Exception
    {
        Database db = DatabaseFactory.createDatabase(url, user, password,
                false, vendor);

        /* History table */
        Column historySeqno = new Column("seqno", Types.BIGINT);
        Column historyTstamp = new Column("tstamp", Types.VARCHAR, 32);
        Column historyStatement = new Column("statement", Types.BLOB);

        Table history = new Table(schema, "history");
        history.AddColumn(historySeqno);
        history.AddColumn(historyTstamp);
        if (db.supportsBLOB())
            history.AddColumn(historyStatement);

        /* Seqno table */
        Column seqnoSeqno = new Column("seqno", Types.BIGINT);
        Column seqnoTrxid = new Column("trxid", Types.VARCHAR, 20);

        Key seqnoPrimary = new Key(Key.Primary);
        seqnoPrimary.AddColumn(seqnoSeqno);

        Key seqnoSecondary = new Key(Key.Unique);
        seqnoSecondary.AddColumn(seqnoTrxid);

        Table seqno = new Table(schema, "seqno");
        seqno.AddColumn(seqnoSeqno);
        seqno.AddColumn(seqnoTrxid);
        seqno.AddKey(seqnoPrimary);
        seqno.AddKey(seqnoSecondary);

        /* Create a fake SQLEvent to log */
        ArrayList<String> trx = new ArrayList<String>();
        trx.add("INSERT INTO EMP VALUE(1, 2)");
        /* Timestamp fakeTime = Timestamp.valueOf("2008-01-01 09:00:00"); */

        ArrayList<DBMSData> arr = new ArrayList<DBMSData>();
        DBMSEvent dbmsEvent = new DBMSEvent("7", arr, new Timestamp(
                System.currentTimeMillis()));
        ReplDBMSEvent fake_sqlEvent = new ReplDBMSEvent(7, dbmsEvent);
        ByteArrayOutputStream baob = new ByteArrayOutputStream();
        ObjectOutputStream oob = new ObjectOutputStream(baob);
        oob.writeObject(fake_sqlEvent);
        byte[] barr = baob.toByteArray();
        InputStream is = new ByteArrayInputStream(barr);
        int fake_SQL_length = barr.length;
        InputStream fake_SQL_is = is;

        // Open database and connect.
        db.connect();
        if (db.supportsUseDefaultSchema())
            db.useDefaultSchema(schema);

        // Create history table.
        db.createTable(history, true);

        // Create seqno table.
        db.createTable(seqno, true);

        // Insert a nice row.
        historySeqno.setValue(10L);
        historyTstamp.setValue("October 3");
        historyStatement.setValue(fake_SQL_is, fake_SQL_length);
        db.insert(history);

        // Update a row.
        seqnoSeqno.setValue(22L);
        seqnoTrxid.setValue("hello");
        db.update(seqno, seqno.getPrimaryKey().getColumns(),
                seqno.getNonKeyColumns());

        // Delete row from table seqno based on last value of PK.
        db.delete(seqno, false);

        // Replace row in seqno with last values of all columns.
        // In Oracle this should casue a DELETE, INSERT */
        // In MySQL this should casue a REPLACE INTO */
        db.replace(seqno);

        db.disconnect();
    }

    /**
     * Ensure we can connect and manipulate SQL. These calls are similar to
     * those used in the THL and appliers. This checks that we can set up tables
     * on data warehouses like Vertica which need to have projections defined
     * before tables can be used. Empty tables are a special case that causes
     * problems on Vertica 6.
     */
    @Test
    public void testEmptyTableOperations() throws Exception
    {
        // Define and create a simple table.
        Column id = new Column("id", Types.BIGINT);
        Column data = new Column("data", Types.VARCHAR, 32);

        Table empty = new Table(schema, "empty");
        empty.AddColumn(id);
        empty.AddColumn(data);

        Key emptyPrimary = new Key(Key.Primary);
        emptyPrimary.AddColumn(id);
        empty.AddKey(emptyPrimary);

        // Open database and connect.
        Database db = DatabaseFactory.createDatabase(url, user, password,
                false, vendor);
        db.connect();

        // Create empty table.
        db.createTable(empty, true);

        // Try to select from the table.
        db.execute("select * from " + empty.fullyQualifiedName());

        // Try to delete from the table, first a single row followed by all
        // rows.
        id.setValue(1);
        int rowsDeleted1 = db.delete(empty, false);
        Assert.assertEquals(0, rowsDeleted1);
        int rowsDeleted2 = db.delete(empty, true);
        Assert.assertEquals(0, rowsDeleted2);

        // Try to update the table.
        id.setValue(1);
        data.setValue("something");
        int rowsUpdated = db.update(empty, empty.getPrimaryKey().getColumns(),
                empty.getNonKeyColumns());
        Assert.assertEquals(0, rowsUpdated);

        // Replace row in seqno with last values of all columns.
        // In Oracle this should casue a DELETE, INSERT */
        // In MySQL this should casue a REPLACE INTO */
        db.replace(empty);

        db.disconnect();
    }

    /**
     * Ensure we can get a list of schemas.
     */
    @Test
    public void testGetSchemas() throws Exception
    {
        // Open database and connect.
        Database db = DatabaseFactory.createDatabase(url, user, password,
                false, vendor);
        if (db.getType() == DBMS.DERBY)
        {
            logger.info("Skipping testGetSchemas() on Derby...");
            return;
        }
        db.connect();

        logger.info("getSchemas() returned:");
        ArrayList<String> schemas = db.getSchemas();
        for (String schema : schemas)
        {
            logger.info(schema);
        }

        assertTrue("Zero schemas returned", schemas.size() > 0);

        db.disconnect();
    }

    /**
     * Does time difference function work?
     */
    @Test
    public void testGetTimeDiff() throws Exception
    {
        // Open database and connect.
        Database db = DatabaseFactory.createDatabase(url, user, password,
                false, vendor);
        if (db.getType() == DBMS.DERBY)
        {
            logger.info("Skipping testGetTimeDiff() on Derby...");
            return;
        }
        db.connect();

        Timestamp now = new Timestamp(System.currentTimeMillis());
        String sql = null;
        PreparedStatement prepareStatement = null;
        ResultSet rs = null;
        int diff = -1;

        // Case A: SQL function vs. SQL function
        sql = "SELECT "
                + db.getTimeDiff(db.getNowFunction(), db.getNowFunction());
        logger.info("getTimeDiff() prepared SQL: " + sql);
        prepareStatement = db.prepareStatement(sql);
        rs = prepareStatement.executeQuery();
        diff = -1;
        if (rs.next())
        {
            diff = rs.getInt(1);
            logger.info("Time difference: " + diff);
        }
        assertTrue("Timestamp difference should be zero", diff == 0);
        rs.close();

        // Case B: Java object vs. SQL function.
        sql = "SELECT " + db.getTimeDiff(null, db.getNowFunction());
        logger.info("getTimeDiff() prepared SQL: " + sql);
        prepareStatement = db.prepareStatement(sql);
        prepareStatement.setTimestamp(1, now);
        rs = prepareStatement.executeQuery();
        if (rs.next())
            logger.info("DB host and local host time difference: "
                    + rs.getInt(1));
        rs.close();

        // Case C: Java object vs. Java object.
        sql = "SELECT " + db.getTimeDiff(null, null);
        logger.info("getTimeDiff() prepared SQL: " + sql);
        prepareStatement = db.prepareStatement(sql);
        prepareStatement.setTimestamp(1, now);
        prepareStatement.setTimestamp(2, now);
        rs = prepareStatement.executeQuery();
        diff = -1;
        if (rs.next())
        {
            diff = rs.getInt(1);
            logger.info("Time difference: " + diff);
        }
        assertTrue("Timestamp difference should be zero", diff == 0);
        rs.close();

        db.disconnect();
    }

    /**
     * Verify that we can create and drop users.
     */
    @Test
    public void testUserManagement() throws Exception
    {
        // Open database and connect, but only if we support user management.
        Database db = DatabaseFactory.createDatabase(url, user, password, true,
                vendor);
        if (!db.supportsUserManagement())
        {
            logger.info("User management is not supported; skipping test...");
            return;
        }
        db.connect();

        // Create users.
        List<User> users = new LinkedList<User>();
        users.add(new User("test23unpriv", "testpassword", false));
        users.add(new User("test23priv", "testpassword", true));

        // Test both user types.
        for (User u : users)
        {
            // Confirm user does not exist, ignoring errors as we drop user.
            db.dropUser(u, true);
            validateConnection(url, u, false);

            // Create the aforesaid user and connect with same.
            db.createUser(u);
            validateConnection(url, u, true);

            // Drop the user and confirm user is gone.
            db.dropUser(u, false);
            validateConnection(url, u, false);
        }

        db.disconnect();
    }

    /**
     * Verify that we can list sessions and drop user sessions at will.
     */
    @Test
    public void testSessionManagement() throws Exception
    {
        // Open database and connect, but only if we support user management.
        Database db = DatabaseFactory.createDatabase(url, user, password, true,
                vendor);
        if (!db.supportsUserManagement())
        {
            logger.info("User management is not supported; skipping test...");
            return;
        }
        db.connect();

        // Create user for the test and ensure said user exists.
        User u = new User("test23unpriv", "testpassword", false);
        db.dropUser(u, true);
        db.createUser(u);
        validateConnection(url, u, true);

        // Form a new connection with our user.
        Connection conn = DriverManager.getConnection(url, u.getLogin(),
                u.getPassword());
        Assert.assertNotNull("Connection returned", conn);

        // List sessions and ensure the user is there once and only once. Kill
        // each user that is found.
        List<Session> sessions = db.listSessions();
        Assert.assertTrue("Must have at least two sessions",
                sessions.size() >= 2);
        int count = 0;
        for (Session session : sessions)
        {
            if (u.getLogin().equals(session.getLogin()))
            {
                count++;
                logger.info("Killing session: login=" + session.getLogin());
                db.kill(session);
            }
        }
        Assert.assertEquals("Expect only one session", 1, count);

        // Prove that the connection is dead.
        try
        {
            DatabaseMetaData meta = conn.getMetaData();
            meta.getCatalogs();
            throw new Exception("Connection is still alive after being killed!");
        }
        catch (SQLException e)
        {
            // Expected
        }

        // List sessions and ensure that user is gone.
        List<Session> sessions2 = db.listSessions();
        Assert.assertTrue("Must have at least one session",
                sessions2.size() >= 1);
        for (Session session : sessions2)
        {
            if (u.getLogin().equals(session.getLogin()))
            {
                throw new Exception("Found killed session: login="
                        + session.getLogin());
            }
        }

        // All done.
        db.disconnect();
    }

    /**
     * Verify that we can create an unprivileged user and then login
     * successfully using Database class. This shows we don't do anything
     * unprivileged on login.
     */
    @Test
    public void testUnprivilegedUser() throws Exception
    {
        // Open database and connect, but only if we support user management.
        Database db = DatabaseFactory.createDatabase(url, user, password, true,
                vendor);
        if (!db.supportsUserManagement())
        {
            logger.info("User management is not supported; skipping test...");
            return;
        }
        db.connect();

        // Create user for the test and ensure said user exists.
        User u = new User("test25unpriv", "testpassword", false);
        db.dropUser(u, true);
        db.createUser(u);

        // Login independently with our non-privileged user.
        Database db2 = DatabaseFactory.createDatabase(url, u.getLogin(),
                u.getPassword(), false, vendor);
        db2.connect();
        db2.disconnect();

        // Clean up the test user and disconnect.
        db.dropUser(u, false);
        db.disconnect();
    }

    // Check that connections to DBMS succeed (or not).
    private void validateConnection(String url, User user, boolean succeed)
            throws Exception
    {
        Connection conn = null;
        try
        {
            conn = DriverManager.getConnection(url, user.getLogin(),
                    user.getPassword());
            if (!succeed)
            {
                throw new Exception("Able to connect unexpectedly: login: "
                        + user.getLogin() + " pw: " + user.getPassword()
                        + " url: " + url);
            }
        }
        catch (SQLException e)
        {
            if (succeed)
            {
                throw new Exception("Unable to connect: login: "
                        + user.getLogin() + " pw: " + user.getPassword()
                        + " url: " + url, e);
            }
        }
        finally
        {
            if (conn != null)
                conn.close();
        }
    }
}
