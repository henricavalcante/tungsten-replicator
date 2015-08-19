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

package com.continuent.tungsten.replicator.scripting;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mozilla.javascript.Undefined;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;

/**
 * Tests ability to handle parameterized SQL scripts using SqlScriptGenerator
 * class.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class JavaScriptExecutorTest
{
    private static Logger logger = Logger.getLogger(JavaScriptExecutorTest.class);

    private static String driver;
    private static String url;
    private static String user;
    private static String password;

    /**
     * Load test properties so we can construct DBMS connections.
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
        driver = tp.getString("database.driver",
                "org.apache.derby.jdbc.EmbeddedDriver", true);
        url = tp.getString("database.url", "jdbc:derby:testdb;create=true",
                true);
        user = tp.getString("database.user");
        password = tp.getString("database.password");

        // Load driver.
        Class.forName(driver);
    }

    /**
     * Test setting up and invoking a simple script.
     */
    @Test
    public void testSimpleScript() throws Exception
    {
        String script = "function echo(argument) { return argument}";
        Object value = execute("testSimpleScript", script, "echo", "hello");
        Assert.assertNotNull("Expect a return value", value);
        Assert.assertTrue("Return value should be string",
                (value instanceof String));
        Assert.assertEquals("Checking echoed value", "hello", (String) value);
    }

    /**
     * Test calling out to the OS from within a script.
     */
    @Test
    public void testExecOSCommand() throws Exception
    {
        String script = "function aCommand(arg) { runtime.exec('echo hello ' + arg); }";
        execute("testExecOSCommand", script, "aCommand", "bob");
    }

    /**
     * Verify that an exception occurs if the script has a syntax error.
     */
    @Test
    public void testSyntaxError() throws Exception
    {
        String script = "function apply(csvinfo) { this is not javascript!!! }";
        try
        {
            execute("testSyntaxError", script, "apply", new Object());
            throw new Exception("Able to execute script with syntax error: "
                    + script);
        }
        catch (ReplicatorException e)
        {
            logger.info("Caught expected exception");
        }
    }

    /**
     * Verify that we can find the default data source name if it is provided.
     */
    @Test
    public void testDsName() throws Exception
    {
        String script = "function doit(csvinfo) { mystring = runtime.getDefaultDataSourceName(); return mystring; }";
        Object val = execute("testDsName", script, "doit", "some input",
                "testDsName", null);
        Assert.assertEquals("Checking for default ds name", "testDsName",
                (String) val.toString());
    }

    /**
     * Verify that we can find a context object if it is inserted into the
     * executor using a context map.
     */
    @Test
    public void testContextObject() throws Exception
    {
        String script = "function context_object() { return myvalue; }";
        Map<String, Object> context = new HashMap<String, Object>();
        context.put("myvalue", new Integer(99));
        Object val = execute("testContextObject", script, "context_object",
                null, "testContextObject", context);
        int convertedValue = (Integer) val;
        Assert.assertEquals("Checking for context object", 99, convertedValue);
    }

    /**
     * Verify that the logger object is available in the context.  We also 
     * check null return values along the way. 
     */
    @Test
    public void testJavascriptLogging() throws Exception
    {
        String script = "function logging() {  }";
        Object val = execute("logging", script, "logging", null, "logging",
                null);
        Assert.assertTrue(val instanceof Undefined);
    }

    // Create a context and execute script using default data source name.
    private Object execute(String name, String script, String method,
            Object argument) throws Exception
    {
        return execute(name, script, method, argument, null, null);
    }

    // Create context and execute method on script.
    private Object execute(String name, String script, String method,
            Object argument, String dsName, Map<String, Object> contextMap)
            throws Exception
    {
        File scriptFile = writeScript(name, script);
        Database db = getDatabase();
        JavascriptExecutor exec = new JavascriptExecutor();
        exec.setScript(scriptFile.getAbsolutePath());
        exec.setDefaultDataSourceName(dsName);
        exec.setContextMap(contextMap);
        exec.prepare(null);
        Object value = exec.execute(method, argument);
        exec.release(null);
        db.close();
        return value;
    }

    // Write script.
    public File writeScript(String name, String script) throws IOException
    {
        File testDir = new File("testJavascriptBatch");
        testDir.mkdirs();
        File scriptFile = new File(testDir, name);
        FileWriter fw = new FileWriter(scriptFile);
        fw.write(script);
        fw.flush();
        fw.close();
        return scriptFile;
    }

    // Create database connection.
    private Database getDatabase() throws SQLException
    {
        Database db = DatabaseFactory.createDatabase(url, user, password);
        Assert.assertNotNull(db);
        db.connect();
        return db;
    }
}