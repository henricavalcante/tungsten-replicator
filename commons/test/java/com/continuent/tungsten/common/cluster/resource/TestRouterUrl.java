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

package com.continuent.tungsten.common.cluster.resource;

import java.sql.SQLException;
import java.util.Properties;

import junit.framework.TestCase;

/**
 * Implements a simple test for t-router URL parsing.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class TestRouterUrl extends TestCase
{
    /**
     * Test a few obvious bad URLs.
     */
    public void testBadUrls() throws Exception
    {
        String[] badUrls = {"", "jdbc:", "jdbc:whatever//", "jdbc:t-router:",
                "jdbc:t-router://", "jdbc:t-router://service/db?qos",
                "jdbc:t-router://service/db?qos=RW_STRICT&missingvalue"};

        for (String badUrl : badUrls)
        {
            try
            {
                new RouterURL(badUrl, null);
                fail("Bad URL was parsed: url=[" + badUrl + "]");

            }
            catch (SQLException e)
            {
                // this is expected
            }
        }
    }

    /**
     * Test URLs with and without properties.
     */
    public void testStandardUrls() throws Exception
    {
        // Test a URL with no arguments and no properties.
        String url = "jdbc:t-router://service/db";
        RouterURL ru = new RouterURL(url, new Properties());
        assertEquals("service", ru.getService());
        assertEquals("db", ru.getDbname());
        assertEquals(QualityOfService.RW_STRICT, ru.getQos());
        assertEquals(0, ru.getProps().size());

        // Test a URL with arguments but no properties.
        url = "jdbc:t-router://service/db?user=auser&password=pw&qos=RW_STRICT";
        ru = new RouterURL(url, new Properties());
        assertEquals("service", ru.getService());
        assertEquals("db", ru.getDbname());
        assertEquals(QualityOfService.RW_STRICT, ru.getQos());
        Properties ruProps = ru.getProps();
        assertEquals(2, ruProps.size());
        assertEquals("auser", ruProps.getProperty("user"));
        assertEquals("pw", ruProps.getProperty("password"));

        // Test a URL with arguments and properties.
        url = "jdbc:t-router://service/db?user=auser&qos=RO_RELAXED";
        Properties props = new Properties();
        props.setProperty("password", "pw");
        ru = new RouterURL(url, props);
        assertEquals("service", ru.getService());
        assertEquals("db", ru.getDbname());
        assertEquals(QualityOfService.RO_RELAXED, ru.getQos());
        ruProps = ru.getProps();
        assertEquals(2, ruProps.size());
        assertEquals("auser", ruProps.getProperty("user"));
        assertEquals("pw", ruProps.getProperty("password"));

        // Test a URL with problematic characters in the password
        tryPassword("semi:colon");
        tryPassword("question!mark^n");
        tryPassword("arobase@and1one");
        tryPassword("var1ou$`1^*l)");

        // check that the parser is permissive enough to allow empty props
        RouterURL perm1 = new RouterURL("jdbc:t-router://service/db?", null);
        assertEquals("Data service name should have been set correctly",
                perm1.getDataServiceName(), "service");
        assertEquals("Database name should have been set correctly",
                perm1.getDbname(), "db");
        assertTrue("No properties should have been set when only giving a '?' "
                + "after the database name", perm1.getProps().isEmpty());
        RouterURL perm2 = new RouterURL("jdbc:t-router://service/db?user=me&",
                null);
        assertEquals("Data service name should have been set correctly",
                perm2.getDataServiceName(), "service");
        assertEquals("Database name should have been set correctly",
                perm2.getDbname(), "db");
        assertEquals("User name should have been set to 'me'", perm2.getProps()
                .get("user"), "me");
        assertEquals(
                "Only the user should have been set when only giving 'user=me&' "
                        + "after the database name", perm2.getProps().size(), 1);
        RouterURL perm3 = new RouterURL(
                "jdbc:t-router://service/db?user=me&qos=RO_RELAXED?qos=RW_STRICT",
                null);
        assertEquals("qos should have been set to the latest value declared",
                QualityOfService.RW_STRICT, perm3.getQos());

    }

    private void tryPassword(String pass) throws SQLException
    {
        String url;
        RouterURL ru;
        Properties ruProps;
        url = "jdbc:t-router://service/db?user=auser&password=" + pass;
        ru = new RouterURL(url, null);
        assertEquals("service", ru.getService());
        assertEquals("db", ru.getDbname());
        ruProps = ru.getProps();
        assertEquals(2, ruProps.size());
        assertEquals("auser", ruProps.getProperty("user"));
        assertEquals(pass, ruProps.getProperty("password"));
    }

    public void testEquals() throws SQLException
    {
        RouterURL url1 = new RouterURL(
                "jdbc:t-router://service/db?user=auser&password=asecret&qos=RW_SESSION&sessionId=DATABASE",
                new Properties());
        RouterURL url2 = new RouterURL(
                "jdbc:t-router://service/db?user=auser&password=asecret&qos=RW_SESSION&sessionId=DATABASE",
                new Properties());

        assertEquals(
                "Two URLs created with the same string should be identical",
                url1, url2);
        Properties userpass = new Properties();
        userpass.put("user", "auser");
        userpass.put("password", "asecret");
        RouterURL url3 = new RouterURL(
                "jdbc:t-router://service/db?qos=RW_SESSION&sessionId=DATABASE",
                userpass);
        // should be equal
        assertEquals(
                "When user/pass are given through properties, the URL should be "
                        + "equal to the one that has the same user/pass in the URL string",
                url1, url3);
        RouterURL url4 = new RouterURL(
                "jdbc:t-router://service/db2?user=auser&password=asecret&qos=RW_SESSION&sessionId=DATABASE",
                new Properties());
        url1.setDbname("db2");
        // database name modification
        //
        assertEquals(
                "Database name modifiction should give the same url as one that "
                        + "has an identical db name set directly in the string",
                url1, url4);
    }

    public void testOverlappingProperties() throws SQLException
    {
        Properties props = new Properties();
        props.put("user", "defInProps");
        RouterURL url = new RouterURL(
                "jdbc:t-router://service/db?user=defInURL", props);
        assertEquals(
                "Values given in the URL string should supersede the ones in the Properties",
                url.getProperty("user"), "defInURL");
        RouterURL url2 = new RouterURL(
                "jdbc:t-router://service/db?user=defInURLbefore&user=defInURLafter",
                null);
        assertEquals(
                "Values defined at the end of the URL should supersede the "
                        + "ones defined at the begining",
                url2.getProperty("user"), "defInURLafter");
    }

    public void testClone() throws SQLException, CloneNotSupportedException
    {
        Properties props = new Properties();
        props.put("user", "defInProps");
        RouterURL orig = new RouterURL(
                "jdbc:t-router://service/db?user=defInURL", props);
        RouterURL copy = (RouterURL) orig.clone();
        assertTrue(orig.equals(copy));
        copy.setDbname("other");
        RouterURL copy2 = (RouterURL) copy.clone();
        assertFalse(orig.equals(copy2));
    }

    public void testUrlOptionsParsing() throws SQLException
    {
        Properties props = new Properties();
        RouterURL.urlOptionsToProperties("qos=RO_RELAXED&affinity=sjc", props);
        assertEquals("Props should contain qos=RO_RELAXED", "RO_RELAXED",
                props.get("qos"));
        assertEquals("Props should contain affinity=sjc", "sjc",
                props.get("affinity"));
        RouterURL.urlOptionsToProperties("qos=RW_STRICT", props);
        assertEquals("Props should contain qos=RW_STRICT", "RW_STRICT",
                props.get("qos"));
        try
        {
            RouterURL.urlOptionsToProperties("invalid", props);
            fail("invalid paramater should trigger an error");
        }
        catch (SQLException sqle)
        {
            // expected
        }
    }
}
