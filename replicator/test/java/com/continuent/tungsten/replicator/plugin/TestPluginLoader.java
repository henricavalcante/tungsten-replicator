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
 * Initial developer(s): Teemu Ollakka
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.plugin;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;


import junit.framework.Assert;
import junit.framework.TestCase;

public class TestPluginLoader extends TestCase
{

    static Logger       logger  = null;
    final static String dpName  = "com.continuent.tungsten.replicator.plugin.DummyPlugin";
    final static String dpiName = "com.continuent.tungsten.replicator.plugin.DummyPluginImplementation";

    public void setUp() throws Exception
    {
        if (logger == null)
        {
            logger = Logger.getLogger(TestPluginLoader.class);
            BasicConfigurator.configure();
        }
    }

    /*
     * Simple test that loads plugin and tests setter and getter methods.
     */
    public void testDummyPlugin() throws Exception
    {
        DummyPlugin dp = (DummyPlugin) PluginLoader.load(dpName);
        PluginConfigurator.setParameter(dp, "setA", "valueA");
        Assert.assertEquals("valueA", PluginConfigurator.getParameter(dp,
                "getA"));
        Assert.assertEquals(null, PluginConfigurator.getParameter(dp, "getB"));
        PluginConfigurator.setParameter(dp, "setB", "valueB");
        Assert.assertEquals("valueB", PluginConfigurator.getParameter(dp,
                "getB"));

    }

    /*
     * Test loading of plugin implementation defined by interface that extends
     * ReplicatorPlugin.
     */
    public void testDummyPluginInterface() throws Exception
    {
        DummyPluginInterface dpi = (DummyPluginInterface) PluginLoader
                .load(dpiName);
        PluginConfigurator.setParameter(dpi, "setC", "valueC");
        Assert.assertEquals("valueC", PluginConfigurator.getParameter(dpi,
                "getC"));
        dpi.configure(null);
        dpi.prepare(null);
        dpi.release(null);
    }

    /*
     * Check that usual error situations result an exception.
     */
    public void testErrors() throws Exception
    {

        /*
         * Trying to load plugin that does not exist must result an exception.
         */
        try
        {
            PluginLoader
                    .load("com.continuent.tungsten.replicator.plugin.PluginThatDoesNotExist");
            throw new Exception(
                    "Trying to load non-existing class does not result an exception");
        }
        catch (PluginException e)
        {

        }

        DummyPluginInterface dpi = (DummyPluginInterface) PluginLoader
                .load(dpiName);

        PluginConfigurator.setParameter(dpi, "setStringVal", "sval");
        Assert.assertEquals("sval", PluginConfigurator.getParameter(dpi,
                "getStringVal"));

        /*
         * setStringVal takes string as an argument, trying to set integer must
         * result an exception.
         */
        try
        {
            PluginConfigurator
                    .setParameter(dpi, "setStringVal", new Integer(1));
            throw new Exception("Illegal argument exception not thrown");
        }
        catch (PluginException e)
        {
            // e.printStackTrace();
        }

        PluginConfigurator.setParameter(dpi, "setIntVal", 1);
        Assert.assertEquals(1, PluginConfigurator
                .getParameter(dpi, "getIntVal"));
        /*
         * setIntVal takes Integer as an argument, trying to set double or long
         * must result an exception.
         */
        try
        {
            PluginConfigurator.setParameter(dpi, "setIntVal", 0.1D);
            throw new Exception("Illegal argument exception not thrown");
        }
        catch (PluginException e)
        {

        }

        try
        {
            PluginConfigurator.setParameter(dpi, "setIntVal", 1L);
            throw new Exception("Illegal argument exception not thrown");
        }
        catch (PluginException e)
        {

        }

        /*
         * Trying to call non-existent method must result an exception.
         */
        try
        {
            PluginConfigurator
                    .setParameter(dpi, "callNonExistentMethod", "foo");
            throw new Exception("Exception not thrown");
        }
        catch (PluginException e)
        {

        }

        /*
         * Trying to load plugin that does not implement desired interface must
         * result an exception.
         */
        try
        {
            DummyPluginInterface ii = (DummyPluginInterface) PluginLoader
                    .load(dpName);
            // Just some use of ii to avoid warning
            PluginConfigurator.getParameter(ii, "getStringVal");
        }
        catch (ClassCastException e)
        {
            // e.printStackTrace();
        }
    }
}
