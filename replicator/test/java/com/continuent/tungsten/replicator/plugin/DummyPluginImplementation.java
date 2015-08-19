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

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;

public class DummyPluginImplementation implements DummyPluginInterface
{
    static Logger logger = Logger.getLogger(DummyPluginImplementation.class);
    public void method1()
    {
        // Do nothing. 
    }

    public void method2()
    {
        // Do nothing. 
    }

    /**
     * {@inheritDoc}
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException
    {
        // Do nothing. 
    }

    /**
     * {@inheritDoc}
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException
    {
        // Do nothing. 
    }

    /**
     * {@inheritDoc}
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException
    {
        // Do nothing. 
    }

    String c = null;
    public void setC(String c)
    {
        logger.debug("value=" + c);
        this.c = c;
    }
    
    public String getC()
    {
        return c;
    }
    
    String s = null;
    public void setStringVal(String s)
    {
        this.s = s;
    }
    
    public String getStringVal()
    {
        return s;
    }
    
    
    Integer i = null;
    public void setIntVal(Integer i)
    {
        this.i = i;
    }

    public Integer getIntVal()
    {
        return i;
    }
    
}
