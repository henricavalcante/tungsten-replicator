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

package com.continuent.tungsten.common.config.test;

import junit.framework.TestCase;

import com.continuent.tungsten.common.config.Interval;

/**
 * Implements a unit test for time intervals, which are a Tungsten property
 * type.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class IntervalTest extends TestCase
{
    /**
     * Verify ability to create and return a time interval. 
     */
    public void testIntervalLong() throws Exception
    {
        Interval i = new Interval(0);
        assertEquals("Empty interval", 0, i.longValue());
        Interval i2 = new Interval(1000);
        assertEquals("Non-zero interval", 1000, i2.longValue());
    }
    
    /**
     * Verify ability to parse interval values from strings. 
     */
    public void testIntervalString() throws Exception
    {
        assertEquals(0, new Interval("0").longValue());
        assertEquals(100, new Interval("100").longValue());
        
        assertEquals(0, new Interval("0s").longValue());
        assertEquals(2000, new Interval("2s").longValue());
        assertEquals(2000, new Interval("2S").longValue());

        assertEquals(0, new Interval("0m").longValue());
        assertEquals(120000, new Interval(" 2m").longValue());
        assertEquals(120000, new Interval("2M").longValue());

        assertEquals(0, new Interval("0h").longValue());
        assertEquals(7200000, new Interval("2h ").longValue());
        assertEquals(7200000, new Interval("2H").longValue());

        assertEquals(0, new Interval("0d").longValue());
        assertEquals(7200000 * 24, new Interval(" 2d ").longValue());
        assertEquals(7200000 * 24, new Interval("2D").longValue());
    }
    
    /** 
     * Ensure that bad intervals generate exceptions. 
     */
    public void testBadValues() throws Exception
    {
        try
        {
            new Interval("100f").longValue();
            throw new Exception("Accepted bad unit");
        }
        catch (NumberFormatException e)
        {
        }
        try
        {
            new Interval("-100").longValue();
            throw new Exception("Accepted bad prefix");
        }
        catch (NumberFormatException e)
        {
        }
        try
        {
            new Interval("").longValue();
            throw new Exception("Accepted empty string");
        }
        catch (NumberFormatException e)
        {
        }
        try
        {
            new Interval("d").longValue();
            throw new Exception("Accepted unit only");
        }
        catch (NumberFormatException e)
        {
        }
    }
}