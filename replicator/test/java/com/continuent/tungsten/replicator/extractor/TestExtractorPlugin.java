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


package com.continuent.tungsten.replicator.extractor;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.plugin.PluginLoader;

import junit.framework.Assert;
import junit.framework.TestCase;

/**
 * 
 * This class checks extractor loading using the DummyExtractor. 
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class TestExtractorPlugin extends TestCase
{

    static Logger logger = null;
    public void setUp() throws Exception
    {
        if (logger == null)
            logger = Logger.getLogger(TestExtractorPlugin.class);
    }
    
   /*
    * Test that dummy extractor works like expected, 
    */
    public void testExtractorBasic() throws Exception
    {
        
        RawExtractor extractor = (RawExtractor) PluginLoader.load(DummyExtractor.class.getName());
        
        extractor.configure(null);
        extractor.prepare(null);
        
        DBMSEvent event = extractor.extract();
        Assert.assertEquals(event.getEventId(), "0");
        event = extractor.extract();
        Assert.assertEquals(event.getEventId(), "1");
        
        extractor.setLastEventId("0");
        event = extractor.extract();
        Assert.assertEquals(event.getEventId(), "1");
        
        extractor.setLastEventId(null);
        event = extractor.extract();
        Assert.assertEquals(event.getEventId(), "0");
        
        for (Integer i = 1; i < 5; ++i)
        {
            event = extractor.extract();
            Assert.assertEquals(event.getEventId(), i.toString());
        }
        
        event = extractor.extract("0");
        Assert.assertEquals(event.getEventId(), "0");

        event = extractor.extract("4");
        Assert.assertEquals(event.getEventId(), "4");

        event = extractor.extract("5");
        Assert.assertEquals(event, null);

        
        
        extractor.release(null);
        
    }
    
    /*
     * Test test that event ID calls work as expected 
     */
     public void testExtractorEventID() throws Exception
     {
         
         RawExtractor extractor = (RawExtractor) PluginLoader.load(DummyExtractor.class.getName());
         
         extractor.configure(null);
         extractor.prepare(null);

         DBMSEvent event = extractor.extract();
         String currentEventId = extractor.getCurrentResourceEventId();
         Assert.assertEquals(event.getEventId(), currentEventId);
         
         event = extractor.extract();
         Assert.assertTrue(event.getEventId().compareTo(currentEventId) > 0);

         currentEventId = extractor.getCurrentResourceEventId();
         Assert.assertTrue(event.getEventId().compareTo(currentEventId) == 0);

         extractor.release(null);
     }
    
}
