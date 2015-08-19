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
 * Contributor(s): Robert Hodges, Stephane Giron
 */

package com.continuent.tungsten.replicator.applier;

import java.sql.Timestamp;
import java.util.ArrayList;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeaderData;
import com.continuent.tungsten.replicator.plugin.PluginLoader;

/**
 * Implements a simple test to ensure dummy applier work.
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class TestApplierPlugin extends TestCase
{

    static Logger logger = null;

    public void setUp() throws Exception
    {
        if (logger == null)
            logger = Logger.getLogger(TestApplierPlugin.class);
    }

    public void testApplierBasic() throws Exception
    {
        RawApplier applier = (RawApplier) PluginLoader.load(DummyApplier.class
                .getName());
        applier.prepare(null);
        ((DummyApplier) applier).setStoreAppliedEvents(true);

        for (Integer i = 0; i < 10; ++i)
        {
            ArrayList<DBMSData> sql = new ArrayList<DBMSData>();
            sql.add(new StatementData("SELECT " + i));
            Timestamp now = new Timestamp(System.currentTimeMillis());
            applier.apply(new DBMSEvent(i.toString(), sql, now),
                    new ReplDBMSHeaderData(i, (short) 0, true, "test", 0,
                            "test", "myshard", now, 0), true, false);
        }

        ArrayList<StatementData> sql = ((DummyApplier) applier).getTrx();
        for (int i = 0; i < 10; ++i)
        {
            Assert.assertEquals("SELECT " + i, sql.get(i).getQuery());
        }

        applier.release(null);

    }
}
