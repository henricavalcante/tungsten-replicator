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

import java.util.ArrayList;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.plugin.PluginContext;

public class DummyApplier implements RawApplier
{
    int                      taskId             = 0;
    ArrayList<StatementData> trx                = null;
    ReplDBMSHeader           lastHeader         = null;
    boolean                  storeAppliedEvents = false;
    long                     eventCount         = 0;
    long                     txnCount           = 0;

    public void setStoreAppliedEvents(boolean store)
    {
        storeAppliedEvents = store;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#setTaskId(int)
     */
    public void setTaskId(int id)
    {
        taskId = 0;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#apply(com.continuent.tungsten.replicator.event.DBMSEvent,
     *      com.continuent.tungsten.replicator.event.ReplDBMSHeader, boolean,
     *      boolean)
     */
    public void apply(DBMSEvent event, ReplDBMSHeader header, boolean doCommit,
            boolean doRollback) throws ReplicatorException
    {
        ArrayList<DBMSData> data = event.getData();
        eventCount++;
        if (storeAppliedEvents)
        {
            for (DBMSData dataElem : data)
            {
                if (dataElem instanceof StatementData)
                    trx.add((StatementData) dataElem);
                else
                {
                    // Other types not supported.
                }
            }
        }
        if (doCommit)
        {
            lastHeader = header;
            txnCount++;
        }
    }

    public void commit()
    {
        // does nothing for now...
    }

    public void rollback() throws InterruptedException
    {
        // does nothing for now...
    }

    public ReplDBMSHeader getLastEvent() throws ReplicatorException,
            InterruptedException
    {
        return lastHeader;
    }

    public void configure(PluginContext context)
    {

    }

    public void prepare(PluginContext context)
    {
        trx = new ArrayList<StatementData>();
    }

    public void release(PluginContext context)
    {
        trx = null;
    }

    public ArrayList<StatementData> getTrx()
    {
        return trx;
    }

    public long getEventCount()
    {
        return eventCount;
    }

    public long getTxnCount()
    {
        return txnCount;
    }
}
