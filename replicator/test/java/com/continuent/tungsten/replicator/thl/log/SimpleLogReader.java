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

package com.continuent.tungsten.replicator.thl.log;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.thl.THLEvent;
import com.continuent.tungsten.replicator.util.AtomicCounter;

/**
 * Implements a log reader task that can be used to test concurrent reading and
 * writing.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class SimpleLogReader implements Runnable
{
    private static Logger logger = Logger.getLogger(SimpleLogReader.class);
    DiskLog               log;
    final long            startSeqno;
    final int             howMany;
    volatile int          eventsRead;
    final AtomicCounter   lastSeqno;
    Throwable             error;
    Thread                myThread;

    /** Store file instance. */
    SimpleLogReader(DiskLog log, long startSeqno, int howMany)
    {
        this.log = log;
        this.startSeqno = startSeqno;
        this.howMany = howMany;
        this.lastSeqno = new AtomicCounter(-1);
    }

    /** Read all records from file. */
    public void run()
    {
        myThread = Thread.currentThread();
        try
        {
            LogConnection conn = log.connect(true);
            conn.seek(startSeqno);
            for (long seqno = startSeqno; seqno < startSeqno + howMany; seqno++)
            {
                THLEvent e = conn.next();
                if (e == null)
                    throw new Exception("Event is null: seqno=" + seqno);
                if (seqno != e.getSeqno())
                {
                    throw new Exception(
                            "Sequence numbers do not match: expected=" + seqno
                                    + " actual=" + e.getSeqno());
                }
                eventsRead++;
                lastSeqno.setSeqno(e.getSeqno());

                if (eventsRead > 0 && eventsRead % 1000 == 0)
                {
                    logger.info("Reading events: threadId="
                            + Thread.currentThread().getId() + " events="
                            + eventsRead);
                }
            }
        }
        catch (InterruptedException e)
        {
        }
        catch (Throwable t)
        {
            error = t;
        }
    }

    /** Wait until the thread is done or at least try to. */
    public boolean waitFinish(long timeout)
    {
        if (myThread == null)
            return false;
        else
        {
            try
            {
                myThread.join(timeout);
            }
            catch (InterruptedException e)
            {
            }
            return !myThread.isAlive();
        }
    }
}