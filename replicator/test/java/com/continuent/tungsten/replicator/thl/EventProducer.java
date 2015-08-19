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

package com.continuent.tungsten.replicator.thl;

import java.sql.Timestamp;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.event.ReplDBMSEvent;

/**
 * Generates transactions for test.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class EventProducer
{
    private static Logger          logger                  = Logger.getLogger(EventProducer.class);

    // Parameters.
    private int                    transactions            = 10;
    private int                    fragmentsPerTransaction = 1;
    private int                    shards                  = 1;
    private long                   timestampOffset         = 1;
    private long                   variation               = 0;

    // Test helper instance.
    private THLParallelQueueHelper helper                  = new THLParallelQueueHelper();

    // Coordination parameters.
    private long                   expectedEvents          = 0;
    private long                   seqno                   = 0;
    private short                  fragno                  = -1;
    private short                  eventCount              = 0;
    private long                   baseTimeMillis;

    /**
     * Creates a new event producer.
     * 
     * @param transactions Number of transactions to generate
     * @param fragmentsPerTransaction Number of fragments per generation
     * @param shards Number of shards
     * @param timestampOffset Offset between timestamps on succeeding
     *            transaction fragments
     * @param variation How much to vary offsets by. If the value is bigger than
     *            timestamp offset, timestamps will occasionally go backwards.
     */
    public EventProducer(int transactions, int fragmentsPerTransaction,
            int shards, int timestampOffset, int variation)
    {
        this.transactions = transactions;
        this.fragmentsPerTransaction = fragmentsPerTransaction;
        this.shards = shards;
        this.timestampOffset = timestampOffset;
        this.variation = variation;

        expectedEvents = transactions * fragmentsPerTransaction;
        baseTimeMillis = System.currentTimeMillis()
                - (transactions * fragmentsPerTransaction * timestampOffset);
    }

    /** Returns number of events we expect to produce. */
    public long getExpectedEvents()
    {
        return expectedEvents;
    }

    /**
     * Return the next properly generated event.
     */
    public ReplDBMSEvent nextEvent()
    {
        // Figure out next seqno and fragno.
        fragno++;
        if (fragno >= fragmentsPerTransaction)
        {
            seqno++;
            fragno = 0;
        }

        // Exit if we have processed desired transactions.
        if (seqno >= transactions)
            return null;
        else
        {
            // Otherwise generate the next event.
            long id = (seqno + 1) % shards;
            String shardId = "db" + id;
            int localVariation = (int) (variation * (Math.random() - .5));
            Timestamp ts = new Timestamp(baseTimeMillis
                    + (timestampOffset * eventCount) + localVariation);
            if (logger.isDebugEnabled())
            {
                logger.debug("Event: seqno=" + seqno + " timestamp="
                        + ts.toString());
            }

            return helper.createEvent(seqno, fragno,
                    (fragno >= (fragmentsPerTransaction - 1)), shardId, ts);
        }
    }

    /**
     * Print properties of the producer.
     */
    public String toString()
    {
        StringBuffer sb = new StringBuffer(this.getClass().getSimpleName());
        sb.append(" transactions=").append(transactions);
        sb.append(" fragmentsPerTransaction=").append(fragmentsPerTransaction);
        sb.append(" shards=").append(shards);
        sb.append(" timestampOffset=").append(timestampOffset);
        sb.append(" variation=").append(variation);
        return sb.toString();
    }
}