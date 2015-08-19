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

package com.continuent.tungsten.replicator.pipeline;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.filter.Filter;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Sample filter definition to test filter operation in pipelines. This class
 * records lifecycle operations and has different policies for skipping events.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class SampleFilter implements Filter
{
    String                      name;
    public static volatile long configured        = 0;
    public static volatile long prepared          = 0;
    public static volatile long released          = 0;
    long                        skipSeqnoStart    = -1;
    long                        skipSeqnoRange    = 1;
    boolean                     skipSeqnoMultiple = false;

    /** Clear life-cycle counters. */
    public static void clearCounters()
    {
        configured = 0;
        prepared = 0;
        released = 0;
    }

    /** If this number is set, skip the sequence number when it appears. */
    public synchronized void setSkipSeqnoStart(long skipSeqnoStart)
    {
        this.skipSeqnoStart = skipSeqnoStart;
    }

    /**
     * Skip this many sequence numbers after start, including the start number
     * itself.
     */
    public synchronized void setSkipSeqnoRange(long skipSeqnoRange)
    {
        this.skipSeqnoRange = skipSeqnoRange;
    }

    /**
     * If this boolean is set, skip any number that is an even multiple of the
     * start. For example, if skipSeqnoStart is 3, we will skip 0, 3, 6, 9.
     */
    public synchronized void setSkipSeqnoMultiple(boolean skipSeqnoMultiple)
    {
        this.skipSeqnoMultiple = skipSeqnoMultiple;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        configured++;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        prepared++;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        released++;
    }

    /**
     * Filter the event in question. If we match a specific seqno or a multiple,
     * return null.
     */
    public ReplDBMSEvent filter(ReplDBMSEvent event)
            throws ReplicatorException, InterruptedException
    {
        // Compute the start. This could be just a single number or
        // the nearest multiple at or below the current sequence number.
        long seqno = event.getSeqno();
        long seqnoStart;
        if (this.skipSeqnoMultiple)
        {
            // Ignore values at or below 0 to prevent nonsensical results.
            if (skipSeqnoStart <= 0)
                return null;
            seqnoStart = (seqno / skipSeqnoStart) * skipSeqnoStart;
        }
        else
        {
            seqnoStart = skipSeqnoStart;
        }

        // Compute the end from the start value.
        long seqnoEnd = seqnoStart + skipSeqnoRange - 1;

        // If the sequence number is in the range including endpoints we
        // need to filter the event.
        if (seqno >= seqnoStart && seqno <= seqnoEnd)
            return null;
        else
            return event;
    }
}