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
 * Initial developer(s): Stephane Giron
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.replicator.thl;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

/**
 * Implements a simple hash map to hold events. If the cache is full we age out
 * old items in FIFO order.
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class EventsCache
{
    static Logger                         logger    = Logger
                                                            .getLogger(EventsCache.class);
    private int                           cacheSize = 0;
    private LinkedBlockingQueue<THLEvent> fifo;
    private HashMap<Long, THLEvent>       cache;

    public EventsCache(int cacheSize)
    {
        this.cacheSize = cacheSize;
        if (cacheSize > 0)
        {
            logger.info("Allocating THL event cache; size=" + cacheSize);
            this.fifo = new LinkedBlockingQueue<THLEvent>(cacheSize);
            this.cache = new HashMap<Long, THLEvent>(cacheSize);
        }
    }

    public boolean isEmpty()
    {
        return (cacheSize <= 0 || cache.isEmpty());
    }

    /**
     * Add an event to the cache, clearing space if necessary.
     */
    public synchronized void put(THLEvent thlEvent) throws InterruptedException
    {
        // If cache is suppressed do nothing.
        if (cacheSize > 0)
        {
            // Clear space.
            while (cache.size() >= cacheSize)
            {
                THLEvent old = fifo.remove();
                cache.remove(old.getSeqno());
            }

            if (thlEvent.getFragno() == 0 && thlEvent.getLastFrag())
            {
                // This event is not fragmented, so just cache it
                fifo.put(thlEvent);
                cache.put(thlEvent.getSeqno(), thlEvent);
            }
            // else fragmented events are not cached as this could bring OOM
            // issues
        }
    }

    /**
     * Look up and return the cached item, if found.
     */
    public synchronized THLEvent get(long seqno)
    {
        if (cacheSize > 0)
            return cache.get(seqno);
        else
            return null;
    }
}
