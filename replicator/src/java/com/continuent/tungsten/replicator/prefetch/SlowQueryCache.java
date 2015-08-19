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
 * Contributor(s):  Stephane Giron
 */

package com.continuent.tungsten.replicator.prefetch;

import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.cache.IndexedLRUCache;

/**
 * Implements a shared cache for slow prefetch queries. The cache supports
 * concurrent access by synchronizing all access. Methods within the cache are
 * relatively fast.
 */
public class SlowQueryCache
{
    private static Logger         logger                  = Logger.getLogger(SlowQueryCache.class);

    // Properties.
    private int                   slowQueryCacheSize      = 10000;
    private int                   slowQueryRows           = 100;
    private double                slowQuerySelectivity    = .05;
    private int                   slowQueryCacheDuration  = 60;

    // Counters.
    private long                  totalCachedQueries      = 0;
    private long                  totalInvalidatedQueries = 0;
    private double                minSelectivity          = 1.0;
    private double                maxSelectivity          = 0.0;
    private long                  minRows                 = Long.MAX_VALUE;
    private long                  maxRows                 = 0;

    // Pending query cache. This keeps a list of queries that are currently
    // running so that individual threads do not execute identical queries
    // twice.
    private IndexedLRUCache<Long> pendingQueries;

    // Our slow query cache.
    private IndexedLRUCache<Long> slowQueries;

    public SlowQueryCache()
    {
    }

    /**
     * Returns the slowQueryCacheSize value.
     * 
     * @return Returns the slowQueryCacheSize.
     */
    public synchronized int getSlowQueryCacheSize()
    {
        return slowQueryCacheSize;
    }

    /**
     * Sets the slowQueryCacheSize value.
     * 
     * @param slowQueryCacheSize The slowQueryCacheSize to set.
     */
    public synchronized void setSlowQueryCacheSize(int slowQueryCacheSize)
    {
        this.slowQueryCacheSize = slowQueryCacheSize;
    }

    /**
     * Returns the slowQueryRows value.
     * 
     * @return Returns the slowQueryRows.
     */
    public synchronized int getSlowQueryRows()
    {
        return slowQueryRows;
    }

    /**
     * Sets the slowQueryRows value.
     * 
     * @param slowQueryRows The slowQueryRows to set.
     */
    public synchronized void setSlowQueryRows(int slowQueryRows)
    {
        this.slowQueryRows = slowQueryRows;
    }

    /**
     * Returns the slowQuerySelectivity value.
     * 
     * @return Returns the slowQuerySelectivity.
     */
    public synchronized double getSlowQuerySelectivity()
    {
        return slowQuerySelectivity;
    }

    /**
     * Sets the slowQuerySelectivity value.
     * 
     * @param slowQuerySelectivity The slowQuerySelectivity to set.
     */
    public synchronized void setSlowQuerySelectivity(double slowQuerySelectivity)
    {
        this.slowQuerySelectivity = slowQuerySelectivity;
    }

    /**
     * Returns the slowQueryCacheDuration value.
     * 
     * @return Returns the slowQueryCacheDuration.
     */
    public synchronized int getSlowQueryCacheDuration()
    {
        return slowQueryCacheDuration;
    }

    /**
     * Sets the slowQueryCacheDuration value.
     * 
     * @param slowQueryCacheDuration The slowQueryCacheDuration to set.
     */
    public synchronized void setSlowQueryCacheDuration(
            int slowQueryCacheDuration)
    {
        this.slowQueryCacheDuration = slowQueryCacheDuration;
    }

    /**
     * Initialize the cache.
     */
    public synchronized void init()
    {
        if (slowQueryCacheSize > 0)
            slowQueries = new IndexedLRUCache<Long>(slowQueryCacheSize, null);
        pendingQueries = new IndexedLRUCache<Long>(100, null);
    }

    /**
     * Returns true if we have not heard of this query *or* if the query has
     * exceeded the slow query limit.
     * 
     * @throws SQLException
     */
    public synchronized boolean shouldExecute(KeySelect keySelect)
    {
        // Ensure cache is active.
        if (slowQueries == null)
            return true;

        // See if this is a pending query. If so some other thread
        // is already running it.
        String queryKey = keySelect.generateKey();
        Long pendingQueryInvocation = pendingQueries.get(queryKey);
        if (pendingQueryInvocation != null)
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Skipped pending query: queryKey=" + queryKey);
            }
            return false;
        }

        // See if we have a slow query.
        Long slowQueryInvocation = slowQueries.get(queryKey);
        if (slowQueryInvocation != null)
        {
            // Mark the query as slow and check the duraction since last
            // execution.
            keySelect.setLastInvocation(slowQueryInvocation);

            // If so, only try again if we have exceeded the slow query
            // cache size.
            long sinceLastMillis = System.currentTimeMillis()
                    - slowQueryInvocation;
            if (sinceLastMillis < (slowQueryCacheDuration * 1000))
            {
                // Mark the execution time and return false. This
                // prevents other threads from uselessly running this one.
                slowQueries.put(queryKey, System.currentTimeMillis());
                return false;
            }
        }

        // We should add this to the pending query cache to prevent
        // other threads from running it and execute.
        pendingQueries.put(queryKey, System.currentTimeMillis());
        return true;
    }

    /**
     * Check for a slow query and update cache accordingly.
     */
    public synchronized void updateCache(KeySelect keySelect, long rowCount)
    {
        // Ensure cache is active.
        if (slowQueries == null)
            return;

        // See if this meets the criteria for a slow query based on its
        // selectivity or the number of rows scanned.
        boolean slow = false;
        long tableCardinality = keySelect.getTable().getMaxCardinality();
        double selectivity = 0;
        if (tableCardinality > 0)
        {
            selectivity = ((double) rowCount) / tableCardinality;
        }
        if (selectivity > this.slowQuerySelectivity
                || rowCount >= this.slowQueryRows)
            slow = true;

        // If this is a slow query add it. Otherwise try to remove it from the
        // cache.
        String queryKey = keySelect.generateKey();
        boolean cached = (slowQueries.get(queryKey) != null);
        if (slow)
        {
            // Increment cached queries if we are currently uncached.
            if (cached)
                this.totalCachedQueries++;

            // Cache the query.
            slowQueries.put(queryKey, System.currentTimeMillis());
            if (logger.isDebugEnabled())
            {
                logger.debug("Added slow prefetch query: selectivity="
                        + selectivity + " rowCount=" + rowCount + " queryKey="
                        + queryKey);
            }

            // Update query stats if appropriate.
            if (selectivity < minSelectivity)
                minSelectivity = selectivity;
            if (selectivity > maxSelectivity)
                maxSelectivity = selectivity;
            if (rowCount < minRows)
                minRows = rowCount;
            if (rowCount > maxRows)
                maxRows = rowCount;
        }
        else if (!slow && cached)
        {
            slowQueries.invalidate(queryKey);
            if (logger.isDebugEnabled())
            {
                logger.debug("Invalidated existing slow prefetch query: selectivity="
                        + selectivity
                        + " rowCount="
                        + rowCount
                        + " queryKey="
                        + queryKey);
            }
            totalInvalidatedQueries++;
        }

        // Remove this query from the pending query cache.
        pendingQueries.invalidate(queryKey);
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        StringBuffer sb = new StringBuffer(this.getClass().getSimpleName());
        sb.append(" slowQueryCacheSize=").append(slowQueryCacheSize);
        sb.append(" slowQueryRows=").append(slowQueryRows);
        sb.append(" slowQuerySelectivity=").append(slowQuerySelectivity);
        sb.append(" slowQueryCacheDuration=").append(slowQueryCacheDuration);
        sb.append(" currentSize=").append(
                slowQueries == null ? 0 : slowQueries.size());
        sb.append(" totalCachedQueries=").append(totalCachedQueries);
        sb.append(" totalInvalidatedQueries=").append(totalInvalidatedQueries);
        sb.append(" minSelectivity=").append(minSelectivity);
        sb.append(" maxSelectivity=").append(maxSelectivity);
        sb.append(" maxRows=").append(maxRows);
        sb.append(" minRows=").append(minRows);
        sb.append(" pendingQueries=").append(pendingQueries.size());
        return sb.toString();
    }
}