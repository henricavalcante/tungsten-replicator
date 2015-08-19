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

package com.continuent.tungsten.common.cache;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.continuent.tungsten.common.cache.CacheResourceManager;
import com.continuent.tungsten.common.cache.IndexedLRUCache;

/**
 * Implements a unit test of IndexedLRUCache features. 
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class IndexedLRUCacheTest
{
    // Implementation of a dummy resource manager. 
    class StringResourceManager implements CacheResourceManager<String>
    {
        private int count = 0;

        public void release(String string)
        {
            count++;
        }
    }

    /**
     * Verify that we can create and release a simple cache.
     */
    @Test
    public void testCreate() throws Exception
    {
        // Create cache.
        StringResourceManager srm = new StringResourceManager();
        IndexedLRUCache<String> cache = new IndexedLRUCache<String>(10, srm);

        Assert.assertEquals("Empty, size=0", 0, cache.size());
        Assert.assertNull("No value found", cache.get("any"));
        Assert.assertEquals("Empty list", 0, cache.lruValues().size());
    }

    /**
     * Verify we can insert, retrieve, and delete a single entry.
     */
    @Test
    public void testCacheOfOne() throws Exception
    {
        // Create cache.
        StringResourceManager srm = new StringResourceManager();
        IndexedLRUCache<String> cache = new IndexedLRUCache<String>(10, srm);

        // Add an entry.
        cache.put("entry", "my string");
        Assert.assertEquals("1 entry", 1, cache.size());

        // Ensure we can find it.
        String myString = cache.get("entry");
        Assert.assertEquals("entry value", "my string", myString);
        Assert.assertEquals("List of 1", 1, cache.lruValues().size());

        // Ensure we can remove it.
        Assert.assertEquals("No released items", 0, srm.count);
        cache.invalidate("entry");
        Assert.assertNull("No value found", cache.get("any"));
        Assert.assertEquals("Empty list", 0, cache.lruValues().size());
        Assert.assertEquals("1 released item", 1, srm.count);
    }

    /**
     * Verify that connections are always ordered from first to last used in the
     * LRU list.
     */
    @Test
    public void testLRUMaintenance() throws Exception
    {
        // Create cache.
        StringResourceManager srm = new StringResourceManager();
        IndexedLRUCache<String> cache = new IndexedLRUCache<String>(10, srm);

        // Add some entries.
        String[] entries = {"a", "a.b", "a.a", "b"};
        for (int i = 0; i < entries.length; i++)
            cache.put(entries[i], new Integer(i).toString());

        // Ensure that entries are in the LRU in reverse order of addition,
        // i.e., in reverse order of addition.
        List<String> lru = cache.lruValues();
        for (int i = 0; i < lru.size(); i++)
        {
            String value = lru.get(i);
            Assert.assertEquals("Initial load", 3 - i, Integer.parseInt(value));
        }

        // Touch the last entry and validate that it moves to the front.
        String v = cache.get("a.b");
        List<String> lru2 = cache.lruValues();
        String v1 = lru2.get(0);
        Assert.assertEquals("Touched member at head of LRU", v, v1);

        // Remove two entries and ensure they leave the list. We get them to
        // ensure their location in the list.
        String v2a = cache.get("a.b");
        String v2b = cache.get("a");
        cache.invalidate("a.b");
        cache.invalidate("a");

        List<String> lru3 = cache.lruValues();
        Assert.assertFalse("Looking for first value", lru3.remove(v2a));
        Assert.assertFalse("Looking for first value", lru3.remove(v2b));
        Assert.assertEquals("LRU has only 2 members", 2, lru3.size());

        // Release all entries and check the LRU size.
        cache.invalidateByPrefix("a");
        List<String> lru4 = cache.lruValues();
        Assert.assertEquals("LRU has only 1 member", 1, lru4.size());

        cache.invalidateAll();
        List<String> lru5 = cache.lruValues();
        Assert.assertEquals("LRU has 0 members", 0, lru5.size());
    }

    /**
     * Verify that entries are purged in LRU order when the index hits maximum
     * capacity.
     */
    @Test
    public void testPurge() throws Exception
    {
        // Create cache.
        StringResourceManager srm = new StringResourceManager();
        IndexedLRUCache<String> cache = new IndexedLRUCache<String>(3, srm);

        // Add some entries. This will overflow capacity and force a purge.
        String[] entries = {"a", "a.b", "a.a", "b"};
        for (int i = 0; i < entries.length; i++)
            cache.put(entries[i], new Integer(i).toString());
        Assert.assertEquals("Initial load", 3, cache.size());

        // Ensure that only the last three entries are present.
        List<String> lru = cache.lruValues();
        for (int i = 0; i < (entries.length - 1); i++)
        {
            String value = lru.get(i);
            Assert.assertEquals("LRU after first purge", 3 - i, Integer
                    .parseInt(value));
        }

        // Add another entry and touch two values to get them at the head of the
        // LRU. Conform that these are all in the LRU in correct order.
        String v1 = cache.get("a.b");
        String v2 = cache.get("a.a");
        cache.put("c.1", "4");
        List<String> lru2 = cache.lruValues();
        Assert.assertEquals("Touched member #1", "4", lru2.get(0));
        Assert.assertEquals("Touched member #1", v2, lru2.get(1));
        Assert.assertEquals("Touched member #1", v1, lru2.get(2));

        cache.invalidateAll();
    }

    /**
     * Verify that values may be invalidated by key, by prefix, and by all values
     * remaining in cache.
     */
    @Test
    public void testInvalidate() throws Exception
    {
        // Create cache.
        StringResourceManager srm = new StringResourceManager();
        IndexedLRUCache<String> cache = new IndexedLRUCache<String>(50, srm);

        // Add some entries.
        String[] entries = {"a", "a.b", "a.a", "b", "b.a", "c", "a.a.b", "b.b"};
        for (int i = 0; i < entries.length; i++)
            cache.put(entries[i], new Integer(i).toString());

        // Remove single key.
        cache.invalidate("b");
        Assert.assertEquals("Remove single key", 7, cache.size());

        // Remove a prefix.
        cache.invalidateByPrefix("a.a");
        Assert.assertEquals("Remove prefix", 5, cache.size());

        // Remove all.
        cache.invalidateAll();
        Assert.assertEquals("Remove all", 0, cache.size());
    }
}