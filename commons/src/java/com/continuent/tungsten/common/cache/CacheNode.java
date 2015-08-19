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

/**
 * Node for a single entry in an indexed LRU cache. The node contains threaded
 * references to earlier and later nodes in the LRU list.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class CacheNode<T>
{

    // Value we are storing.
    private String key;
    private long   lastAccessMillis;
    private T      value;

    // Previous and after nodes in the LRU list.
    private CacheNode<T> before;
    private CacheNode<T> after;

    /** Create node and set initial access time. */
    public CacheNode(String key, T value)
    {
        this.key = key;
        this.value = value;
        this.lastAccessMillis = System.currentTimeMillis();
    }

    /**
     * Release resources associated with the value. Must be overridden by
     * clients to implement type-specific resource management. The node is
     * unusable after this call.
     */
    public void release()
    {
        value = null;
    }

    /* Returns the key to this node. */
    public String getKey()
    {
        return key;
    }

    /** Return the node value. */
    public T get()
    {
        lastAccessMillis = System.currentTimeMillis();
        return value;
    }

    /** Returns time of last access. */
    public long getLastAccessMillis()
    {
        return lastAccessMillis;
    }

    /** Return the before (newer) node or null in LRU list. */
    public CacheNode<T> getBefore()
    {
        return before;
    }

    /** Set the before node in the LRU list. */
    public void setBefore(CacheNode<T> previous)
    {
        this.before = previous;
    }

    /** Return the after (older) node in the LRU list. */
    public CacheNode<T> getAfter()
    {
        return after;
    }

    /** Set the after node in the LRU list. */
    public void setAfter(CacheNode<T> next)
    {
        this.after = next;
    }
}