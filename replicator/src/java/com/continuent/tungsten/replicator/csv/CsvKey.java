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

package com.continuent.tungsten.replicator.csv;

/**
 * Defines a key for a CSV file with a CSV file set. A key is a value used to
 * partition CSV writes.
 */
public class CsvKey implements Comparable<CsvKey>
{
    private static final CsvKey emptyKey = new CsvKey("");

    public String               key;

    /**
     * Instantiates a new CSV key.
     */
    public CsvKey(String key)
    {
        this.key = key;
    }

    /**
     * Returns a standard key for cases where there is only a single key used.
     * This enables clients to use the same code path regardless of whether
     * distributed by keys are used.
     */
    public static CsvKey emptyKey()
    {
        return emptyKey;
    }

    /** Returns true if this is the empty key. */
    public boolean isEmptyKey()
    {
        return "".equals(key);
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        return key;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    public int compareTo(CsvKey anotherKey)
    {
        return key.compareTo(anotherKey.toString());
    }
}