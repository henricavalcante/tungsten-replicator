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

package com.continuent.tungsten.replicator.database;

/**
 * Denotes a native event ID, which is the ID used to identify [re-]start
 * locations in the DBMS log when extracting events.
 */
public interface EventId extends Comparable<EventId>
{
    /**
     * Return the event ID DBMS type.
     */
    public String getDbmsType();

    /** 
     * Returns true if this is a syntactically valid event ID.
     */
    public boolean isValid();

    /**
     * Compares two event IDs using the file index and offset as determinants
     * for collation. If the DBMS types are not the same or the eventID is invalid
     * the comparison result is undefined.
     * 
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    public int compareTo(EventId eventId);

    /**
     * Prints event ID in standard format for this DBMS type.
     * 
     * @see java.lang.Object#toString()
     */
    public String toString();
}