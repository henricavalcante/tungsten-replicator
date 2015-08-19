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
 * Factory to generate event IDs.
 */
public class EventIdFactory
{
    // Uses singleton design pattern.
    private static EventIdFactory instance = new EventIdFactory();

    private EventIdFactory()
    {
    }

    /**
     * Return factory instance.
     */
    public static EventIdFactory getInstance()
    {
        return instance;
    }

    /**
     * Return proper instance for a raw event ID or null if type cannot be
     * discovered.
     */
    public EventId createEventId(String rawEventId)
    {
        if (rawEventId.toLowerCase().startsWith("ora:"))
            return new OracleEventId(rawEventId);
        if (rawEventId.startsWith("mysql") || rawEventId.indexOf(":") > -1)
            return new MySQLEventId(rawEventId);
        else
            return null;
    }
}
