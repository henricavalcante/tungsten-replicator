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
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.event;

import java.sql.Timestamp;

/**
 * This class defines a DBMSEmptyEvent
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class DBMSEmptyEvent extends DBMSEvent
{
    private static final long serialVersionUID = 1300L;

    /**
     * Creates a new empty event.
     * 
     * @param id Event Id
     * @param extractTime Time of commit or failing that extraction
     */
    public DBMSEmptyEvent(String id, Timestamp extractTime)
    {
        super(id, null, extractTime);
    }

    /**
     * Creates a new empty event with the current time as timestamp. WARNING: do
     * not put this type of event into the log as it can mess up parallel
     * replication.
     * 
     * @param id Event Id
     */
    public DBMSEmptyEvent(String id)
    {
        this(id, new Timestamp(System.currentTimeMillis()));
    }
}
