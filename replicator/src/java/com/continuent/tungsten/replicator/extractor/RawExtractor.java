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
 * Initial developer(s): Teemu Ollakka
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.replicator.extractor;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.plugin.ReplicatorPlugin;

/**
 * Denotes an extractor that is responsible for extracting raw events from a
 * database or other replication source. Extractors must be prepared to be
 * interrupted, which mechanism is used to cancel processing.
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public interface RawExtractor extends ReplicatorPlugin
{
    /**
     * Set the value of the last event ID we have processed. The extractor is
     * responsible for returning the next event ID in sequence after this one
     * the next time extract() is called.
     * 
     * @param eventId Event ID at which to begin extracting
     * @throws ReplicatorException
     */
    public void setLastEventId(String eventId) throws ReplicatorException;

    /**
     * Extract the next available DBMSEvent from the database log.
     * 
     * @return next DBMSEvent found in the logs
     */
    public DBMSEvent extract() throws ReplicatorException, InterruptedException;

    /**
     * Extract starting after the event ID provided as an argument. This is
     * equivalent to invoking setLastEventId() followed by extract().
     * 
     * @param eventId Event ID at which to begin extracting
     * @return DBMSEvent corresponding to the id
     * @throws ReplicatorException Thrown if extractor processing fails
     * @throws InterruptedException Thrown if the applier is interrupted
     */
    public DBMSEvent extract(String eventId) throws ReplicatorException,
            InterruptedException;

    /**
     * Returns the last event ID committed in the database from which we are
     * extracting. It is used to help synchronize state between the database and
     * the transaction history log. Values returned from this call must
     * correspond with the last extracted DBMSEvent.eventId as follows:
     * <ol>
     * <li>If the returned value is greater than DBMSEvent.eventId, the database
     * has more recent updates</li>
     * <li>If the returned value is equal to DBMSEvent.eventId, all events have
     * been extracted</li>
     * </ol>
     * It should not be possible to receive a value that is less than the last
     * extracted DBMSEvent.eventId as this implies that the extractor is somehow
     * ahead of the state of the database, which would be inconsistent.
     * 
     * @return A current event ID that can be compared with event IDs in
     *         DBMSEvent
     * @throws ReplicatorException
     * @throws InterruptedException
     */
    public String getCurrentResourceEventId() throws ReplicatorException,
            InterruptedException;
}
