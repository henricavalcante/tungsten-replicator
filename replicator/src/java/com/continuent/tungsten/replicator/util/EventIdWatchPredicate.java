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

package com.continuent.tungsten.replicator.util;

import com.continuent.tungsten.replicator.event.ReplDBMSHeader;

/**
 * Implements a WatchPredicate to identify that a particular native event ID has
 * been reached. This returns true for any event ID equal to
 * <em>or higher than</em> the number we are seeking.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class EventIdWatchPredicate implements WatchPredicate<ReplDBMSHeader>
{
    private final String eventId;

    public EventIdWatchPredicate(String eventId)
    {
        this.eventId = eventId;
    }

    public boolean match(ReplDBMSHeader event)
    {
        if (event == null)
            return false;
        else if (event.getEventId() == null)
            return false;
        else if (event.getEventId().compareTo(eventId) < 0)
            return false;
        else
            return true;
    }

    /**
     * Returns the class name and the event id for which we waiting.
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        return this.getClass().getSimpleName() + " eventId=" + eventId;
    }
}