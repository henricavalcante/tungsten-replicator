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

import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplOptionParams;

/**
 * Implements a WatchPredicate that returns true when we see an event that is
 * marked as a heartbeat.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class HeartbeatWatchPredicate implements WatchPredicate<ReplDBMSHeader>
{
    private final String  name;
    private final boolean matchAny;

    public HeartbeatWatchPredicate(String name)
    {
        this.name = name;
        matchAny = "*".equals(name) || name == null;
    }

    /**
     * Return true if we have a ReplDBMSEvent instance *and* it has a matching
     * heartbeat name.
     */
    public boolean match(ReplDBMSHeader event)
    {
        if (event == null)
            return false;
        else if (event instanceof ReplDBMSEvent)
        {
            String heartbeatName = ((ReplDBMSEvent) event).getDBMSEvent()
                    .getMetadataOptionValue(ReplOptionParams.HEARTBEAT);
            if (heartbeatName != null)
            {
                if (matchAny)
                    return true;
                else
                    return name.equals(heartbeatName);
            }
            else
                return false;
        }
        else
            return false;
    }

    /**
     * Returns the class name and the event for which we waiting.
     * 
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        return this.getClass().getSimpleName() + " name=" + name;
    }
}