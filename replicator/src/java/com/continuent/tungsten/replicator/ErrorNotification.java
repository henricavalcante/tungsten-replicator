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

package com.continuent.tungsten.replicator;

import com.continuent.tungsten.fsm.core.Event;
import com.continuent.tungsten.fsm.event.CriticalEvent;
import com.continuent.tungsten.fsm.event.OutOfBandEvent;

/**
 * This class defines a ErrorNotification, which denotes a severe replication
 * error that causes replication to fail. It implements the OutOfBandEvent
 * interface to ensure it is processed out-of-band no matter how it is submitted
 * to the state machine. It also implements critical event so that event
 * processing is not interrupted, which prevents race conditions around
 * shutdown.
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class ErrorNotification extends Event
        implements
            OutOfBandEvent,
            CriticalEvent
{
    private final String userMessage;
    private final long   seqno;
    private final String eventId;

    /**
     * Create new instance with underlying error and message for presentation to
     * users.
     */
    public ErrorNotification(String userMessage, Throwable e)
    {
        super(e);
        this.userMessage = userMessage;
        this.seqno = -1;
        this.eventId = null;
    }

    /**
     * Creates an error notification with user, a message, and replication
     * position information.
     */
    public ErrorNotification(String userMessage, long seqno, String eventId,
            Throwable e)
    {
        super(e);
        this.userMessage = userMessage;
        this.seqno = seqno;
        this.eventId = eventId;
    }

    /**
     * Returns the original source of the error.
     */
    public Throwable getThrowable()
    {
        return (Throwable) getData();
    }

    /**
     * Returns a message suitable for users.
     */
    public String getUserMessage()
    {
        return userMessage;
    }

    /**
     * Returns the log sequence number associated with failure or -1 if there is
     * no such number.
     */
    public long getSeqno()
    {
        return seqno;
    }

    /**
     * Returns the native event ID associated with failure or null if there is
     * no such ID.
     */
    public String getEventId()
    {
        return eventId;
    }
}