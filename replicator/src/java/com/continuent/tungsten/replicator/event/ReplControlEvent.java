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

package com.continuent.tungsten.replicator.event;

/**
 * An implementation of ReplEvent used to transmit control information within
 * pipelines. Control events add extra information that affects the disposition
 * of processing following a particular event. They are not serialized and
 * should never be handled by an applier.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
@SuppressWarnings("serial")
public class ReplControlEvent extends ReplEvent
{
    // Control event types.
    /**
     * Event indicates end of processing. Task should immediately commit current
     * work and exit.
     */
    public static final int      STOP = 1;

    /**
     * Event is provided for synchronization purposes when waiting for a
     * particular sequence number or event. Synchronization events ensure that
     * all tasks "see" an event on which we are waiting when parallel apply is
     * active.
     */
    public static final int      SYNC = 2;

    // Control event data.
    private final int            eventType;
    private final long           seqno;
    private final ReplDBMSHeader header;

    /**
     * Creates a new control event instance.
     * 
     * @param eventType A static control event type
     */
    public ReplControlEvent(int eventType, long seqno, ReplDBMSHeader header)
    {
        this.eventType = eventType;
        this.seqno = seqno;
        this.header = header;
    }

    /** Returns the control event type. */
    public int getEventType()
    {
        return eventType;
    }

    /**
     * Returns the event to which control information applies or null if
     * inapplicable.
     */
    public ReplDBMSHeader getHeader()
    {
        return header;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.event.ReplEvent#getSeqno()
     */
    public long getSeqno()
    {
        return seqno;
    }
}