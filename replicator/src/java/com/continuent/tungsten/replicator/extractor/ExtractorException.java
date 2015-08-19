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
 * Initial developer(s): Robert Hodges and Csaba Simon.
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.extractor;

import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * This class defines a ExtractorException
 *
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class ExtractorException extends ReplicatorException
{
    static final long    serialVersionUID = 1L;
    private final String eventId;

    /**
     * Creates a new exception with only a message.
     *
     * @param msg
     */
    public ExtractorException(String msg)
    {
        this(msg, null, null);
    }

    /**
     * Creates a new exception with only a cause but no message.
     *
     * @param t exception to link cause to
     */
    public ExtractorException(Throwable t)
    {
        this(null, t, null);
    }

    /**
     * Creates a new exception with message and cause,
     *
     * @param msg
     * @param cause
     */
    public ExtractorException(String msg, Throwable cause)
    {
        this(msg, cause, null);
    }

    /**
     * Creates a new exception with message, cause, and associated native
     * eventId.
     */
    public ExtractorException(String msg, Throwable cause, String eventId)
    {
        super(msg, cause);
        this.eventId = eventId;
    }

    public String getEventId()
    {
        return eventId;
    }
}
