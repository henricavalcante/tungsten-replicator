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

package com.continuent.tungsten.replicator.thl;

import java.util.HashMap;
import java.util.Map;

/**
 * This class defines a ProtocolHandshakeResponse, which clients return to the
 * THL server. Clients can specify options, which affect the network connection
 * from the server.
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class ProtocolHandshakeResponse extends ProtocolMessage
{
    static final long           serialVersionUID = 123452346L;
    private final String        sourceId;
    private final long          lastEpochNumber;
    private final long          lastSeqno;
    private final int           heartbeatMillis;
    private Map<String, String> options          = new HashMap<String, String>();

    /**
     * Create a new instance.
     * 
     * @param sourceId Source ID of client.
     */
    public ProtocolHandshakeResponse(String sourceId, long lastEpochNumber,
            long lastSeqno, int heartbeatMillis)
    {
        super(null);
        this.sourceId = sourceId;
        this.lastEpochNumber = lastEpochNumber;
        this.lastSeqno = lastSeqno;
        this.heartbeatMillis = heartbeatMillis;
    }

    /** Returns the source ID. */
    public String getSourceId()
    {
        return this.sourceId;
    }

    /** Returns the last epoch number in log. */
    public long getLastEpochNumber()
    {
        return this.lastEpochNumber;
    }

    /** Returns the sequence number in log. */
    public long getLastSeqno()
    {
        return this.lastSeqno;
    }

    /** Returns the number of milliseconds between heartbeats. */
    public int getHeartbeatMillis()
    {
        return this.heartbeatMillis;
    }

    /**
     * Returns the current option settings or null if no options exist. Older
     * replicators do not return options.
     */
    public Map<String, String> getOptions()
    {
        // Required for compatibility with older classes.
        if (options == null)
            options = new HashMap<String, String>();
        return options;
    }

    /** Gets an option value. */
    public String getOption(String name)
    {
        return getOptions().get(name);
    }

    /** Sets option value. */
    public void setOption(String name, String value)
    {
        getOptions().put(name, value);
    }
}