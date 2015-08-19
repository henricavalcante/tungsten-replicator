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

package com.continuent.tungsten.replicator.database;

import java.util.Scanner;

/**
 * This class defines a OracleEventId
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class OracleEventId implements EventId
{
    private long    scn = -1;
    private boolean valid;

    public OracleEventId(String rawEventId)
    {
        String eventId;
        if (rawEventId.startsWith("ora:"))
            eventId = rawEventId.substring(4).trim();
        else
            eventId = rawEventId;

        Scanner scan = new Scanner(eventId);
        if (scan.hasNextLong())
            scn = scan.nextLong();
        else
            valid = false;
        if (scan.hasNext())
            valid = false;
        else
            valid = true;
        scan.close();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.EventId#getDbmsType()
     */
    @Override
    public String getDbmsType()
    {
        return "oracle";
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.EventId#isValid()
     */
    @Override
    public boolean isValid()
    {
        return valid;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.EventId#compareTo(com.continuent.tungsten.replicator.database.EventId)
     */
    @Override
    public int compareTo(EventId eventId)
    {
        OracleEventId event = (OracleEventId) eventId;
        long l = event.getSCN() - this.getSCN();
        return (l == 0 ? 0 : (l > 0 ? -1 : 1));
    }

    public long getSCN()
    {
        return scn;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        return "ora:" + String.valueOf(scn);
    }

}
