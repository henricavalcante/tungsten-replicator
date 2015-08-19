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
 * Initial developer(s):
 * Contributor(s):
 */

package com.continuent.tungsten.common.patterns.order;

import java.io.Serializable;

/**
 * A HighWater resource allows to represent and compare replicators applied
 * event IDs. It is especially used by session consistency/smart-scale to
 * compared master and slave progresses.<br>
 * It is composed of a epoch and a event id. The string representation being
 * <epoch>(<eventId>)
 * 
 * @author <a href="mailto:edward.archibald@continuent.com">Edward Archibald</a>
 */
public class HighWaterResource implements Serializable, Comparable<HighWaterResource>
{
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private long                highWaterEpoch    = -1;
    private String              highWaterEventId  = "";

    /** New event ids are in the form "<log file #>:<offset>;<sessionId>" */
    private static final String SESSION_DELIMITER = ";";

    public HighWaterResource()
    {

    }

    public HighWaterResource(long epoch, String eventId)
    {
        this.highWaterEpoch = epoch;
        this.highWaterEventId = eventId;
    }

    public HighWaterResource(String resourceAsString)
    {
        String epochStr = resourceAsString.substring(0,
                resourceAsString.indexOf('('));
        this.highWaterEpoch = Long.valueOf(epochStr);
        this.highWaterEventId = resourceAsString.substring(
                resourceAsString.indexOf('(') + 1,
                resourceAsString.length() - 1);
    }

    /**
     * Whether this HW is initialized
     * 
     * @return true if this HW's epoch is 0 or more
     */
    public boolean isInitialized()
    {
        return highWaterEpoch >= 0;
    }

    /**
     * Compare ourselves to what is passed in.<br>
     * Two uninitialized HW will be equal<br>
     * An uninitialized HW is always older than an initialized one Epochs
     * comparison is made first, then the offset. Session Ids, if any are
     * ignored
     * 
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    public int compareTo(HighWaterResource o)
    {
        // Two uninitialized HW are equal
        if (!this.isInitialized() && !o.isInitialized())
            return 0;
        // An uninitialized HW is always the oldest
        if (!this.isInitialized())
            return -1;
        if (!o.isInitialized())
            return 1;

        if (this.highWaterEpoch > o.getHighWaterEpoch())
            return 1;
        else if (this.highWaterEpoch < o.getHighWaterEpoch())
            return -1;
        else
        {
            String oToCompare = o.getHighWaterEventId();
            String thisToCompare = this.highWaterEventId;
            int sessionDelimiter;

            if ((sessionDelimiter = o.highWaterEventId
                    .indexOf(SESSION_DELIMITER)) != -1)
            {
                oToCompare = o.getHighWaterEventId().substring(0,
                        sessionDelimiter);
            }
            else
            {
                oToCompare = o.getHighWaterEventId();
            }
            if ((sessionDelimiter = this.highWaterEventId
                    .indexOf(SESSION_DELIMITER)) != -1)
            {
                thisToCompare = this.highWaterEventId.substring(0,
                        sessionDelimiter);
            }
            else
            {
                thisToCompare = this.highWaterEventId;
            }

            if (oToCompare.length() == 0 && thisToCompare.length() > 0)
            {
                return 1;
            }
            else if (thisToCompare.length() == 0 && oToCompare.length() > 0)
            {
                return -1;
            }

            return (thisToCompare.compareTo(oToCompare));

        }
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null || !(obj instanceof HighWaterResource))
            return false;
        return this.compareTo((HighWaterResource) obj) == 0;
    }

    public void update(long epoch, String eventId)
    {
        this.highWaterEpoch = epoch;
        this.highWaterEventId = eventId;
    }

    public String toString()
    {
        return String.format("%d(%s)", highWaterEpoch, highWaterEventId);
    }

    public static String getSessionId(String eventId)
    {
        int sessionDelimiter;

        if ((sessionDelimiter = eventId.indexOf(SESSION_DELIMITER)) != -1)
        {
            return eventId.substring(sessionDelimiter + 1);
        }

        return null;
    }

    public long getHighWaterEpoch()
    {
        return highWaterEpoch;
    }

    public void setHighWaterEpoch(long highWaterEpoch)
    {
        this.highWaterEpoch = highWaterEpoch;
    }

    public String getHighWaterEventId()
    {
        return highWaterEventId;
    }

    public void setHighWaterEventId(String highWaterEventId)
    {
        this.highWaterEventId = highWaterEventId;
    }

}
