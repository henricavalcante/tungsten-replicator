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
 * Implements a WatchPredicate to identify that a particular sequence number has
 * been reached. This returns true for any sequence number equal to
 * <em>or higher than</em> the number we are seeking.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class SeqnoWatchPredicate implements WatchPredicate<ReplDBMSHeader>
{
    private final long seqno;

    public SeqnoWatchPredicate(long seqno)
    {
        this.seqno = seqno;
    }

    /**
     * Return true if the sequence number is greater than or equal to what we
     * are seeking. Note: If the event is the same, we must ensure it is the
     * last fragment.
     * 
     * @see com.continuent.tungsten.replicator.util.WatchPredicate#match(java.lang.Object)
     */
    public boolean match(ReplDBMSHeader event)
    {
        if (event == null)
            return false;
        else if (event.getSeqno() > seqno)
            return true;
        else if (event.getSeqno() == seqno && event.getLastFrag())
            return true;
        else
            return false;
    }

    public long getSeqno()
    {
        return seqno;
    }

    /**
     * Returns the class name and the seqno for which we waiting. 
     * 
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        return this.getClass().getSimpleName() + " seqno=" + seqno;
    }
}