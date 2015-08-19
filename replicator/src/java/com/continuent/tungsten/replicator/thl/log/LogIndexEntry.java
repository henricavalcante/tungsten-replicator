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
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.replicator.thl.log;

/**
 * Implements a sortable index entry, where entries are sorted by sequence
 * number.
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class LogIndexEntry implements Comparable<LogIndexEntry>
{
    long   startSeqno;
    long   endSeqno;
    String fileName;

    /**
     * Creates a new <code>IndexEntry</code> object
     * 
     * @param startSeqno
     * @param fileName
     */
    public LogIndexEntry(long startSeqno, long endSeqno, String fileName)
    {
        this.startSeqno = startSeqno;
        this.endSeqno = endSeqno;
        this.fileName = fileName;
    }

    /** Returns true if the index entry contains this sequence number. */
    public boolean hasSeqno(long seqno)
    {
        return (seqno >= startSeqno && seqno <= endSeqno);
    }

    /**
     * Implementation required for Comparable so that we can sort entries.
     */
    public int compareTo(LogIndexEntry o)
    {
        if (this.startSeqno < o.startSeqno)
            return -1;
        else if (this.startSeqno == o.startSeqno)
            return 0;
        else
            return 1;
    }

    /** Returns true if the given seqno is in the file that this entry indexes. */
    public boolean contains(long seqno)
    {
        return startSeqno <= seqno && seqno <= endSeqno;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        return this.getClass().getSimpleName() + " " + fileName + "("
                + startSeqno + ":" + endSeqno + ")";
    }
}
