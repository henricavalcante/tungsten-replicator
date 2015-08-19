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
 * Contributor(s): Stephane Giron
 */

package com.continuent.tungsten.replicator.datasource;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;

/**
 * Manages commit sequence numbers using files.
 */
public class FileCommitSeqnoAccessor implements CommitSeqnoAccessor
{
    // Properties.
    private FileCommitSeqno commitSeqno;
    private int             taskId;

    /** Create a new instance. */
    public FileCommitSeqnoAccessor(FileCommitSeqno commitSeqno)
    {
        this.commitSeqno = commitSeqno;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.CommitSeqnoAccessor#setTaskId(int)
     */
    public void setTaskId(int taskId)
    {
        this.taskId = taskId;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.CommitSeqnoAccessor#prepare()
     */
    public void prepare() throws ReplicatorException, InterruptedException
    {
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.CommitSeqnoAccessor#close()
     */
    public void close()
    {
    }

    /**
     * Updates the last committed seqno for a single channel. This is a client
     * call used by appliers to mark the restart position.
     */
    public void updateLastCommitSeqno(ReplDBMSHeader header, long appliedLatency)
            throws ReplicatorException
    {
        String fname = commitSeqno.getPrefix() + "." + taskId;
        commitSeqno.store(fname, header, appliedLatency, true);
    }

    /**
     * Fetches header data for last committed transaction for a particular
     * channel. This is a client call to get the restart position.
     */
    public ReplDBMSHeader lastCommitSeqno() throws ReplicatorException
    {
        String fname = commitSeqno.getPrefix() + "." + taskId;
        return commitSeqno.retrieve(fname);
    }
}