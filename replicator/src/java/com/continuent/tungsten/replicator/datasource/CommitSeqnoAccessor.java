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
 * Denotes an accessor for stage tasks to update metadata in the CommitSeqno
 * table. This performs operations that may need to be integrated with
 * transactions.
 */
public interface CommitSeqnoAccessor
{
    /**
     * Set the task ID for this accessor.
     */
    public void setTaskId(int taskId);

    /**
     * Prepare for use. This method is assumed to allocate any required
     * resources
     * 
     * @throws ReplicatorException Thrown if resource allocation fails
     */
    public void prepare() throws ReplicatorException, InterruptedException;

    /**
     * Release all resources. Clients must call this to avoid resource leaks.
     * 
     * @throws ReplicatorException Thrown if resource deallocation fails
     */
    public void close() throws ReplicatorException, InterruptedException;

    /**
     * Updates the last committed seqno for a single channel. This is a client
     * call used by appliers to mark the restart position.
     */
    public void updateLastCommitSeqno(ReplDBMSHeader header, long appliedLatency)
            throws ReplicatorException, InterruptedException;

    /**
     * Fetches header data for last committed transaction for a particular
     * channel. This is a client call to get the restart position.
     */
    public ReplDBMSHeader lastCommitSeqno() throws ReplicatorException,
            InterruptedException;
}