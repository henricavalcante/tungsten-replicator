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
 * Contributor(s): Stephane Giron, Linas Virbalas
 */

package com.continuent.tungsten.replicator.datasource;

import java.util.List;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;

/**
 * Denotes a catalog table that remembers the current replicator commit position
 * and retrieves it on demand. Here are the rules for use of implementations
 * <p>
 * <ul>
 * <li>Master - Masters update trep_commit_seqno whenever an extracted event is
 * stored.</li>
 * <li>Slave - Slaves update trep_commit_seqno whenever an event is applied</li>
 * </ul>
 * This interface is a generalization of the CommitSeqnoTable class.
 */
public interface CommitSeqno extends CatalogEntity
{
    /**
     * Set the number of channels to track. This is the basic mechanism to
     * support parallel replication.
     */
    public void setChannels(int channels);

    /**
     * Copies the single task 0 row left by a clean offline operation to add
     * rows for each task in multi-channel operation. This fails if task 0 does
     * not exist.
     * 
     * @throws ReplicatorException Thrown if the task ID 0 does not exist
     * @throws InterruptedException
     */
    public void expandTasks() throws ReplicatorException, InterruptedException;

    /**
     * Reduces the trep_commit_seqno table to task 0 entry *provided* there is a
     * task 0 row and provide all rows are at the same sequence number. This
     * operation allows the table to convert to a different number of apply
     * threads.
     */
    public boolean reduceTasks() throws ReplicatorException,
            InterruptedException;

    /**
     * Returns the header for the lowest committed sequence number or null if
     * none such can be found.
     */
    public ReplDBMSHeader minCommitSeqno() throws ReplicatorException,
            InterruptedException;

    /**
     * Returns the header for the highest committed sequence number or null if
     * none such can be found.
     */
    public ReplDBMSHeader maxCommitSeqno() throws ReplicatorException,
            InterruptedException;
    
    /**
     * Returns all available position headers.
     */
    public List<ReplDBMSHeader> getHeaders() throws ReplicatorException,
            InterruptedException;

    /**
     * Set position for task ID zero. If there are no tasks, single one with ID
     * zero will be created. If there are one or mare (in case of parallel
     * replication) tasks, setting will throw an exception and suggest doing a
     * reset first.
     */
    public void initPosition(long seqno, String sourceId, long epoch,
            String eventId) throws ReplicatorException, InterruptedException;

    /**
     * Returns an accessor suitable for performing operations for a particular
     * task ID.
     * 
     * @param taskId The stage task ID
     * @param conn A connection to the data source
     */
    public CommitSeqnoAccessor createAccessor(int taskId,
            UniversalConnection conn) throws ReplicatorException,
            InterruptedException;
}