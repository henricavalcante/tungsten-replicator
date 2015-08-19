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

package com.continuent.tungsten.replicator.applier;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.consistency.ConsistencyException;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.plugin.ReplicatorPlugin;

/**
 * Denotes an applier that can process events with full metadata.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @see com.continuent.tungsten.replicator.applier.RawApplier
 */
public interface Applier extends ReplicatorPlugin
{
    /**
     * Apply the proffered event to the replication target.
     * 
     * @param event Event to be applied
     * @param doCommit Boolean flag indicating whether this is the last part of
     *            multipart event
     * @param doRollback Boolean flag indicating whether this transaction should
     *            rollback
     * @param syncTHL Should this applier synchronize the trep_commit_seqno
     *            table? This should be false for slave.
     * @throws ReplicatorException Thrown if applier processing fails
     * @throws ConsistencyException Thrown if the applier detects that a
     *             consistency check has failed
     * @throws InterruptedException Thrown if the applier is interrupted
     */
    public void apply(ReplDBMSEvent event, boolean doCommit,
            boolean doRollback, boolean syncTHL) throws ReplicatorException,
            ConsistencyException, InterruptedException;

    /**
     * Update current recovery position but do not apply an event.
     * 
     * @param header Header containing seqno, event ID, etc.
     * @param doCommit Boolean flag indicating whether this is the last part of
     *            multipart event
     * @param syncTHL Should this applier synchronize the trep_commit_seqno
     *            table? This should be false for slave.
     * @throws ReplicatorException Thrown if applier processing fails
     * @throws InterruptedException Thrown if the applier is interrupted
     */
    public void updatePosition(ReplDBMSHeader header, boolean doCommit,
            boolean syncTHL) throws ReplicatorException, InterruptedException;

    /**
     * Commits current open transaction to ensure data applied up to current
     * point are durable.
     * 
     * @throws ReplicatorException Thrown if applier processing fails
     * @throws InterruptedException Thrown if the applier is interrupted
     */
    public void commit() throws ReplicatorException, InterruptedException;

    /**
     * Rolls back any current work.
     * 
     * @throws InterruptedException
     */
    public void rollback() throws InterruptedException;

    /**
     * Return header information corresponding to last committed transaction.
     * 
     * @return Header data for last committed transaction
     * @throws ReplicatorException Thrown if getting sequence number fails
     * @throws InterruptedException Thrown if the applier is interrupted
     */
    public ReplDBMSHeader getLastEvent() throws ReplicatorException,
            InterruptedException;

}
