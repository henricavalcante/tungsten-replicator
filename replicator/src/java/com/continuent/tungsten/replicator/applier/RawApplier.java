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
 * Contributor(s): Robert Hodges, Stephane Giron
 */

package com.continuent.tungsten.replicator.applier;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.consistency.ConsistencyException;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.plugin.ReplicatorPlugin;

/**
 * Denotes an applier, which is responsible for applying raw DBMS events to a
 * database or other replication target. Appliers must be prepared to be
 * interrupted, which mechanism is used to cancel processing.
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public interface RawApplier extends ReplicatorPlugin
{
    /**
     * Sets the ID of the task using this raw applier.
     * 
     * @param id Task ID
     */
    public void setTaskId(int id);

    /**
     * Apply the proffered event to the replication target.
     * 
     * @param event Event to be applied. If a DBMSEmptyEvent, just mark the
     *            apply position.
     * @param header Header data corresponding to event
     * @param doCommit Boolean flag indicating whether this is the last part of
     *            multi-part event
     * @param doRollback Boolean flag indicating whether this transaction should
     *            rollback
     * @throws ReplicatorException Thrown if applier processing fails
     * @throws ConsistencyException Thrown if the applier detects that a
     *             consistency check has failed
     * @throws InterruptedException Thrown if the applier is interrupted
     */
    public void apply(DBMSEvent event, ReplDBMSHeader header, boolean doCommit,
            boolean doRollback) throws ReplicatorException,
            ConsistencyException, InterruptedException;

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
     * @throws InterruptedException Thrown if the applier is interrupted.
     */
    public void rollback() throws InterruptedException;

    /**
     * Return header information corresponding to last committed event.
     * 
     * @return Header data for last committed event.
     * @throws ReplicatorException Thrown if getting sequence number fails
     * @throws InterruptedException Thrown if the applier is interrupted
     */
    public ReplDBMSHeader getLastEvent() throws ReplicatorException,
            InterruptedException;
}
