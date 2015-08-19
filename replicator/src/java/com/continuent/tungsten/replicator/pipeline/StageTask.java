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

package com.continuent.tungsten.replicator.pipeline;

import java.util.SortedSet;

import com.continuent.tungsten.fsm.event.EventDispatcher;

/**
 * Denotes a Runnable task that implements stage processing.  Set methods are
 * called before the run() method. 
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface StageTask extends Runnable
{
    /**
     * Provides the stage instance that this runnable operates on.
     */
    public void setStage(Stage stage);

    /**
     * Provides an event dispatcher to log error.
     */
    public void setEventDispatcher(EventDispatcher eventDispatcher);

    /** 
     * Sets number of apply operations to skip at start-up. 
     */
    public void setApplySkipCount(long applySkipCount);

    /**
     * Sets the schedule, which controls task cancellation. 
     */
    public void setSchedule(Schedule schedule);

    public void setApplySkipSeqnos(SortedSet<Long> seqnosToBeSkipped);
}