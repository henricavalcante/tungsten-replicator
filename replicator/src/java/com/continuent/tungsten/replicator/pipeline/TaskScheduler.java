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

import com.continuent.tungsten.replicator.event.ReplDBMSHeader;

/**
 * Denotes a class that provides scheduling information for stage tasks.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface TaskScheduler
{
    /**
     * Controls whether stage task should continue processing events. This call
     * may do of the following:
     * <ul>
     * <li>Return true in which case stage should proceed to handle the next
     * event.
     * <li>Return false in which case the stage should exit.
     * <li>Pause (hang), then return one of the preceeding statuses
     * </ul>
     * The final case is how we implement a pause operation on stage tasks.
     * 
     * @param lastEvent Last event processed by this task
     */
    public boolean proceed(ReplDBMSHeader lastEvent);
}