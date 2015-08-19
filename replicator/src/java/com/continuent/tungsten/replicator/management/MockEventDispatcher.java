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

package com.continuent.tungsten.replicator.management;

import com.continuent.tungsten.fsm.core.Event;
import com.continuent.tungsten.fsm.event.EventCompletionListener;
import com.continuent.tungsten.fsm.event.EventDispatcher;
import com.continuent.tungsten.fsm.event.EventRequest;

/**
 * Dummy event dispatcher used for testing.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class MockEventDispatcher implements EventDispatcher
{
    @Override
    public void setListener(EventCompletionListener listener)
    {
        // Do nothing.
    }

    @Override
    public EventRequest put(Event event) throws InterruptedException
    {
        // Do nothing.
        return null;
    }

    @Override
    public EventRequest putOutOfBand(Event event) throws InterruptedException
    {
        // Do nothing.
        return null;
    }

    @Override
    public boolean cancelActive(EventRequest request,
            boolean mayInterruptIfRunning) throws InterruptedException
    {
        // Do nothing.
        return false;
    }
}
