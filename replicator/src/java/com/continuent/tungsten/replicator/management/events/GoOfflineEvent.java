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
 * Initial developer(s): Seppo Jaakola
 * Contributor(s): Robert Hodges
 */
package com.continuent.tungsten.replicator.management.events;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.fsm.core.Event;

/**
 * Signals that the replicator should move to the off-line state. This event may
 * be submitted by underlying code to initiate a controlled shutdown.
 */
public class GoOfflineEvent extends Event
{
    private TungstenProperties params;

    public GoOfflineEvent()
    {
        this(new TungstenProperties());
    }

    public GoOfflineEvent(TungstenProperties params)
    {
        super(null);
        this.params = params;
    }

    public TungstenProperties getParams()
    {
        return params;
    }
}
