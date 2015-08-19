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
 * Initial developer(s): Alex Yurchenko
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.replicator.event;

import java.io.Serializable;

/**
 * This class is the superclass from which all replication events inherit. It
 * defines minimal shared behavior. This is currently restricted to providing a
 * common serialization interface and estimated size to help with memory
 * management. Estimated size is a hint and does not have to be exact. It is
 * designed to help us tell whether the object in question needs a lot of heap
 * memory.
 * 
 * @author <a href="mailto:alexey.yurchenko@continuent.com">Alex Yurchenko</a>
 * @version 1.0
 */
public abstract class ReplEvent implements Serializable
{
    private static final long serialVersionUID = 1300;
    private transient int     estimatedSize;

    public ReplEvent()
    {
    }

    /** 
     * Returns the sequence number of this event. 
     */
    public abstract long getSeqno();
    
    /**
     * Returns the estimated serialized size of this event, if known.
     */
    public int getEstimatedSize()
    {
        return estimatedSize;
    }

    /**
     * Sets the estimated serialized size of this event.
     */
    public void setEstimatedSize(int estimatedSize)
    {
        this.estimatedSize = estimatedSize;
    }
}
