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
 */
package com.continuent.tungsten.replicator.management;

import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * Defines a non-fatal exception that occurred during replicator state machine
 * processing. 
 * 
 * @author <a href="mailto:jussi-pekka.kurikka@continuent.com">Jussi-Pekka Kurikka</a>
 * @version 1.0
 */
public class ReplicatorStateException extends ReplicatorException
{
    private static final long serialVersionUID = 1L;
    
    /**
     * Creates a new <code>ReplicatorStateException</code> object
     * 
     * @param message Message suitable for display to clients
     */
    public ReplicatorStateException(String message)
    {
        super(message);
    }

    public ReplicatorStateException(String message, Throwable e)
    {
        super(message, e);
    }
}