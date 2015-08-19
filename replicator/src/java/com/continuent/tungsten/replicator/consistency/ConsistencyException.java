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
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.consistency;

import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * This class defines a ConsistencyException
 * 
 * @author <a href="mailto:alexey.yurchenko@continuent.com">Alexey Yurchenko</a>
 * @version 1.0
 */
public class ConsistencyException extends ReplicatorException
{

    /**
     * 
     */
    private static final long serialVersionUID = 6105152751419283356L;

    /**
     * Creates a new <code>ConsistencyException</code> object
     * 
     */
    public ConsistencyException()
    {
        super();
    }

    /**
     * Creates a new <code>ConsistencyException</code> object
     * 
     * @param arg0
     */
    public ConsistencyException(String arg0)
    {
        super(arg0);
    }

    /**
     * Creates a new <code>ConsistencyException</code> object
     * 
     * @param arg0
     */
    public ConsistencyException(Throwable arg0)
    {
        super(arg0);
    }

    /**
     * Creates a new <code>ConsistencyException</code> object
     * 
     * @param arg0
     * @param arg1
     */
    public ConsistencyException(String arg0, Throwable arg1)
    {
        super(arg0, arg1);
    }

}
