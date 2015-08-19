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

package com.continuent.tungsten.common.jmx;

/**
 * Denotes an unexpected error in server processing. The current operation
 * cannot continue.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ServerRuntimeException extends RuntimeException
{
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new <code>ServerRuntimeException</code> object
     * 
     * @param msg Message describing the problem
     */
    public ServerRuntimeException(String msg)
    {
        super(msg);
    }

    /**
     * Creates a new <code>ServerRuntimeException</code> object
     * 
     * @param msg Message describing the problem
     * @param cause Root cause of the exception
     */
    public ServerRuntimeException(String msg, Throwable cause)
    {
        super(msg, cause);
    }
}
