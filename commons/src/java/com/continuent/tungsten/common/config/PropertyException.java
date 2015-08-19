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

package com.continuent.tungsten.common.config;

/**
 * Represents an error that occurs during property processing such as a datatype
 * violation or a missing required value.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class PropertyException extends RuntimeException
{
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new exception with a message.
     */
    public PropertyException(String msg)
    {
        super(msg);
    }

    /**
     * Creates a new exception with a message and underlying exception.
     */
    public PropertyException(String msg, Throwable t)
    {
        super(msg, t);
    }
}