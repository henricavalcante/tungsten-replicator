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
 * Initial developer(s): Scott Martin
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.database;

import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * 
 * This class defines a DatabaseException
 * 
 * @author <a href="mailto:scott.martin@continuent.com">Scott Martin</a>
 * @version 1.0
 */
public class DatabaseException extends ReplicatorException
{
    static final long serialVersionUID = 1L;
   
    /**
     * 
     * Creates a new <code>DatabaseException</code> object
     *
     */
    public DatabaseException()
    {
        super();
    }
    
    /**
     * 
     * Creates a new <code>DatabaseException</code> object
     * 
     * @param msg
     */
    public DatabaseException(String msg)
    {
        super(msg);
    }
    
    /**
     * 
     * Creates a new <code>DatabaseException</code> object
     * 
     * @param msg
     * @param cause
     */
    public DatabaseException(String msg, Throwable cause)
    {
        super(msg, cause);
    }
    
    /**
     * 
     * Creates a new <code>DatabaseException</code> object
     * 
     * @param cause
     */
    public DatabaseException(Throwable cause)
    {
        super(cause);
    }
    
}
