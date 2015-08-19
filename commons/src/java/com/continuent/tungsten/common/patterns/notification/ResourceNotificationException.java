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
 * Initial developer(s): Edward Archibald
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.common.patterns.notification;

/**
 * This class defines a ResourceNotificationException
 * 
 * @author <a href="mailto:edward.archibald@continuent.com">Edward Archibald</a>
 * @version 1.0
 */
public class ResourceNotificationException extends Exception
{
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new <code>ResourceNotificationException</code> object
     */
    public ResourceNotificationException()
    {
       
    }

    /**
     * Creates a new <code>ResourceNotificationException</code> object
     * 
     * @param message
     */
    public ResourceNotificationException(String message)
    {
        super(message);
       
    }

    /**
     * Creates a new <code>ResourceNotificationException</code> object
     * 
     * @param cause
     */
    public ResourceNotificationException(Throwable cause)
    {
        super(cause);
       
    }

    /**
     * Creates a new <code>ResourceNotificationException</code> object
     * 
     * @param message
     * @param cause
     */
    public ResourceNotificationException(String message, Throwable cause)
    {
        super(message, cause);
       
    }

}
