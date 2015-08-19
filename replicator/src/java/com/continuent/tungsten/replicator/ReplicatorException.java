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

package com.continuent.tungsten.replicator;

/**
 * This class defines a ReplicatorException, a parent for all other exceptions
 * 
 * @author <a href="mailto:alexey.yurchenko@continuent.com">Alex Yurchenko</a>
 * @version 1.0
 */
public class ReplicatorException extends Exception
{

    /**
     * 
     */
    private static final long serialVersionUID     = -2849591301389282829L;

    private String            originalErrorMessage = null;
    private String            extraData            = null;

    /**
     * Creates a new <code>ReplicatorException</code> object
     */
    public ReplicatorException()
    {
        super();
    }

    /**
     * Creates a new <code>ReplicatorException</code> object
     * 
     * @param arg0
     */
    public ReplicatorException(String arg0)
    {
        super(arg0);
    }

    /**
     * Creates a new <code>ReplicatorException</code> object
     * 
     * @param arg0
     */
    public ReplicatorException(Throwable arg0)
    {
        super(arg0);
        if (arg0 instanceof ReplicatorException)
        {
            ReplicatorException exc = (ReplicatorException) arg0;
            this.extraData = exc.extraData;
            this.originalErrorMessage = exc.originalErrorMessage;
        }
    }

    /**
     * Creates a new <code>ReplicatorException</code> object
     * 
     * @param arg0
     * @param arg1
     */
    public ReplicatorException(String arg0, Throwable arg1)
    {
        super(arg0, arg1);
        if (arg1 instanceof ReplicatorException)
        {
            ReplicatorException exc = (ReplicatorException) arg1;
            this.extraData = exc.extraData;
            this.originalErrorMessage = exc.originalErrorMessage;
        }
        else 
            this.originalErrorMessage = arg0;
    }

    public void setOriginalErrorMessage(String originalErrorMessage)
    {
        this.originalErrorMessage = originalErrorMessage;
    }
    

    public String getOriginalErrorMessage()
    {
        return originalErrorMessage;
    }

    public String getExtraData()
    {
        return extraData;
    }

    public void setExtraData(String extraData)
    {
        this.extraData = extraData;
    }
}
