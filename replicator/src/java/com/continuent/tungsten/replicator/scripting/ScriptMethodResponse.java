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

package com.continuent.tungsten.replicator.scripting;

/**
 * Implements a response from script method execution. This includes the
 * original request as well as the return value if successful or the throwable
 * if not.
 */
public class ScriptMethodResponse
{
    private final ScriptMethodRequest request;
    private final Object              value;
    private final Throwable           throwable;
    private final boolean             successful;

    /**
     * Creates a new script method invocation response.
     * 
     * @param request The request to which this response applies
     * @param value The return value or null if there is no return value
     * @param throwable The exception resulting from a failed execution
     * @param successful If true the invocation completed normally, otherwise
     *            false in which case we also return the exception
     */
    public ScriptMethodResponse(ScriptMethodRequest request, Object value,
            Throwable throwable, boolean successful)
    {
        this.request = request;
        this.value = value;
        this.throwable = throwable;
        this.successful = successful;
    }

    public ScriptMethodRequest getRequest()
    {
        return request;
    }

    public Object getValue()
    {
        return value;
    }

    public Throwable getThrowable()
    {
        return throwable;
    }

    public boolean isSuccessful()
    {
        return successful;
    }
}