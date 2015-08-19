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
 * Implements a request to execute a script method.
 */
public class ScriptMethodRequest
{
    private final String method;
    private final Object argument;

    /**
     * Creates a new method invocation request.
     * 
     * @param method The name of the method we want to execute
     * @param argument An argument to that method or null if there is none
     */
    public ScriptMethodRequest(String method, Object argument)
    {
        this.method = method;
        this.argument = argument;
    }

    public String getMethod()
    {
        return method;
    }

    public Object getArgument()
    {
        return argument;
    }
}