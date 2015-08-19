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
 * Provides a response from a task executor that can be used to assess whether
 * the task completed successfully.
 */
public class ScriptExecutorTaskStatus
{
    private final int                  count;
    private final boolean              successful;
    private final ScriptMethodResponse failedResponse;

    /**
     * Creates a new <code>ScriptExecutorTaskResponse</code> object
     * 
     * @param count Count of requests processed
     * @param successful If true, all requests were successful
     * @param failedResponse Filled in with failing response if the last request
     *            was unsuccessful
     */
    public ScriptExecutorTaskStatus(int count, boolean successful,
            ScriptMethodResponse failedResponse)
    {
        this.count = count;
        this.successful = successful;
        this.failedResponse = failedResponse;
    }

    public int getCount()
    {
        return count;
    }

    public boolean isSuccessful()
    {
        return successful;
    }

    public ScriptMethodResponse getFailedResponse()
    {
        return failedResponse;
    }
}