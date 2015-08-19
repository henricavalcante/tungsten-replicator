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
 * Initial developer(s):
 * Contributor(s): 
 */

package com.continuent.tungsten.common.utils;

public class ExecutionResult
{
    private ExecutionStatus status        = ExecutionStatus.UNDEFINED;
    private String          message       = null;
    private Exception       lastException = null;

    public ExecutionResult(ExecutionStatus status, Exception lastException,
            String message)
    {
        this.status = status;
        this.lastException = lastException;
        this.message = message;
    }

    public ExecutionResult(ExecutionStatus status)
    {
        this.status = status;
        this.lastException = null;
        this.message = null;
    }

    /**
     * Returns the message value.
     * 
     * @return Returns the message.
     */
    public synchronized String getMessage()
    {
        return message;
    }

    /**
     * Sets the message value.
     * 
     * @param message The message to set.
     */
    public synchronized void setMessage(String message)
    {
        this.message = message;
    }

    /**
     * Returns the lastException value.
     * 
     * @return Returns the lastException.
     */
    public synchronized Exception getLastException()
    {
        return lastException;
    }

    /**
     * Sets the lastException value.
     * 
     * @param lastException The lastException to set.
     */
    public synchronized void setLastException(Exception lastException)
    {
        this.lastException = lastException;
    }

    /**
     * Returns the status value.
     * 
     * @return Returns the status.
     */
    public synchronized ExecutionStatus getStatus()
    {
        return status;
    }

    /**
     * Sets the status value.
     * 
     * @param status The status to set.
     */
    public synchronized void setStatus(ExecutionStatus status)
    {
        this.status = status;
    }

    public String toString()
    {
        String ret = "status=" + status + ", message=" + message;

        if (lastException != null)
        {
            ret += ", exception=" + lastException;
        }

        return ret;
    }

}
