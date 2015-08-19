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

import java.io.Serializable;

public class OSCommandResult implements Serializable
{
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private String            stdout           = null;
    private String            stderr           = null;
    private int               exitCode         = -1;

    public OSCommandResult(String stdout, String stderr, int exitCode)
    {
        this.stdout = stdout;
        this.stderr = stderr;
        this.exitCode = exitCode;
    }

    public int getExitCode()
    {
        return exitCode;
    }

    public void setExitCode(int exitCode)
    {
        this.exitCode = exitCode;
    }

    public String getStdout()
    {
        return stdout;
    }

    public void setStdout(String stdout)
    {
        this.stdout = stdout;
    }

    public String getStderr()
    {
        return stderr;
    }

    public void setStderr(String stderr)
    {
        this.stderr = stderr;
    }

    public String getMessages()
    {
        StringBuilder builder = new StringBuilder();

        if (stdout != null)
        {
            builder.append(stdout);
        }

        if (stderr != null && stderr.length() > 0)
        {
            if (builder.length() != 0)
                builder.append("\n");

            builder.append(stderr);
        }

        return builder.toString();
    }

    public String toString()
    {
        return String.format("Exit code=%d\n%s", exitCode, getMessages());
    }

}
