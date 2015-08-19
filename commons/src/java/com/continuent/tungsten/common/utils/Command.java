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
 * Contributor(s):
 */

package com.continuent.tungsten.common.utils;

import java.util.Vector;

public class Command
{
    public static final int FLAG_RECURSIVE    = 0;
    public static final int FLAG_LONG         = 1;
    public static final int FLAG_BACKGROUND   = 2;
    public static final int FLAG_REDIRECT_OUT = 3;
    public static final int FLAG_REDIRECT_IN  = 4;
    public static final int FLAG_ABSOLUTE     = 5;
    public static final int FLAG_PARENTS      = 6;
    public static final int FLAG_COUNT        = FLAG_PARENTS + 1;

    private boolean[]       flags             = new boolean[FLAG_COUNT];

    private String          commandLine       = null;
    Vector<String>          processedTokens   = new Vector<String>();
    private String          output            = null;
    private String          input             = null;

    public Command(String commandLine)
    {
        this.commandLine = commandLine;
    }

    public void addToken(String token)
    {
        this.processedTokens.add(token);
    }

    public void setIsBackground(boolean isBackground)
    {
        flags[FLAG_BACKGROUND] = isBackground;
    }

    public boolean isBackground()
    {
        return flags[FLAG_BACKGROUND];
    }

    public void setIsRecursive(boolean isRecursive)
    {
        flags[FLAG_RECURSIVE] = isRecursive;
    }

    public boolean isRecursive()
    {
        return flags[FLAG_RECURSIVE];
    }

    public void setIsAbsolute(boolean isAbsolute)
    {
        flags[FLAG_ABSOLUTE] = isAbsolute;
    }

    public boolean isAbsolute()
    {
        return flags[FLAG_ABSOLUTE];
    }

    public void setIncludeParents(boolean includeParents)
    {
        flags[FLAG_PARENTS] = includeParents;
    }

    public boolean includeParents()
    {
        return flags[FLAG_PARENTS];
    }

    public void setIsLong(boolean isLong)
    {
        flags[FLAG_LONG] = isLong;
    }

    public boolean isLong()
    {
        return flags[FLAG_LONG];
    }

    public void setIsRedirectOutput(boolean isRedirectOutput, String output)
    {
        flags[FLAG_REDIRECT_OUT] = true;
        this.output = output;
    }

    public boolean isRedirectOutput()
    {
        return flags[FLAG_REDIRECT_OUT];
    }

    public void setIsRedirectInput(boolean isRedirectInput, String input)
    {
        flags[FLAG_REDIRECT_IN] = true;
        this.input = input;
    }

    public boolean isRedirectInput()
    {
        return flags[FLAG_REDIRECT_IN];
    }

    public void clear()
    {
        commandLine = null;
        processedTokens = null;
        input = null;
        output = null;

        for (int i = 0; i < FLAG_COUNT; i++)
            flags[i] = false;
    }

    public String getCommandLine()
    {
        return commandLine;
    }

    public void setCommandLine(String commandLine)
    {
        this.commandLine = commandLine;
    }

    public String getOutput()
    {
        return output;
    }

    public void setOutput(String output)
    {
        this.output = output;
    }

    public String getInput()
    {
        return input;
    }

    public void setInput(String input)
    {
        this.input = input;
    }

    public String[] getTokens()
    {
        if (processedTokens.size() > 0)
            return processedTokens.toArray(new String[processedTokens.size()]);

        return null;
    }

    public String toString()
    {
        return commandLine;
    }
}
