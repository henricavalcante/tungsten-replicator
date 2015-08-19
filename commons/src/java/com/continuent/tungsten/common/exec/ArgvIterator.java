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
 * Contributor(s): Linas Virbalas
 */

package com.continuent.tungsten.common.exec;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Simple iterator class for argv arrays.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ArgvIterator implements Iterator<String>
{
    String argv[];
    int    index;

    public ArgvIterator(String[] argv)
    {
        this.argv = argv;
        index = 0;
    }

    public ArgvIterator(String[] argv, int index)
    {
        this.argv = argv;
        this.index = index;
    }

    public boolean hasNext()
    {
        return (index < argv.length);
    }

    public String next()
    {
        return argv[index++];
    }

    public void remove()
    {
        // Do nothing.
    }
    
    public String peek()
    {
        if (hasNext())
            return argv[index];
        else
            return null;
    }

    public boolean contains(String arg)
    {
        return Arrays.asList(this.argv).contains(arg);
    }
}