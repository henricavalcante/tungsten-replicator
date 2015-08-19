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

package com.continuent.tungsten.common.file;

import java.util.LinkedList;
import java.util.List;

/**
 * Implements a class to manipulate generic file system path structures that can
 * apply to any type of file system-like structure including Linux, Hadoop, and
 * Zookeeper.
 */
public class FilePath
{
    private LinkedList<String> elements = new LinkedList<String>();
    private boolean            absolute;

    /** Construct a path with no arguments, which creates a root path. */
    public FilePath()
    {
        this("/");
    }

    /** Constructs a path from a string, which may be a full or partial path. */
    public FilePath(String path)
    {
        path = path.trim();
        if (path.startsWith("/"))
            absolute = true;
        else
            absolute = false;
        append(path);
    }

    /**
     * Constructs a path from a list of elements and a flag to indicate whether
     * it is absolute.
     */
    public FilePath(List<String> base, boolean absolute)
    {
        this.elements = new LinkedList<String>(base);
        this.absolute = absolute;
    }

    /** Constructs a path from an existing path. */
    public FilePath(FilePath base)
    {
        this(base.elements, base.isAbsolute());
    }

    /** Constructs a path constructed from an existing path plus a string. */
    public FilePath(FilePath base, String subpath)
    {
        this(base);
        append(subpath);
    }

    /** Returns the elements that compose the path. */
    public List<String> elements()
    {
        return elements;
    }

    /** Returns the i'th element in the path. */
    public String element(int i)
    {
        return elements.get(i);
    }

    /**
     * Returns true if this is an absolute path.
     */
    public boolean isAbsolute()
    {
        return absolute;
    }

    /**
     * Appends a string, which may consist of one or more elements.
     */
    public FilePath append(String subPath)
    {
        String[] subElements = subPath.trim().split("\\/");
        for (String subElement : subElements)
        {
            appendSub(subElement);
        }
        return this;
    }

    /** Appends another path to end of this one. */
    public FilePath append(FilePath subPath)
    {
        return append(subPath.elements());
    }

    /** Appends 0 or more path elements to the end. */
    public FilePath append(List<String> subElements)
    {
        for (String subElement : subElements)
        {
            appendSub(subElement);
        }
        return this;
    }

    // Append a subsequent single element to an existing path.
    private void appendSub(String subElement)
    {
        if ("".equals(subElement) || ".".equals(subElement))
        {
            // Empty elements and "." add nothing to path.
        }
        else if ("..".equals(subElement))
        {
            // A .. moves us up a level by removing the last element on an
            // absolute path. For relative paths we just add it.
            if (absolute)
            {
                if (elements.size() > 0)
                {
                    elements.removeLast();
                }
                else
                    throw new IllegalArgumentException(
                            "The .. operator may not step past top element in a absolute path");
            }
            else
            {
                elements.add(subElement);
            }
        }
        else
        {
            // Anything else gets added to the end.
            elements.add(subElement);
        }

    }

    /**
     * Returns a full path with leading "/" if absolute.
     * 
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        StringBuffer path = new StringBuffer();
        if (absolute)
            path.append("/");
        for (int i = 0; i < elements.size(); i++)
        {
            if (i > 0)
                path.append("/");
            path.append(elements.get(i));
        }
        return path.toString();
    }

    /**
     * Returns true if the argument is a FilePath instance and denotes the same
     * location. {@inheritDoc}
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    public boolean equals(Object other)
    {
        // Return false for obvious cases that cannot be true.
        if (other == null)
            return false;
        else if (!(other instanceof FilePath))
            return false;

        // Convert and test properties carefully.
        FilePath otherPath = (FilePath) other;
        if (absolute != otherPath.isAbsolute())
            return false;
        else if (elements.size() != otherPath.elements().size())
            return false;

        for (int i = 0; i < elements.size(); i++)
        {
            String myElement = elements.get(i);
            String otherElement = otherPath.element(i);
            if (!myElement.equals(otherElement))
                return false;
        }

        // If we got here we must be equal.
        return true;
    }
}