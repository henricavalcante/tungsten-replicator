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

package com.continuent.tungsten.replicator.extractor.mysql;

import java.io.File;

/**
 * Simple class to track the relay log position using synchronized methods to
 * ensure the file and offset are always updated consistently.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class RelayLogPosition
{
    protected File curFile;
    protected long curOffset;

    public RelayLogPosition()
    {
    }

    public synchronized void setPosition(File file, long offset)
    {
        this.curFile = file;
        this.curOffset = offset;
    }

    public synchronized void setOffset(int offset)
    {
        this.curOffset = offset;
    }
    
    public synchronized File getFile()
    {
        return curFile;
    }

    public synchronized long getOffset()
    {
        return curOffset;
    }

    /**
     * Return a consistent clone of this position.
     */
    public synchronized RelayLogPosition clone()
    {
        RelayLogPosition clone = new RelayLogPosition();
        clone.setPosition(curFile, curOffset);
        return clone;
    }

    /**
     * Return true if we have reached a desired file:offset position. 
     */
    public synchronized boolean hasReached(String fileName, long offset)
    {
        if (curFile == null)
            return false;
        else if (curFile.getName().compareTo(fileName) < 0)
        {
            // Our file name is greater, position has not been reached. 
            return false;
        }
        else if (curFile.getName().compareTo(fileName) == 0)
        {
            // Our file name is the same, we must compare the offset. 
            if (offset > curOffset)
                return false;
            else 
                return true;
        }
        else 
        {
            // Our file name is less.  We have reached the position. 
            return true;
        }
    }
    
    /**
     * Return a string representation of the position. 
     */
    public synchronized String toString()
    {
        if (curFile == null)
            return "(no position set)";
        else
            return curFile.getName() + ":" + curOffset;
    }
}