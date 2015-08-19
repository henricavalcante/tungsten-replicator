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

package com.continuent.tungsten.common.directory;

import java.io.Serializable;

public class DirectorySession implements Serializable
{
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    Directory                 parent           = null;
    String                    sessionID        = null;
    ResourceNode              currentNode      = null;
    private long              timeCreated      = 0L;
    private long              lastTimeAccessed = 0L;

    public DirectorySession(Directory parent, String sessionID,
            ResourceNode currentNode)
    {
        this.parent = parent;
        this.sessionID = sessionID;
        this.currentNode = currentNode;
        this.timeCreated = System.currentTimeMillis();
        this.lastTimeAccessed = this.timeCreated;
    }

    /**
     * @return the parent
     */
    public Directory getParent()
    {
        return parent;
    }

    /**
     * @param parent the parent to set
     */
    public void setParent(Directory parent)
    {
        this.parent = parent;
    }

    /**
     * @return the sessionID
     */
    public String getSessionID()
    {
        return sessionID;
    }

    /**
     * @param sessionID the sessionID to set
     */
    public void setSessionID(String sessionID)
    {
        this.sessionID = sessionID;
    }

    /**
     * @return the currentNode
     */
    public ResourceNode getCurrentNode()
    {
        return currentNode;
    }

    /**
     * @param currentNode the currentNode to set
     */
    public void setCurrentNode(ResourceNode currentNode)
    {
        this.currentNode = currentNode;
    }

    public String toString()
    {
        return "sessionID=" + getSessionID();
    }

    public long getTimeCreated()
    {
        return timeCreated;
    }

    public void setTimeCreated(long timeCreated)
    {
        this.timeCreated = timeCreated;
    }

    public long getLastTimeAccessed()
    {
        return lastTimeAccessed;
    }

    public void setLastTimeAccessed(long lastTimeAccessed)
    {
        this.lastTimeAccessed = lastTimeAccessed;
    }

}
