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

package com.continuent.tungsten.replicator.thl.log;

/**
 * Maintains a cursor on a particular log file for reading or writing.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class LogCursor
{
    private LogFile logFile;
    private long    lastSeqno;
    private long    lastAccessMillis;
    private boolean loaned;
    private boolean rotateNext;

    /**
     * Create a new log connection.
     */
    public LogCursor(LogFile logFile, long lastSeqno)
    {
        this.logFile = logFile;
        this.lastSeqno = lastSeqno;
        this.lastAccessMillis = System.currentTimeMillis();
    }

    public LogFile getLogFile()
    {
        return logFile;
    }

    public long getLastSeqno()
    {
        return lastSeqno;
    }

    public void setLastSeqno(long lastSeqno)
    {
        this.lastSeqno = lastSeqno;
    }

    public long getLastAccessMillis()
    {
        return lastAccessMillis;
    }

    public void setLastAccessMillis(long lastAccessMillis)
    {
        this.lastAccessMillis = lastAccessMillis;
    }

    public boolean isLoaned()
    {
        return loaned;
    }

    public void setLoaned(boolean loaned)
    {
        this.loaned = loaned;
    }

    public boolean isRotateNext()
    {
        return rotateNext;
    }

    public void setRotateNext(boolean rotateNext)
    {
        this.rotateNext = rotateNext;
    }

    /**
     * Releases underlying log file.
     */
    public void release()
    {
        if (logFile != null)
        {
            logFile.close();
            logFile = null;
        }
    }
}