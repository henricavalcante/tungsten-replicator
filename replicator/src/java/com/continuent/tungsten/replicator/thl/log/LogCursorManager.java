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

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Map;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.thl.THLException;

/**
 * Encapsulates management of connections and their log cursors. Log cursors are
 * a position in a particular log file and can only move in a forward direction.
 * If clients move backward in the log we need to allocated a new cursor.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class LogCursorManager
{
    private static Logger                 logger           = Logger.getLogger(LogCursorManager.class);

    // Map of active cursors and flag to indicate we are closed for
    // business.
    private Map<LogConnection, LogCursor> cursors          = new Hashtable<LogConnection, LogCursor>();
    private boolean                       done;

    // Connection timeout variables.
    private long                          lastTimeoutCheck = System.currentTimeMillis();
    private int                           timeoutMillis    = 5000;
    private long                          nextTimeoutCheck;

    /**
     * Create a new log cursor manager.
     */
    public LogCursorManager() throws ReplicatorException
    {
        nextTimeoutCheck = lastTimeoutCheck + timeoutMillis;
    }

    /**
     * Returns the number of cursors currently managed.
     */
    public synchronized int getSize()
    {
        return cursors.size();
    }

    /**
     * Returns the timeout for idle cursors.
     */
    public int getTimeoutMillis()
    {
        return timeoutMillis;
    }

    /**
     * Sets the timeout for idle cursors. This recomputes the timeout.
     */
    public void setTimeoutMillis(int timeoutMillis)
    {
        this.timeoutMillis = timeoutMillis;
        nextTimeoutCheck = lastTimeoutCheck + timeoutMillis;
    }

    /**
     * Return a cursor to the log file belonging to this client. If the next
     * seqno is before the current position, we release the existing file and
     * return null.
     * 
     * @return A cursor. This must be returned using returnLogConnection().
     */
    public synchronized LogCursor getLogCursor(LogConnection client,
            long nextSeqno) throws ReplicatorException
    {
        assertNotDone(client);

        if (logger.isDebugEnabled())
        {
            logger.debug("Seeking cursor to disk log: client=" + client
                    + " requested seqno=" + nextSeqno);
        }

        // Check for and remove idle cursors. This may include the
        // cursor we are seeking but doing it this way makes unit tests
        // easier to write.
        long currentTimeMillis = System.currentTimeMillis();
        if (currentTimeMillis > nextTimeoutCheck)
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Checking for old cursors to free");
            }
            // Compute lower bound for last access time.
            long oldestAccessMillis = currentTimeMillis - timeoutMillis;

            // Use two loops to avoid concurrent access error.
            ArrayList<LogConnection> cRefs = new ArrayList<LogConnection>();
            for (LogConnection c : cursors.keySet())
            {
                LogCursor lc = cursors.get(c);
                if (!lc.isLoaned()
                        && lc.getLastAccessMillis() < oldestAccessMillis)
                {
                    cRefs.add(c);
                }
            }
            for (LogConnection c : cRefs)
            {
                releaseConnection(c);
            }

            // Set next timeout check.
            nextTimeoutCheck = currentTimeMillis + timeoutMillis;
        }

        // Look up the cursor.
        LogCursor logCursor = cursors.get(client);
        if (logCursor != null)
        {
            // Assert that this cursor was not already loaned.
            if (logCursor.isLoaned())
            {
                throw new THLException(
                        "Log cursor already loaned to client: name="
                                + Thread.currentThread().getName() + " client="
                                + client);
            }

            // See if log cursor is positioned before requested sequence
            // number.
            if (logCursor.getLastSeqno() <= nextSeqno)
            {
                // Connection is usable, return it.
                logCursor.setLastSeqno(nextSeqno);
                logCursor.setLoaned(true);
                logCursor.setLastAccessMillis(currentTimeMillis);
                return logCursor;
            }
            else
            {
                if (logger.isDebugEnabled())
                {
                    logger.debug("Requested next sequence number is less than cursor seqno: requested seqno="
                            + nextSeqno
                            + " last seqno="
                            + logCursor.getLastSeqno());
                }
                cursors.remove(client);
                logCursor.release();
                return null;
            }
        }
        else
            return null;
    }

    /**
     * Sets the log file and last accessed sequence number for this thread. This
     * allows clients to roll over the log cursor and receive it back as a new
     * loaned cursor.
     * 
     * @return A cursor. This must be returned using returnLogConnection().
     */
    public synchronized LogCursor createAndGetLogCursor(LogConnection client,
            LogFile logFile, long lastSeqno) throws ReplicatorException
    {
        assertNotDone(client);

        // Clear previous cursor if any for this thread.
        LogCursor logCursor = cursors.remove(client);
        if (logCursor != null)
            logCursor.release();

        // Add new cursor for this thread.
        logCursor = new LogCursor(logFile, lastSeqno);
        logCursor.setLoaned(true);
        cursors.put(client, logCursor);

        if (logger.isDebugEnabled())
        {
            logger.debug("Creating new log cursor: client=" + client + " file="
                    + logFile.getFile().getName() + " seqno=" + lastSeqno);
        }
        return logCursor;
    }

    /**
     * Returns a loaned log cursor. This must be called on any cursor before
     * asking for it again.
     * 
     * @param logCursor
     */
    public synchronized void returnLogCursor(LogCursor logCursor)
    {
        logCursor.setLoaned(false);
    }

    /**
     * Releases all cursors. This must be called when terminating to ensure file
     * descriptors are released.
     */
    public synchronized void release()
    {
        if (!done)
        {
            // Use two loops to avoid concurrent access error.
            ArrayList<LogConnection> cRefs = new ArrayList<LogConnection>(
                    cursors.size());
            for (LogConnection c : cursors.keySet())
            {
                cRefs.add(c);
            }
            for (LogConnection c : cRefs)
            {
                releaseConnection(c);
            }

            cursors = null;
            done = true;
        }
    }

    // Ensure that log is still accessible.
    private void assertNotDone(LogConnection client) throws ReplicatorException
    {
        if (done)
        {
            throw new THLException("Illegal access on closed log: client="
                    + client);
        }
    }

    /**
     * Releases log cursor for specific thread ID.
     */
    public synchronized void releaseConnection(LogConnection client)
    {
        if (cursors == null)
            logger.warn("Attempt to free connection after manager is done: client="
                    + client);
        else
        {
            LogCursor logCursor = cursors.get(client);
            if (logCursor != null)
            {
                if (logger.isDebugEnabled())
                {
                    logger.debug("Releasing log cursor: file="
                            + logCursor.getLogFile().getFile().getName()
                            + " client=" + client);
                }
                logCursor.release();
                cursors.remove(client);
            }
        }
    }
}