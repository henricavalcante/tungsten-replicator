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
import java.util.List;

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
public class LogConnectionManager
{
    private static Logger       logger          = Logger.getLogger(LogConnectionManager.class);

    // Map of active cursors and flag to indicate we are closed for
    // business.
    private boolean             done;

    // Connection pools.
    private LogConnection       writeConnection;
    private List<LogConnection> readConnections = new ArrayList<LogConnection>();

    /**
     * Create a new log cursor manager.
     */
    public LogConnectionManager()
    {
    }

    /**
     * Stores a new connection.
     */
    public synchronized void store(LogConnection connection)
            throws ReplicatorException
    {
        // Ensure we are still open for business.
        assertNotDone(connection);

        // Clean up all finished connections.
        if (writeConnection != null && writeConnection.isDone())
            writeConnection = null;
        int readConnectionsSize = readConnections.size();
        for (int i = 0; i < readConnectionsSize; i++)
        {
            // Have to walk backwards through the read connections.
            int index = readConnectionsSize - i - 1;
            if (readConnections.get(index).isDone())
                readConnections.remove(index);
        }

        // To prevent chaos only a single write connection is allowed.
        if (!connection.isReadonly() && writeConnection != null)
            throw new THLException(
                    "Write connection already exists: connection="
                            + writeConnection.toString());

        // Allocate, store, and return the connection.
        if (connection.isReadonly())
            readConnections.add(connection);
        else
            writeConnection = connection;

    }

    /**
     * Releases an existing connection.
     */
    public synchronized void release(LogConnection connection)
    {
        // Warn if we are shut down.
        if (done)
        {
            logger.warn("Attempt to release connection after connection manager shutdown: "
                    + connection);
            return;
        }

        // Release the connection.
        connection.releaseInternal();
        if (connection.isReadonly())
        {
            if (!readConnections.remove(connection))
                logger.warn("Unable to free read-only connection: "
                        + connection);
        }
        else
        {
            if (writeConnection == connection)
                writeConnection = null;
            else
            {
                logger.warn("Unable to free write connection: " + connection);
            }
        }
    }

    /**
     * Releases all connections. This must be called when terminating to ensure
     * file descriptors are released.
     */
    public synchronized void releaseAll()
    {
        if (!done)
        {
            // Release all connections.
            if (this.writeConnection != null)
            {
                // This flushes pending output.
                writeConnection.releaseInternal();
                writeConnection = null;
            }
            for (LogConnection connection : readConnections)
            {
                connection.releaseInternal();
            }
            readConnections = null;

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
}