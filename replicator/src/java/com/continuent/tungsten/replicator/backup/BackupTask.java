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

package com.continuent.tungsten.replicator.backup;

import java.net.URI;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import com.continuent.tungsten.fsm.event.EventDispatcher;
import com.continuent.tungsten.replicator.ErrorNotification;

/**
 * Processes a backup command including dumping data from the database and
 * storing the resulting file(s).
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class BackupTask implements Callable<String>
{
    private static final Logger   logger = Logger.getLogger(BackupTask.class);
    private final EventDispatcher eventDispatcher;
    private final String          backupAgentName;
    private final BackupAgent     backupAgent;
    private final StorageAgent    storageAgent;

    public BackupTask(EventDispatcher dispatcher, String backupAgentName,
            BackupAgent backupAgent, StorageAgent storageAgent)
    {
        this.eventDispatcher = dispatcher;
        this.backupAgentName = backupAgentName;
        this.backupAgent = backupAgent;
        this.storageAgent = storageAgent;
    }

    /**
     * Execute the backup task.
     */
    public String call() throws BackupException
    {
        logger.info("Backup task starting...");
        URI uri = null;
        BackupSpecification bspec = null;
        try
        {
            // Run the backup.
            logger.info("Starting backup using agent: "
                    + backupAgent.getClass().getName());

            // Create a backup specification for storage.
            bspec = backupAgent.backup();

            // Turn the resulting file over to storage.
            logger.info("Storing backup result...");
            bspec.setAgentName(backupAgentName);
            uri = storageAgent.store(bspec);
        }
        catch (InterruptedException e)
        {
            logger.warn("Backup was cancelled");
        }
        catch (Exception e)
        {
            String message = "Backup operation failed: " + e.getMessage();
            logger.error(message, e);
            try
            {
                eventDispatcher.put(new ErrorNotification(message, e));
            }
            catch (InterruptedException ie)
            {
                // No need to handle; thread is dying anyway.
            }
        }
        finally
        {
            if (bspec != null)
                bspec.releaseLocators();
        }

        // Post a backup completion event.
        if (uri == null)
        {
            logger.warn("Backup task did not complete");
        }
        else
        {
            logger.info("Backup completed normally: uri=" + uri);
            try
            {
                eventDispatcher.put(new BackupCompletionNotification(
                        uri));
            }
            catch (InterruptedException ie)
            {
                logger
                        .warn("Backup task interrupted while posting completion event");
            }
        }
        if (uri == null)
            return null;
        else
            return uri.toString();
    }
}