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
 * Processes a restore command including retrieving the backup file and loading
 * into the database.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class RestoreTask implements Callable<String>
{
    private static final Logger   logger = Logger.getLogger(RestoreTask.class);
    private final URI             uri;
    private final EventDispatcher eventDispatcher;
    private final BackupAgent     backupAgent;
    private final StorageAgent    storageAgent;

    public RestoreTask(URI uri, EventDispatcher dispatcher,
            BackupAgent backupAgent, StorageAgent storageAgent)
    {
        this.uri = uri;
        this.eventDispatcher = dispatcher;
        this.backupAgent = backupAgent;
        this.storageAgent = storageAgent;
    }

    /**
     * Execute the backup task.
     */
    public String call() throws BackupException
    {
        logger.info("Restore task starting...");
        boolean completed = false;
        try
        {
            // Retrieve the file.
            logger.info("Retrieving backup file: uri=" + uri);
            BackupSpecification bspec = storageAgent.retrieve(uri);

            // Restore database.
            logger.info("Restoring database from file: uri=" + uri);
            backupAgent.restore(bspec);

            // Turn the resulting file over to storage.
            completed = true;
            logger.info("Restore completed successfully; uri=" + uri);
        }
        catch (InterruptedException e)
        {
            logger.warn("Restore was cancelled");
        }
        catch (Exception e)
        {
            String message = "Restore operation failed: " + e.getMessage();
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
        }

        // Post a backup completion event.
        if (completed)
        {
            logger.info("Restore task completed normally: uri=" + uri);
            try
            {
                eventDispatcher.put(new RestoreCompletionNotification(uri));
            }
            catch (InterruptedException ie)
            {
                logger.warn("Restore task interrupted while posting completion event");
            }
            return uri.toString();
        }
        else
        {
            logger.warn("Restore task did not complete");
            return null;
        }
    }
}