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
 * Initial developer(s): Stephane Giron
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.backup;

import java.io.FileNotFoundException;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.backup.postgresql.PostgreSqlDumpAgent;
import com.continuent.tungsten.replicator.util.ProcessHelper;

/**
 * This class defines a AbstractBackupAgent
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public abstract class AbstractBackupAgent implements BackupAgent
{
    protected static Logger      logger = Logger
                                                .getLogger(PostgreSqlDumpAgent.class);

    protected ProcessHelper      processHelper;
    protected boolean            restoreCompleted;
    protected BackupCapabilities capabilities;

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.backup.BackupAgent#backup()
     */
    public abstract BackupSpecification backup() throws BackupException,
            InterruptedException;

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.backup.BackupAgent#restore(com.continuent.tungsten.replicator.backup.BackupSpecification)
     */
    public void restore(BackupSpecification bspec) throws BackupException,
            InterruptedException
    {
        try
        {
            restoreCompleted = false;
            initRestore();
            for (BackupLocator locator : bspec.getBackupLocators())
            {
                try
                {
                    // Load the backup storage.
                    locator.open();

                    restoreOneLocator(locator);

                }
                catch (BackupException e)
                {
                    throw e;
                }
                catch (Exception e)
                {
                    throw new BackupException("Unexpected error on restore: "
                            + e.getMessage(), e);
                }
                finally
                {
                    locator.release();
                }
            }
            restoreCompleted = true;
        }
        finally
        {
            completeRestore();
        }
    }

    /**
     * Pre-restore operations.
     * 
     * @throws BackupException if anything wrong happened while preparing to
     *             restore
     */
    protected void initRestore() throws BackupException
    {
        // By default, nothing to do. To be overridden in a backup agent if
        // needed.
    }

    /**
     * Post-restore operations.
     */
    protected void completeRestore()
    {
        // By default, nothing to do. To be overridden in a backup agent if
        // needed.
    }

    /**
     * restoreOneLocator is used to restore a database using one locator.
     * 
     * @param locator the locator to be used to restore the database
     * @throws BackupException if something happens while restoring
     * @throws FileNotFoundException if the dump file cannot be found
     */
    protected abstract void restoreOneLocator(BackupLocator locator)
            throws BackupException, FileNotFoundException;

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.backup.BackupPlugin#configure()
     */
    public void configure() throws BackupException
    {
        // Create default capabilities. 
        capabilities = new BackupCapabilities();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.backup.BackupPlugin#release()
     */
    public abstract void release() throws BackupException;

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.backup.BackupAgent#capabilities()
     */
    public BackupCapabilities capabilities()
    {
        return capabilities; 
    }
}
