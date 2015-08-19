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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;

/**
 * Implements a dummy backup agent used for unit testing.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class DummyBackupAgent implements BackupAgent
{
    private File    directory = new File(".");
    private boolean fail      = false;

    public DummyBackupAgent()
    {
    }

    public void setDirectory(File directory)
    {
        this.directory = directory;
    }

    public void setFail(boolean fail)
    {
        this.fail = fail;
    }

    public BackupSpecification backup() throws BackupException
    {
        BackupSpecification spec = new BackupSpecification();
        spec.setBackupDate(new Date());

        // Fail if that's what we are supposed to do.
        if (fail)
        {
            throw new BackupException("Backup failing on command!");
        }

        // Produce a dummy file.
        File temp = null;
        FileWriter fw = null;
        try
        {
            temp = File.createTempFile("dummyBackup", "dat", directory);
            fw = new FileWriter(temp);
            fw.write("dummy output");
        }
        catch (IOException e)
        {
            throw new BackupException("Unable to write to temporary file", e);
        }
        finally
        {
            if (fw != null)
            {
                try
                {
                    fw.close();
                }
                catch (IOException e)
                {
                }
            }
        }
        spec.addBackupLocator(new FileBackupLocator(temp, true));
        return spec;
    }

    public void restore(BackupSpecification spec) throws BackupException
    {
        // Fail if that's what we are supposed to do.
        if (fail)
        {
            throw new BackupException("Restore failing on command!");
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.backup.BackupPlugin#configure()
     */
    public void configure() throws BackupException
    {
        if (!directory.isDirectory() || !directory.canWrite())
        {
            throw new BackupException(
                    "Test file directory does not exist or is not writable: "
                            + directory.getAbsolutePath());
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.backup.BackupPlugin#release()
     */
    public void release() throws BackupException
    {
        // Nothing to do!
    }

    /**
     * Returns default capabilities. 
     * {@inheritDoc}
     * @see com.continuent.tungsten.replicator.backup.BackupAgent#capabilities()
     */
    public BackupCapabilities capabilities()
    {
        return new BackupCapabilities();
    }
}
