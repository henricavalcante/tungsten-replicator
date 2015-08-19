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

public class FileBackupLocator implements BackupLocator
{
    private final File    backup;
    private final boolean deleteOnRelease;
    private String        databaseName;

    public FileBackupLocator(File backup, boolean deleteOnRelease)
    {
        this(null, backup, deleteOnRelease);
    }

    public FileBackupLocator(String databaseName, File backup,
            boolean deleteOnRelease)
    {
        this.backup = backup;
        this.deleteOnRelease = deleteOnRelease;
        this.databaseName = databaseName;
    }

    public File getContents()
    {
        return backup;
    }

    public String getDatabaseName()
    {
        return databaseName;
    }

    public void open()
    {
        // Nothing to do.
    }

    public void release()
    {
        if (deleteOnRelease && backup.exists())
        {
            backup.delete();
        }
    }
}
