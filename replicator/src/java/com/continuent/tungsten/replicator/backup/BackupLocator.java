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

/**
 * Maintains references to storage used during backups.  Encapsulates
 * the backup location and logic required (if necessary) to mount 
 * storage for copying backup, e.g., by mounting an LVM snapshot. 
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface BackupLocator
{
    /**
     * Makes the storage contents for use.  Must be called before looking
     * at contents. 
     */
    public void open();

    /**
     * Returns a reference to the file containing the backup. 
     */
    public File getContents();

    /**
     * Releases the storage.  This must be called after using the storage
     * to ensure all resources are released. 
     */
    public void release();

    public String getDatabaseName();
}