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

/**
 * Denotes a class that implements a storage agent that can store and retrieve
 * files.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface StorageAgent extends BackupPlugin
{
    /**
     * Returns the number of backup files that should be retained before
     * deleting old files.
     */
    public int getRetention();

    /**
     * Sets the number of backup files to retain.
     */
    public void setRetention(int numberOfBackups);

    /**
     * Returns the storage specifications of all backups in storage ordered from
     * oldest to most recent.
     */
    public StorageSpecification[] list() throws BackupException;

    /**
     * Returns the URI of the most recent backup in storage or null if no
     * backups exist
     */
    public URI last() throws BackupException;

    /**
     * Returns the storage specification of a particular backup or null if no
     * such specification exists.
     */
    public StorageSpecification getSpecification(URI uri)
            throws BackupException;

    /**
     * Stores a backup described by a particular backup specification, returning
     * the URL of the backup.
     */
    public URI store(BackupSpecification specification) throws BackupException;

    /**
     * Retrieves the backup corresponding to a particular URI.
     */
    public BackupSpecification retrieve(URI uri) throws BackupException;

    /**
     * Deletes the indicated backup if it exists.
     * 
     * @return True if backup was found and deleted, otherwise false
     */
    public boolean delete(URI uri) throws BackupException;

    /**
     * Deletes all backups.
     * 
     * @return True if all backups were successfully deleted (also returns true
     *         if there are no backups found)
     */
    public boolean deleteAll() throws BackupException;
}