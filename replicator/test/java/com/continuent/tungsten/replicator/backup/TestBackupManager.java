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
import java.util.concurrent.Future;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.management.MockEventDispatcher;

/**
 * This class tests the backup manager class (BackupManager) using a dummy
 * backup agent and the SimpleFileStorageAgent.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class TestBackupManager extends TestCase
{
    private static Logger logger           = Logger.getLogger(TestBackupManager.class);
    private static String STORAGE_DIR_NAME = "backup-manager-test/";

    /**
     * Setup.
     * 
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
    }

    /**
     * Teardown.
     * 
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

    /**
     * Tests basic configuration and release.
     */
    public void testConfiguration() throws Exception
    {
        TungstenProperties props = createProperties("testConfig", false);
        BackupManager bmgr = new BackupManager(new MockEventDispatcher());
        bmgr.initialize(props);
        bmgr.release();
    }

    /**
     * Verifies an empty configuration with no backup or storage properties set
     * initializes but cannot run backups. (This would be the case if users do
     * not have an updated replicator.properties file.)
     */
    public void testBackupOnEmpty() throws Exception
    {
        BackupManager bmgr = new BackupManager(new MockEventDispatcher());
        bmgr.initialize(new TungstenProperties());
        try
        {
            bmgr.spawnBackup("foo", "file-foo", false);
            throw new Exception(
                    "Backup spawned when properties are unconfigured");
        }
        catch (BackupException e)
        {
            // OK.
        }
        try
        {
            bmgr.spawnRestore("storage://simple-file/foo.properties");
            throw new Exception(
                    "Restore spawned when properties are unconfigured");
        }
        catch (BackupException e)
        {
            // OK.
        }

        bmgr.release();
    }

    /**
     * Verify that all of the following produce BackupExceptions.
     * <ol>
     * <li>A non-existent backup name</li>
     * <li>A non-existent storage name</li>
     * <li>A non-existent URI</li>
     * </ol>
     */
    public void testBadNamesAndUris() throws Exception
    {
        TungstenProperties props = createProperties("testConfig", false);
        BackupManager bmgr = new BackupManager(new MockEventDispatcher());
        bmgr.initialize(props);

        // Bad backup name.
        try
        {
            bmgr.spawnBackup("foo", "file", false);
            throw new Exception("Backup spawned with bad backup name");
        }
        catch (BackupException e)
        {
            // OK.
        }
        // Bad storage name.
        try
        {
            bmgr.spawnBackup("dummy", "foo", false);
            throw new Exception("Backup spawned with bad storage name");
        }
        catch (BackupException e)
        {
            // OK.
        }
        // Bad URI for restore
        try
        {
            bmgr.spawnRestore("storage://simple-storage/fooo.properties");
            throw new Exception("Restore spawned with non-existent URI");
        }
        catch (BackupException e)
        {
            // OK.
        }

        bmgr.release();
    }

    /**
     * Verify that backups that are not enabled for hot backup fail if we try to
     * run them hot.
     */
    public void testNoHotBackupAvailable() throws Exception
    {
        TungstenProperties props = createProperties("testConfig", false);
        BackupManager bmgr = new BackupManager(new MockEventDispatcher());
        bmgr.initialize(props);

        try
        {
            bmgr.spawnBackup("foo", "file", true);
            throw new Exception("Backup spawned when online");
        }
        catch (BackupException e)
        {
            // OK.
        }
        bmgr.release();
    }

    /**
     * Tests that we can run a backup followed by a restore using the default
     * backup and storage agents.
     */
    public void testBackupRestoreDefault() throws Exception
    {
        backupRestore("testBackupRestoreDefault", null, null);
    }

    /**
     * Tests that we can run a backup followed by a restore using explicit
     * names.
     */
    public void testBackupRestore() throws Exception
    {
        backupRestore("testBackupRestoreDefault", "dummy", "file");
    }

    // Prepare TungstenProperty instance to configure backup manager.
    protected TungstenProperties createProperties(String name,
            boolean failBackup) throws BackupException
    {
        // Set up file storage.
        File storageDir = new File(STORAGE_DIR_NAME + File.separator + name);
        logger.debug("Preparing storage directory for test: "
                + storageDir.getAbsolutePath());
        if (storageDir.exists())
            deleteRecursive(storageDir);
        storageDir.mkdirs();

        // Create directory for temp files.
        File tempDir = new File(STORAGE_DIR_NAME + File.separator + name
                + "_temp");
        logger.debug("Preparing temporary file directory for test: "
                + tempDir.getAbsolutePath());
        if (tempDir.exists())
            deleteRecursive(storageDir);
        tempDir.mkdirs();

        TungstenProperties props = new TungstenProperties();

        // Backup agent properties.
        props.setString(BackupManager.BACKUP_AGENTS, "dummy");
        String dummyKey = BackupManager.BACKUP_AGENT + ".dummy";
        props.setString(dummyKey, DummyBackupAgent.class.getName());
        props.setFile(dummyKey + ".directory", tempDir);
        props.setBoolean(dummyKey + ".fail", failBackup);
        props.setString(BackupManager.BACKUP_DEFAULT, "dummy");

        // Storage agent properties.
        props.setString(BackupManager.STORAGE_AGENTS, "file");
        String fileKey = BackupManager.STORAGE_AGENT + ".file";
        props.setString(fileKey, FileSystemStorageAgent.class.getName());
        props.setFile(fileKey + ".directory", tempDir);
        props.setInt(fileKey + ".retention", 3);
        props.setString(BackupManager.STORAGE_DEFAULT, "file");

        logger.debug("Created new test properties: " + props);
        return props;
    }

    // Run a generic backup/restore test.
    protected void backupRestore(String testName, String backupName,
            String storageName) throws Exception
    {
        // NOTE: Use timeouts to prevent build hangs.
        TungstenProperties props = createProperties(testName, false);
        BackupManager bmgr = new BackupManager(new MockEventDispatcher());
        bmgr.initialize(props);

        // Run a backup and wait for URI to be returned.
        Future<String> backup = bmgr
                .spawnBackup(backupName, storageName, false);
        String uri = backup.get();
        assertNotNull("Expect backup URI to be non-null", uri);

        // Run a restore task using the said URI.
        Future<String> restore = bmgr.spawnRestore(uri);
        boolean success = restore.get() != null;
        assertTrue("Expect restore to succeed", success);

        bmgr.release();
    }

    // Recursively delete directory
    protected void deleteRecursive(File f)
    {
        if (f.isDirectory())
        {
            for (File child : f.listFiles())
            {
                deleteRecursive(child);
            }
        }
        logger.debug("Deleting file: " + f.getAbsolutePath());
        f.delete();
    }
}