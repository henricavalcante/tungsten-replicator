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

package com.continuent.tungsten.replicator.backup.mysql;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.Date;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.backup.AbstractBackupAgent;
import com.continuent.tungsten.replicator.backup.BackupCapabilities;
import com.continuent.tungsten.replicator.backup.BackupException;
import com.continuent.tungsten.replicator.backup.BackupLocator;
import com.continuent.tungsten.replicator.backup.BackupSpecification;
import com.continuent.tungsten.replicator.backup.FileBackupLocator;
import com.continuent.tungsten.replicator.backup.LvmHelper;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;
import com.continuent.tungsten.replicator.util.ProcessHelper;

/**
 * Implements a backup agent that works using mysqldump to dump data and mysql
 * to restore.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class MySqlLvmDumpAgent extends AbstractBackupAgent
{
    private static Logger logger             = Logger
                                                     .getLogger(MySqlLvmDumpAgent.class);

    // Plugin parameters.
    private String        host               = "localhost";
    private int           port               = 3306;
    private String        user               = "tungsten";
    private String        password           = "secret";
    private String        volumeGroup        = "VolGroup00";
    private String        logicalVolume      = "LogGroup00";
    private String        logicalVolumeMount = "/";
    private String        dataDir            = "/var/lib/mysql";
    private String        snapshotName       = "mysqlsnap";
    private String        snapshotSize       = "10G";
    private String        snapshotMount      = "/mnt/snapshots";
    private String        dumpDir            = "/tmp";
    private String        commandPrefix      = "sudo";
    private String        lvcreate           = "/usr/sbin/lvcreate";
    private String        lvremove           = "/usr/sbin/lvremove";
    private String        mysqlStart         = "/sbin/service mysql start";
    private String        mysqlStop          = "/sbin/service mysql stop";

    // Directory locations for dumping and reloading files.
    File                  dumpDirLocation;
    File                  dataDirLocation;

    // Helper classes for LVM and process execution.
    private ProcessHelper processHelper;
    private LvmHelper     lvmHelper;

    private String        url;
    private boolean       stoppedServer;

    public MySqlLvmDumpAgent()
    {
    }

    public String getHost()
    {
        return host;
    }

    public void setHost(String host)
    {
        this.host = host;
    }

    public int getPort()
    {
        return port;
    }

    public void setPort(int port)
    {
        this.port = port;
    }

    public String getUser()
    {
        return user;
    }

    public void setUser(String user)
    {
        this.user = user;
    }

    public String getPassword()
    {
        return password;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    public String getVolumeGroup()
    {
        return volumeGroup;
    }

    public void setVolumeGroup(String volumeGroup)
    {
        this.volumeGroup = volumeGroup;
    }

    public String getLogicalVolume()
    {
        return logicalVolume;
    }

    public void setLogicalVolume(String logicalVolume)
    {
        this.logicalVolume = logicalVolume;
    }

    public String getLogicalVolumeMount()
    {
        return logicalVolumeMount;
    }

    public void setLogicalVolumeMount(String logicalVolumeMount)
    {
        this.logicalVolumeMount = logicalVolumeMount;
    }

    public String getDataDir()
    {
        return dataDir;
    }

    public void setDataDir(String dataDir)
    {
        this.dataDir = dataDir;
    }

    public String getSnapshotName()
    {
        return snapshotName;
    }

    public void setSnapshotName(String snapshotName)
    {
        this.snapshotName = snapshotName;
    }

    public String getSnapshotSize()
    {
        return snapshotSize;
    }

    public void setSnapshotSize(String snapshotSize)
    {
        this.snapshotSize = snapshotSize;
    }

    public String getSnapshotMount()
    {
        return snapshotMount;
    }

    public void setSnapshotMount(String snapshotMount)
    {
        this.snapshotMount = snapshotMount;
    }

    public String getDumpDir()
    {
        return dumpDir;
    }

    public void setDumpDir(String dumpDir)
    {
        this.dumpDir = dumpDir;
    }

    public String getCommandPrefix()
    {
        return commandPrefix;
    }

    public void setCommandPrefix(String commandPrefix)
    {
        this.commandPrefix = commandPrefix;
    }

    public String getMysqlStart()
    {
        return mysqlStart;
    }

    public void setMysqlStart(String mysqlStart)
    {
        this.mysqlStart = mysqlStart;
    }

    public String getMysqlStop()
    {
        return mysqlStop;
    }

    public void setMysqlStop(String mysqlStop)
    {
        this.mysqlStop = mysqlStop;
    }

    public String getLvcreate()
    {
        return lvcreate;
    }

    public void setLvcreate(String lvcreate)
    {
        this.lvcreate = lvcreate;
    }

    public String getLvremove()
    {
        return lvremove;
    }

    public void setLvremove(String lvremove)
    {
        this.lvremove = lvremove;
    }

    /**
     * Runs backup using LVM snapshot. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.backup.BackupAgent#backup()
     */
    public BackupSpecification backup() throws BackupException
    {
        BackupSpecification spec = new BackupSpecification();
        spec.setBackupDate(new Date());

        Database conn = null;
        File dumpFile = null;
        boolean createdSnapshot = false;
        boolean mountedSnapshot = false;
        boolean lockedTables = false;

        try
        {
            // Connect to the database.
            logger.debug("Connecting to database and flushing tables");
            conn = DatabaseFactory.createDatabase(url, user, password, true);
            conn.connect();

            // Flush to ensure that tables are consistent.
            logger.info("Flushing and locking tables");
            conn.execute("FLUSH TABLES WITH READ LOCK");
            lockedTables = true;

            // Generate snapshot.
            lvmHelper.createSnapshot();
            createdSnapshot = true;

            // Unlock tables.
            logger.info("Unlocking tables after creating snapshot");
            conn.execute("UNLOCK TABLES");
            lockedTables = false;

            // Mount the snapshot.
            lvmHelper.mountSnapShot();
            mountedSnapshot = true;

            // Generate the tar command and dump data.
            dumpFile = File.createTempFile("mysql-lvm-dump-", ".tar.gz",
                    this.dumpDirLocation);
            String[] tarCmdArray = {"tar", "-czf", dumpFile.getAbsolutePath(),
                    "-C", lvmHelper.getSnapshotDataDir().getAbsolutePath(), "."};
            String[] tarCmd = tarCmdArray;
            processHelper
                    .exec("Dumping database data directory to temporary file",
                            tarCmd);

            spec.addBackupLocator(new FileBackupLocator(dumpFile, true));
            // Return the backup specification
            return spec;
        }
        catch (BackupException e)
        {
            throw e;
        }
        catch (SQLException e)
        {
            throw new BackupException("SQL operation failed during backup: "
                    + e.getMessage(), e);
        }
        catch (Exception e)
        {
            throw new BackupException("Unexpected error on backup: "
                    + e.getMessage(), e);
        }
        finally
        {
            // Make sure we really unlocked and disconnected.
            if (conn != null)
            {
                if (lockedTables)
                {
                    logger
                            .info("Unlocking tables following abnormal termination");
                    try
                    {
                        conn.execute("UNLOCK TABLES");
                    }
                    catch (SQLException e)
                    {
                    }
                }
                conn.disconnect();
            }

            // Unmount snapshot.
            if (mountedSnapshot)
            {
                try
                {
                    lvmHelper.unmountSnapshot();
                }
                catch (BackupException e)
                {
                    logger
                            .warn("Unable to unmount snapshot: "
                                    + e.getMessage());
                }
            }

            // Remove the snapshot.
            if (createdSnapshot)
            {
                try
                {
                    lvmHelper.removeSnapshot();
                }
                catch (BackupException e)
                {
                    logger.warn("Unable to remove snapshot: " + e.getMessage());
                }
            }
        }
    }

    @Override
    protected void restoreOneLocator(BackupLocator locator)
            throws BackupException, FileNotFoundException
    {
        // Generate the tar command.
        locator.open();
        File restoreFile = locator.getContents();
        String[] tarCmdArray = {"tar", "-xzf", restoreFile.getAbsolutePath(),
                "-C", dataDirLocation.getAbsolutePath()};
        String[] tarCmd = tarCmdArray;
        processHelper.exec("Restoring data directory contents", tarCmd);
        logger.info("Completed restore operation");
    }

    @Override
    protected void initRestore() throws BackupException
    {
        // Stop the MySQL server.
        processHelper.exec("Stopping MySQL server", mysqlStop);
        stoppedServer = true;

        // Remove any existing storage contents.
        File dataDirSpec = new File(dataDir);
        if (dataDirSpec.listFiles().length > 0)
        {
            lvmHelper.removeStorage();
        }
    }

    @Override
    protected void completeRestore()
    {
        // Restart the server if we stopped and successfully restored.
        if (stoppedServer && restoreCompleted)
        {
            try
            {
                processHelper.exec("Starting MySQL server", mysqlStart);
            }
            catch (BackupException e)
            {
                logger
                        .warn("Unable to restart MySQL server: "
                                + e.getMessage());
            }
        }
        else
        {
            logger
                    .info("Server was not stopped or restored did not complete; skipping server restart");
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.backup.BackupPlugin#configure()
     */
    public void configure() throws BackupException
    {
        // Configure process helper.
        processHelper = new ProcessHelper();
        if (!"".equals(commandPrefix))
            processHelper.setCmdPrefix(commandPrefix);
        processHelper.configure();

        // Configure the LVM helper.
        lvmHelper = new LvmHelper(processHelper);
        lvmHelper.setDataDir(this.dataDir);
        lvmHelper.setLogicalVolume(logicalVolume);
        lvmHelper.setLogicalVolumeMount(logicalVolumeMount);
        lvmHelper.setLvcreate(lvcreate);
        lvmHelper.setLvremove(lvremove);
        lvmHelper.setSnapshotMount(snapshotMount);
        lvmHelper.setSnapshotName(snapshotName);
        lvmHelper.setSnapshotSize(snapshotSize);
        lvmHelper.setVolumeGroup(volumeGroup);
        lvmHelper.configure();

        // Ensure the dump directory exists.
        dumpDirLocation = new File(dumpDir);
        lvmHelper.validateStorage("dump directory location", dumpDirLocation);

        // Ensure data directory exists.
        dataDirLocation = new File(dataDir);
        lvmHelper.validateStorage("data directory location", dataDirLocation);

        // Generate MySQL URL.
        if (url == null)
        {
            StringBuffer sb = new StringBuffer();
            sb.append("jdbc:mysql://");
            sb.append(host);
            if (port > 0)
            {
                sb.append(":");
                sb.append(port);
            }
            sb.append("/");

            url = sb.toString();
        }

        // Record capabilities.
        capabilities = new BackupCapabilities();
        capabilities.setHotBackupEnabled(true);
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
}