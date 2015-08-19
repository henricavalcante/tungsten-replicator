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

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.util.ProcessHelper;

/**
 * Implements a helper for running LVM commands across full life-cycle from
 * creating a snapshot to mounting it to unmounting and discarding.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class LvmHelper
{
    private static Logger logger             = Logger
                                                     .getLogger(LvmHelper.class);

    // Plugin parameters.
    private String        volumeGroup        = "VolGroup00";
    private String        logicalVolume      = "LogGroup00";
    private String        logicalVolumeMount = "/";
    private String        snapshotName       = "mysqlsnap";
    private String        snapshotSize       = "10G";
    private String        snapshotMount      = "/mnt/snapshots";
    private String        dataDir            = "/database-storage-directory";
    private String        lvcreate           = "/usr/sbin/lvcreate";
    private String        lvremove           = "/usr/sbin/lvremove";

    // Root command prefix and various generated commands in array form.
    private String[]      lvcreateCmd;
    private String[]      lvremoveCmd;
    private String[]      mountCmd;
    private String[]      umountCmd;
    private String[]      rmCmd;
    File                  snapshotDataDirSpec;
    File                  dataDirSpec;

    // Helper to execute OS processes.
    ProcessHelper         processHelper;

    /**
     * Creates a new LVM helper.
     */
    public LvmHelper(ProcessHelper processHelper)
    {
        this.processHelper = processHelper;
    }

    public String getDataDir()
    {
        return dataDir;
    }

    public void setDataDir(String dataDir)
    {
        this.dataDir = dataDir;
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
    
    public File getSnapshotDataDir()
    {
        return snapshotDataDirSpec;
    }

    /**
     * Configure the commands and locations to manage snapshots.  Must be called
     * before executing any other command. 
     */
    public void configure() throws BackupException
    {
        // Generate and validate database storage locations.
        File logicalVolumeSpec = new File("/dev/" + volumeGroup + "/"
                + logicalVolume);
        File logicalVolumeMountSpec = new File(logicalVolumeMount);
        validateStorage("logical volume mount point", logicalVolumeMountSpec);

        // Generate and validate database storage locations.
        logicalVolumeSpec = new File("/dev/" + volumeGroup + "/"
                + logicalVolume);
        dataDirSpec = new File(dataDir);
        logicalVolumeMountSpec = new File(logicalVolumeMount);
        validateStorage("Database storage directory", dataDirSpec);
        validateStorage("logical volume mount point", logicalVolumeMountSpec);

        // Compute and validate path from mount point to data directory.
        String dataDirLocalSpec;
        if (!dataDir.startsWith(logicalVolumeMount))
        {
            throw new BackupException(
                    "Database data directory does not match mount point for logical volume: "
                            + " dataDir=" + dataDir + "logicalVolumeMount="
                            + logicalVolumeMount);
        }
        if (dataDir.length() == logicalVolumeMount.length())
            dataDirLocalSpec = "";
        else
            dataDirLocalSpec = dataDir.substring(logicalVolumeMount.length());


        // Compute snapshot volume specification and mounted position.
        File snapshotSpec = new File("/dev/" + volumeGroup + "/" + snapshotName);
        File snapshotMountSpec = new File(snapshotMount);
        snapshotDataDirSpec = new File(snapshotMount + "/" + dataDirLocalSpec);
        validateStorage("snapshot volume mount point", snapshotMountSpec);

        // Create lvcreate command like the following example:
        // lvcreate -s -n rootsnapshot /dev/VolGroup00/LogVol00
        String[] cmdArray1 = {lvcreate, "-L" + snapshotSize, "-s", "-n",
                snapshotName, logicalVolumeSpec.getAbsolutePath()};
        lvcreateCmd = cmdArray1;

        // Create lvremove command like the following:
        // lvremove /dev/VolGroup00/rootsnapshot
        String[] cmdArray2 = {lvremove, "-f", snapshotSpec.getAbsolutePath()};
        lvremoveCmd = cmdArray2;

        // Create mount command as in: mount /dev/VolGroup00/rootsnapshot
        // /mnt/rootsnapshot
        String[] cmdArray3 = {"mount", snapshotSpec.getAbsolutePath(),
                snapshotMountSpec.getAbsolutePath()};
        mountCmd = cmdArray3;

        // Create umount command, as in: umount /mnt/rootsnapshot
        String[] cmdArray4 = {"umount", snapshotMountSpec.getAbsolutePath()};
        umountCmd = cmdArray4;
        
        // Create rm command used to clear storage directory for restore.  
        // Note:  We use 'sh -c' to ensure file name expansion. 
        String[] cmdArray5 = {"sh", "-c", "rm -rf " + this.dataDirSpec.getAbsolutePath() + "/*"};
        rmCmd = cmdArray5;
    }

    /**
     * Creates a new snapshot, which is assumed not to exist already.
     * 
     * @throws BackupException If the snapshot creation fails, for example
     *             because the snapshot already exists.
     */
    public void createSnapshot() throws BackupException
    {
        logger.debug("Creating snapshot: " +  this.snapshotName);
        processHelper.exec("Creating file system snapshot", lvcreateCmd);
    }

    /**
     * Removes an existing snapshot.
     * 
     * @throws BackupException If the snapshot removal fails, for example
     *             because the snapshot does not exist.
     */
    public void removeSnapshot() throws BackupException
    {
        logger.debug("Removing snapshot: " +  this.snapshotName);
        processHelper.exec("Remove the snapshot", lvremoveCmd);
    }

    /**
     * Mounts a snapshot and validates that the storage is readable.
     * 
     * @throws BackupException Thrown if the mount command is unsuccessful for
     *             any reason
     */
    public void mountSnapShot() throws BackupException
    {
        logger.debug("Mounting snapshot: " +  this.snapshotName);
        processHelper.exec("Mounting the snapshot", mountCmd);
        validateStorage("database storage directory from snapshot", snapshotDataDirSpec);
    }

    /**
     * Unmounts a snapshot.
     * 
     * @throws BackupException Thrown if the unmount command is unsuccessful for
     *             any reason
     */
    public void unmountSnapshot() throws BackupException
    {
        logger.debug("Unmounting snapshot: " +  this.snapshotName);
        processHelper.exec("Unmounting snapshot", umountCmd);
    }

    /**
     * Clears storage in preparation for restore operation. 
     * 
     * @throws BackupException Thrown if rm command fails. 
     */
    public void removeStorage() throws BackupException
    {
        logger.debug("Removing contents of storage directory: " + dataDirSpec.getAbsolutePath());
        processHelper.exec("Removing contents of storage directory", rmCmd);
    }

    /**
     * Ensure that indicated storage is a readable directory.
     */
    public void validateStorage(String name, File storageDir)
            throws BackupException
    {
        String suffix = " name=" + name + " location=" + storageDir;
        if (!storageDir.isDirectory())
            throw new BackupException("Storage location is not a directory:"
                    + suffix);
        else if (!storageDir.canRead())
            throw new BackupException("Storage location is not readable:"
                    + suffix);
    }
}