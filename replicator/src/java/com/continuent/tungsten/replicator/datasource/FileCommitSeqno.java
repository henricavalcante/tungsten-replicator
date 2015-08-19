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
 * Contributor(s): Stephane Giron, Linas Virbalas
 */

package com.continuent.tungsten.replicator.datasource;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.file.FileIO;
import com.continuent.tungsten.common.file.FilePath;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplDBMSHeaderData;

/**
 * Manages commit sequence numbers using files.
 */
public class FileCommitSeqno implements CommitSeqno
{
    private static Logger logger   = Logger.getLogger(FileCommitSeqno.class);

    // Properties.
    private String        serviceName;
    private int           channels = -1;
    private FilePath      serviceDir;
    private String        prefix   = "commitseqno";

    // Instances to handle file system operations.
    private final FileIO  fileIO;

    /**
     * Create a new instance.
     * 
     * @param fileIO FileIO instance appropriately configured for the file
     *            system type
     */
    public FileCommitSeqno(FileIO fileIO)
    {
        this.fileIO = fileIO;
    }

    public void setChannels(int channels)
    {
        this.channels = channels;
    }

    public String getServiceName()
    {
        return serviceName;
    }

    public void setServiceName(String serviceName)
    {
        this.serviceName = serviceName;
    }

    public FilePath getServiceDir()
    {
        return serviceDir;
    }

    public void setServiceDir(FilePath serviceDir)
    {
        this.serviceDir = serviceDir;
    }

    public String getPrefix()
    {
        return prefix;
    }

    public void setPrefix(String prefix)
    {
        this.prefix = prefix;
    }

    public void configure() throws ReplicatorException, InterruptedException
    {
        if (channels < 0)
        {
            throw new ReplicatorException(
                    "Channels are not set for commit seqno file");
        }
        if (serviceDir == null)
        {
            throw new ReplicatorException(
                    "Directory is not set for commit seqno file; must specify a location to write files");
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.CatalogEntity#prepare()
     */
    public void prepare() throws ReplicatorException, InterruptedException
    {
        // Ensure everything exists now.
        if (!fileIO.readable(serviceDir))
        {
            throw new ReplicatorException(
                    "Seqno file directory does not exist or is not readable: "
                            + serviceDir.toString());
        }
        else if (!fileIO.writable(serviceDir))
        {
            throw new ReplicatorException(
                    "Seqno file directory is not writable: "
                            + serviceDir.toString());
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.CatalogEntity#release()
     */
    public void release() throws ReplicatorException, InterruptedException
    {
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.CatalogEntity#initialize()
     */
    public void initialize() throws ReplicatorException, InterruptedException
    {
        // If there are no files, we need to place initial file.
        String[] seqnoFileNames = fileIO.list(serviceDir, prefix);
        if (seqnoFileNames.length == 0)
        {
            String fname = prefix + ".0";
            logger.info("Initializing file-based seqno tracking: directory="
                    + serviceDir.toString() + " file=" + fname);
            ReplDBMSHeaderData header = new ReplDBMSHeaderData(-1, (short) -1,
                    false, "", -1, "", "", new Timestamp(
                            System.currentTimeMillis()), 0);
            store(fname, header, header.getAppliedLatency(), false);
        }

        // Count again and check the number of files.
        //
        // a) If the number equals the number of channels, we leave it
        // alone.
        // b) If there is just one row, we expand to the number of channels.
        //
        // Any other number is an error.
        seqnoFileNames = fileIO.list(serviceDir, prefix);
        if (seqnoFileNames.length == channels)
        {
            logger.info("Validated that trep_commit_seqno file count matches channels: files="
                    + seqnoFileNames.length + " channels=" + channels);
        }
        else if (seqnoFileNames.length == 1)
        {
            expandTasks();
        }
        else
        {
            String msg = String
                    .format("Rows in trep_commit_seqno are inconsistent with channel count: channels=%d files=%d",
                            channels, seqnoFileNames.length);
            logger.error("Replication configuration error: table trep_commit_seqno does not match channels");
            logger.info("This may be due to resetting the number of channels after an unclean replicator shutdown");
            throw new ReplicatorException(msg);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    public void initPosition(long seqno, String sourceId, long epoch,
            String eventId) throws ReplicatorException, InterruptedException
    {
        // Create initial position file.
        String[] seqnoFileNames = fileIO.list(serviceDir, prefix);
        if (seqnoFileNames.length == 0)
        {
            String fname = prefix + ".0";
            logger.info("Initializing file-based seqno tracking: directory="
                    + serviceDir.toString() + " file=" + fname);
            // Set last frag to true, so the pipeline would start from the
            // *next* available event, as opposed to given one.
            ReplDBMSHeaderData header = new ReplDBMSHeaderData(seqno,
                    (short) -1, true, sourceId, epoch, eventId, "",
                    new Timestamp(System.currentTimeMillis()), 0);
            store(fname, header, -1, false);
        }
        else
        {
            throw new ReplicatorException(
                    "Cannot set position unless existing position data is removed - reset first");
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.CatalogEntity#clear()
     */
    public boolean clear()
    {
        // Delete files if they exist.
        for (FilePath seqnoFile : listSeqnoFiles())
        {
            if (fileIO.exists(seqnoFile))
            {
                if (!fileIO.delete(seqnoFile))
                    logger.warn("Unable to delete file: " + seqnoFile);
            }
        }
        return true;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.CommitSeqno#expandTasks()
     */
    public void expandTasks() throws ReplicatorException
    {
        String[] seqnoFileNames = fileIO.list(serviceDir, prefix);
        if (seqnoFileNames.length == 0)
        {
            throw new ReplicatorException(
                    "Attempt to expand commit seqno data when first channel is not initialized: directory="
                            + serviceDir);
        }
        else if (seqnoFileNames.length == 1)
        {
            // Read the first file and apply the contents to the remaining
            // files.
            String fname0 = prefix + ".0";
            logger.info("Expanding channel data: directory="
                    + serviceDir.toString() + " base file=" + fname0
                    + " channels=" + channels);
            ReplDBMSHeader header0 = retrieve(fname0);

            for (int i = 1; i < channels; i++)
            {
                String fnameN = prefix + "." + i;
                store(fnameN, header0, header0.getAppliedLatency(), false);
            }
        }
        else if (seqnoFileNames.length != channels)
        {
            throw new ReplicatorException(
                    "Existing seqno files do not match number of channels; replicator may not have shut down cleanly: directory="
                            + serviceDir
                            + " channels="
                            + channels
                            + " number of seqno files=" + seqnoFileNames.length);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.CommitSeqno#reduceTasks()
     */
    public boolean reduceTasks() throws ReplicatorException
    {
        String[] seqnoFileNames = fileIO.list(serviceDir, prefix);
        if (seqnoFileNames.length == 0)
        {
            throw new ReplicatorException(
                    "Attempt to reduce commit seqno data when first channel is not initialized: directory="
                            + serviceDir);
        }
        else if (seqnoFileNames.length == 1)
        {
            // Do nothing.
            return true;
        }
        else
        {
            // Read the first file.
            String fname0 = prefix + ".0";
            logger.info("Attempting to reduce channel data: directory="
                    + serviceDir.toString() + " base file=" + fname0
                    + " channels=" + channels);
            ReplDBMSHeader header0 = retrieve(fname0);

            // Compare the first header to all others. Depending on how
            // previous reduce operations went there may be missing files,
            // which we ignore.
            LinkedList<String> deleteList = new LinkedList<String>();
            for (int i = 1; i < channels; i++)
            {
                String fnameN = prefix + "." + i;
                FilePath fpathN = new FilePath(serviceDir, fnameN);
                if (fileIO.exists(fpathN))
                {
                    ReplDBMSHeader headerN = retrieve(fnameN);
                    if (header0.getSeqno() == headerN.getSeqno()
                            && header0.getLastFrag() == headerN.getLastFrag())
                    {
                        deleteList.add(fnameN);
                    }
                    else
                    {
                        logger.info("Channel positions do not match; unable to reduce");
                        return false;
                    }
                }
            }

            // If we get to this point all files after the .0 file are
            // the same and can be deleted.
            logger.info("Found " + deleteList.size() + " files to reduce");
            for (String fname : deleteList)
            {
                FilePath fpath = new FilePath(serviceDir, fname);
                if (!fileIO.delete(fpath))
                {
                    logger.warn("Unable to delete file to reduce channels: name="
                            + fpath);
                }
            }

            // Indicate success.
            return true;
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.CommitSeqno#minCommitSeqno()
     */
    public ReplDBMSHeader minCommitSeqno() throws ReplicatorException
    {
        ReplDBMSHeader minHeader = null;
        for (String seqnoFileName : listSeqnoFileNames())
        {
            ReplDBMSHeader header = retrieve(seqnoFileName);
            if (minHeader == null || header.getSeqno() < minHeader.getSeqno())
                minHeader = header;
        }
        return minHeader;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.CommitSeqno#maxCommitSeqno()
     */
    public List<ReplDBMSHeader> getHeaders() throws ReplicatorException,
            InterruptedException
    {
        ArrayList<ReplDBMSHeader> headers = new ArrayList<ReplDBMSHeader>();
        for (String seqnoFileName : listSeqnoFileNames())
        {
            ReplDBMSHeader header = retrieve(seqnoFileName);
            headers.add(header);
        }
        return headers;
    }
    
    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.CommitSeqno#maxCommitSeqno()
     */
    public ReplDBMSHeader maxCommitSeqno() throws ReplicatorException,
            InterruptedException
    {
        ReplDBMSHeader maxHeader = null;
        for (String seqnoFileName : listSeqnoFileNames())
        {
            ReplDBMSHeader header = retrieve(seqnoFileName);
            if (maxHeader == null || header.getSeqno() > maxHeader.getSeqno())
                maxHeader = header;
        }
        return maxHeader;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.CommitSeqno#createAccessor(int,
     *      com.continuent.tungsten.replicator.datasource.UniversalConnection)
     */
    public CommitSeqnoAccessor createAccessor(int taskId,
            UniversalConnection conn)
    {
        FileCommitSeqnoAccessor accessor = new FileCommitSeqnoAccessor(this);
        accessor.setTaskId(taskId);
        return accessor;
    }

    /**
     * Return a list of seqno file names or an empty list if none eist.
     */
    private String[] listSeqnoFileNames()
    {
        return fileIO.list(serviceDir, prefix);
    }

    /**
     * Return a list of seqno file paths or an empty list of none exist.
     */
    private List<FilePath> listSeqnoFiles()
    {
        LinkedList<FilePath> children = new LinkedList<FilePath>();
        for (String fileName : listSeqnoFileNames())
        {
            FilePath fp = new FilePath(serviceDir, fileName);
            children.add(fp);
        }
        return children;
    }

    /**
     * Stores commit seqno data in a file, replacing previous contents. The
     * method is package protected to allow accessors to call it.
     * 
     * @param seqnoFileName Name of the file to write.
     * @param header ReplDbmsHeader record to place in the file
     * @param appliedLatency The current applied latency, which is outside the
     *            header because data within it is final
     * @param mustExist If true, the file to which we are storing must exist.
     *            This protects against accidentally updating a non-existing
     *            channel.
     */
    void store(String seqnoFileName, ReplDBMSHeader header,
            long appliedLatency, boolean mustExist) throws ReplicatorException
    {
        // Create a JSON string representation.
        TungstenProperties props = new TungstenProperties();
        props.setLong("seqno", header.getSeqno());
        props.setInt("fragno", header.getFragno());
        props.setBoolean("lastFrag", header.getLastFrag());
        props.setString("sourceId", header.getSourceId());
        props.setLong("epochNumber", header.getEpochNumber());
        props.setString("eventId", header.getEventId());
        props.setString("shardId", header.getShardId());
        props.setLong("extractedTstamp", header.getExtractedTstamp().getTime());
        props.setLong("appliedLatency", appliedLatency);

        // Make sure the file exists.
        FilePath seqnoFile = new FilePath(serviceDir, seqnoFileName);
        if (mustExist && !fileIO.exists(seqnoFile))
        {
            throw new ReplicatorException(
                    "Unable to update seqno position for non-existent channel: file="
                            + seqnoFileName + " props=" + props.toString());
        }

        // Write the JSON and flush to storage. This overwrites any
        // previous version.
        try
        {
            String json = props.toJSON(true);
            fileIO.write(seqnoFile, json, "UTF-8");
        }
        catch (Exception e)
        {
            throw new ReplicatorException(
                    "Unable to write seqno position: file=" + seqnoFileName
                            + " props=" + props.toString(), e);
        }
    }

    /**
     * Retrieves a header from the corresponding file. The method is package
     * protected to allow accessors to call it.
     * 
     * @param seqnoFileName Name of the file to write.
     * @return header ReplDbmsHeader record read from the file
     */
    ReplDBMSHeader retrieve(String seqnoFileName) throws ReplicatorException
    {
        // Read JSON from storage.
        TungstenProperties props;
        try
        {
            FilePath seqnoFile = new FilePath(serviceDir, seqnoFileName);
            String json = fileIO.read(seqnoFile, "UTF-8");
            props = TungstenProperties.loadFromJSON(json);
        }
        catch (Exception e)
        {
            throw new ReplicatorException(
                    "Unable to read seqno position: file=" + seqnoFileName, e);
        }

        // Recreate header from the resulting Tungsten properties instance.
        ReplDBMSHeaderData header = new ReplDBMSHeaderData(
                props.getLong("seqno"), (short) props.getInt("fragno"),
                props.getBoolean("lastFrag"), props.getString("sourceId"),
                props.getLong("epochNumber"), props.getString("eventId"),
                props.get("shardId"), new Timestamp(
                        props.getLong("extractedTstamp")),
                props.getLong("appliedLatency"));
        return header;
    }
}