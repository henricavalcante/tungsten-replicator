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

package com.continuent.tungsten.common.io;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

import org.apache.log4j.Logger;

/**
 * Merges the capabilities of the following stream classes into a single class:
 * FileOutputStream, BufferedOutputStream, and DataOutputStream. This allows us
 * to manage buffered data writes to files efficiently.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class BufferedFileDataOutput
{
    private static Logger        logger = Logger
                                                .getLogger(BufferedFileDataOutput.class);
    // Read parameters.
    private File                 file;
    private int                  bufferSize;

    // Variables to control writing.
    private FileOutputStream     fileOutput;
    private BufferedOutputStream bufferedOutput;
    private DataOutputStream     dataOutput;
    private long                 offset = -1;

    /**
     * Creates instance positioned on end of file and read to write.
     * 
     * @param file File to which to write.
     * @param bufferSize Size of buffer for buffered I/O
     */
    public BufferedFileDataOutput(File file, int bufferSize)
            throws FileNotFoundException, IOException
    {
        this.file = file;
        this.bufferSize = bufferSize;
        open();
    }

    /**
     * Creates instance with default buffer size.
     */
    public BufferedFileDataOutput(File file) throws FileNotFoundException,
            IOException
    {
        this(file, 4096);
    }

    /**
     * Returns the current offset position.
     * 
     * @throws IOException Thrown if position cannot be determined
     */
    public long getOffset() throws IOException
    {
        return offset;
    }

    /**
     * Open for writes at the tail of the file.
     * 
     * @throws IOException Thrown if position cannot be found
     * @throws FileNotFoundException Thrown if file is not found
     */
    private void open() throws FileNotFoundException, IOException
    {
        fileOutput = new FileOutputStream(file, true);
        bufferedOutput = new BufferedOutputStream(fileOutput, bufferSize);
        dataOutput = new DataOutputStream(bufferedOutput);
        fileOutput.getFD().sync();
        // NOTE:  Channel.position() does not return correct position on some platforms.
        offset = fileOutput.getChannel().size();
    }

    /**
     * Writes a single byte.
     */
    public void writeByte(byte v) throws IOException
    {
        dataOutput.writeByte(v);
        offset += 1;
    }

    /**
     * Writes a single short.
     */
    public void writeShort(short v) throws IOException
    {
        dataOutput.writeShort(v);
        offset += 2;
    }

    /**
     * Writes a single int.
     */
    public void writeInt(int v) throws IOException
    {
        dataOutput.writeInt(v);
        offset += 4;
    }

    /**
     * Writes a single long.
     */
    public void writeLong(long v) throws IOException
    {
        dataOutput.writeLong(v);
        offset += 8;
    }

    /**
     * Writes a byte array completely.
     * 
     * @throws IOException Thrown if full byte array cannot be written
     */
    public void write(byte[] bytes) throws IOException
    {
        dataOutput.write(bytes);
        offset += bytes.length;
    }

    /**
     * Flush buffered data to stream. This does not guarantee persistence, only
     * that lower streams can see it.
     * 
     * @throws IOException Thrown if flush fails
     */
    public void flush() throws IOException
    {
        dataOutput.flush();
    }

    /**
     * Synchronizes file contents to disk using fsync. You must call this method
     * to commit data.  Does an automatic flush. 
     */
    public void fsync() throws IOException
    {
        flush();
        //fileOutput.getFD().sync();
    }

    /**
     * Truncate the file to the provided length. Performs an automatic fsync and
     * reopens the file.
     */
    public void setLength(long length) throws IOException
    {
        FileChannel channel = fileOutput.getChannel();
        channel.truncate(length);
        fsync();
        close();
        open();
    }

    /** Close and release all resources. */
    public void close()
    {
        if (fileOutput != null)
        {
            try
            {
                dataOutput.close();
            }
            catch (IOException e)
            {
                logger.warn("Unable to close log file writer: file="
                        + file.getName() + " exception=" + e.getMessage());
            }
            fileOutput = null;
            bufferedOutput = null;
            dataOutput = null;
        }
    }

    /**
     * Print contents of the reader.
     */
    public String toString()
    {
        // Try to get the offset.
        long offset = -1;
        try
        {
            offset = getOffset();
        }
        catch (IOException e)
        {
        }

        // Print a string.
        StringBuffer sb = new StringBuffer();
        sb.append(this.getClass().getSimpleName());
        sb.append(" file=").append(file.getName());
        sb.append(" buffersize=").append(bufferSize);
        sb.append(" offset=").append(offset);
        return sb.toString();
    }
}