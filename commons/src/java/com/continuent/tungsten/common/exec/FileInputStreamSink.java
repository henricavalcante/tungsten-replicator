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

package com.continuent.tungsten.common.exec;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.log4j.Logger;

/**
 * Runnable to read from an InputStream and store the results in a file.
 */
public class FileInputStreamSink implements InputStreamSink
{
    private final static Logger    logger = Logger
                                                  .getLogger(FileInputStreamSink.class);
    private final String           tag;
    private final InputStream      inputStream;
    private final File             outputFile;
    private final FileOutputStream fos;

    /**
     * Creates a new instance.
     * 
     * @param tag A tag for this processor to help with logging
     * @param in InputStream from which we read
     * @param outputFile File in which to store output
     * @param append If true append output to file, otherwise overwrite
     */
    public FileInputStreamSink(String tag, InputStream in, File outputFile,
            boolean append) throws FileNotFoundException
    {
        this.tag = tag;
        this.inputStream = in;
        this.outputFile = outputFile;
        fos = new FileOutputStream(outputFile, append);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.exec.InputStreamSink#run()
     */
    public void run()
    {

        try
        {
            // Write output from stream.
            byte[] buff = new byte[1024];
            int len = 0;
            while ((len = inputStream.read(buff)) != -1)
            {
                fos.write(buff, 0, len);
            }
        }
        catch (IOException e)
        {
            logger.warn("[" + tag + "] Writing of data to output file "
                    + this.outputFile.getAbsolutePath()
                    + " halted by exception", e);
        }
        finally
        {
            try
            {
                inputStream.close();
            }
            catch (IOException e)
            {
                logger.warn("[" + tag
                        + "] Input stdin close operation generated exception",
                        e);
            }
            try
            {
                fos.close();
            }
            catch (IOException e)
            {
                logger
                        .warn(
                                "["
                                        + tag
                                        + "] Process stdin close operation generated exception",
                                e);
            }
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.exec.InputStreamSink#getOutput()
     */
    public String getOutput()
    {
        return null;
    }
}
